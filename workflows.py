import asyncio
from datetime import timedelta
from enum import StrEnum

from temporalio import workflow
from temporalio.common import SearchAttributeKey

with workflow.unsafe.imports_passed_through():
    from activities import (
        send_reminder_email,
        send_second_reminder_email,
        send_welcome_email,
        send_winback_email,
    )

class Stage(StrEnum):
    """所有 stage 名字的唯一定义源头 —— _current_stage、search attribute、
    REMINDER_STAGES、run() 里的 return 值，都从这里引用，不再各处手写字符串。
    """

    WELCOME = "welcome"
    REMINDER_1 = "reminder_1"
    REMINDER_2 = "reminder_2"
    WINBACK = "winback"
    ACTIVATED = "activated"
    CHURNED = "churned"


# (stage, activity 函数, 等待时长) —— 每一级"没激活就发这封邮件"用的是
# 同一套 wait_condition 竞态逻辑，只有 stage/activity/等待时长不一样，
# 抽成数据驱动循环。
REMINDER_STAGES = [
    (Stage.REMINDER_1, send_reminder_email, timedelta(seconds=10)),  # 代表 3 天
    (Stage.REMINDER_2, send_second_reminder_email, timedelta(seconds=15)),  # 代表 7 天(累计 10 天)
    (Stage.WINBACK, send_winback_email, timedelta(seconds=20)),  # 代表 20 天(累计 30 天)
]

# 对应 `temporal operator search-attribute create --name stage --type Keyword`
# 手动注册过的那个字段，名字/类型必须完全对上
STAGE_KEY = SearchAttributeKey.for_keyword("stage")


@workflow.defn
class OnboardingWorkflow:
    """Onboarding drip campaign。

    welcome email → 最多三级 reminder(REMINDER_STAGES),每级用
    wait_condition 让"激活信号"和"这一级超时"竞态:超时就发这一级的邮件、
    进下一级;激活就立刻结束。三级都跑完还没激活 → churned。

    状态:
      - self._current_stage:当前 stage,同时通过 get_current_stage()
        query 和 `stage` search attribute 暴露给外部(见 _set_stage)。
      - self._pending:只被 user_activated signal handler 追加,handler
        本身不做任何决策——决策全部收在 run() 主循环里,避免 handler 和
        主循环各自独立醒来、互相抢着做出矛盾的决定。
    """

    def __init__(self) -> None:
        self._pending: list[str] = []
        self._current_stage: str = Stage.WELCOME.value

    def _set_stage(self, stage: Stage) -> None:
        self._current_stage = stage.value
        workflow.upsert_search_attributes([STAGE_KEY.value_set(stage.value)])

    @workflow.signal
    def user_activated(self) -> None:
        self._pending.append("activated")

    @workflow.query
    def get_current_stage(self) -> str:
        return self._current_stage

    @workflow.run
    async def run(self, user_id: str) -> str:
        self._set_stage(Stage.WELCOME)
        await workflow.execute_activity(
            send_welcome_email,
            user_id,
            start_to_close_timeout=timedelta(seconds=10),
        )

        for stage, reminder_activity, wait_time in REMINDER_STAGES:
            self._set_stage(stage)
            # 谁先发生：信号到达（_pending 非空）还是这一级的超时
            try:
                await workflow.wait_condition(
                    lambda: len(self._pending) > 0,
                    timeout=wait_time,
                )
            except asyncio.TimeoutError:
                # 这一级超时了，期间没收到激活信号 → 发这一级对应的邮件，
                # 进入下一级继续等
                await workflow.execute_activity(
                    reminder_activity,
                    user_id,
                    start_to_close_timeout=timedelta(seconds=10),
                )
                continue

            # wait_condition 正常返回 = 条件在超时前变成了 True，即信号先到了
            self._set_stage(Stage.ACTIVATED)
            return Stage.ACTIVATED.value

        # 三级 for 循环都跑完了，还是没激活
        self._set_stage(Stage.CHURNED)
        return Stage.CHURNED.value
