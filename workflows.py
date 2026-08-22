import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import (
        send_reminder_email,
        send_second_reminder_email,
        send_welcome_email,
        send_winback_email,
    )

# (stage 名字, activity 函数, 等待时长) —— 每一级"没激活就发这封邮件"用的是
# 同一套 wait_condition 竞态逻辑，只有名字/activity/等待时长不一样，
# 抽成数据驱动循环。stage 名字同时也是 Query 能查到的 _current_stage 的值。
REMINDER_STAGES = [
    ("reminder_1", send_reminder_email, timedelta(seconds=10)),  # 代表 3 天
    ("reminder_2", send_second_reminder_email, timedelta(seconds=15)),  # 代表 7 天(累计 10 天)
    ("winback", send_winback_email, timedelta(seconds=20)),  # 代表 20 天(累计 30 天)
]


@workflow.defn
class OnboardingWorkflow:
    """注册 → welcome email → 等待(demo 里压缩成 20 秒代表 3 天) → reminder email。

    跑一次这个 workflow,Server 端的 Event History 大致会长这样(时间线):

        t=0     WorkflowExecutionStarted
                → 生成 Workflow Task #1

        Worker 领到 Workflow Task #1，跑到 execute_activity(welcome) 就停住，
        交出命令 "排一个 Activity"：
                ActivityTaskScheduled(welcome)
                → 生成 Activity Task(welcome)

        Worker 领到 Activity Task(welcome)，真正执行函数体，打印邮件：
                ActivityTaskCompleted(welcome, result="welcome_sent")
                → 生成 Workflow Task #2

        Worker 领到 Workflow Task #2，重放(replay)：
          - 跑到 execute_activity(welcome) 那一行 → History 里已有对应的
            Scheduled+Completed，直接读缓存结果 "welcome_sent"，不重新执行
          - 跑到 workflow.sleep(20s) 那一行 → 第一次到这，停住，交出命令
            "设一个 20 秒后到期的 Timer"：
                TimerStarted
                (这次没有 Activity Task，因为命令是设 Timer 不是排活儿)

        t=20s   Server 自己的时钟发现 Timer 到期：
                TimerFired
                → 生成 Workflow Task #3

        Worker 领到 Workflow Task #3，再次重放：
          - welcome 那行、sleep 那行 → 都命中缓存，跳过
          - 跑到 execute_activity(reminder) 那一行 → 第一次到这，停住，交出命令：
                ActivityTaskScheduled(reminder)
                → 生成 Activity Task(reminder)

        Worker 领到 Activity Task(reminder)，真正执行函数体，打印邮件：
                ActivityTaskCompleted(reminder, result="reminder_sent")
                → 生成 Workflow Task #4

        Worker 领到 Workflow Task #4，最后一次重放，全部命中缓存，
        跑到 `return "reminder_sent"`，交出命令 "workflow 执行完毕"：
                WorkflowExecutionCompleted

    全程 send_welcome_email / send_reminder_email 的函数体各自只被真正执行过
    一次(在各自的 Activity Task 里)，其余几次跑到同一行都是直接读 History
    里的缓存结果，不会重复发邮件。

    Phase 2 (5a) 更新：上面这条时间线是 Phase 1 单纯 sleep() 的版本。现在
    welcome 之后的等待换成了 workflow.wait_condition(..., timeout=20s)，
    等的是"_pending 里有东西"和"20 秒超时"两件事谁先发生（竞态）：
      - 信号先到：_pending 变成非空，wait_condition 正常返回，
        跳过 reminder，直接 return "activated"
      - 超时先到：wait_condition 抛 asyncio.TimeoutError，
        走 except 分支照常发 reminder，return "reminder_sent"
    这也是为什么 History 里等待期间会先出现 TimerStarted（wait_condition
    底层设的超时计时器），如果信号先到，这个 Timer 还没触发就已经不再
    影响流程了。

    Phase 3 (6a/6b) 更新：原来"只有一级、等一次"的 wait_condition，现在
    循环三次（REMINDER_STAGES），每一级复用同一套竞态逻辑：
      - 某一级超时（没激活）→ 发那一级对应的邮件 → continue 进下一级
      - 任意一级 wait_condition 正常返回（激活了）→ 直接 return，不再进入
        后面的级别
      - 三级都跑完还没激活 → return "churned"

    Phase 4 (7a) 更新：加了 self._current_stage + get_current_stage 这个
    Query。Query handler 必须是同步方法（没有 async/await），不能调用
    execute_activity/sleep/wait_condition 这些会产出命令的 API —— 因为
    Query 不允许产生任何新的 History 事件，纯粹是"读现有状态，不打扰这个
    workflow 未来的重放结果"。它能拿到正确答案，靠的是：查询时如果这个
    workflow 没缓存在内存里，worker 会做一次只读的重放，把
    self._current_stage 之类的实例状态重建出来，重放完直接读，不产生
    任何新命令交回 Server。
    """

    def __init__(self) -> None:
        self._pending: list[str] = []
        self._current_stage: str = "welcome"

    @workflow.signal
    def user_activated(self) -> None:
        self._pending.append("activated")

    @workflow.query
    def get_current_stage(self) -> str:
        return self._current_stage

    @workflow.run
    async def run(self, user_id: str) -> str:
        await workflow.execute_activity(
            send_welcome_email,
            user_id,
            start_to_close_timeout=timedelta(seconds=10),
        )

        for stage_name, reminder_activity, wait_time in REMINDER_STAGES:
            self._current_stage = stage_name
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
            self._current_stage = "activated"
            return "activated"

        # 三级 for 循环都跑完了，还是没激活
        self._current_stage = "churned"
        return "churned"
