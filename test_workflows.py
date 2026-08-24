from datetime import timedelta

import temporalio.api.enums.v1 as enums
import temporalio.api.operatorservice.v1 as operatorservice
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities import (
    notify_manual_followup_needed,
    send_reminder_email,
    send_second_reminder_email,
    send_welcome_email,
    send_winback_email,
)
from workflows import OnboardingWorkflow

TASK_QUEUE = "test-queue"


async def _register_stage_search_attribute(env: WorkflowEnvironment) -> None:
    """time-skipping 测试用的是一个全新的、内存里的临时 server,不是本地
    `temporal server start-dev`——之前手动跑过的
    `temporal operator search-attribute create --name stage --type Keyword`
    只对本地 dev server 生效,这个临时 server 每次测试都得重新注册一遍,
    不然 workflow 一调用 upsert_search_attributes 就会被拒绝。
    """
    await env.client.operator_service.add_search_attributes(
        operatorservice.AddSearchAttributesRequest(
            namespace=env.client.namespace,
            search_attributes={"stage": enums.IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD},
        )
    )


async def test_churned_after_30_days():
    """全程不激活,三级 reminder 依次超时,最终 churned。"""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        await _register_stage_search_attribute(env)
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[OnboardingWorkflow],
            activities=[
                send_welcome_email,
                send_reminder_email,
                send_second_reminder_email,
                send_winback_email,
                notify_manual_followup_needed,
            ],
        ):
            result = await env.client.execute_workflow(
                OnboardingWorkflow.run,
                "user-churned",
                id="test-churned",
                task_queue=TASK_QUEUE,
            )
            assert result == "churned"


async def test_activated_immediately():
    """welcome 之后马上激活,一封 reminder 都不该发。"""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        await _register_stage_search_attribute(env)
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[OnboardingWorkflow],
            activities=[
                send_welcome_email,
                send_reminder_email,
                send_second_reminder_email,
                send_winback_email,
                notify_manual_followup_needed,
            ],
        ):
            handle = await env.client.start_workflow(
                OnboardingWorkflow.run,
                "user-activated-immediately",
                id="test-activated-immediately",
                task_queue=TASK_QUEUE,
            )
            await handle.signal(OnboardingWorkflow.user_activated)
            result = await handle.result()
            assert result == "activated"


async def test_activated_after_first_reminder():
    """3 天后(reminder_1 已发出)、7 天前激活:只发过 reminder_1,没有 reminder_2/winback。"""
    calls: list[str] = []

    def _tracking(real_fn):
        @activity.defn(name=real_fn.__name__)
        async def wrapper(user_id: str) -> str:
            calls.append(real_fn.__name__)
            return await real_fn(user_id)

        return wrapper

    async with await WorkflowEnvironment.start_time_skipping() as env:
        await _register_stage_search_attribute(env)
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[OnboardingWorkflow],
            activities=[
                _tracking(send_welcome_email),
                _tracking(send_reminder_email),
                _tracking(send_second_reminder_email),
                _tracking(send_winback_email),
                _tracking(notify_manual_followup_needed),
            ],
        ):
            handle = await env.client.start_workflow(
                OnboardingWorkflow.run,
                "user-activated-after-reminder-1",
                id="test-activated-after-reminder-1",
                task_queue=TASK_QUEUE,
            )
            await env.sleep(timedelta(days=4))  # 越过 3 天,reminder_1 应该已经发出
            await handle.signal(OnboardingWorkflow.user_activated)
            result = await handle.result()

            assert result == "activated"
            assert calls == ["send_welcome_email", "send_reminder_email"]


async def test_needs_manual_followup_on_permanent_winback_failure():
    """winback 邮件永久失败(重试耗尽):补偿分支生效,结果是 needs_manual_followup 不是 Failed。"""

    @activity.defn(name="send_winback_email")
    async def always_fails(user_id: str) -> str:
        raise RuntimeError("simulated permanent failure")

    notified: list[str] = []

    @activity.defn(name="notify_manual_followup_needed")
    async def tracking_notify(user_id: str) -> str:
        notified.append(user_id)
        return await notify_manual_followup_needed(user_id)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        await _register_stage_search_attribute(env)
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[OnboardingWorkflow],
            activities=[
                send_welcome_email,
                send_reminder_email,
                send_second_reminder_email,
                always_fails,
                tracking_notify,
            ],
        ):
            result = await env.client.execute_workflow(
                OnboardingWorkflow.run,
                "user-needs-followup",
                id="test-needs-followup",
                task_queue=TASK_QUEUE,
            )
            assert result == "needs_manual_followup"
            assert notified == ["user-needs-followup"]
