import asyncio
import uuid

from temporalio.client import Client

from workflows import OnboardingWorkflow

TASK_QUEUE = "onboarding-task-queue"


async def main():
    """模拟"用户注册"这个触发点，真实场景会长在 webhook handler / API endpoint 里。

    这里用的是 client.execute_workflow(...)，几个关键点：

    - Client.connect(...) 和 worker.py 里是同一个 Client 类，但角色相反：
      worker.py 用它不停 poll 任务、汇报结果；这里用它发起一个新的 workflow。

    - id=f"onboarding-{user_id}" —— Workflow ID 的幂等语义：同一个 ID 不能
      同时存在两个"运行中"的实例。生产里用真实、天然唯一的 user_id 做这个 ID，
      这样即使触发逻辑被意外重复调用两次，Temporal 会识别出是同一个 ID，
      不会真的给用户跑出两条重复的 onboarding 流程、发两遍邮件——这个去重
      是白拿的。demo 里为了能反复跑测试，用随机后缀避免和之前跑过的 ID 撞车。

    - execute_workflow(...) 会阻塞等到 workflow 跑完才返回结果，图 demo 直观。
      真实场景通常用 start_workflow(...)：发起后立刻返回一个 handle 不等，
      因为不会想让一个 HTTP 请求真的挂着等 3 天甚至 30 天。

    - task_queue 必须和 worker.py 里的完全一致，这是两个独立进程之间唯一的
      接头暗号，对不上 starter 发起的 workflow 会一直没人处理。
    """
    client = await Client.connect("localhost:7233")
    user_id = f"user-{uuid.uuid4().hex[:8]}"
    print(f"Starting onboarding workflow: onboarding-{user_id}")

    result = await client.execute_workflow(
        OnboardingWorkflow.run,
        user_id,
        id=f"onboarding-{user_id}",
        task_queue=TASK_QUEUE,
    )
    print(f"Workflow finished for {user_id}, result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
