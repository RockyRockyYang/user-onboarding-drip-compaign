import asyncio
import sys

from temporalio.client import Client

from workflows import OnboardingWorkflow


async def main():
    """模拟"用户点击激活链接"：对一个已经在跑的 workflow 发 user_activated 信号。

    用法：uv run python activate.py <workflow_id>
    workflow_id 从 starter.py 的输出或 Web UI 里复制。

    注意：get_workflow_handle 不发任何请求，只是本地构造一个指向已存在
    workflow 的引用；真正的网络请求发生在 handle.signal(...) 这一行。
    这个 await 等到的是"Server 确认收到、记进 History 了"，不是等 workflow
    真正处理完这个信号——信号是异步、单向的，发送方不等回复。
    """
    workflow_id = sys.argv[1]
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)
    await handle.signal(OnboardingWorkflow.user_activated)
    print(f"Sent 'user_activated' signal to {workflow_id}")


if __name__ == "__main__":
    asyncio.run(main())
