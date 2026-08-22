import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities import (
    send_reminder_email,
    send_second_reminder_email,
    send_welcome_email,
    send_winback_email,
)
from workflows import OnboardingWorkflow

TASK_QUEUE = "onboarding-task-queue"


async def main():
    """
    Worker 进程，跑在后台，poll task queue，执行 workflow/activities。
    """

    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OnboardingWorkflow],
        activities=[
            send_welcome_email,
            send_reminder_email,
            send_second_reminder_email,
            send_winback_email,
        ],
    )
    print(f"Worker started, polling task queue '{TASK_QUEUE}'...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
