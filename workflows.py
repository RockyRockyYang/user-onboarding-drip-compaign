from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import send_reminder_email, send_welcome_email


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
    """

    @workflow.run
    async def run(self, user_id: str) -> str:
        await workflow.execute_activity(
            send_welcome_email,
            user_id,
            start_to_close_timeout=timedelta(seconds=10),
        )

        # 代表"3 天未激活"的等待窗口，demo 里压缩成 20 秒
        await workflow.sleep(timedelta(seconds=20))

        await workflow.execute_activity(
            send_reminder_email,
            user_id,
            start_to_close_timeout=timedelta(seconds=10),
        )

        return "reminder_sent"
