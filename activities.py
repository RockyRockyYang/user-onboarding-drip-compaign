import asyncio

from temporalio import activity


@activity.defn
async def send_welcome_email(user_id: str) -> str:
    print(f"[Email] Welcome email sent to {user_id}")
    return "welcome_sent"


@activity.defn
async def send_reminder_email(user_id: str) -> str:
    print(f"[Email] Reminder email sent to {user_id}")
    return "reminder_sent"


@activity.defn
async def send_second_reminder_email(user_id: str) -> str:
    print(f"[Email] Second reminder email sent to {user_id}")
    return "second_reminder_sent"


@activity.defn
async def notify_manual_followup_needed(user_id: str) -> str:
    print(f"[Ops Alert] {user_id} needs manual follow-up (winback email failed permanently)")
    return "notified"


@activity.defn
async def send_winback_email(user_id: str) -> str:
    for i in range(5):
        await asyncio.sleep(1)  # 模拟卡住的第三方 API,分成几段慢慢跑
        activity.heartbeat(f"step {i + 1}/5")  # 每一段结束后汇报一次"我还活着"
    print(f"[Email] Win-back email sent to {user_id}")
    return "winback_sent"
