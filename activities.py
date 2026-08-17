from temporalio import activity


@activity.defn
async def send_welcome_email(user_id: str) -> str:
    print(f"[Email] Welcome email sent to {user_id}")
    return "welcome_sent"


@activity.defn
async def send_reminder_email(user_id: str) -> str:
    print(f"[Email] Reminder email sent to {user_id}")
    return "reminder_sent"
