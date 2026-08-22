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
async def send_winback_email(user_id: str) -> str:
    print(f"[Email] Win-back email sent to {user_id}")
    return "winback_sent"
