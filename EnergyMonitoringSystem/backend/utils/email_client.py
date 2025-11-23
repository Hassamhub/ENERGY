import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER or "noreply@example.com")
ALERTS_ENABLED = os.getenv("ALERTS_ENABLED", "false").lower() in ("1", "true", "yes")


def send_email(subject: str, body: str, to_emails: List[str], html: bool = False) -> bool:
    if not ALERTS_ENABLED:
        return False
    if not SMTP_HOST or not SMTP_PORT or not SMTP_FROM or not to_emails:
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = ", ".join(to_emails)

    if html:
        msg.attach(MIMEText(body, "html"))
    else:
        msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
            server.ehlo()
            try:
                server.starttls()
            except Exception:
                pass
            if SMTP_USER and SMTP_PASSWORD:
                server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, to_emails, msg.as_string())
        return True
    except Exception:
        return False
