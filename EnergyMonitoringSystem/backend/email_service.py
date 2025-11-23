"""
Email service for sending alerts and notifications
Handles SMTP configuration and email queue processing.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from typing import Dict, List, Optional
from datetime import datetime
import asyncio

from backend.dal.database import db_helper

class EmailService:
    """Email service for sending notifications"""

    def __init__(self):
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_use_ssl = os.getenv("SMTP_USE_SSL", "false").lower() in ("1", "true", "yes") or self.smtp_port == 465
        self.smtp_username = os.getenv("SMTP_USERNAME")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.from_email = os.getenv("SMTP_FROM_EMAIL", "alerts@pac3220.local")

        # Check if SMTP is configured
        self.smtp_enabled = all([
            self.smtp_username,
            self.smtp_password,
            self.from_email
        ])

        if not self.smtp_enabled:
            print("WARNING: SMTP not configured. Email alerts will be disabled.")
        else:
            print(f"Email service initialized with SMTP: {self.smtp_server}:{self.smtp_port}")

    def send_email(self, to_email: str, subject: str, body: str) -> bool:
        """Send a single email"""
        if not self.smtp_enabled:
            print(f"EMAIL DISABLED: Would send to {to_email}: {subject}")
            return False

        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = to_email
            msg['Subject'] = subject

            # Add body
            msg.attach(MIMEText(body, 'html'))

            # Send email (SSL or TLS based on config)
            text = msg.as_string()
            if self.smtp_use_ssl:
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
                server.login(self.smtp_username, self.smtp_password)
                server.sendmail(self.from_email, to_email, text)
                server.quit()
            else:
                server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                server.sendmail(self.from_email, to_email, text)
                server.quit()

            print(f"Email sent successfully to {to_email}")
            return True

        except Exception as e:
            print(f"Failed to send email to {to_email}: {e}")
            return False

    def process_email_queue(self) -> int:
        """Process pending emails from queue using ops.EmailQueue schema."""
        try:
            pending_emails = db_helper.execute_query(
                """
                SELECT TOP 10 EmailID, EmailTo, Subject, Body, Priority
                FROM ops.EmailQueue
                WHERE SendStatus = 'PENDING'
                ORDER BY CASE WHEN Priority='CRITICAL' THEN 3 WHEN Priority='HIGH' THEN 2 ELSE 1 END DESC, QueuedAt ASC
                """
            )

            if not pending_emails:
                return 0

            processed = 0
            failed = 0
            for email in pending_emails:
                email_id = email["EmailID"]
                to_email = email["EmailTo"]
                subject = email["Subject"]
                body = email["Body"]
                attempts = int(email.get("Attempts", 0) or 0)

                success = self.send_email(to_email, subject, body)

                if success:
                    db_helper.execute_query(
                        """
                        UPDATE ops.EmailQueue
                        SET SendStatus = 'SENT', SentAt = GETUTCDATE()
                        WHERE EmailID = ?
                        """,
                        (email_id,)
                    )
                    processed += 1
                else:
                    # increment attempts and capture error if possible next run
                    db_helper.execute_query(
                        """
                        UPDATE ops.EmailQueue
                        SET SendStatus = 'FAILED'
                        WHERE EmailID = ?
                        """,
                        (email_id,)
                    )
                    failed += 1

            print(f"Email queue processed: {processed} sent, {failed} failed")
            return processed

        except Exception as e:
            print(f"Error processing email queue: {e}")
            return 0

    def queue_low_balance_alert(self, user_id: int) -> bool:
        """Queue a low balance alert email"""
        try:
            # Get user info
            user_info = db_helper.execute_query("""
                SELECT Email, RemainingKWh, FullName
                FROM app.Users
                WHERE UserID = ? AND Email IS NOT NULL
            """, (user_id,))

            if not user_info or not user_info[0]["Email"]:
                return False

            user = user_info[0]
            email = user["Email"]
            remaining_kwh = user["RemainingKWh"]
            full_name = user["FullName"]

            # Create email content
            subject = "⚠️ Low Energy Balance Warning"
            body = f"""
            <html>
            <body>
                <h2>Low Energy Balance Alert</h2>
                <p>Dear {full_name},</p>
                <p><strong>Warning:</strong> Your prepaid energy balance is running low.</p>
                <p><strong>Remaining Balance:</strong> {remaining_kwh:.2f} KWh</p>
                <p>Please recharge your account soon to avoid service interruption.</p>
                <br>
                <p>Best regards,<br>PAC3220 Energy Monitoring System</p>
            </body>
            </html>
            """

            # Insert into queue
            db_helper.execute_query("""
                INSERT INTO ops.EmailQueue (UserID, EmailTo, Subject, Body, Priority)
                VALUES (?, ?, ?, ?, 'HIGH')
            """, (user_id, email, subject, body))

            print(f"Low balance alert queued for user {user_id}")
            return True

        except Exception as e:
            print(f"Error queuing low balance alert: {e}")
            return False

    def queue_device_offline_alert(self, analyzer_id: int) -> bool:
        """Queue a device offline alert email"""
        try:
            # Get analyzer info and owner
            analyzer_info = db_helper.execute_query("""
                SELECT a.SerialNumber, u.Email, u.FullName
                FROM app.Analyzers a
                JOIN app.Users u ON a.UserID = u.UserID
                WHERE a.AnalyzerID = ? AND u.Email IS NOT NULL
            """, (analyzer_id,))

            if not analyzer_info:
                return False

            info = analyzer_info[0]
            email = info["Email"]
            serial = info["SerialNumber"]
            full_name = info["FullName"]

            # Create email content
            subject = "🚨 Device Offline Alert"
            body = f"""
            <html>
            <body>
                <h2>Device Offline Alert</h2>
                <p>Dear {full_name},</p>
                <p><strong>Alert:</strong> Your energy monitoring device has gone offline.</p>
                <p><strong>Device Serial:</strong> {serial}</p>
                <p>Please check your device connection and network settings.</p>
                <br>
                <p>Best regards,<br>PAC3220 Energy Monitoring System</p>
            </body>
            </html>
            """

            # Insert into queue
            db_helper.execute_query("""
                INSERT INTO ops.EmailQueue (EmailTo, Subject, Body, Priority)
                VALUES (?, ?, ?, 'CRITICAL')
            """, (email, subject, body))

            print(f"Device offline alert queued for analyzer {analyzer_id}")
            return True

        except Exception as e:
            print(f"Error queuing device offline alert: {e}")
            return False

# Global email service instance
email_service = EmailService()

# Background task for processing email queue
async def process_email_queue_background():
    """Background task to process email queue periodically"""
    while True:
        try:
            email_service.process_email_queue()
        except Exception as e:
            print(f"Error in email queue processing task: {e}")

        # Process every 5 minutes
        await asyncio.sleep(300)