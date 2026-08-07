from __future__ import annotations

import json
import os
import smtplib
import ssl
import traceback
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.message import EmailMessage
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class NotificationSettings:
    enabled: bool
    provider: str
    notify_on_success: bool
    telegram_bot_token: str
    telegram_chat_id: str
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    smtp_from: str
    smtp_to: List[str]
    smtp_use_tls: bool
    smtp_use_ssl: bool


def load_notification_settings() -> NotificationSettings:
    return NotificationSettings(
        enabled=_bool_env("NOTIFY_ENABLED", False),
        provider=os.environ.get("NOTIFY_PROVIDER", "telegram").strip().lower(),
        notify_on_success=_bool_env("NOTIFY_ON_SUCCESS", False),
        telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_chat_id=os.environ.get("TELEGRAM_CHAT_ID", "").strip(),
        smtp_host=os.environ.get("SMTP_HOST", "").strip(),
        smtp_port=int(os.environ.get("SMTP_PORT", "587")),
        smtp_user=os.environ.get("SMTP_USER", "").strip(),
        smtp_password=os.environ.get("SMTP_PASSWORD", ""),
        smtp_from=os.environ.get("SMTP_FROM", "").strip(),
        smtp_to=_csv_env("SMTP_TO"),
        smtp_use_tls=_bool_env("SMTP_USE_TLS", True),
        smtp_use_ssl=_bool_env("SMTP_USE_SSL", False),
    )


def build_crash_report(run_date: date, exc: BaseException) -> Dict[str, Any]:
    return {
        "run_date": run_date.isoformat(),
        "started_at": None,
        "finished_at": datetime.now(tz=timezone.utc).isoformat(),
        "status": "failed",
        "profiles_total": 0,
        "profiles_success": 0,
        "profiles_error": 0,
        "posts_found": 0,
        "posts_inserted": 0,
        "posts_updated": 0,
        "stories_found": 0,
        "comment_jobs_enqueued": 0,
        "comments_inserted": 0,
        "replies_inserted": 0,
        "jobs_processed": 0,
        "jobs_failed": 0,
        "jobs_pending": 0,
        "profile_results": [],
        "errors": [
            {
                "stage": "scheduled-run",
                "error": str(exc),
                "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-4000:],
            }
        ],
    }


def send_report_notification(
    report: Dict[str, Any],
    force: bool = False,
    settings: Optional[NotificationSettings] = None,
) -> bool:
    notification_settings = settings or load_notification_settings()
    if not force and not notification_settings.enabled:
        return False
    if not notification_settings.notify_on_success and not report_has_failure(report):
        return False

    subject, body = format_report_message(report)
    provider = notification_settings.provider
    if provider == "telegram":
        send_telegram(notification_settings, body)
    elif provider in {"email", "smtp"}:
        send_email(notification_settings, subject, body)
    else:
        raise ValueError(f"Unsupported NOTIFY_PROVIDER: {provider}")
    return True


def report_has_failure(report: Dict[str, Any]) -> bool:
    return any(
        [
            report.get("status") == "failed",
            int(report.get("profiles_error") or 0) > 0,
            int(report.get("jobs_failed") or 0) > 0,
            bool(report.get("errors")),
        ]
    )


def format_report_message(report: Dict[str, Any]) -> tuple[str, str]:
    run_date = str(report.get("run_date") or date.today().isoformat())
    status = _status_label(report)
    subject = f"Instagram collector {status} - {run_date}"
    stories_saved = sum(int(item.get("stories_saved", 0)) for item in report.get("profile_results", []))
    lines = [
        f"Coleta Instagram - {run_date}",
        f"Status: {status}",
        "",
        f"Perfis: {int(report.get('profiles_success') or 0)}/{int(report.get('profiles_total') or 0)} ok; "
        f"{int(report.get('profiles_error') or 0)} com erro",
        f"Posts: {int(report.get('posts_found') or 0)} encontrados; "
        f"{int(report.get('posts_inserted') or 0)} inseridos; {int(report.get('posts_updated') or 0)} atualizados",
        f"Stories: {int(report.get('stories_found') or 0)} arquivos; {stories_saved} salvos",
        f"Comentarios: {int(report.get('comment_jobs_enqueued') or 0)} jobs; "
        f"{int(report.get('comments_inserted') or 0)} comentarios; {int(report.get('replies_inserted') or 0)} replies",
        f"Fila: {int(report.get('jobs_pending') or 0)} pendentes; {int(report.get('jobs_failed') or 0)} falhas",
    ]
    elapsed = report.get("elapsed_seconds")
    if elapsed is not None:
        lines.append(f"Duracao: {float(elapsed):.1f}s")

    export_path = report.get("export_path")
    if export_path:
        lines.append(f"Export: {export_path}")

    errors = _error_lines(report)
    if errors:
        lines.extend(["", "Erros:"])
        lines.extend(errors[:12])
        if len(errors) > 12:
            lines.append(f"... e mais {len(errors) - 12} erro(s).")

    body = "\n".join(lines)
    return subject, body[:3900]


def send_telegram(settings: NotificationSettings, text: str) -> None:
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required for Telegram notifications.")

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    payload = urllib.parse.urlencode(
        {
            "chat_id": settings.telegram_chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    request = urllib.request.Request(url, data=payload, method="POST")
    with urllib.request.urlopen(request, timeout=20) as response:
        response.read()


def send_email(settings: NotificationSettings, subject: str, body: str) -> None:
    if not settings.smtp_host or not settings.smtp_to:
        raise ValueError("SMTP_HOST and SMTP_TO are required for email notifications.")

    sender = settings.smtp_from or settings.smtp_user
    if not sender:
        raise ValueError("SMTP_FROM or SMTP_USER is required for email notifications.")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(settings.smtp_to)
    message.set_content(body)

    if settings.smtp_use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port, context=context, timeout=20) as smtp:
            _login_if_configured(smtp, settings)
            smtp.send_message(message)
        return

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20) as smtp:
        if settings.smtp_use_tls:
            smtp.starttls(context=ssl.create_default_context())
        _login_if_configured(smtp, settings)
        smtp.send_message(message)


def _login_if_configured(smtp: smtplib.SMTP, settings: NotificationSettings) -> None:
    if settings.smtp_user:
        smtp.login(settings.smtp_user, settings.smtp_password)


def _status_label(report: Dict[str, Any]) -> str:
    if report.get("status") == "failed":
        return "FALHA"
    if report_has_failure(report):
        return "PARCIAL"
    return "OK"


def _error_lines(report: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    for item in report.get("profile_results", []):
        item_errors = item.get("errors") or []
        if item_errors:
            errors.append(f"- @{item.get('username')}: {' ; '.join(str(error) for error in item_errors)[:500]}")
    for item in report.get("errors", []):
        if isinstance(item, dict):
            stage = item.get("stage") or item.get("username") or "geral"
            errors.append(f"- {stage}: {str(item.get('error') or item)[:500]}")
        else:
            errors.append(f"- {str(item)[:500]}")
    return errors


def _bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _csv_env(name: str) -> List[str]:
    value = os.environ.get(name, "")
    return [item.strip() for item in value.split(",") if item.strip()]
