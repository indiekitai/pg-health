"""Notification providers for PG Health alerts."""

import json
import os
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Optional
from .models import HealthReport, Severity

SEVERITY_EMOJI = {
    Severity.OK: "✅",
    Severity.INFO: "ℹ️",
    Severity.WARNING: "⚠️",
    Severity.CRITICAL: "❌",
}


@dataclass
class NotifyResult:
    """Result of notification attempt."""
    success: bool
    provider: str
    message: Optional[str] = None
    error: Optional[str] = None


def format_report_text(report: HealthReport, include_ok: bool = False) -> str:
    """Format health report as text for notifications."""
    lines = [f"🐘 PG Health Report: {report.database_name}"]
    lines.append(f"Status: {SEVERITY_EMOJI.get(report.worst_severity, '❓')} {report.worst_severity.value.upper()}")
    lines.append("")
    
    # Group by severity
    warnings = [c for c in report.checks if c.severity == Severity.WARNING]
    criticals = [c for c in report.checks if c.severity == Severity.CRITICAL]
    
    if criticals:
        lines.append("❌ CRITICAL:")
        for c in criticals:
            lines.append(f"  • {c.name}: {c.message}")
        lines.append("")
    
    if warnings:
        lines.append("⚠️ WARNINGS:")
        for c in warnings:
            lines.append(f"  • {c.name}: {c.message}")
        lines.append("")
    
    if include_ok:
        oks = [c for c in report.checks if c.severity == Severity.OK]
        if oks:
            lines.append(f"✅ {len(oks)} checks passed")
    
    return "\n".join(lines)


def send_telegram(
    report: HealthReport,
    bot_token: Optional[str] = None,
    chat_id: Optional[str] = None,
    only_on_issues: bool = True,
) -> NotifyResult:
    """
    Send health report to Telegram.
    
    Args:
        report: Health report to send
        bot_token: Telegram bot token (or PG_HEALTH_TELEGRAM_TOKEN env var)
        chat_id: Telegram chat ID (or PG_HEALTH_TELEGRAM_CHAT_ID env var)
        only_on_issues: Only send if there are warnings/criticals
    
    Returns:
        NotifyResult with success status
    """
    token = bot_token or os.getenv("PG_HEALTH_TELEGRAM_TOKEN")
    chat = chat_id or os.getenv("PG_HEALTH_TELEGRAM_CHAT_ID")
    
    if not token or not chat:
        return NotifyResult(
            success=False,
            provider="telegram",
            error="Missing TELEGRAM_TOKEN or TELEGRAM_CHAT_ID",
        )
    
    # Check if we should send
    if only_on_issues and not report.has_issues:
        return NotifyResult(
            success=True,
            provider="telegram",
            message="Skipped - no issues to report",
        )
    
    text = format_report_text(report)
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({
        "chat_id": chat,
        "text": text,
        "parse_mode": "HTML",
    }).encode()
    
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                return NotifyResult(
                    success=True,
                    provider="telegram",
                    message=f"Sent to chat {chat}",
                )
            else:
                return NotifyResult(
                    success=False,
                    provider="telegram",
                    error=result.get("description", "Unknown error"),
                )
    except urllib.error.URLError as e:
        return NotifyResult(
            success=False,
            provider="telegram",
            error=str(e),
        )


def send_webhook(
    report: HealthReport,
    webhook_url: Optional[str] = None,
    only_on_issues: bool = True,
) -> NotifyResult:
    """
    Send health report to a webhook endpoint.
    
    Args:
        report: Health report to send
        webhook_url: Webhook URL (or PG_HEALTH_WEBHOOK_URL env var)
        only_on_issues: Only send if there are warnings/criticals
    
    Returns:
        NotifyResult with success status
    """
    url = webhook_url or os.getenv("PG_HEALTH_WEBHOOK_URL")
    
    if not url:
        return NotifyResult(
            success=False,
            provider="webhook",
            error="Missing webhook URL",
        )
    
    # Check if we should send
    if only_on_issues and not report.has_issues:
        return NotifyResult(
            success=True,
            provider="webhook",
            message="Skipped - no issues to report",
        )
    
    # Build payload
    payload = {
        "database": report.database_name,
        "status": report.worst_severity.value,
        "has_issues": report.has_issues,
        "checks": [
            {
                "name": c.name,
                "severity": c.severity.value,
                "message": c.message,
                "suggestion": c.suggestion,
            }
            for c in report.checks
        ],
        "summary": {
            "total_checks": len(report.checks),
            "warnings": len([c for c in report.checks if c.severity == Severity.WARNING]),
            "criticals": len([c for c in report.checks if c.severity == Severity.CRITICAL]),
        },
    }
    
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return NotifyResult(
                success=True,
                provider="webhook",
                message=f"Posted to {url[:50]}...",
            )
    except urllib.error.URLError as e:
        return NotifyResult(
            success=False,
            provider="webhook",
            error=str(e),
        )


def send_email(
    report: HealthReport,
    smtp_host: Optional[str] = None,
    smtp_port: Optional[int] = None,
    smtp_user: Optional[str] = None,
    smtp_pass: Optional[str] = None,
    from_addr: Optional[str] = None,
    to_addr: Optional[str] = None,
    only_on_issues: bool = True,
) -> NotifyResult:
    """
    Send health report via email (SMTP).
    
    Environment variables:
      - PG_HEALTH_SMTP_HOST
      - PG_HEALTH_SMTP_PORT (default: 587)
      - PG_HEALTH_SMTP_USER
      - PG_HEALTH_SMTP_PASS
      - PG_HEALTH_EMAIL_FROM
      - PG_HEALTH_EMAIL_TO
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    host = smtp_host or os.getenv("PG_HEALTH_SMTP_HOST")
    port = smtp_port or int(os.getenv("PG_HEALTH_SMTP_PORT", "587"))
    user = smtp_user or os.getenv("PG_HEALTH_SMTP_USER")
    password = smtp_pass or os.getenv("PG_HEALTH_SMTP_PASS")
    sender = from_addr or os.getenv("PG_HEALTH_EMAIL_FROM")
    recipient = to_addr or os.getenv("PG_HEALTH_EMAIL_TO")
    
    if not all([host, user, password, sender, recipient]):
        return NotifyResult(
            success=False,
            provider="email",
            error="Missing SMTP configuration (host/user/pass/from/to)",
        )
    
    if only_on_issues and not report.has_issues:
        return NotifyResult(
            success=True,
            provider="email",
            message="Skipped - no issues to report",
        )
    
    # Build email
    status = report.worst_severity.value.upper()
    subject = f"[pg-health] {report.database_name}: {status}"
    
    text_body = format_report_text(report, include_ok=True)
    
    # HTML body
    html_parts = [f"<h2>🐘 PG Health Report: {report.database_name}</h2>"]
    html_parts.append(f"<p><strong>Status:</strong> {SEVERITY_EMOJI.get(report.worst_severity, '❓')} {status}</p>")
    
    criticals = [c for c in report.checks if c.severity == Severity.CRITICAL]
    warnings = [c for c in report.checks if c.severity == Severity.WARNING]
    oks = [c for c in report.checks if c.severity == Severity.OK]
    
    if criticals:
        html_parts.append("<h3>❌ Critical Issues</h3><ul>")
        for c in criticals:
            html_parts.append(f"<li><strong>{c.name}:</strong> {c.message}</li>")
        html_parts.append("</ul>")
    
    if warnings:
        html_parts.append("<h3>⚠️ Warnings</h3><ul>")
        for c in warnings:
            html_parts.append(f"<li><strong>{c.name}:</strong> {c.message}</li>")
        html_parts.append("</ul>")
    
    html_parts.append(f"<p>✅ {len(oks)} checks passed</p>")
    html_parts.append("<hr><p><small>Sent by pg-health</small></p>")
    
    html_body = "\n".join(html_parts)
    
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))
    
    try:
        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(user, password)
            server.sendmail(sender, [recipient], msg.as_string())
        
        return NotifyResult(
            success=True,
            provider="email",
            message=f"Sent to {recipient}",
        )
    except Exception as e:
        return NotifyResult(
            success=False,
            provider="email",
            error=str(e),
        )


def send_slack(
    report: HealthReport,
    webhook_url: Optional[str] = None,
    only_on_issues: bool = True,
) -> NotifyResult:
    """
    Send health report to Slack via webhook.
    
    Args:
        report: Health report to send
        webhook_url: Slack webhook URL (or PG_HEALTH_SLACK_WEBHOOK env var)
        only_on_issues: Only send if there are warnings/criticals
    """
    url = webhook_url or os.getenv("PG_HEALTH_SLACK_WEBHOOK")
    
    if not url:
        return NotifyResult(
            success=False,
            provider="slack",
            error="Missing Slack webhook URL",
        )
    
    if only_on_issues and not report.has_issues:
        return NotifyResult(
            success=True,
            provider="slack",
            message="Skipped - no issues to report",
        )
    
    # Build Slack message
    color = {
        Severity.OK: "good",
        Severity.INFO: "#439FE0",
        Severity.WARNING: "warning",
        Severity.CRITICAL: "danger",
    }.get(report.worst_severity, "#808080")
    
    text = format_report_text(report)
    
    payload = {
        "attachments": [
            {
                "color": color,
                "title": f"PG Health: {report.database_name}",
                "text": text,
                "footer": "pg-health",
            }
        ]
    }
    
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return NotifyResult(
                success=True,
                provider="slack",
                message="Sent to Slack",
            )
    except urllib.error.URLError as e:
        return NotifyResult(
            success=False,
            provider="slack",
            error=str(e),
        )
