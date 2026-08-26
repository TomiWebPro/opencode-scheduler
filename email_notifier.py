#!/usr/bin/env python3
"""Email notification module for the opencode scheduler.

Sends HTML emails via Gmail SMTP after task runs.
Config is read from email_config.json in the same directory.
"""

import json
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "email_config.json"


def load_config():
    if not CONFIG_PATH.exists():
        return None
    return json.loads(CONFIG_PATH.read_text())


def send_email(subject, html_body, config=None, in_reply_to=None, references=None):
    """Send an HTML email via Gmail SMTP. Returns True on success."""
    if config is None:
        config = load_config()
    if not config or not config.get("enabled"):
        return False

    msg = MIMEMultipart("alternative")
    msg["From"] = f"OpenCode Scheduler <{config['smtp_user']}>"
    msg["To"] = config["notify_to"]
    msg["Subject"] = subject
    msg["X-Mailer"] = "opencode-scheduler"
    msg.attach(MIMEText(html_body, "html"))

    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
    if references:
        msg["References"] = references
    elif in_reply_to:
        msg["References"] = in_reply_to

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(config.get("smtp_host", "smtp.gmail.com"),
                          config.get("smtp_port", 587)) as server:
            server.starttls(context=ctx)
            server.login(config["smtp_user"], config["smtp_password"])
            server.sendmail(config["smtp_user"], config["notify_to"], msg.as_string())
        return True
    except Exception as e:
        print(f"email send failed: {e}")
        return False


def format_run_email(job_name, run_id, exit_code, duration_s, score, verdict,
                     check_passed, prompt, output_tail, report_text):
    """Build an HTML email body for a finished run."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if exit_code == 0:
        status_color = "#22c55e"
        status_label = "SUCCESS"
    elif exit_code == -1:
        status_color = "#f59e0b"
        status_label = "TIMEOUT"
    else:
        status_color = "#ef4444"
        status_label = "FAILED"

    check_html = ""
    if check_passed is not None:
        cp = "PASS" if check_passed else "FAIL"
        cc = "#22c55e" if check_passed else "#ef4444"
        check_html = f'<tr><td style="padding:4px 12px;font-weight:600;">Verifier</td><td style="padding:4px 12px;color:{cc};font-weight:700;">{cp}</td></tr>'

    score_html = ""
    if score is not None:
        score_html = f'<tr><td style="padding:4px 12px;font-weight:600;">Quality Score</td><td style="padding:4px 12px;font-weight:700;">{score}/10</td></tr>'

    verdict_html = ""
    if verdict:
        vc = {"SUCCESS": "#22c55e", "PARTIAL": "#f59e0b", "FAILURE": "#ef4444"}.get(verdict, "#6b7280")
        verdict_html = f'<tr><td style="padding:4px 12px;font-weight:600;">Verdict</td><td style="padding:4px 12px;color:{vc};font-weight:700;">{verdict}</td></tr>'

    report_section = ""
    if report_text:
        escaped = report_text.strip().replace("<", "&lt;").replace(">", "&gt;")
        report_section = f"""
        <div style="margin-top:20px;padding:16px;background:#f8fafc;border-radius:8px;border:1px solid #e2e8f0;">
          <h3 style="margin:0 0 8px 0;color:#1e293b;font-size:14px;">Agent Quality Report</h3>
          <pre style="margin:0;white-space:pre-wrap;font-size:13px;color:#334155;font-family:monospace;">{escaped}</pre>
        </div>"""

    output_section = ""
    if output_tail:
        escaped_out = output_tail.strip().replace("<", "&lt;").replace(">", "&gt;")
        output_section = f"""
        <div style="margin-top:16px;padding:16px;background:#f1f5f9;border-radius:8px;border:1px solid #e2e8f0;">
          <h3 style="margin:0 0 8px 0;color:#1e293b;font-size:14px;">Task Output</h3>
          <pre style="margin:0;white-space:pre-wrap;font-size:13px;color:#334155;font-family:monospace;max-height:300px;overflow:auto;">{escaped_out}</pre>
        </div>"""

    return f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f8fafc;">
  <div style="max-width:600px;margin:20px auto;background:#fff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,0.1);overflow:hidden;">
    <div style="padding:24px 24px 16px 24px;border-bottom:1px solid #e2e8f0;">
      <h1 style="margin:0;font-size:20px;color:#1e293b;">OpenCode Scheduler</h1>
      <p style="margin:8px 0 0 0;font-size:13px;color:#64748b;">Task notification &mdash; {ts}</p>
    </div>
    <div style="padding:24px;">
      <div style="margin-bottom:20px;padding:16px;background:{status_color}11;border-left:4px solid {status_color};border-radius:0 8px 8px 0;">
        <span style="font-size:18px;font-weight:700;color:{status_color};">{status_label}</span>
        <span style="margin-left:8px;font-size:14px;color:#64748b;">Job: <strong>{job_name}</strong> (run #{run_id})</span>
      </div>
      <table style="width:100%;border-collapse:collapse;font-size:14px;">
        <tr><td style="padding:4px 12px;font-weight:600;color:#64748b;">Duration</td><td style="padding:4px 12px;">{duration_s}s</td></tr>
        <tr><td style="padding:4px 12px;font-weight:600;color:#64748b;">Exit Code</td><td style="padding:4px 12px;">{exit_code}</td></tr>
        {check_html}
        {score_html}
        {verdict_html}
      </table>
      {report_section}
      {output_section}
    </div>
    <div style="padding:16px 24px;background:#f8fafc;border-top:1px solid #e2e8f0;text-align:center;">
      <p style="margin:0;font-size:12px;color:#94a3b8;">opencode scheduler &bull; auto-generated notification</p>
    </div>
  </div>
</body>
</html>"""


def format_user_result_email(job_name, run_id, exit_code, duration_s,
                              output_tail, report_text, score, verdict,
                              check_passed):
    """Email sent to the user's own thread with the ACTUAL task deliverable.

    Leads with the real task output (the answer the user asked for), and only
    then shows the QA verdict/score as secondary context. This is what guarantees
    the user receives the result, not just a quality report.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if exit_code == 0:
        status_color = "#22c55e"
        status_label = "DONE"
    elif exit_code == -1:
        status_color = "#f59e0b"
        status_label = "TIMEOUT"
    else:
        status_color = "#ef4444"
        status_label = "FAILED"

    meta_bits = [
        f"<tr><td style='padding:4px 12px;font-weight:600;color:#64748b;'>Duration</td><td style='padding:4px 12px;'>{duration_s}s</td></tr>",
        f"<tr><td style='padding:4px 12px;font-weight:600;color:#64748b;'>Job</td><td style='padding:4px 12px;'><strong>{job_name}</strong> (run #{run_id})</td></tr>",
    ]
    if score is not None:
        meta_bits.append(f"<tr><td style='padding:4px 12px;font-weight:600;color:#64748b;'>Quality</td><td style='padding:4px 12px;'>{score}/10 {verdict or ''}</td></tr>")
    if check_passed is not None:
        cp = "PASS" if check_passed else "FAIL"
        meta_bits.append(f"<tr><td style='padding:4px 12px;font-weight:600;color:#64748b;'>Verifier</td><td style='padding:4px 12px;'>{cp}</td></tr>")
    meta_html = "".join(meta_bits)

    output_section = ""
    if output_tail and output_tail.strip():
        capped = output_tail.strip()
        if len(capped) > 40000:
            capped = capped[:40000] + "\n\n… (output truncated for email)"
        escaped_out = capped.replace("<", "&lt;").replace(">", "&gt;")
        output_section = f"""
        <div style="margin-top:16px;padding:16px;background:#1e293b;border-radius:8px;border:1px solid #334155;">
          <h3 style="margin:0 0 8px 0;color:#e2e8f0;font-size:14px;">Result</h3>
          <pre style="margin:0;white-space:pre-wrap;font-size:13px;color:#e2e8f0;font-family:monospace;max-height:480px;overflow:auto;">{escaped_out}</pre>
        </div>"""
    else:
        output_section = """
        <div style="margin-top:16px;padding:16px;background:#fef2f2;border-radius:8px;border:1px solid #fecaca;">
          <p style="margin:0;color:#b91c1c;font-size:13px;">The job finished but produced no captured output.
          Check the run log for details.</p>
        </div>"""

    report_section = ""
    if report_text:
        escaped = report_text.strip().replace("<", "&lt;").replace(">", "&gt;")
        report_section = f"""
        <details style="margin-top:16px;">
          <summary style="cursor:pointer;color:#64748b;font-size:13px;">Quality report (optional)</summary>
          <div style="margin-top:8px;padding:16px;background:#f8fafc;border-radius:8px;border:1px solid #e2e8f0;">
            <pre style="margin:0;white-space:pre-wrap;font-size:13px;color:#334155;font-family:monospace;">{escaped}</pre>
          </div>
        </details>"""

    return f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f8fafc;">
  <div style="max-width:640px;margin:20px auto;background:#fff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,0.1);overflow:hidden;">
    <div style="padding:20px 24px;border-bottom:1px solid #e2e8f0;">
      <h1 style="margin:0;font-size:18px;color:#1e293b;">&#9989; Your request is ready</h1>
      <p style="margin:6px 0 0 0;font-size:13px;color:#64748b;">OpenCode Scheduler &mdash; {ts}</p>
    </div>
    <div style="padding:24px;">
      <div style="margin-bottom:16px;padding:12px 16px;background:{status_color}11;border-left:4px solid {status_color};border-radius:0 8px 8px 0;">
        <span style="font-size:16px;font-weight:700;color:{status_color};">{status_label}</span>
      </div>
      <table style="width:100%;border-collapse:collapse;font-size:14px;">{meta_html}</table>
      {output_section}
      {report_section}
    </div>
    <div style="padding:14px 24px;background:#f8fafc;border-top:1px solid #e2e8f0;text-align:center;">
      <p style="margin:0;font-size:12px;color:#94a3b8;">opencode scheduler &bull; auto-generated result</p>
    </div>
  </div>
</body>
</html>"""


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("usage: python3 email_notifier.py test")
        sys.exit(1)
    if sys.argv[1] == "test":
        ok = send_email(
            "OpenCode Scheduler — Test",
            "<h2>Test email</h2><p>If you see this, the notifier works.</p>",
        )
        print("sent" if ok else "failed")
    else:
        print(f"unknown command: {sys.argv[1]}")
