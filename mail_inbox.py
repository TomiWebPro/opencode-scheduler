#!/usr/bin/env python3
"""Email-to-task bridge for the opencode scheduler (v2).

Polls Gmail via IMAP for self-sent emails with subject exactly "sched".
Each request gets its own Gmail thread. Python sends an immediate ACK,
then hands only the body text to an opencode agent for interpretation.

No UNSEEN flag — uses UID cursor to track what's been scanned.
Agents follow strict autonomy rules: act freely on code, never on
real-life consequences without explicit user answer.

Run:
    python3 mail_inbox.py --once       # poll once and exit
    python3 mail_inbox.py              # daemon loop (default 120s)
    python3 mail_inbox.py --test       # send a test sched email
"""

import argparse
import email
import email.header
import email.utils
import imaplib
import json
import re
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta
from email.message import Message
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "scheduler.db"
MAIL_DB_PATH = BASE_DIR / "mail_inbox.db"
SCHEDULER_DB_PATH = BASE_DIR / "scheduler.db"
LOG_DIR = BASE_DIR / "logs"
ACTIVITY_LOG = LOG_DIR / "activity.jsonl"
SCHEDULER = str(BASE_DIR / "scheduler.py")

EMAIL_JOBS_SCHEMA = """
CREATE TABLE IF NOT EXISTS email_jobs (
    message_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    created_at REAL NOT NULL,
    result_sent INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (message_id, job_name)
);
"""

MAIL_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_messages (
    message_id TEXT PRIMARY KEY,
    uid INTEGER,
    subject TEXT,
    received_at REAL,
    action_taken TEXT,
    result TEXT
);
CREATE TABLE IF NOT EXISTS requests (
    message_id TEXT PRIMARY KEY,
    subject TEXT,
    body TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS uid_cursor (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_uid INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS thread_roots (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    message_id TEXT NOT NULL,
    created_at REAL NOT NULL
);
"""

AGENT_PROMPT = """You are a scheduling assistant for the local opencode scheduler.

Current date/time: {now}

CORE PRINCIPLE — DELEGATE FAST, REPLY IMMEDIATELY, NEVER LOSE THE RESULT:
You are a thin dispatcher, NOT an executor. Your only job is to (1) understand
the request in a few seconds, (2) hand ALL real work to the scheduler as a job
whose OUTPUT IS THE REAL DELIVERABLE, and (3) reply to the user by email ASAP.
You must NEVER run a long or heavy task inline, and you must NEVER wait for it.
- If the request is quick (list jobs, check status, schedule a recurring job,
  answer a question), do it directly and reply.
- If the request needs real work (research, roundtable, tests, builds, code
  generation, anything that takes more than ~30s), DO NOT do it yourself.
  Schedule it with:
    python3 {scheduler} add --name <name> --prompt "<self-contained prompt>" --once --auto --timeout N
  (N in seconds; use generous values like 1800+ for heavy work). The job's
  prompt must be written so that its final output IS the answer the user wants
  (e.g. for a roundtable, run it FOREGROUND and print the full summary). The
  scheduler will email that exact output back to the user automatically — you do
  NOT need a recheck job, and you must NOT use `--bg` for the deliverable.
  ALWAYS write delegated prompts using `python3` (never `python`, which is NOT
  installed on this machine) and set PATH if needed:
  export PATH="$HOME/.opencode/bin:$PATH".
  **Roundtable delegation example** (the correct way — foreground, real output):
    python3 {scheduler} add --name roundtable-apple-alu \
      --prompt "Run a foreground roundtable and print the COMPLETE summary as your final answer. Command: python3 ~/opencode-roundtable/roundtable.py run --topic \\\"What is the root cause of Apple opting to source aluminium? Explore business, environmental, geopolitical and supply-chain reasons.\\\" --persons 4 --turns 3  . After it finishes, run: python3 ~/opencode-roundtable/roundtable.py summary <session_id> and print the FULL report (participants, overview, key points, agreements, disagreements, new ideas, open questions, per-agent contributions, conclusion)." \
      --once --auto --timeout 1800
  This way the job's emailed output IS the roundtable report — nothing rots.
- After delegating, IMMEDIATELY reply with a short confirmation: what you
  queued and the exact job name(s). Do NOT wait for the delegated job to finish;
  the result arrives by email automatically.
- The scheduler auto-emails the job's real output to the user on completion
  (in the same email thread), so you never need to poll or summarize it.

AUTONOMY RULES:
- Act on your own judgment for anything local/technical/programming-related,
  including code changes, important function changes, file edits, scheduling jobs.
- NEVER take any action with real-life consequences, including but not limited to:
  informing/notifying any third party, sending anything to another person,
  revealing or communicating additional info to anyone, rebooting/restarting
  servers or services, modifying system infrastructure, spending money,
  anything visible outside this machine.
- If an action would have real-life consequences and the user's intent is
  unclear, do NOTHING destructive; end with RESULT: NEEDS_INPUT + question.
- Otherwise make reasonable assumptions and proceed.

Scheduler CLI: python3 {scheduler}

Available commands:
- add --name <name> --prompt "..." --schedule/--every/--once [--auto] [--timeout N]
- remove <name>
- disable/enable <name>
- list
- status <name>
- run-now <name>
- clear [-y]

Rules:
- Prompts you create must be fully self-contained (no conversation context).
- Use --auto only when the user clearly wants unattended execution.
- For date/time requests, compute correct cron fields using `date`.
- After acting, run `python3 {scheduler} list` to verify.
- Keep your reply concise.
- Before your final RESULT line, output exactly one line listing every job you
  created, e.g.:
    CREATED_JOBS: roundtable-apple-alu
  (If you created none, output CREATED_JOBS: none)
- IMPORTANT: The VERY LAST LINE of your reply MUST be exactly one of:
    RESULT: OK
    RESULT: FAILED
    RESULT: NEEDS_INPUT <question>
  No other text may appear after the RESULT: line.

{pending_section}
USER REQUEST:
{body}
"""

ADMIN_PROMPT = """You are a scheduling assistant answering a follow-up question.

The user previously sent a scheduling request that you couldn't process
because clarification was needed. Here is the context:

ORIGINAL REQUEST: {original_body}
YOUR QUESTION: {question}

CORE PRINCIPLE — NON-BLOCKING, DELEGATE THEN REPLY:
You are a thin dispatcher, NOT an executor. Understand the reply, then delegate
any real work to the scheduler as a detached background job (or answer directly
if it's just a question). Reply by email ASAP and never wait inline for a heavy
task to finish — the scheduler auto-emails a result when the delegated job
completes. For any work taking more than ~30s, use:
    python3 {scheduler} add --name <name> --prompt "<self-contained prompt>" --once --auto --timeout N
ALWAYS use `python3` (never `python`, which is NOT installed) in delegated prompts,
and set PATH if needed (export PATH="$HOME/.opencode/bin:$PATH").

The user's reply is below. Process it according to the autonomy rules and
complete the original request if possible.

Scheduler CLI: python3 {scheduler}

{pending_section}
USER REPLY:
{body}
"""


def log_event(event, **data):
    LOG_DIR.mkdir(exist_ok=True)
    entry = {"ts": time.time(), "time": datetime.now().isoformat(timespec="seconds"),
             "event": event, **data}
    with open(ACTIVITY_LOG, "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def load_config():
    p = BASE_DIR / "email_config.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text())


def get_mail_db():
    MAIL_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(MAIL_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(MAIL_DB_SCHEMA)
    return conn


def get_scheduler_db():
    """Shared DB with the scheduler (jobs/runs + email_jobs link table)."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SCHEDULER_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(EMAIL_JOBS_SCHEMA)
    return conn


def link_email_to_job(message_id, job_name):
    """Record that `job_name` was spawned in response to email `message_id`."""
    conn = get_scheduler_db()
    conn.execute(
        "INSERT OR REPLACE INTO email_jobs (message_id, job_name, created_at, result_sent) "
        "VALUES (?,?,?,COALESCE((SELECT result_sent FROM email_jobs WHERE message_id=? AND job_name=?),0))",
        (message_id, job_name, time.time(), message_id, job_name),
    )
    conn.commit()
    conn.close()


def get_email_link_for_job(job_name):
    """Return (message_id) of the originating email for a job, or None."""
    conn = get_scheduler_db()
    row = conn.execute(
        "SELECT message_id FROM email_jobs WHERE job_name=? ORDER BY created_at DESC LIMIT 1",
        (job_name,),
    ).fetchone()
    conn.close()
    return row["message_id"] if row else None


def mark_email_result_sent(message_id, job_name):
    conn = get_scheduler_db()
    conn.execute(
        "UPDATE email_jobs SET result_sent=1 WHERE message_id=? AND job_name=?",
        (message_id, job_name),
    )
    conn.commit()
    conn.close()


def link_jobs_in_window(message_id, start_ts, end_ts=None):
    """Link every scheduler job created within [start_ts, end_ts] to the email.

    Robust fallback when the agent's output can't be parsed (empty / killed):
    we snapshot the scheduler DB around the agent run instead of relying on the
    agent to report job names.
    """
    end_ts = end_ts or (time.time() + 2)
    conn = get_scheduler_db()
    rows = conn.execute(
        "SELECT name FROM jobs WHERE created_at >= ? AND created_at <= ?",
        (start_ts - 1, end_ts + 1),
    ).fetchall()
    for r in rows:
        link_email_to_job(message_id, r["name"])
    conn.close()
    return [r["name"] for r in rows]


def get_last_uid(conn):
    row = conn.execute("SELECT last_uid FROM uid_cursor WHERE id=1").fetchone()
    return row["last_uid"] if row else 0


def set_last_uid(conn, uid):
    conn.execute(
        "INSERT OR REPLACE INTO uid_cursor (id, last_uid, updated_at) VALUES (1, ?, ?)",
        (uid, time.time()),
    )
    conn.commit()


def get_thread_root(conn):
    row = conn.execute("SELECT message_id FROM thread_roots WHERE id=1").fetchone()
    return row["message_id"] if row else None


def set_thread_root(conn, message_id):
    conn.execute(
        "INSERT OR REPLACE INTO thread_roots (id, message_id, created_at) VALUES (1, ?, ?)",
        (message_id, time.time()),
    )
    conn.commit()


def get_pending_request(conn):
    row = conn.execute(
        "SELECT * FROM requests WHERE status='pending' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def create_request(conn, message_id, subject, body):
    now = time.time()
    conn.execute(
        "INSERT OR REPLACE INTO requests (message_id, subject, body, status, created_at, updated_at) VALUES (?,?,?,?,?,?)",
        (message_id, subject, body, "pending", now, now),
    )
    conn.commit()


def update_request_status(conn, message_id, status):
    conn.execute(
        "UPDATE requests SET status=?, updated_at=? WHERE message_id=?",
        (status, time.time(), message_id),
    )
    conn.commit()


def expire_old_requests(conn, hours=24):
    cutoff = time.time() - hours * 3600
    expired = conn.execute(
        "SELECT message_id, body FROM requests WHERE status='pending' AND created_at < ?",
        (cutoff,),
    ).fetchall()
    for row in expired:
        conn.execute(
            "UPDATE requests SET status='expired', updated_at=? WHERE message_id=?",
            (time.time(), row["message_id"]),
        )
    conn.commit()
    return expired


def decode_header_value(raw) -> str:
    if raw is None:
        return ""
    decoded_parts = email.header.decode_header(raw)
    result = []
    for data, charset in decoded_parts:
        if isinstance(data, bytes):
            result.append(data.decode(charset or "utf-8", errors="replace"))
        else:
            result.append(data)
    return "".join(result)


def extract_body(msg: Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            disp = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in disp:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
        for part in msg.walk():
            ct = part.get_content_type()
            disp = str(part.get("Content-Disposition", ""))
            if ct == "text/html" and "attachment" not in disp:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    html = payload.decode(charset, errors="replace")
                    return re.sub(r"<[^>]+>", "", html).strip()
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
    return ""


def build_agent_prompt(body, pending=None):
    now = datetime.now().isoformat(timespec="seconds")
    if pending:
        return ADMIN_PROMPT.format(
            now=now, scheduler=SCHEDULER,
            original_body=pending["body"][:2000],
            question=pending.get("question", ""),
            pending_section="", body=body,
        )
    return AGENT_PROMPT.format(
        now=now, scheduler=SCHEDULER, body=body, pending_section="",
    )


def run_agent(body, config, pending=None):
    prompt = build_agent_prompt(body, pending)
    timeout = config.get("mail_inbox", {}).get("agent_timeout", 480)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = str(LOG_DIR / f"mail-agent-{ts}.log")

    cmd = ["opencode", "run", prompt,
           "--title", "mail-scheduler-agent",
           "--format", "json",
           "--auto"]

    # Stream stdout into a buffer so partial output survives a timeout kill.
    # opencode buffers when piped; a hard SIGKILL (proc.kill) would discard the
    # whole buffer and we'd lose the agent's reply. We read concurrently and
    # SIGTERM on timeout so opencode can flush before we collect the remainder.
    captured = []

    def _reader(pipe):
        try:
            for line in iter(pipe.readline, ""):
                captured.append(line)
                with open(log_path, "a") as logf:
                    logf.write(line)
        except Exception:
            pass

    with open(log_path, "w") as logf:
        logf.write(f"$ opencode run <PROMPT> --title mail-scheduler-agent --format json --auto\n\n")
        logf.flush()
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=logf, text=True, bufsize=1)
        except Exception:
            return -1, "", log_path
        try:
            t = threading.Thread(target=_reader, args=(proc.stdout,), daemon=True)
            t.start()
            proc.wait(timeout=timeout)
            t.join(timeout=5)
            stdout = "".join(captured)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
            t.join(timeout=5)
            stdout = "".join(captured)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
            stdout = "".join(captured)

    return proc.returncode, (stdout or "").strip(), log_path


def parse_agent_result(output):
    # First try to find RESULT: in raw text (handles both plain and JSON output)
    for line in reversed(output.splitlines()):
        # Check for plain text RESULT: line
        m = re.match(r"RESULT:\s*(OK|FAILED|NEEDS_INPUT)\s*(.*)", line.strip(), re.IGNORECASE)
        if m:
            result = m.group(1).upper()
            extra = m.group(2).strip() if m.group(2) else ""
            return result, extra
        # Also check inside JSON text fields
        if '"text"' in line:
            try:
                obj = json.loads(line)
                if obj.get("type") == "text":
                    text = obj.get("part", {}).get("text", "")
                    for tline in reversed(text.splitlines()):
                        m2 = re.match(r"RESULT:\s*(OK|FAILED|NEEDS_INPUT)\s*(.*)", tline.strip(), re.IGNORECASE)
                        if m2:
                            return m2.group(1).upper(), m2.group(2).strip() if m2.group(2) else ""
            except (json.JSONDecodeError, AttributeError):
                pass
    return "UNKNOWN", ""


def parse_created_jobs(output):
    """Extract job names listed in a `CREATED_JOBS: a, b, c` line (if present)."""
    if not output:
        return []
    for line in output.splitlines():
        m = re.match(r"\s*CREATED_JOBS:\s*(.+)", line.strip(), re.IGNORECASE)
        if m:
            raw = m.group(1).strip()
            if raw.lower() in ("none", "n/a", "-", ""):
                return []
            return [p.strip() for p in raw.split(",") if p.strip()]
    return []


def ensure_thread_root(conn, config):
    root_id = get_thread_root(conn)
    if root_id:
        return root_id
    root_id = f"<scheduler-events-{uuid.uuid4().hex[:12]}@scheduler>"
    set_thread_root(conn, root_id)
    from email_notifier import send_email
    send_email(
        "⚙ Scheduler — event log",
        "<h2>⚙ Scheduler event log</h2>"
        "<p>This is the root of your scheduler event thread. "
        "All task notifications will appear here.</p>",
        config,
    )
    log_event("thread_root_created", message_id=root_id)
    return root_id


def send_ack(config, request_mid, body):
    preview = body.strip().replace("\n", " ")[:150]
    if len(body.strip()) > 150:
        preview += "..."

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f8fafc;">
<div style="max-width:600px;margin:0 auto;padding:24px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
    
    <div style="background:white;border-radius:12px;border:1px solid #e0e7ff;overflow:hidden;">
        <div style="background:#eef2ff;padding:16px 20px;border-bottom:1px solid #c7d2fe;">
            <p style="margin:0;font-size:18px;font-weight:700;color:#4f46e5;">&#128232; Request Received</p>
        </div>
        
        <div style="padding:20px;">
            <div style="margin-bottom:16px;">
                <p style="margin:0 0 4px 0;font-size:12px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;">Your Request</p>
                <p style="margin:0;color:#1e293b;font-size:14px;background:#f8fafc;padding:10px 12px;border-radius:6px;border:1px solid #e2e8f0;">{preview}</p>
            </div>
            
            <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:12px 16px;">
                <p style="margin:0;color:#0369a1;font-size:14px;">&#9881;&#65039; Agent is working on it...</p>
            </div>
        </div>
    </div>
    
    <p style="margin:16px 0 0 0;text-align:center;font-size:12px;color:#94a3b8;">You'll receive a completion notification shortly</p>
    
</div>
</body>
</html>"""

    from email_notifier import send_email
    return send_email(
        "Re: sched",
        html, config,
        in_reply_to=request_mid, references=request_mid,
    )


def extract_text_from_output(output):
    """Extract human-readable text from opencode JSON output."""
    if not output:
        return ""
    texts = []
    for line in output.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            ev = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if ev.get("type") == "text":
            part = ev.get("part", {})
            text = part.get("text", "").strip()
            if text:
                texts.append(text)
    return "\n\n".join(texts)


def send_completion(config, request_mid, body, result, extra, output, created=None):
    created = created or []
    if result == "OK":
        color = "#16a34a"
        bg = "#f0fdf4"
        border = "#bbf7d0"
        icon = "&#9989;"
        label = "Task Completed"
    elif result == "FAILED":
        color = "#dc2626"
        bg = "#fef2f2"
        border = "#fecaca"
        icon = "&#10060;"
        label = "Task Failed"
    elif result == "NEEDS_INPUT":
        color = "#d97706"
        bg = "#fffbeb"
        border = "#fde68a"
        icon = "&#10067;"
        label = "Needs Your Input"
    else:
        color = "#6b7280"
        bg = "#f9fafb"
        border = "#e5e7eb"
        icon = "&#9888;&#65039;"
        label = "Unknown Status"

    request_preview = body.strip().replace("\n", " ")[:150]
    if len(body.strip()) > 150:
        request_preview += "..."

    question_html = ""
    if result == "NEEDS_INPUT" and extra:
        q = extra.replace("<", "&lt;").replace(">", "&gt;")
        question_html = (
            f"<div style='margin:16px 0;padding:16px;background:#fffbeb;border:1px solid #fde68a;border-radius:8px;'>"
            f"<p style='margin:0 0 8px 0;font-weight:600;color:#92400e;font-size:14px;'>Agent Question:</p>"
            f"<p style='margin:0;color:#78350f;font-size:15px;'>{q}</p>"
            f"</div>"
        )

    jobs_html = ""
    if created:
        items = "".join(
            f"<li style='margin:2px 0;color:#1e293b;font-size:13px;'><code>{c.replace('<','&lt;').replace('>','&gt;')}</code></li>"
            for c in created
        )
        jobs_html = (
            f"<div style='margin:16px 0;padding:14px 16px;background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;'>"
            f"<p style='margin:0 0 6px 0;font-weight:600;color:#0369a1;font-size:13px;'>Queued job(s):</p>"
            f"<ul style='margin:0;padding-left:18px;'>{items}</ul>"
            f"<p style='margin:8px 0 0 0;color:#0369a1;font-size:12px;'>The result will be emailed to you automatically when it finishes.</p>"
            f"</div>"
        )

    readable_output = extract_text_from_output(output)
    out_text = readable_output.strip() if readable_output.strip() else ""
    # Never send a useless "(no output)" — if the agent said nothing useful,
    # explain what was actually queued instead.
    if not out_text:
        if created:
            out_text = ("I've queued the following job(s) for you:\n- "
                        + "\n- ".join(created)
                        + "\n\nYou'll receive the result by email when it finishes.")
        else:
            out_text = ("Your request was received and handed off for processing. "
                        "You'll receive the result by email when it's ready.")
    out_escaped = out_text.replace("<", "&lt;").replace(">", "&gt;")

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f8fafc;">
<div style="max-width:600px;margin:0 auto;padding:24px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
    
    <div style="background:white;border-radius:12px;border:1px solid {border};overflow:hidden;">
        <div style="background:{bg};padding:16px 20px;border-bottom:1px solid {border};">
            <p style="margin:0;font-size:18px;font-weight:700;color:{color};">{icon} {label}</p>
        </div>
        
        <div style="padding:20px;">
            <div style="margin-bottom:16px;">
                <p style="margin:0 0 4px 0;font-size:12px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;">Request</p>
                <p style="margin:0;color:#1e293b;font-size:14px;background:#f1f5f9;padding:10px 12px;border-radius:6px;">{request_preview}</p>
            </div>
            
                {question_html}
                {jobs_html}
            
            <div>
                <p style="margin:0 0 8px 0;font-size:12px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;">Agent Response</p>
                <pre style="margin:0;background:#1e293b;color:#e2e8f0;padding:16px;border-radius:8px;font-size:13px;line-height:1.5;white-space:pre-wrap;max-height:350px;overflow:auto;font-family:'SF Mono',Monaco,Consolas,monospace;">{out_escaped}</pre>
            </div>
        </div>
    </div>
    
    <p style="margin:16px 0 0 0;text-align:center;font-size:12px;color:#94a3b8;">OpenCode Scheduler</p>
    
</div>
</body>
</html>"""

    from email_notifier import send_email
    return send_email(
        "Re: sched",
        html, config,
        in_reply_to=request_mid, references=request_mid,
    )


def send_expiry(config, request_mid, body):
    html = """<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f8fafc;">
<div style="max-width:600px;margin:0 auto;padding:24px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
    
    <div style="background:white;border-radius:12px;border:1px solid #fecaca;overflow:hidden;">
        <div style="background:#fef2f2;padding:16px 20px;border-bottom:1px solid #fecaca;">
            <p style="margin:0;font-size:18px;font-weight:700;color:#dc2626;">&#9200; Request Expired</p>
        </div>
        
        <div style="padding:20px;">
            <p style="margin:0;color:#1e293b;font-size:14px;">This request has expired after 24 hours without a response.</p>
            <p style="margin:12px 0 0 0;color:#64748b;font-size:13px;">Please send a new request if you'd like to try again.</p>
        </div>
    </div>
    
</div>
</body>
</html>"""
    from email_notifier import send_email
    return send_email(
        "Re: sched",
        html, config,
        in_reply_to=request_mid, references=request_mid,
    )


def send_event_log_reply(config, subject, html_body):
    root_id = ensure_thread_root(get_mail_db(), config)
    from email_notifier import send_email
    return send_email(subject, html_body, config, in_reply_to=root_id, references=root_id)


def process_message(conn, config, msg, message_id, subject, sender_email, uid):
    mail_cfg = config.get("mail_inbox", {})
    allowed = [s.lower() for s in mail_cfg.get("allowed_senders", [])]

    if allowed and sender_email not in allowed:
        log_event("email_cmd_rejected", message_id=message_id, reason="sender_not_allowed")
        return "rejected_sender"

    body = extract_body(msg)
    if not body.strip():
        log_event("email_cmd_rejected", message_id=message_id, reason="empty_body")
        return "rejected_empty"

    log_event("email_cmd_received", message_id=message_id, body_len=len(body))

    existing = get_pending_request(conn)
    if existing:
        send_ack(config, message_id, body)
        start_ts = time.time()
        code, output, log_path = run_agent(body, config, pending=existing)
        result, extra = parse_agent_result(output)
        created = parse_created_jobs(output)
        link_jobs_in_window(message_id, start_ts)
        for jn in created:
            link_email_to_job(message_id, jn)
        update_request_status(conn, existing["message_id"], "answered")
        send_completion(config, message_id, body, result, extra, output, created)
        mark_processed(conn, message_id, uid, subject, "agent_followup", result)
        log_event("email_cmd_done", message_id=message_id, result=result, followup=True)
        return f"done:{result}"

    create_request(conn, message_id, subject, body)
    send_ack(config, message_id, body)

    start_ts = time.time()
    code, output, log_path = run_agent(body, config)
    result, extra = parse_agent_result(output)
    created = parse_created_jobs(output)
    link_jobs_in_window(message_id, start_ts)
    for jn in created:
        link_email_to_job(message_id, jn)

    if result == "NEEDS_INPUT":
        update_request_status(conn, message_id, "needs_input")
        conn.execute(
            "UPDATE requests SET body=body || ? WHERE message_id=?",
            (f"\n\nAGENT_QUESTION: {extra}", message_id),
        )
        conn.commit()
    elif result in ("OK", "FAILED", "UNKNOWN"):
        update_request_status(conn, message_id, "done")

    send_completion(config, message_id, body, result, extra, output, created)
    mark_processed(conn, message_id, uid, subject, "agent_invoked", result)
    log_event("email_cmd_done", message_id=message_id, result=result,
              created_jobs=created)
    return f"done:{result}"


def mark_processed(conn, message_id, uid, subject, action, result):
    conn.execute(
        "INSERT OR REPLACE INTO processed_messages (message_id, uid, subject, received_at, action_taken, result) VALUES (?,?,?,?,?,?)",
        (message_id, uid, subject, time.time(), action, result),
    )
    conn.commit()


def poll_once(config):
    mail_cfg = config.get("mail_inbox", {})
    imap_host = config.get("imap_host", "imap.gmail.com")
    smtp_user = config["smtp_user"]
    password = config["smtp_password"]
    max_uid = mail_cfg.get("last_uid", 0)

    conn = get_mail_db()
    last_uid = get_last_uid(conn)

    processed_count = 0
    try:
        mail = imaplib.IMAP4_SSL(imap_host)
        mail.login(smtp_user, password)
        mail.select("INBOX")

        # UID search: get all messages with UID > last_uid
        # On first run (last_uid=0), only fetch the most recent 20 messages
        if last_uid == 0:
            status, data = mail.uid("search", None, "ALL")
            if status == "OK" and data[0]:
                all_uids = [int(u) for u in data[0].split()]
                if len(all_uids) > 20:
                    last_uid = max(all_uids) - 20
                    set_last_uid(conn, last_uid)
            status, data = mail.uid("search", None, f"UID {last_uid + 1}:*")
        else:
            status, data = mail.uid("search", None, f"UID {last_uid + 1}:*")
        if status != "OK" or not data[0]:
            mail.logout()
            conn.close()
            return 0

        uid_list = data[0].split()
        if not uid_list:
            mail.logout()
            conn.close()
            return 0

        new_uids = []
        for uid_bytes in uid_list:
            uid_int = int(uid_bytes)
            if uid_int > last_uid:
                new_uids.append((uid_bytes, uid_int))

        highest_uid = last_uid

        for uid_bytes, uid_int in new_uids:
            status, msg_data = mail.uid("fetch", uid_bytes, "(RFC822)")
            if status != "OK" or not msg_data or not msg_data[0]:
                continue

            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            subject = decode_header_value(msg.get("Subject", "")).strip()
            sender = decode_header_value(msg.get("From", "")).strip()
            message_id = msg.get("Message-ID", "").strip()

            sender_match = re.search(r"<([^>]+)>", sender)
            sender_email = sender_match.group(1).lower() if sender_match else sender.lower()

            # STRICT: subject must be exactly "sched" (case-insensitive)
            # "Re: sched" etc. are replies from the scheduler - skip them
            if subject.lower() == "sched" and sender_email == smtp_user.lower():
                if not message_id:
                    continue
                already = conn.execute(
                    "SELECT 1 FROM processed_messages WHERE message_id=?", (message_id,)
                ).fetchone()
                if already:
                    log_event("email_cmd_dedup", message_id=message_id)
                    if uid_int > highest_uid:
                        highest_uid = uid_int
                    continue

                try:
                    action = process_message(conn, config, msg, message_id, subject, sender_email, uid_int)
                    processed_count += 1
                    log_event("email_scanned", uid=uid_int, action=action)
                except Exception as e:
                    log_event("email_cmd_error", message_id=message_id, error=str(e))
                    traceback.print_exc()
                    # Mark as processed to avoid re-processing loops
                    mark_processed(conn, message_id, uid_int, subject, "error", str(e)[:200])

            if uid_int > highest_uid:
                highest_uid = uid_int

        if highest_uid > last_uid:
            set_last_uid(conn, highest_uid)

        mail.logout()
    except Exception as e:
        log_event("mail_inbox_error", error=str(e))
        traceback.print_exc()
    finally:
        conn.close()

    return processed_count


def sweep_expiry(config):
    conn = get_mail_db()
    expired = expire_old_requests(conn, hours=24)
    for row in expired:
        send_expiry(config, row["message_id"], row["body"])
        log_event("request_expired", message_id=row["message_id"])
    conn.close()
    return len(expired)


def main():
    LOG_DIR.mkdir(exist_ok=True)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="poll once and exit")
    parser.add_argument("--poll", type=int, default=120, help="poll interval in seconds (default 120)")
    parser.add_argument("--test", action="store_true", help="send a test sched email")
    args = parser.parse_args()

    config = load_config()
    if not config.get("enabled"):
        sys.exit("email notifications disabled")

    mail_cfg = config.get("mail_inbox", {})
    if not mail_cfg.get("enabled", True):
        sys.exit("mail_inbox disabled in config")

    if args.test:
        from email_notifier import send_email
        ok = send_email("sched", "list all my scheduled jobs", config)
        print("test email sent" if ok else "failed")
        return

    poll_interval = args.poll or mail_cfg.get("poll_seconds", 120)

    if args.once:
        sweep_expiry(config)
        n = poll_once(config)
        print(f"processed {n} message(s)")
        return

    log_event("mail_inbox_started", poll=poll_interval)
    print(f"mail_inbox daemon started (poll={poll_interval}s)")
    while True:
        try:
            sweep_expiry(config)
            n = poll_once(config)
            if n:
                print(f"[{datetime.now():%H:%M:%S}] processed {n} message(s)")
        except Exception as e:
            print(f"poll error: {e}", file=sys.stderr)
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
