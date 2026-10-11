#!/usr/bin/env python3
"""opencode task scheduler.

Schedules prompts to be run headlessly via `opencode run`, records results
(session id, output, exit code) in SQLite, optionally runs a verifier task
and always generates an agent quality report after each run. Everything is
journalled to logs/activity.jsonl.
"""

import argparse
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from email_notifier import send_email, format_run_email, load_config, format_user_result_email
from mail_inbox import get_mail_db, get_email_link_for_job, mark_email_result_sent

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "scheduler.db"
LOG_DIR = BASE_DIR / "logs"
ACTIVITY_LOG = LOG_DIR / "activity.jsonl"
JOURNAL_MD = LOG_DIR / "journal.md"

# When more jobs are due than the worker pool can run at once, overflow jobs are
# pushed back by this many seconds (instead of all firing on the same poll) so
# at most `workers` tasks run simultaneously and the rest are spread out.
STAGGER_SECONDS = 30

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    prompt TEXT NOT NULL,
    dir TEXT NOT NULL DEFAULT '.',
    schedule TEXT NOT NULL,
    agent TEXT,
    model TEXT,
    auto_approve INTEGER NOT NULL DEFAULT 0,
    check_prompt TEXT,
    report_enabled INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_run_at REAL,
    next_run_at REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    started_at REAL NOT NULL,
    finished_at REAL,
    duration_s REAL,
    exit_code INTEGER,
    session_id TEXT,
    output_text TEXT,
    log_path TEXT,
    check_session_id TEXT,
    check_output TEXT,
    check_passed INTEGER,
    report_session_id TEXT,
    report_text TEXT,
    report_score INTEGER,
    status TEXT NOT NULL DEFAULT 'running'
);
"""


def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_schema():
    """Create tables if needed and migrate existing DB (add columns)."""
    conn = db()
    conn.executescript(SCHEMA)
    # migration: add timeout_s if missing
    try:
        conn.execute("SELECT timeout_s FROM jobs LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE jobs ADD COLUMN timeout_s INTEGER NOT NULL DEFAULT 600")
    conn.commit()
    conn.close()


def log_event(event, **data):
    """Append one JSONL line to the activity log and mirror it into journal.md."""
    LOG_DIR.mkdir(exist_ok=True)
    entry = {"ts": time.time(), "time": datetime.now().isoformat(timespec="seconds"),
             "event": event, **data}
    with open(ACTIVITY_LOG, "a") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    with open(JOURNAL_MD, "a") as f:
        f.write(f"- `{entry['time']}` **{event}** " +
                " ".join(f"{k}={_short(v)}" for k, v in data.items() if v is not None) + "\n")


def _short(v):
    s = str(v).replace("\n", "\\n")
    return s if len(s) <= 120 else s[:117] + "..."


# ---------------- cron parsing (5-field) ----------------

FIELD_SPECS = [("minute", 0, 59), ("hour", 0, 23), ("dom", 1, 31),
               ("month", 1, 12), ("dow", 0, 6)]  # dow: 0=Sunday

ALIASES = {
    "@hourly": "0 * * * *", "@daily": "0 0 * * *", "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0", "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *",
}


def parse_field(expr, lo, hi):
    values = set()
    for part in expr.split(","):
        part = part.strip()
        step = 1
        if "/" in part:
            part, step_s = part.split("/", 1)
            step = int(step_s)
        if part == "*" or part == "":
            start, end = lo, hi
        elif "-" in part:
            a, b = part.split("-", 1)
            start, end = int(a), int(b)
        else:
            start = end = int(part)
            if "/" in expr:
                end = hi
        for v in range(start, end + 1, step):
            values.add(v)
    if not values or min(values) < lo or max(values) > hi:
        raise ValueError(f"field out of range [{lo}-{hi}]: {expr}")
    return values


def parse_schedule(schedule):
    """Return ('interval', seconds) or ('cron', field-dict) or ('once', None)."""
    schedule = schedule.strip()
    if schedule == "@once":
        return ("once", None)
    m = re.match(r"(?:@every|every)\s+(\d+)\s*([smhd])$", schedule)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if n < 1:
            raise ValueError("interval must be >= 1")
        return ("interval", n * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit])
    if schedule.startswith("@every") or schedule.startswith("every"):
        raise ValueError(f"bad interval (use like '@every 30m'): {schedule}")
    if schedule in ALIASES:
        schedule = ALIASES[schedule]
    parts = schedule.split()
    if len(parts) != 5:
        raise ValueError(f"schedule must be cron 'm h dom mon dow', '@every N<unit>' or '@once': {schedule}")
    fields = {}
    for (name, lo, hi), expr in zip(FIELD_SPECS, parts):
        vals = parse_field(expr, lo, hi)
        if name == "dow" and 7 in vals:
            vals |= {0}
        fields[name] = vals
    return ("cron", fields)


def next_cron_time(fields, after=None):
    candidate = (after or datetime.now()).replace(second=0, microsecond=0) + timedelta(minutes=1)
    # python weekday(): Monday=0..Sunday=6 ; our dow: Sunday=0..Saturday=6 -> py = (dow+6)%7
    for _ in range(366 * 24 * 60):
        py_dow = (candidate.weekday() + 1) % 7
        if (
            candidate.minute in fields["minute"]
            and candidate.hour in fields["hour"]
            and candidate.day in fields["dom"]
            and candidate.month in fields["month"]
            and py_dow in fields["dow"]
        ):
            return candidate.timestamp()
        candidate += timedelta(minutes=1)
    raise ValueError("no matching time within a year")


def compute_next_run(job_row):
    kind, data = parse_schedule(job_row["schedule"])
    base = max(job_row["last_run_at"] or 0, time.time())
    if kind == "once":
        return None  # never again; caller decides what to do
    if kind == "interval":
        ref = job_row["last_run_at"] or time.time()
        t = ref + data
        while t < time.time():
            t += data
        return t
    t = next_cron_time(data, datetime.fromtimestamp(base))
    while t <= base:
        t = next_cron_time(data, datetime.fromtimestamp(t))
    return t


# ---------------- opencode execution ----------------

def build_cmd(message, job, title):
    # opencode run (v2.x) has NO --dir flag. The working directory is selected
    # with a LEADING POSITIONAL:  `opencode run [flags] [<dir>] <message>`
    #   - no dir   -> the single positional is the message
    #   - with dir -> the first positional is the directory, the rest the message
    # (Verified empirically: a lone positional is the message; with a directory
    # the directory must come first. opencode otherwise defaults to $HOME and
    # ignores the subprocess cwd.)
    cmd = ["opencode", "run", "--title", title, "--format", "json"]
    if job["model"]:
        cmd += ["-m", job["model"]]
    if job["auto_approve"]:
        cmd += ["--auto"]
    run_dir = job["dir"] if (job["dir"] and job["dir"] != ".") else None
    if run_dir:
        cmd.append(run_dir)
    cmd.append(message)
    return cmd


def compose_message(job):
    """Build the final user message for a job's MAIN task.

    Custom agents (`.opencode/agents/<name>.md`) are not reliably resolved by
    name from a non-interactive daemon (the `Agent not found: "<name>"` failure
    — name lookup depends on the interactive session's project context). So we
    run the DEFAULT agent and have it read + follow the named agent's protocol
    file, keeping that file the single source of truth.
    """
    prompt = job["prompt"]
    agent = job["agent"]
    if not agent:
        return prompt
    base = job["dir"] if (job["dir"] and job["dir"] != ".") else "."
    agent_path = f"{base}/.opencode/agents/{agent}.md"
    return (
        f"You are operating as the '{agent}' agent for this run.\n"
        f"FIRST read and follow the protocol in {agent_path} exactly.\n\n"
        f"{prompt}"
    )


UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
SES_RE = re.compile(r"ses_[A-Za-z0-9]+")


def opencode_exe():
    """Path to the opencode CLI (used for server-side session control)."""
    return shutil.which("opencode") or os.path.expanduser("~/.opencode/bin/opencode")


def interrupt_session(session_id, timeout=30):
    """Best-effort abort of a server-side agent session via `opencode api`.

    Killing the `opencode run` client does NOT stop the agent: the session
    keeps executing inside `opencode serve`. Returns True if the server
    confirms it interrupted a live execution.
    """
    if not session_id:
        return False
    try:
        p = subprocess.run(
            [opencode_exe(), "api", "session.interrupt", "--param", f"sessionID={session_id}"],
            capture_output=True, text=True, timeout=timeout)
        if p.returncode != 0:
            return False
        try:
            return bool(json.loads(p.stdout.strip()).get("interrupted"))
        except Exception:
            return False
    except Exception:
        return False


def run_opencode(cmd, log_path, timeout=None, cwd=None):
    """Run opencode headlessly. Returns (exit_code, session_id|None, text_output).

    cwd: directory the opencode process is launched in. The directory opencode
    actually operates in is selected by the leading directory positional that
    build_cmd emits (opencode run has no --dir flag); cwd is passed too, for
    consistency and config discovery.
    """
    if timeout is None:
        timeout = 600  # default 10 min
    with open(log_path, "w") as logf:
        logf.write(f"$ {' '.join(cmd)}\n\n")
        logf.flush()
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=logf, text=True, cwd=cwd,
                                      start_new_session=True)
            # Drain stdout on a reader thread so the live session ID is known
            # BEFORE the run ends — it is needed to abort the server-side
            # session on timeout (below). `communicate()` only yields output
            # at the very end, which is too late for a run that must be killed.
            chunks = []
            live = {}

            def _drain():
                try:
                    for line in iter(proc.stdout.readline, ""):
                        chunks.append(line)
                        if "sid" not in live:
                            m = SES_RE.search(line)
                            if m:
                                live["sid"] = m.group(0)
                except Exception:
                    pass

            drainer = threading.Thread(target=_drain, daemon=True)
            drainer.start()
            keep_partial = True
            try:
                proc.wait(timeout=timeout)
                exit_code = proc.returncode
            except subprocess.TimeoutExpired:
                # Kill the whole process GROUP (not just the wrapper): timed-out
                # `opencode run` wrappers otherwise leave orphaned children with
                # no DB run tracking them.
                # CRITICAL: that only kills the CLIENT. The agent session keeps
                # running server-side inside `opencode serve`, so abort it via
                # the API too — otherwise the "timed out" job keeps working
                # unsupervised for hours (observed 2026-10-11: timed-out runs
                # still active 1-4h after their timeout).
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                except Exception:
                    pass
                sid = live.get("sid")
                if sid:
                    ok = interrupt_session(sid)
                    logf.write(f"\n[timeout] server-side session interrupt {sid}: {'ok' if ok else 'FAILED'}\n")
                    logf.flush()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    except Exception:
                        pass
                    try:
                        proc.wait(timeout=10)
                    except Exception:
                        pass
                exit_code = -1
            except Exception:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                if live.get("sid"):
                    interrupt_session(live["sid"])
                keep_partial = False
                stdout = ""
                exit_code = -2
            try:
                proc.stdout.close()
            except Exception:
                pass
            drainer.join(timeout=10)
            if keep_partial:
                stdout = "".join(chunks)
        except Exception as e:
            # failed to start process
            stdout = ""
            exit_code = -2
    session_id = None
    text_parts = []

    # try structured json events first (--format json style lines)
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            # plain (non-JSON) stdout is still real output — keep it
            if line:
                text_parts.append(line)
            continue
        try:
            ev = json.loads(line)
        except Exception:
            continue
        props = ev.get("properties", {})
        sid = ev.get("sessionID") or props.get("sessionID")
        if not sid and isinstance(props.get("info"), dict):
            sid = props["info"].get("id") or props["info"].get("sessionID")
        if sid:
            session_id = sid
        # capture assistant text replies
        part = props.get("part") or ev.get("part") or {}
        if isinstance(part, dict) and part.get("type") == "text" and part.get("text", "").strip():
            text_parts.append(part["text"].strip())
        # capture tool results (e.g. a command's stdout) so command output
        # isn't lost when the agent doesn't echo it back as text
        if isinstance(part, dict) and part.get("type") == "tool":
            out = part.get("state", {}).get("output") or part.get("output")
            if isinstance(out, str) and out.strip():
                text_parts.append(out.strip())

    text = "\n\n".join(text_parts).strip() or stdout.strip()

    if not session_id:
        m = UUID_RE.search(stdout)
        if m:
            session_id = m.group(0)
    with open(log_path, "a") as logf:
        logf.write(stdout)
    return exit_code, session_id, text


CHECK_RE = re.compile(r"CHECK:\s*(PASS|FAIL)", re.IGNORECASE)
SCORE_RE = re.compile(r"SCORE:\s*(\d{1,2})(?:\s*/\s*10)?", re.IGNORECASE)

REPORT_TEMPLATE = (
    "You are an automated QA reporter for a scheduled agent system.\n\n"
    "TASK PROMPT:\n{prompt}\n\n"
    "TASK OUTPUT:\n{output}\n\n"
    "{check_section}"
    "Write a concise quality report of what was done, when, and how well:\n"
    "- SUMMARY: 1-3 sentences on what was accomplished.\n"
    "- QUALITY: assessment of correctness/completeness.\n"
    "- ISSUES: anything wrong, missing or risky (say 'none' if clean).\n\n"
    "End with exactly two machine-readable lines:\n"
    "SCORE: <0-10>/10 overall quality\n"
    "VERDICT: SUCCESS | PARTIAL | FAILURE"
)

VERDICT_RE = re.compile(r"VERDICT:\s*(SUCCESS|PARTIAL|FAILURE)", re.IGNORECASE)


def generate_report(conn, job, run_row, ts):
    """Third opencode call: agent reviews the finished task and scores it."""
    check_section = ""
    if run_row["check_passed"] is not None:
        check_section = (
            f"A verifier already judged it: {'PASS' if run_row['check_passed'] else 'FAIL'}.\n"
            f"VERIFIER OUTPUT:\n{run_row['check_output']}\n\n"
        )
    prompt = REPORT_TEMPLATE.format(
        prompt=job["prompt"], output=run_row["output_text"] or "(no output)",
        check_section=check_section,
    )
    fake_job = job  # same agent/model/dir settings
    run_cwd = job["dir"] if (job["dir"] and job["dir"] != ".") else None
    log_path = str(LOG_DIR / f"{job['name']}-{ts}-report.log")
    report_timeout = max(job["timeout_s"], 120)  # at least 2 min for QA
    code, session_id, text = run_opencode(
        build_cmd(prompt, fake_job, f"{job['name']}-{ts}-report"), log_path,
        timeout=report_timeout, cwd=run_cwd
    )
    score_m = SCORE_RE.search(text or "")
    verdict_m = VERDICT_RE.search(text or "")
    score = int(score_m.group(1)) if score_m else None
    verdict = verdict_m.group(1).upper() if verdict_m else None
    conn.execute(
        "UPDATE runs SET report_session_id=?, report_text=?, report_score=?, status=? WHERE id=?",
        (session_id, text[-30000:], score, verdict or "UNKNOWN", run_row["id"]),
    )
    conn.commit()
    return score, verdict, text


# ---------------- pipeline ----------------

def execute_job(job_id):
    conn = db()
    try:
        _execute_job_inner(conn, job_id)
    except Exception as e:
        log_event("job_error", job_id=job_id, error=str(e))
        traceback.print_exc()
        conn.execute("UPDATE jobs SET status='pending' WHERE id=?", (job_id,))
        conn.commit()
    finally:
        conn.close()


def _execute_job_inner(conn, job_id):
    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not job:
        return
    started = time.time()
    cur = conn.execute(
        "INSERT INTO runs (job_id, started_at, status) VALUES (?, ?, 'running')",
        (job_id, started),
    )
    run_id = cur.lastrowid
    # Release the write lock NOW so other jobs are not blocked while the
    # (potentially long) opencode subprocess runs. Without this commit the
    # transaction stays open for the whole subprocess duration and every other
    # job's status write fails with "database is locked".
    conn.commit()
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_event("run_start", job=job["name"], run=run_id)

    run_cwd = job["dir"] if (job["dir"] and job["dir"] != ".") else None
    log_path = str(LOG_DIR / f"{job['name']}-{ts}.log")
    code, session_id, output = run_opencode(
        build_cmd(compose_message(job), job, f"{job['name']}-{ts}"), log_path,
        timeout=job["timeout_s"], cwd=run_cwd
    )
    log_event("task_finished", job=job["name"], run=run_id, exit_code=code,
              session=session_id, duration_s=round(time.time() - started, 1))

    # --- optional verifier task ---
    check_session = check_output = None
    check_passed = None
    if job["check_prompt"] and code == 0:
        full_prompt = (
            "You are verifying the result of an automated task.\n\n"
            f"TASK PROMPT:\n{job['prompt']}\n\n"
            f"TASK OUTPUT:\n{output}\n\n"
            f"VERIFICATION INSTRUCTIONS:\n{job['check_prompt']}\n\n"
            "You may inspect files/commands yourself to verify. "
            "End your reply with exactly one line: 'CHECK: PASS' or 'CHECK: FAIL'."
        )
        c_log = str(LOG_DIR / f"{job['name']}-{ts}-check.log")
        check_timeout = max(job["timeout_s"], 120)  # at least 2 min for verification
        _, check_session, check_output = run_opencode(
            build_cmd(full_prompt, job, f"{job['name']}-{ts}-check"), c_log,
            timeout=check_timeout, cwd=run_cwd
        )
        m = CHECK_RE.search(check_output or "")
        check_passed = 1 if (m and m.group(1).upper() == "PASS") else 0
        log_event("check_finished", job=job["name"], run=run_id,
                  passed=bool(check_passed))

    finished = time.time()
    conn.execute(
        """UPDATE runs SET finished_at=?, duration_s=?, exit_code=?, session_id=?,
           output_text=?, log_path=?, check_session_id=?, check_output=?, check_passed=?
           WHERE id=?""",
        (finished, round(finished - started, 1), code, session_id,
         output[-50000:], log_path, check_session,
         (check_output or "")[-20000:] if check_output else None,
         check_passed, run_id),
    )
    conn.commit()

    # --- deliver the REAL result to the user's email thread (ASAP) ---
    # If this job was spawned from an email request, reply directly to that
    # thread with the actual task output (not just a QA report), so the user
    # reliably receives the deliverable without waiting for the QA pass below.
    try:
        email_cfg = load_config()
        if email_cfg and email_cfg.get("enabled"):
            origin_mid = get_email_link_for_job(job["name"])
            if origin_mid:
                user_html = format_user_result_email(
                    job_name=job["name"], run_id=run_id,
                    exit_code=code, duration_s=round(finished - started, 1),
                    output_tail=(output or "").strip(), report_text=None,
                    score=None, verdict=None, check_passed=check_passed,
                )
                user_sent = send_email(
                    "Re: sched", user_html, email_cfg,
                    in_reply_to=origin_mid, references=origin_mid,
                )
                if user_sent:
                    mark_email_result_sent(origin_mid, job["name"])
                log_event("user_email_sent", job=job["name"], run=run_id, ok=user_sent)
    except Exception as e:
        log_event("user_email_error", job=job["name"], run=run_id, error=str(e))

    # --- finalize run + job status NOW (before any QA email) so a slow/hanging
    #     SMTP send can never strand the job in 'running' and trigger the
    #     self_heal re-dispatch loop. ---
    final = _run_final_status({"exit_code": code, "check_passed": check_passed})
    conn.execute("UPDATE runs SET status=? WHERE id=?", (final, run_id))
    nxt = compute_next_run(dict(job, last_run_at=finished))
    if nxt is None:
        conn.execute(
            "UPDATE jobs SET last_run_at=?, next_run_at=?, enabled=0, status='done' WHERE id=?",
            (finished, finished, job_id),
        )
        log_event("job_completed_once", job=job["name"])
    else:
        conn.execute(
            "UPDATE jobs SET last_run_at=?, next_run_at=?, status='pending' WHERE id=?",
            (finished, nxt, job_id),
        )
    conn.commit()
    print(f"[{datetime.now():%H:%M:%S}] '{job['name']}' exit={code} -> "
          f"{'done' if nxt is None else datetime.fromtimestamp(nxt).strftime('%Y-%m-%d %H:%M')}")

    # --- QA email + agent report run off the critical path in a detached
    #     thread; they must NOT finalize job/run status (done above). ---
    def _send_mail():
        try:
            email_cfg = load_config()
            if email_cfg and email_cfg.get("enabled"):
                _send_event_email(email_cfg, job, run_id, code, started, finished,
                                  output, check_passed)
        except Exception as e:
            log_event("email_error", job=job["name"], run=run_id, error=str(e))
    threading.Thread(target=_send_mail, daemon=True).start()


def _send_event_email(email_cfg, job, run_id, code, started, finished,
                      output, check_passed):
    """Build and send the operational QA email + agent quality report.

    Runs in a detached thread: must NOT touch the caller's DB connection or
    job/run status (those are finalized by the caller in the main thread).
    Opens its own DB connection for the optional report generation.
    """
    out_tail = (output or "").strip()
    # optional agent quality report (own connection; may take a few minutes)
    score = verdict = None
    report = None
    if job["report_enabled"]:
        try:
            c = db()
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            run_row = c.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            score, verdict, report = generate_report(c, job, run_row, ts)
            c.close()
            log_event("report_generated", job=job["name"], run=run_id,
                      score=score, verdict=verdict)
        except Exception as e:
            log_event("report_error", job=job["name"], run=run_id, error=str(e))
    rpt = (report or "") if job["report_enabled"] else None
    if code == 0 and (score is None or score >= 7):
        label = "SUCCESS"
    elif code == -1:
        label = "TIMEOUT"
    elif code == 0:
        label = "DONE"
    else:
        label = "FAILED"
    subject = f"[Scheduler] {job['name']} — {label}"
    if score is not None:
        subject += f" (score {score}/10)"
    html = format_run_email(
        job_name=job["name"], run_id=run_id, exit_code=code,
        duration_s=round(finished - started, 1), score=score,
        verdict=verdict, check_passed=check_passed,
        prompt=job["prompt"], output_tail=out_tail, report_text=rpt,
    )
    try:
        from mail_inbox import send_event_log_reply, ensure_thread_root
        ensure_thread_root(get_mail_db(), email_cfg)
        sent = send_event_log_reply(email_cfg, subject, html)
    except Exception:
        sent = send_email(subject, html, email_cfg)
    log_event("email_sent", job=job["name"], run=run_id, ok=sent)


class Runner:
    def __init__(self, workers):
        self.workers = workers
        self.threads = []
        self.submitted_at = {}  # job_id -> timestamp of last submit

    def busy(self):
        self.threads[:] = [t for t in self.threads if t.is_alive()]
        return len(self.threads)

    def submit(self, job_id):
        self.submitted_at[job_id] = time.time()
        # prune entries older than 10 min
        cutoff = time.time() - 600
        for jid in [j for j, ts in self.submitted_at.items() if ts < cutoff]:
            del self.submitted_at[jid]
        t = threading.Thread(target=execute_job, args=(job_id,), daemon=True)
        t.start()
        self.threads.append(t)

    def just_submitted(self, job_id, window_s=120):
        return time.time() - self.submitted_at.get(job_id, 0) < window_s


def _run_final_status(run):
    """Derive a final run status from exit_code / verifier result."""
    if run["check_passed"] is not None:
        return "SUCCESS" if run["check_passed"] else "FAILURE"
    ec = run["exit_code"]
    if ec == -1:
        return "TIMEOUT"
    if ec == 0:
        return "SUCCESS"
    return "FAILED"


def self_heal(conn, now, just_submitted=None):
    """Repair run/job records stranded by daemon crashes or mid-run kills.

    - runs that logged a finish (finished_at set) but are still status='running'
      get a real final status.
    - runs that never finished and are older than (timeout_s + grace) with no
      live worker are marked ABORTED.
    - jobs stranded in 'running' with no in-progress run are reset to 'pending'
      (or 'done' for @once jobs that already completed), so the daemon can pick
      them up again.
    - just_submitted(job_id) -> True skips jobs dispatched moments ago whose
      worker thread has not inserted its run row yet (prevents double-fire).
    """
    repairs = []
    # 1. finished-but-still-running runs -> real final status
    for r in conn.execute(
        "SELECT id, exit_code, check_passed FROM runs "
        "WHERE status='running' AND finished_at IS NOT NULL"
    ).fetchall():
        st = _run_final_status(r)
        conn.execute("UPDATE runs SET status=? WHERE id=?", (st, r["id"]))
        repairs.append(f"run#{r['id']} finish->{st}")
    # 2. orphan runs: running, never finished, past their timeout window
    for j in conn.execute("SELECT id, timeout_s FROM jobs").fetchall():
        grace = (j["timeout_s"] or 600) + 180
        for r in conn.execute(
            "SELECT id, started_at FROM runs "
            "WHERE job_id=? AND status='running' AND finished_at IS NULL",
            (j["id"],),
        ).fetchall():
            if (now - (r["started_at"] or now)) > grace:
                conn.execute(
                    "UPDATE runs SET status='ABORTED', finished_at=? "
                    "WHERE id=? AND status='running' AND finished_at IS NULL",
                    (now, r["id"]),
                )
                repairs.append(f"run#{r['id']} orphan->ABORTED")
    # 3. jobs stranded in 'running' with no live run -> reset
    for j in conn.execute(
        "SELECT id, name, schedule, timeout_s FROM jobs WHERE status='running' AND name NOT LIKE 'calc-%'"
    ).fetchall():
        if just_submitted and just_submitted(j["id"]):
            continue  # dispatched moments ago; worker thread still starting
        grace = (j["timeout_s"] or 600) + 180
        live = conn.execute(
            "SELECT 1 FROM runs WHERE job_id=? AND status='running' "
            "AND finished_at IS NULL AND (? - started_at) <= ? LIMIT 1",
            (j["id"], now, grace),
        ).fetchone()
        if live:
            continue
        if parse_schedule(j["schedule"])[0] == "once":
            done = conn.execute(
                "SELECT 1 FROM runs WHERE job_id=? AND status IN "
                "('SUCCESS','DONE','TIMEOUT','FAILURE') LIMIT 1",
                (j["id"],),
            ).fetchone()
            if done:
                conn.execute("UPDATE jobs SET status='done', enabled=0 WHERE id=?", (j["id"],))
            else:
                conn.execute("UPDATE jobs SET status='pending' WHERE id=?", (j["id"],))
        else:
            conn.execute(
                "UPDATE jobs SET status='pending', next_run_at=? WHERE id=?",
                (now, j["id"]),
            )
        repairs.append(f"job#{j['id']} {j['name']} -> pending")
    if repairs:
        conn.commit()
        log_event("self_heal", repairs=repairs)
    return repairs


def daemon_loop(poll, workers):
    LOG_DIR.mkdir(exist_ok=True)
    runner = Runner(workers)
    log_event("daemon_started", poll=poll, workers=workers)
    print(f"scheduler daemon started (poll={poll}s, workers={workers}) db={DB_PATH}")
    # self-heal at startup (catches leftovers from a previous crashed instance).
    try:
        c = db()
        self_heal(c, time.time())
        c.close()
    except Exception as e:
        print(f"self-heal error: {e}", file=sys.stderr)
    while True:
        try:
            runner.threads[:] = [t for t in runner.threads if t.is_alive()]
            conn = db()
            # heal FIRST so a job dispatched below is never reaped in the same
            # iteration before its worker thread inserts the run row.
            try:
                self_heal(conn, time.time(), runner.just_submitted)
            except Exception as e:
                print(f"self-heal error: {e}", file=sys.stderr)
            due = conn.execute(
                "SELECT id FROM jobs WHERE enabled=1 AND status='pending' AND next_run_at<=?"
                " AND name NOT LIKE 'calc-%'",
                (time.time(),),
            ).fetchall()
            for row in due:
                if runner.busy() >= workers:
                    # More than `workers` jobs already running: don't start this
                    # one now. Delay it by STAGGER_SECONDS so a burst of due jobs
                    # is spread out (waves of `workers`) instead of all firing at
                    # once. It stays 'pending' and is reconsidered next poll.
                    conn.execute(
                        "UPDATE jobs SET next_run_at=? WHERE id=? AND status='pending'",
                        (time.time() + STAGGER_SECONDS, row["id"]),
                    )
                    conn.commit()
                    continue
                cur = conn.execute(
                    "UPDATE jobs SET status='running' WHERE id=? AND status='pending'",
                    (row["id"],),
                )
                conn.commit()
                if cur.rowcount:
                    runner.submit(row["id"])
            conn.close()
        except Exception as e:
            print(f"loop error: {e}", file=sys.stderr)
        time.sleep(poll)


# ---------------- CLI ----------------

def get_job(conn, ident):
    return conn.execute("SELECT * FROM jobs WHERE id=? OR name=?", (ident, ident)).fetchone()


def cmd_add(args):
    if args.once:
        schedule = "@once"
    elif args.every:
        schedule = f"@every {args.every}"
    elif args.schedule:
        schedule = args.schedule
    else:
        sys.exit("error: need --schedule, --every or --once")
    parse_schedule(schedule)  # validate early
    nr = time.time() + 5
    conn = db()
    try:
        cur = conn.execute(
            """INSERT INTO jobs (name, prompt, dir, schedule, agent, model, auto_approve,
               check_prompt, report_enabled, timeout_s, next_run_at, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (args.name, args.prompt, args.dir, schedule, args.agent, args.model,
             1 if args.auto else 0, args.check_prompt,
             0 if args.no_report else 1, args.timeout, nr, time.time()),
        )
        conn.commit()
        log_event("job_added", job=args.name, schedule=schedule,
                  has_check=bool(args.check_prompt), report=not args.no_report)
        print(f"added job #{cur.lastrowid}: {args.name} [{schedule}]")
    except sqlite3.IntegrityError:
        sys.exit(f"error: job named '{args.name}' already exists")
    finally:
        conn.close()


def cmd_list(_):
    conn = db()
    rows = conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
    if not rows:
        print("no jobs")
    for r in rows:
        nxt = (datetime.fromtimestamp(r["next_run_at"]).strftime("%Y-%m-%d %H:%M")
               if r["next_run_at"] and r["enabled"] else "-")
        print(f"#{r['id']:>3} [{'on ' if r['enabled'] else 'off'}] {r['name']:<24} "
              f"{r['schedule']:<14} status={r['status']:<8} next={nxt}")
    conn.close()


def cmd_remove(args):
    conn = db()
    j = get_job(conn, args.job)
    if j:
        conn.execute("DELETE FROM runs WHERE job_id=?", (j["id"],))
        conn.execute("DELETE FROM jobs WHERE id=?", (j["id"],))
        conn.commit()
        log_event("job_removed", job=j["name"])
        print(f"removed job #{j['id']} '{j['name']}'")
    else:
        print("no such job")
    conn.close()


def cmd_toggle(args, enabled):
    conn = db()
    j = get_job(conn, args.job)
    if j:
        conn.execute("UPDATE jobs SET enabled=?, status='pending' WHERE id=?", (enabled, j["id"]))
        conn.commit()
        log_event("job_toggled", job=j["name"], enabled=enabled)
        print(f"{'enabled' if enabled else 'disabled'} '{j['name']}'")
    else:
        print("no such job")
    conn.close()


def cmd_status(args):
    conn = db()
    j = get_job(conn, args.job)
    if not j:
        sys.exit(f"no such job: {args.job}")
    print(f"Job #{j['id']} '{j['name']}' enabled={bool(j['enabled'])} status={j['status']} "
          f"schedule={j['schedule']} dir={j['dir']} agent={j['agent']} model={j['model']} auto={bool(j['auto_approve'])} timeout={j['timeout_s']}s")
    print(f"prompt: {j['prompt'][:200]}")
    if j["check_prompt"]:
        print(f"check: {j['check_prompt'][:200]}")
    print(f"reports: {'on' if j['report_enabled'] else 'off'}")
    runs = conn.execute("SELECT * FROM runs WHERE job_id=? ORDER BY id DESC LIMIT ?",
                        (j["id"], args.limit)).fetchall()
    for r in runs:
        st = datetime.fromtimestamp(r["started_at"]).strftime("%m-%d %H:%M:%S")
        ck = f" check={'PASS' if r['check_passed'] else 'FAIL'}" if r["check_passed"] is not None else ""
        sc = f" score={r['report_score']}/10 ({r['status']})" if r["report_score"] is not None else ""
        sess = r["session_id"] or "-"
        print(f"  run#{r['id']} @{st} dur={r['duration_s']}s exit={r['exit_code']} session={sess}{ck}{sc}")
        print(f"    log={r['log_path']}")
    conn.close()


def cmd_runs(args):
    conn = db()
    q = ("SELECT r.*, j.name AS jname FROM runs r JOIN jobs j ON j.id=r.job_id ")
    params = []
    if args.job:
        q += "WHERE r.job_id=(SELECT id FROM jobs WHERE id=? OR name=?) "
        params += [args.job, args.job]
    q += "ORDER BY r.id DESC LIMIT ?"
    params.append(args.limit)
    for r in conn.execute(q, params):
        st = datetime.fromtimestamp(r["started_at"]).strftime("%m-%d %H:%M:%S")
        ck = f" check={'P' if r['check_passed'] else 'F'}" if r["check_passed"] is not None else ""
        sc = f" score={r['report_score']}" if r["report_score"] is not None else ""
        print(f"run#{r['id']:>4} {r['jname']:<20} @{st} exit={r['exit_code']}{ck}{sc} {r['status']}")
        if args.verbose and r["report_text"]:
            for line in r["report_text"].strip().splitlines()[:8]:
                print(f"  | {line}")
    conn.close()


def cmd_journal(args):
    if args.clear:
        ACTIVITY_LOG.write_text("")
        JOURNAL_MD.write_text("# Scheduler Journal\n\n")
        print("journal cleared")
        return
    JOURNAL_MD.parent.mkdir(exist_ok=True)
    if not JOURNAL_MD.exists():
        print("(empty)")
        return
    lines = JOURNAL_MD.read_text().splitlines()
    print("\n".join(lines[-args.tail:]))


def cmd_notify(_):
    """Send a test email notification."""
    cfg = load_config()
    if not cfg:
        sys.exit("no email_config.json found")
    if not cfg.get("enabled"):
        sys.exit("email notifications disabled (enabled=false)")
    ok = send_email(
        "OpenCode Scheduler — Test",
        "<h2>Test notification</h2>"
        "<p>If you see this, email notifications are working.</p>"
        "<p>Successful task runs will automatically trigger emails like this.</p>",
        cfg,
    )
    print("sent" if ok else "failed")
    log_event("email_test", ok=ok)


def cmd_report(args):
    conn = db()
    r = conn.execute(
        "SELECT r.*, j.name AS jname FROM runs r JOIN jobs j ON j.id=r.job_id WHERE r.id=?",
        (args.run,),
    ).fetchone()
    if not r:
        sys.exit(f"no such run: {args.run}")
    print(f"=== Run #{r['id']} of '{r['jname']}' ===")
    print(f"started : {datetime.fromtimestamp(r['started_at'])}  duration: {r['duration_s']}s")
    print(f"exit    : {r['exit_code']}   session: {r['session_id']}")
    if r["check_passed"] is not None:
        print(f"check   : {'PASS' if r['check_passed'] else 'FAIL'} (verifier session {r['check_session_id']})")
    print(f"score   : {r['report_score']}/10  verdict: {r['status']}")
    print("--- agent report ---")
    print((r["report_text"] or "(none)").strip())
    print("--- task output tail ---")
    out = (r["output_text"] or "").strip().splitlines()
    print("\n".join(out[-15:]))
    conn.close()


def cmd_clear(args):
    """Remove all jobs and their runs."""
    if not args.yes:
        ans = input("Delete ALL jobs and runs? [y/N] ")
        if ans.lower() != "y":
            print("aborted")
            return
    conn = db()
    n_jobs = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    conn.execute("DELETE FROM runs")
    conn.execute("DELETE FROM jobs")
    conn.commit()
    conn.close()
    log_event("all_jobs_cleared", count=n_jobs)
    print(f"cleared {n_jobs} job(s) and all runs")


def cmd_daemon(args):
    daemon_loop(args.poll, args.workers)


def cmd_heal(args):
    conn = db()
    try:
        repairs = self_heal(conn, time.time())
    finally:
        conn.close()
    if repairs:
        print(f"repaired {len(repairs)} record(s):")
        for r in repairs:
            print(f"  - {r}")
    else:
        print("nothing to repair; all run/job records are consistent")


# ---------------- live agents / TUI ----------------

TITLE_RE = re.compile(r"^(?P<job>.+)-(?P<ts>\d{8}-\d{6})(?P<phase>-check|-report)?$")


def _iter_proc_cmdlines():
    """Yield (pid, argv) for local processes. Linux /proc only, stdlib only."""
    try:
        names = os.listdir("/proc")
    except FileNotFoundError:
        return
    for name in names:
        if not name.isdigit():
            continue
        pid = int(name)
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                raw = f.read()
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if not raw.strip(b"\x00"):
            continue
        argv = [p.decode("utf-8", "replace") for p in raw.rstrip(b"\x00").split(b"\x00")]
        yield pid, argv


def _proc_meta(pid):
    """RSS / state / start-time for one pid. Returns {} on failure."""
    meta = {}
    try:
        st = os.stat(f"/proc/{pid}")
        meta["start_ts"] = st.st_mtime
    except (FileNotFoundError, PermissionError):
        return {}
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    meta["mem_kb"] = int(line.split()[1])
                elif line.startswith("State:"):
                    meta["state"] = line.split(":")[1].strip().split()[0]
    except (FileNotFoundError, PermissionError, ValueError):
        pass
    return meta


def _ps_enrich(pids):
    """Best-effort ELAPSED/CPU/MEM via `ps`. Returns {pid: {...}}."""
    out = {}
    if not pids:
        return out
    try:
        r = subprocess.run(
            ["ps", "-o", "pid=,etime=,pcpu=,pmem=,args=", "-p", ",".join(map(str, pids))],
            capture_output=True, text=True, timeout=5,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return out
    if r.returncode != 0:
        return out
    for line in r.stdout.splitlines():
        parts = line.strip().split(None, 4)
        if len(parts) < 5:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        out[pid] = {"etime": parts[1], "pcpu": parts[2], "pmem": parts[3]}
    return out


def _resolve_live_log(job_name, started_at):
    """Running runs have log_path=NULL (written only at finish) — find the
    live log file by name pattern instead."""
    try:
        cands = [p for p in LOG_DIR.glob(f"{job_name}-*.log")
                 if not p.name.endswith(("-check.log", "-report.log"))]
    except OSError:
        return None
    if not cands:
        return None
    if started_at:
        # the run's own log started within seconds of started_at
        cands.sort(key=lambda p: abs(p.stat().st_mtime - started_at))
    else:
        cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return str(cands[0])


def _title_of(argv):
    if "--title" in argv:
        i = argv.index("--title")
        if i + 1 < len(argv):
            return argv[i + 1]
    return None


def live_agent_snapshot(now=None):
    """Combine DB running-runs with live `opencode run` OS processes.

    Returns a dict with daemon info, per-agent rows, orphans and counts.
    """
    now = now or time.time()
    conn = db()
    try:
        jobs = {r["id"]: dict(r) for r in conn.execute("SELECT * FROM jobs")}
        running_runs = [
            dict(r) for r in conn.execute(
                """SELECT r.*, j.name AS job_name, j.dir AS job_dir, j.agent AS job_agent,
                          j.model AS job_model, j.timeout_s AS job_timeout
                   FROM runs r JOIN jobs j ON j.id=r.job_id
                   WHERE r.status='running' AND r.finished_at IS NULL
                   ORDER BY r.started_at"""
            )
        ]
    finally:
        conn.close()

    # --- scan OS processes ---
    opencode_procs = []  # (pid, argv, title, job_guess, phase, meta)
    daemon = None
    mail_procs = []
    for pid, argv in _iter_proc_cmdlines():
        flat = " ".join(argv)
        if "scheduler.py" in flat and "daemon" in argv:
            workers = None
            if "--workers" in argv:
                try:
                    workers = int(argv[argv.index("--workers") + 1])
                except (ValueError, IndexError):
                    pass
            daemon = {"pid": pid, "workers": workers, "argv": argv,
                      **_proc_meta(pid)}
        if "opencode" not in "\x00".join(argv) or "run" not in argv:
            continue
        # only `<...>/opencode run ...` agent processes (skips `opencode serve`, auth, etc.)
        base = os.path.basename(argv[0]) if argv else ""
        if "opencode" not in base:
            continue
        if "serve" in argv or "web" in argv:
            continue
        try:
            if argv.index("run") != 1:
                continue
        except ValueError:
            continue
        title = _title_of(argv)
        if title == "mail-scheduler-agent":
            mail_procs.append((pid, argv))
            continue
        if not title:
            continue
        m = TITLE_RE.match(title)
        if m:
            job_guess, phase = m.group("job"), (m.group("phase") or "-main").lstrip("-")
        else:
            job_guess, phase = title, "main?"
        # keep only real agent runs (title carried a run timestamp or is mail agent)
        opencode_procs.append({
            "pid": pid, "argv": argv, "title": title,
            "job_guess": job_guess, "phase": phase,
            **_proc_meta(pid),
        })

    ps_info = _ps_enrich([p["pid"] for p in opencode_procs])
    for p in opencode_procs:
        p.update(ps_info.get(p["pid"], {}))

    by_job = {}
    for p in opencode_procs:
        by_job.setdefault(p["job_guess"], []).append(p)

    agents = []
    matched_pids = set()
    for r in running_runs:
        procs = by_job.get(r["job_name"], [])
        # prefer a main-phase proc; check/report procs belong to the same run window
        main = [p for p in procs if p["phase"] == "main"] or procs
        proc = main[0] if main else None
        if proc:
            matched_pids.add(proc["pid"])
        elapsed = max(0.0, now - (r["started_at"] or now))
        timeout = r["job_timeout"] or 600
        live_log = r["log_path"] or _resolve_live_log(r["job_name"], r["started_at"])
        agents.append({
            "job": r["job_name"], "run_id": r["id"], "phase": "main",
            "started_at": r["started_at"], "elapsed_s": elapsed,
            "timeout_s": timeout, "remaining_s": max(0.0, timeout - elapsed),
            "pid": proc["pid"] if proc else None,
            "etime": proc.get("etime") if proc else None,
            "pcpu": proc.get("pcpu") if proc else None,
            "mem_kb": proc.get("mem_kb") if proc else None,
            "state": proc.get("state") if proc else None,
            "session_id": r["session_id"],
            "log_path": live_log,
            "dir": r["job_dir"], "agent": r["job_agent"], "model": r["job_model"],
            "live_proc": proc is not None,
        })
        # extra check/report procs for the same job get their own rows
        for p in procs:
            if p["pid"] in matched_pids:
                continue
            matched_pids.add(p["pid"])
            agents.append({
                "job": r["job_name"], "run_id": r["id"], "phase": p["phase"],
                "started_at": r["started_at"], "elapsed_s": elapsed,
                "timeout_s": timeout, "remaining_s": max(0.0, timeout - elapsed),
                "pid": p["pid"], "etime": p.get("etime"), "pcpu": p.get("pcpu"),
                "mem_kb": p.get("mem_kb"), "state": p.get("state"),
                "session_id": r["session_id"], "log_path": live_log,
                "dir": r["job_dir"], "agent": r["job_agent"], "model": r["job_model"],
                "live_proc": True,
            })

    orphans = [p for p in opencode_procs if p["pid"] not in matched_pids]

    phases = {"main": 0, "check": 0, "report": 0}
    for a in agents:
        phases[a["phase"]] = phases.get(a["phase"], 0) + 1
    return {
        "now": now, "daemon": daemon,
        "agents": sorted(agents, key=lambda a: a["started_at"] or 0),
        "orphans": orphans,
        "mail_agents": [{"pid": pid, **_proc_meta(pid)} for pid, _ in mail_procs],
        "counts": {"total": len(agents), **phases,
                   "orphan_procs": len(orphans),
                   "mail_agents": len(mail_procs)},
        "jobs_total": len(jobs),
        "jobs_enabled": sum(1 for j in jobs.values() if j["enabled"]),
        "jobs_running": sum(1 for j in jobs.values() if j["status"] == "running"),
    }


def _fmt_dur(sec):
    sec = int(max(0, sec or 0))
    if sec < 90:
        return f"{sec}s"
    if sec < 5400:
        return f"{sec // 60}m{sec % 60:02d}"
    h, rem = divmod(sec, 3600)
    if h < 48:
        return f"{h}h{rem // 60:02d}"
    return f"{sec // 86400}d{h % 24:02d}h"


def format_agents_table(snap, width=0):
    L = []
    now = datetime.fromtimestamp(snap["now"]).strftime("%Y-%m-%d %H:%M:%S")
    d = snap["daemon"]
    if d:
        pool = f"workers={d['workers']}" if d.get("workers") else "workers=?"
        dline = f"daemon pid={d['pid']} {pool}"
    else:
        dline = "daemon NOT FOUND (no `scheduler.py daemon` process)"
    L.append(f"agents deployed: {snap['counts']['total']}  "
             f"(main={snap['counts'].get('main', 0)} "
             f"check={snap['counts'].get('check', 0)} "
             f"report={snap['counts'].get('report', 0)})  "
             f"| jobs {snap['jobs_running']} running / {snap['jobs_enabled']} enabled / "
             f"{snap['jobs_total']} total  |  {dline}  |  {now}")
    if snap["counts"]["orphan_procs"]:
        L.append(f"! {snap['counts']['orphan_procs']} orphan opencode proc(s) with no DB run "
                 f"(see bottom)")
    if snap["counts"]["mail_agents"]:
        L.append(f"i mail bridge agents: {snap['counts']['mail_agents']} "
                 f"(pids {', '.join(str(m['pid']) for m in snap['mail_agents'])})")
    L.append("")
    if not snap["agents"]:
        L.append("(no agents deployed — nothing is running)")
    else:
        hdr = (f"{'JOB':<18} {'RUN#':>5} {'PHASE':<6} {'ELAPSED':>7} {'LEFT':>7} "
               f"{'PID':>7} {'CPU%':>6} {'MEM':>7} {'SESSION':<9} {'TASK DETAIL'}")
        L.append(hdr)
        L.append("-" * len(hdr))
        for a in snap["agents"]:
            sess = (a["session_id"] or "-")[:8]
            mem = f"{a['mem_kb'] // 1024}M" if a.get("mem_kb") else "-"
            pid = str(a["pid"]) if a.get("pid") else "no-proc!"
            detail = a["job"]
            bits = []
            if a.get("agent"):
                bits.append(f"agent={a['agent']}")
            if a.get("model"):
                bits.append(str(a["model"]).split("/")[-1][:28])
            if a.get("dir") and a["dir"] != ".":
                bits.append(os.path.basename(a["dir"].rstrip("/")) if a["dir"] else "")
            if bits:
                detail += "  [" + " ".join(b for b in bits if b) + "]"
            L.append(f"{a['job']:<18} #{a['run_id']:>4} {a['phase']:<6} "
                     f"{_fmt_dur(a['elapsed_s']):>7} {_fmt_dur(a['remaining_s']):>7} "
                     f"{pid:>7} {(a.get('pcpu') or '-'):>6} {mem:>7} {sess:<9} {detail}")
            L.append(f"  {'started':<18} {datetime.fromtimestamp(a['started_at']).strftime('%m-%d %H:%M:%S') if a['started_at'] else '-':<14} "
                     f"log={a['log_path'] or '-'}")
    if snap["orphans"]:
        L.append("")
        L.append("orphan opencode procs (no matching DB run — may be manual `opencode run`):")
        for p in snap["orphans"]:
            L.append(f"  pid={p['pid']} title={p['title'][:60]}")
    out = "\n".join(L)
    if width and width > 0:
        out = "\n".join(line[:width] for line in out.splitlines())
    return out


def cmd_agents(args):
    snap = live_agent_snapshot()
    if getattr(args, "json", False):
        print(json.dumps({
            "now": snap["now"], "counts": snap["counts"],
            "daemon": ({k: v for k, v in (snap["daemon"] or {}).items() if k != "argv"}
                       if snap["daemon"] else None),
            "agents": snap["agents"], "orphans": snap["orphans"],
        }, default=str, indent=2))
    else:
        import shutil
        w = shutil.get_terminal_size((120, 40)).columns
        print(format_agents_table(snap, width=w))


def cmd_top(args):
    """Live refreshing TUI: how many agents are deployed, on what task."""
    interval = max(0.5, getattr(args, "interval", 2.0) or 2.0)
    if getattr(args, "once", False):
        return cmd_agents(args)
    import shutil
    try:
        while True:
            snap = live_agent_snapshot()
            w = shutil.get_terminal_size((120, 40)).columns
            # ANSI clear + home (no curses dependency; works over ssh/plain tty)
            sys.stdout.write("\x1b[2J\x1b[H")
            sys.stdout.write(format_agents_table(snap, width=w))
            sys.stdout.write(f"\n\nrefresh {interval:g}s — Ctrl+C / q to quit "
                             f"(`scheduler.py agents` for a one-shot, `--json` for scripts)\n")
            sys.stdout.flush()
            # sleep in small slices so Ctrl+C is responsive even with long intervals
            end = time.time() + interval
            while time.time() < end:
                time.sleep(min(0.2, end - time.time()))
    except KeyboardInterrupt:
        print("\nbye")


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    ensure_schema()

    p = argparse.ArgumentParser(prog="scheduler.py", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("add", help="add a scheduled job")
    a.add_argument("--name", required=True)
    a.add_argument("--prompt", required=True)
    a.add_argument("--dir", default=".", help="working dir for opencode (default .)")
    a.add_argument("--schedule", help="cron expr, e.g. '*/30 * * * *'")
    a.add_argument("--every", help="interval, e.g. 30m, 2h, 1d")
    a.add_argument("--once", action="store_true", help="run once then disable")
    a.add_argument("--agent")
    a.add_argument("--model")
    a.add_argument("--auto", action="store_true", help="auto-approve permissions (unattended!)")
    a.add_argument("--check-prompt", help="verifier instructions (runs as second opencode task)")
    a.add_argument("--no-report", action="store_true", help="skip the post-run agent quality report")
    a.add_argument("--timeout", type=int, default=600, help="max seconds per opencode run (default 600)")
    a.set_defaults(fn=cmd_add)

    l = sub.add_parser("list"); l.set_defaults(fn=cmd_list)
    rm = sub.add_parser("remove"); rm.add_argument("job"); rm.set_defaults(fn=cmd_remove)
    en = sub.add_parser("enable"); en.add_argument("job"); en.set_defaults(fn=lambda x: cmd_toggle(x, True))
    di = sub.add_parser("disable"); di.add_argument("job"); di.set_defaults(fn=lambda x: cmd_toggle(x, False))

    s = sub.add_parser("status", help="job details + recent runs")
    s.add_argument("job"); s.add_argument("--limit", type=int, default=5); s.set_defaults(fn=cmd_status)

    r = sub.add_parser("runs", help="run history")
    r.add_argument("job", nargs="?"); r.add_argument("--limit", type=int, default=20)
    r.add_argument("-v", "--verbose", action="store_true", help="include report excerpts")
    r.set_defaults(fn=cmd_runs)

    rp = sub.add_parser("report", help="full agent report for one run")
    rp.add_argument("run", type=int); rp.set_defaults(fn=cmd_report)

    j = sub.add_parser("journal", help="timeline of everything that happened")
    j.add_argument("--tail", type=int, default=50)
    j.add_argument("--clear", action="store_true")
    j.set_defaults(fn=cmd_journal)

    n = sub.add_parser("notify", help="send a test email notification")
    n.set_defaults(fn=cmd_notify)

    clr = sub.add_parser("clear", help="remove ALL jobs and runs")
    clr.add_argument("-y", "--yes", action="store_true", help="skip confirmation")
    clr.set_defaults(fn=cmd_clear)

    rn = sub.add_parser("run-now", help="trigger a job immediately")
    rn.add_argument("job"); rn.set_defaults(fn=None)

    d = sub.add_parser("daemon", help="start scheduler daemon")
    d.add_argument("--poll", type=int, default=10)
    d.add_argument("--workers", type=int, default=2)
    d.set_defaults(fn=cmd_daemon)

    h = sub.add_parser("heal", help="repair stranded run/job records now")
    h.set_defaults(fn=cmd_heal)

    ag = sub.add_parser("agents", help="show deployed agents: count + which task each runs")
    ag.add_argument("--json", action="store_true", help="machine-readable output")
    ag.add_argument("--once", action="store_true", help="alias: same as agents (for top --once symmetry)")
    ag.set_defaults(fn=cmd_agents)

    t = sub.add_parser("top", help="live TUI: agents deployed per task, refreshing")
    t.add_argument("-n", "--interval", type=float, default=2.0,
                   help="refresh seconds (default 2)")
    t.add_argument("--once", action="store_true", help="single snapshot, no refresh")
    t.add_argument("--json", action="store_true", help="with --once: machine-readable output")
    t.set_defaults(fn=cmd_top)

    args = p.parse_args()
    if args.cmd == "run-now":
        conn = db()
        jj = get_job(conn, args.job)
        if not jj:
            sys.exit(f"no such job: {args.job}")
        if jj["status"] == "running":
            sys.exit("job already running")
        conn.execute("UPDATE jobs SET status='pending', next_run_at=? WHERE id=? AND status!='running'",
                     (time.time(), jj["id"]))
        conn.commit(); conn.close()
        log_event("job_triggered", job=jj["name"])
        print(f"triggered '{jj['name']}'; daemon picks it up within poll interval")
    else:
        args.fn(args)


if __name__ == "__main__":
    main()
