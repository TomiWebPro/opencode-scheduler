# opencode Scheduler

Schedule prompts as headless opencode tasks, with automatic **agent quality reports**
after every run and a full journal of what was done, when, and how well.

## How tasks run

Each execution spawns (with a per-job `--timeout` in seconds, default 600):

```
opencode run "<prompt>" --title "<job>-<timestamp>" [--dir D] [--agent A] [-m M] [--auto] --format json
```

After the task finishes, the scheduler may run up to two more opencode calls:

1. **Verifier** (only if `--check-prompt` given): an agent inspects the result
   (can run commands itself) and answers `CHECK: PASS` or `CHECK: FAIL`.
   Verification gets at least 120 seconds regardless of the job's timeout.
2. **Reporter** (on by default): an agent writes a quality report — summary,
   quality assessment, issues — ending with `SCORE: n/10` and
   `VERDICT: SUCCESS|PARTIAL|FAILURE`. Disable per job with `--no-report`.
   Report generation gets at least 120 seconds.

## Files

| path | purpose |
|---|---|
| `scheduler.py` | CLI + daemon |
| `scheduler.db` | SQLite store (jobs + runs) |
| `mail_inbox.py` | Email-to-task bridge (IMAP poller + agent) |
| `mail_inbox.db` | IMAP cursor + request tracking |
| `email_notifier.py` | Gmail SMTP sender |
| `email_config.json` | SMTP/IMAP credentials (gitignore this) |
| `logs/activity.jsonl` | machine-readable event log (every event, timestamped) |
| `logs/journal.md` | human-readable timeline (`scheduler.py journal`) |
| `logs/<name>-<ts>.log` | raw stdout/stderr of each run (+ `-check.log`, `-report.log`) |

## Schedules

- cron: `--schedule "*/30 * * * *"` (min hour dom mon dow)
- interval: `--every 30m` / `2h` / `1d`
- once: `--once` (runs once then disables itself)

## Usage

```bash
# start the daemon (keep alive with systemd/tmux)
python3 scheduler.py daemon --poll 10 --workers 2

# schedule a task every 30 minutes, unattended, with a verifier + report
python3 scheduler.py add \
  --name nightly-tests \
  --prompt "Run the test suite in this repo and fix trivial failures" \
  --dir ~/myproject --every 30m --auto --timeout 900 \
  --check-prompt "Confirm the test suite passes; run it yourself to be sure."

# one-off task
python3 scheduler.py add --name cleanup --prompt "..." --once

# inspect
python3 scheduler.py list
python3 scheduler.py status nightly-tests
python3 scheduler.py runs -v                 # history w/ report excerpts
python3 scheduler.py report <run_id>         # full agent report for a run
python3 scheduler.py journal                 # timeline of everything

# control
python3 scheduler.py run-now nightly-tests
python3 scheduler.py disable nightly-tests   # / enable / remove
```

## For agents (programmatic scheduling)

Everything is a subprocess call. Examples:

```bash
# schedule something
scheduler.py add --name "$NAME" --prompt "$PROMPT" --schedule "$CRON" --auto

# check how well past runs did
scheduler.py runs --limit 10
scheduler.py report 42        # read SCORE/VERDICT from the report text
```

Machine-readable event stream for automation: `logs/activity.jsonl`
(one JSON object per line: `run_start`, `task_finished`, `check_finished`,
`report_generated`, ...). Query SQLite directly if needed:
`sqlite3 scheduler.db 'select id,report_score,status from runs order by id desc limit 10;'`

## Email Notifications

The scheduler always emails on completion (success **or** failure — a timed-out or
failed job still notifies you, it is never silently dropped). Two emails go out per
job:

1. **Your result** — sent as a reply in *your own* `sched` email thread, leading
   with the **actual task output** (the answer you asked for: a roundtable report,
   research findings, etc.). This is the email you actually read.
2. **Operational QA** — sent to the separate "event log" thread, with the quality
   score/verdict and a condensed view. Secondary; safe to ignore.

### Email-to-Task (mail_inbox.py)

Schedule tasks by emailing yourself with subject exactly **`sched`**.
The bridge polls Gmail (every 120s by default), picks up self-sent `sched` emails,
and hands the body to an opencode agent which delegates the real work to the scheduler.

**This is for human use only — agents should not use this route.**

**How it works:**

1. You email yourself: Subject = `sched`, Body = natural language request
2. Immediate ACK email confirms receipt
3. A thin dispatcher agent schedules the work as a scheduler job whose **output
   is the real deliverable** (e.g. a roundtable is run *foreground* with a generous
   `--timeout`, so its summary becomes the job's output)
4. The job runs headlessly; the moment it finishes, the **actual result is emailed
   back to you in the same `sched` thread** — no manual recheck, nothing rots
5. A queued-confirmation reply also lists the exact job name(s) created

**Key reliability fixes:**
- The dispatcher streams stdout and uses SIGTERM (not SIGKILL) on timeout, so its
  reply is never lost as "(no output)".
- Email→job links are stored in `email_jobs` (shared DB) by snapshotting jobs
  created during the agent run, so the result always finds its way back to your thread.
- Heavy work (roundtables, research) runs **foreground** in the job, not as a
  detached `--bg` session needing a separate recheck — eliminating the "started but
  never delivered" failure mode.

**Agent autonomy rules:**
- Acts freely on programming/code tasks
- NEVER takes real-life actions (notifying third parties, rebooting, etc.)
- If unclear about real-life consequences → sends `RESULT: NEEDS_INPUT` question
- Pending questions expire after 24 hours

**Usage:**

```bash
# start the mail bridge (keep alive with systemd)
python3 mail_inbox.py              # daemon mode (poll every 120s)
python3 mail_inbox.py --once       # poll once
python3 mail_inbox.py --test       # send a test sched email
```

**Example emails:**

```
Subject: sched
Body: run the test suite every 30 minutes

Subject: sched
Body: back up ~/projects daily at 3am

Subject: sched
Body: research the root cause of Apple sourcing aluminium, use a 4-person 3-turn roundtable and tell me the results
```

### Setup

1. Enable 2-Step Verification on your Google account
2. Go to https://myaccount.google.com/apppasswords → generate a password for "Mail"
3. Edit `email_config.json`:

```json
{
  "enabled": true,
  "smtp_host": "smtp.gmail.com",
  "smtp_port": 587,
  "smtp_user": "you@gmail.com",
  "smtp_password": "xxxx xxxx xxxx xxxx",
  "notify_to": "you@gmail.com",
  "imap_host": "imap.gmail.com",
  "mail_inbox": {
    "enabled": true,
    "poll_seconds": 120,
    "allowed_senders": ["you@gmail.com"],
    "subject": "sched",
    "agent_timeout": 330,
    "pending_expiry_hours": 24
  }
}
```

4. Test: `python3 scheduler.py notify`

### CLI

```bash
python3 scheduler.py notify          # send a test email
```

### Programmatic (for agents)

The scheduler automatically emails on successful runs. No extra calls needed.
Check `logs/activity.jsonl` for `email_sent` events.

## Security note

`--auto` auto-approves all permissions — required for unattended runs but
dangerous. Only combine it with prompts you trust.
