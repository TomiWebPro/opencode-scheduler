---
name: scheduling
description: Schedule recurring or one-shot tasks via the local opencode scheduler daemon
---

# Task Scheduling

Use `~/opencode-scheduler/scheduler.py` (or `~/bin/ocsched` if on PATH) to schedule,
manage, and inspect automated opencode tasks. This is the single entry point for
all scheduling.

## When to use

Activate this skill when the user asks to:
- Schedule, automate, or repeat a task ("run X every hour", "do Y at 9pm")
- Check on scheduled tasks ("show my scheduled jobs", "how did the last run go")
- Trigger a task immediately ("run nightly-tests now")
- View task history or reports ("show me the last run report")

## Core commands

```bash
# Full path (always works)
SCHEDULER=~/opencode-scheduler/scheduler.py

# Or use the symlink (if ~/bin is in PATH)
SCHEDULER=ocsched

# --- Add a job ---
# cron expression (5 fields: min hour dom month dow)
python3 $SCHEDULER add --name <name> --prompt "<prompt>" --schedule "0 21 * * *" --auto

# interval
python3 $SCHEDULER add --name <name> --prompt "<prompt>" --every 30m --auto

# one-shot (runs once then disables itself)
python3 $SCHEDULER add --name <name> --prompt "<prompt>" --once --auto

# with verifier (optional)
python3 $SCHEDULER add --name <name> --prompt "<prompt>" --every 1h --auto \
  --check-prompt "Verify that <expected outcome>."

# --- Manage ---
python3 $SCHEDULER list                    # all jobs
python3 $SCHEDULER status <name>           # job details + recent runs
python3 $SCHEDULER run-now <name>          # trigger immediately
python3 $SCHEDULER enable <name>           # re-enable
python3 $SCHEDULER disable <name>          # pause
python3 $SCHEDULER remove <name>           # delete one job
python3 $SCHEDULER clear                   # delete ALL jobs (asks confirmation)
python3 $SCHEDULER clear -y                # delete ALL jobs (skip confirmation)

# --- History ---
python3 $SCHEDULER runs                    # recent runs
python3 $SCHEDULER runs -v                 # with report excerpts
python3 $SCHEDULER report <run_id>         # full agent report
python3 $SCHEDULER journal                 # timeline of everything
```

## Schedules

| User says | Use |
|---|---|
| "every 30 minutes" | `--every 30m` |
| "every 2 hours" | `--every 2h` |
| "once at 9pm today" | `--once` + compute cron `0 21 <day> <month> *` |
| "daily at 9am" | `--schedule "0 9 * * *"` |
| "weekdays at noon" | `--schedule "0 12 * * 1-5"` |

### Computing "at TIME on DATE"

Use the `date` command to figure out the cron fields:

```bash
# get current time components
date "+%H %M %d %m"   # e.g. "14 30 23 08"

# for "at 9pm today" (August 23), if it's currently before 9pm:
#   cron = "0 21 23 8 *"
# if it's already past 9pm, it means tonight (which may be impossible)
# so use tomorrow's date or ask the user.
```

## Rules

1. **Prompts must be self-contained.** Conversation context does not survive.
   Include all necessary details in the prompt string itself.

2. **Use `--auto` only with trusted prompts.** It auto-approves all tool
   permissions (bash, edit, etc.). Never use with untrusted user input.

3. **Default timeout is 600s (10 min).** Override with `--timeout N`.
   Verification and report steps get at least 120s each.

4. **Successful runs auto-email TomiWebPro@gmail.com** with a quality report.
   Check `scheduler.py journal` for `email_sent` events.

5. **Prompts should be concise but complete.** The agent running the task
   has no memory of prior context.

## Rechecks (when the user asks to recheck a task at some later time)

If the user wants a result rechecked/reported at a specific later time (e.g.
"check back at 9pm", "recheck the roundtable tomorrow"), there are two valid
approaches. Pick based on whether the dispatcher must stay non-blocking.

### Approach 1 — Schedule a separate recheck task (recommended for email/non-blocking)

Delegate the recheck to its own scheduler job that runs at the requested time.
This keeps the original dispatcher free to reply immediately; the recheck job
inspects the original task's status and the scheduler auto-emails the result on
completion.

```bash
# one-shot, delayed to the requested time
python3 $SCHEDULER add --name <task>-recheck \
  --prompt "Inspect the status of <original task / session>. Report its current
state (or print the full summary if done). Do NOT start a new run. If still
incomplete, say so." \
  --schedule "0 21 23 8 *" --auto --timeout 1200
```

- Use `--once` for a single delayed check, or a cron time matching the user's
  request (compute fields with `date`, see above).
- Because successful runs auto-email, the user gets the recheck result without
  the dispatcher waiting.

### Approach 2 — Delegate the recheck inside the original prompt (inline)

Tell the agent executing the original task to perform the recheck itself at the
end of its own run — e.g. "after it finishes, print/email the summary as your
final output." Best when the runner is already detached from the user (a durable
scheduler worker or background shell), so the user is not blocked synchronously.

```
... run the task, then at the end print the full result and present conclusions
as your final answer.
```

- Single task, single notification, no extra scheduling needed.
- Only use when blocking for the full duration is acceptable for that runner.

**Rule of thumb:** an interactive/email handler (mail_inbox agent) should prefer
**Approach 1** — schedule the recheck and let the scheduler auto-email, so the
handler replies ASAP and never waits. A detached worker that the user isn't
blocking on can use **Approach 2** (inline recheck in the same task).

## Example: "schedule a backup at 3am daily"

```bash
python3 ~/opencode-scheduler/scheduler.py add \
  --name daily-backup \
  --prompt "Back up ~/projects to ~/backups using rsync. Delete backups older than 7 days." \
  --schedule "0 3 * * *" \
  --auto \
  --timeout 3600
```
