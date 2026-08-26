#!/usr/bin/env python3
"""Web UI for the opencode scheduler. Stdlib only.

Run:  python3 ~/opencode-scheduler/webui.py [port]     (default 8788)
"""
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "scheduler.db")
SCHED = os.path.join(BASE, "scheduler.py")
ACTIVITY_LOG = os.path.join(BASE, "logs", "activity.jsonl")
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8788


def db():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def sched(*args):
    r = subprocess.run([sys.executable, SCHED, *args],
                       capture_output=True, text=True, timeout=120)
    return {"ok": r.returncode == 0, "out": (r.stdout or "") + (r.stderr or "")}


# ----------------------------- API -----------------------------

def api_jobs():
    conn = db()
    jobs = [dict(r) for r in conn.execute(
        "SELECT * FROM jobs ORDER BY enabled DESC, next_run_at")]
    now = time.time()
    for j in jobs:
        j["running"] = j["status"] == "running"
        if j["enabled"] and j["next_run_at"]:
            d = j["next_run_at"] - now
            j["next_in"] = "due now" if d <= 0 else f"in {fmt_delta(d)}"
        else:
            j["next_in"] = None
        j["last_ago"] = fmt_delta(now - j["last_run_at"]) if j["last_run_at"] else None
    # last run summary per job
    for j in jobs:
        row = conn.execute(
            "SELECT status, exit_code, duration_s, finished_at FROM runs "
            "WHERE job_id=? AND status!='running' ORDER BY id DESC LIMIT 1",
            (j["id"],)).fetchone()
        j["last_result"] = dict(row) if row else None
    conn.close()
    return {"jobs": jobs, "now": now}


def api_runs(job_id=None, limit=100):
    conn = db()
    q = ("SELECT r.*, j.name AS job_name FROM runs r JOIN jobs j ON j.id=r.job_id ")
    args = []
    if job_id:
        if isinstance(job_id, (list, tuple)):
            job_id = job_id[0]
        q += "WHERE r.job_id=? "; args.append(job_id)
    q += "ORDER BY r.id DESC LIMIT ?"; args.append(limit)
    runs = []
    for r in conn.execute(q, args):
        d = dict(r)
        d["output_text"] = (d.get("output_text") or "")[:4000]
        runs.append(d)
    conn.close()
    return {"runs": runs}


def api_report(run_id):
    conn = db()
    r = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    conn.close()
    if not r:
        return {"error": "not found"}
    return dict(r)


def api_journal(limit=200):
    entries = []
    try:
        with open(ACTIVITY_LOG) as f:
            lines = f.readlines()[-limit:]
        for line in reversed(lines):
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    except FileNotFoundError:
        pass
    return {"entries": entries}


def api_add(body):
    args = ["add", "--name", body["name"], "--prompt", body["prompt"]]
    kind = body.get("kind", "interval")
    if kind == "cron":
        args += ["--schedule", body["schedule"]]
    elif kind == "once":
        args += ["--schedule", body["schedule"], "--once"]
    else:
        args += ["--every", body.get("every", "30m")]
    if body.get("auto"):
        args.append("--auto")
    if body.get("timeout"):
        args += ["--timeout", str(int(body["timeout"]))]
    if body.get("check_prompt", "").strip():
        args += ["--check-prompt", body["check_prompt"]]
    res = sched(*args)
    return res


def fmt_delta(sec):
    sec = int(sec)
    sign = "-" if sec < 0 else ""
    sec = abs(sec)
    if sec < 90: return f"{sign}{sec}s"
    if sec < 5400: return f"{sign}{sec // 60}m"
    if sec < 172800: return f"{sign}{sec // 3600}h"
    return f"{sign}{sec // 86400}d"


def ts(iso_or_real):
    try:
        v = float(iso_or_real)
        return datetime.fromtimestamp(v).strftime("%Y-%m-%d %H:%M:%S") if v > 0 else "-"
    except (TypeError, ValueError):
        return str(iso_or_real or "-")


ROUTES_GET = {
    "jobs": lambda q: api_jobs(),
    "runs": lambda q: api_runs(q.get("job_id")),
    "report": lambda q: api_report(int(q["id"][0])),
    "journal": lambda q: api_journal(),
}
ACTIONS = {"enable", "disable", "remove", "run-now"}


class Handler(BaseHTTPRequestHandler):
    def _json(self, obj, code=200):
        data = json.dumps(obj, default=ts).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        p = urlparse(self.path)
        if p.path.startswith("/api/"):
            name = p.path[5:]
            fn = ROUTES_GET.get(name)
            if not fn:
                return self._json({"error": "unknown"}, 404)
            try:
                return self._json(fn(parse_qs(p.query)))
            except Exception as e:
                return self._json({"error": str(e)}, 500)
        body = PAGE.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        p = urlparse(self.path)
        parts = p.path.strip("/").split("/")
        n = int(self.headers.get("Content-Length") or 0)
        body = {}
        if n:
            try:
                body = json.loads(self.rfile.read(n))
            except json.JSONDecodeError:
                return self._json({"error": "bad json"}, 400)
        try:
            if p.path == "/api/add":
                return self._json(api_add(body))
            if len(parts) == 4 and parts[1] == "jobs" and parts[3] in ACTIONS:
                ident = body.get("name") or parts[2]
                cmd = {"enable": "enable", "disable": "disable",
                       "run-now": "run-now", "remove": "remove"}[parts[3]]
                return self._json(sched(cmd, str(ident)))
        except Exception as e:
            return self._json({"error": str(e)}, 500)
        return self._json({"error": "unknown"}, 404)

    def log_message(self, *_a):
        pass


# ---------------------------- Frontend ----------------------------

PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Scheduler Console</title>
<style>
:root{
  --bg:#0b0e14;--panel:#11151f;--panel2:#161b28;--line:#232a3b;
  --txt:#e6eaf2;--dim:#8b94a7;--acc:#5b8cff;--acc2:#7c5bff;
  --ok:#34d399;--warn:#fbbf24;--err:#f87171;--run:#38bdf8;
  --rad:12px;--mono:'SF Mono',ui-monospace,Menlo,monospace;
}
*{box-sizing:border-box;margin:0}
body{background:var(--bg);color:var(--txt);font:14px/1.5 Inter,-apple-system,'Segoe UI',sans-serif;
  background-image:radial-gradient(1200px 600px at 80% -10%,rgba(91,140,255,.08),transparent),
  radial-gradient(900px 500px at -10% 110%,rgba(124,91,255,.06),transparent);}
header{display:flex;align-items:center;gap:14px;padding:16px 26px;border-bottom:1px solid var(--line);
  position:sticky;top:0;background:rgba(11,14,20,.85);backdrop-filter:blur(10px);z-index:50}
.logo{width:32px;height:32px;border-radius:9px;background:linear-gradient(135deg,var(--acc),var(--acc2));
  display:flex;align-items:center;justify-content:center;font-weight:800;font-size:15px}
h1{font-size:16px;font-weight:650;letter-spacing:.2px}
.sub{color:var(--dim);font-size:12px;margin-left:auto;display:flex;gap:18px;align-items:center}
.dot{width:8px;height:8px;border-radius:50%;background:var(--ok);display:inline-block;
  box-shadow:0 0 8px var(--ok);animation:pulse 2s infinite}
@keyframes pulse{50%{opacity:.4}}
main{max-width:1280px;margin:0 auto;padding:24px 26px 80px}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;margin-bottom:22px}
.stat{background:var(--panel);border:1px solid var(--line);border-radius:var(--rad);padding:14px 18px}
.stat .n{font-size:24px;font-weight:700;font-family:var(--mono)}
.stat .l{color:var(--dim);font-size:11.5px;text-transform:uppercase;letter-spacing:.8px}
.toolbar{display:flex;gap:10px;margin-bottom:16px;align-items:center}
button,.btn{background:var(--panel2);border:1px solid var(--line);color:var(--txt);padding:8px 15px;
  border-radius:9px;font:inherit;font-size:13px;cursor:pointer;transition:.15s;display:inline-flex;gap:7px;align-items:center}
button:hover{border-color:var(--acc);background:#1b2334}
.primary{background:linear-gradient(135deg,var(--acc),var(--acc2));border:none;font-weight:600}
.primary:hover{filter:brightness(1.12)}
.danger:hover{border-color:var(--err);color:var(--err)}
input,select,textarea{background:#0d1119;border:1px solid var(--line);border-radius:8px;color:var(--txt);
  padding:8px 12px;font:inherit;width:100%}
input:focus,textarea:focus{outline:none;border-color:var(--acc)}
.tabs{display:flex;gap:4px;background:#0d1119;padding:4px;border-radius:10px;border:1px solid var(--line);width:max-content}
.tabs button{border:none;background:transparent;padding:6px 14px;border-radius:7px;font-size:12.5px}
.tabs button.on{background:var(--panel2);color:var(--acc)}
table{width:100%;border-collapse:collapse}
.card{background:var(--panel);border:1px solid var(--line);border-radius:var(--rad);overflow:hidden}
th{font-size:11px;text-transform:uppercase;letter-spacing:.8px;color:var(--dim);text-align:left;
  padding:11px 16px;border-bottom:1px solid var(--line);background:var(--panel2)}
td{padding:12px 16px;border-bottom:1px solid rgba(35,42,59,.55);vertical-align:middle}
tr:last-child td{border-bottom:none}
tbody tr:hover{background:rgba(91,140,255,.04)}
.pill{display:inline-flex;align-items:center;gap:6px;padding:3px 10px;border-radius:99px;font-size:11.5px;font-weight:600}
.pill::before{content:'';width:6px;height:6px;border-radius:50%}
.run{background:rgba(56,189,248,.12);color:var(--run)}.run::before{background:var(--run)}
.okp{background:rgba(52,211,153,.1);color:var(--ok)}.okp::before{background:var(--ok)}
.off{background:rgba(139,148,167,.1);color:var(--dim)}.off::before{background:var(--dim)}
.errp{background:rgba(248,113,113,.1);color:var(--err)}.errp::before{background:var(--err)}
.warnp{background:rgba(251,191,36,.1);color:var(--warn)}.warnp::before{background:var(--warn)}
.mono{font-family:var(--mono);font-size:12.5px}
.dim{color:var(--dim)}
.name{font-weight:600;cursor:pointer}.name:hover{color:var(--acc)}
.rowbtns button{padding:4px 9px;font-size:11.5px}
.modal-bg{position:fixed;inset:0;background:rgba(5,7,12,.7);backdrop-filter:blur(4px);
  display:none;align-items:flex-start;justify-content:center;z-index:100;padding-top:8vh}
.modal-bg.open{display:flex}
.modal{background:var(--panel);border:1px solid var(--line);border-radius:16px;width:640px;max-width:94vw;
  max-height:82vh;overflow:auto;padding:26px;box-shadow:0 30px 80px rgba(0,0,0,.6)}
.modal h2{font-size:17px;margin-bottom:18px;display:flex;justify-content:space-between;align-items:center}
label{display:block;font-size:12px;color:var(--dim);margin:14px 0 5px;font-weight:600;letter-spacing:.3px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.footer-actions{display:flex;gap:10px;justify-content:flex-end;margin-top:22px}
pre{background:#0d1119;border:1px solid var(--line);border-radius:10px;padding:14px;overflow:auto;
  font-family:var(--mono);font-size:12px;white-space:pre-wrap;max-height:420px;line-height:1.55}
.jl{padding:7px 16px;border-bottom:1px solid rgba(35,42,59,.4);font-family:var(--mono);font-size:12px;display:flex;gap:10px}
.jl .ev{min-width:130px;font-weight:600}
.view{display:none}.view.on{display:block}
.nav{display:flex;gap:2px;margin-bottom:20px;border-bottom:1px solid var(--line)}
.nav button{border:none;background:none;border-radius:0;border-bottom:2px solid transparent;color:var(--dim)}
.nav button.on{color:var(--txt);border-bottom-color:var(--acc)}
.toast{position:fixed;bottom:24px;right:24px;background:var(--panel2);border:1px solid var(--line);
  padding:12px 18px;border-radius:10px;font-size:13px;display:none;z-index:200;box-shadow:0 10px 40px rgba(0,0,0,.5)}
.x{cursor:pointer;color:var(--dim);font-size:20px;line-height:1}.x:hover{color:var(--err)}
</style></head><body>
<header><div class="logo">SC</div><h1>Scheduler Console</h1>
<div class="sub"><span><span class="dot"></span> live</span><span id="clock" class="mono dim"></span>
<button class="primary" onclick="openAdd()">+ New Task</button></div></header>
<main>
<div class="nav">
<button class="on" data-v="jobs" onclick="setView(this,'jobs')">Tasks</button>
<button data-v="runs" onclick="setView(this,'runs')">Run History</button>
<button data-v="journal" onclick="setView(this,'journal')">Activity</button>
</div>

<section id="v-jobs" class="view on">
<div class="stats" id="stats"></div>
<div class="card"><table><thead><tr>
<th>Task</th><th>Schedule</th><th>Status</th><th>Last Run</th><th>Next Run</th><th>Last Result</th><th></th>
</tr></thead><tbody id="jobsBody"></tbody></table></div>
</section>

<section id="v-runs" class="view">
<div class="card"><table><thead><tr>
<th>#</th><th>Task</th><th>Started</th><th>Duration</th><th>Exit</th><th>Check</th><th>Score</th><th>Report</th>
</tr></thead><tbody id="runsBody"></tbody></table></div>
</section>

<section id="v-journal" class="view">
<div class="card" id="journalBox"></div>
</section>
</main>

<div class="modal-bg" id="mbg" onclick="if(event.target===this)this.classList.remove('open')">
<div class="modal" id="modal"></div></div>
<div class="toast" id="toast"></div>

<script>
let jobs=[],cur=null,timer=null,kind='cron';
const $=s=>document.querySelector(s);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function toast(m){const t=$('#toast');t.textContent=m;t.style.display='block';clearTimeout(t._x);t._x=setTimeout(()=>t.style.display='none',2600)}
async function api(p,opt){const r=await fetch('/api/'+p,opt);return r.json()}
function act(name,a){return api('jobs/'+encodeURIComponent(name)+'/'+a,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name})})}

function setView(btn,v){
 document.querySelectorAll('.nav button').forEach(b=>b.classList.toggle('on',b===btn));
 document.querySelectorAll('.view').forEach(x=>x.classList.toggle('on',x.id==='v-'+v));
 if(v==='runs')loadRuns();if(v==='journal')loadJournal();
}

function pill(status){
 const m={running:['run','Running'],done:['okp','Done'],pending:['warnp','Pending'],
   error:['errp','Error'],failed:['errp','Failed'],disabled:['off','Paused']};
 const[c,l]=m[status]||['off',status];return `<span class="pill ${c}">${l}</span>`;
}

function render(){
 const on=jobs.filter(j=>j.enabled),running=jobs.filter(j=>j.running);
 const failed=jobs.filter(j=>j.last_result&&j.last_result.exit_code!==0&&j.last_result.exit_code!==null);
 $('#stats').innerHTML=[
  ['Tasks',jobs.length],['Active',on.length],['Running now',running.length],['Last failures',failed.length]
 ].map(([l,n])=>`<div class="stat"><div class="n">${n}</div><div class="l">${l}</div></div>`).join('');

 $('#jobsBody').innerHTML=jobs.map(j=>{
  const lr=j.last_result;
  const res=lr?(lr.exit_code===0||lr.exit_code===null?'<span class="pill okp">OK</span>'
    :`<span class="pill errp">exit ${lr.exit_code}</span>`)
    +(lr.duration_s!=null?` <span class="dim mono">${Math.round(lr.duration_s)}s</span>`:''):'<span class="dim">—</span>';
  return `<tr>
   <td><div class="name" onclick="showJob(${j.id})">${esc(j.name)}</div>
       <div class="dim mono" style="font-size:11px">#${j.id}${j.auto_approve?' · auto':''}</div></td>
   <td class="mono">${esc(j.schedule)}</td>
    <td>${j.running?pill('running'):j.status==='done'?pill('done'):j.enabled?pill('pending'):pill('disabled')}</td>
   <td>${j.last_ago?`<span class="mono">${esc(j.last_ago)} ago</span>`:'<span class="dim">never</span>'}</td>
    <td>${j.enabled?`<span class="mono">${j.next_in?esc(j.next_in):'—'}</span>`
       :'<span class="dim">paused</span>'}</td>
   <td>${res}</td>
   <td class="rowbtns" style="white-space:nowrap">
     <button onclick="act('${esc(j.name)}','run-now').then(r=>toast(r.out||'Triggered'));refresh()">Run now</button>
     <button onclick="act('${esc(j.name)}','${j.enabled?'disable':'enable'}').then(refresh)">${j.enabled?'Pause':'Resume'}</button>
     <button class="danger" onclick="delJob('${esc(j.name)}')">Delete</button>
   </td></tr>`;
 }).join('')||'<tr><td colspan="7" class="dim" style="text-align:center;padding:40px">No tasks yet</td></tr>';
}

async function refresh(){const d=await api('jobs');jobs=d.jobs;render();}
function delJob(n){if(!confirm('Delete task "'+n+'"?'))return;act(n,'remove').then(r=>{toast(r.out||'Deleted');refresh()})}

function openModal(html){$('#modal').innerHTML=html;$('#mbg').classList.add('open')}
function closeModal(){$('#mbg').classList.remove('open')}

function openAdd(){
 openModal(`<h2>New Task <span class="x" onclick="closeModal()">×</span></h2>
 <label>Name</label><input id="f-name" placeholder="my-task">
 <label>Prompt <span class="dim">(self-contained — the agent has no prior context)</span></label>
 <textarea id="f-prompt" rows="5"></textarea>
 <div class="tabs" style="margin-top:14px">
  <button id="k-cron" class="on" onclick="setKind('cron')">Cron</button>
  <button id="k-every" onclick="setKind('every')">Interval</button>
  <button id="k-once" onclick="setKind('once')">One-shot</button>
 </div>
 <div class="grid2" style="margin-top:12px">
  <div id="w-cron"><label>Cron expression <span class="dim">(min hour dom month dow)</span></label>
    <input id="f-cron" placeholder="0 21 * * *" value="0 * * * *"></div>
  <div id="w-once" style="display:none"><label>Date &amp; time</label>
    <input id="f-at" type="datetime-local"></div>
  <div id="w-every" style="display:none"><label>Every</label>
    <input id="f-every" placeholder="30m / 2h / 1d" value="30m"></div>
 </div>
 <div class="grid2">
  <div><label>Timeout (seconds)</label><input id="f-timeout" type="number" value="600"></div>
  <div><label>&nbsp;</label><label style="display:flex;gap:8px;align-items:center;color:var(--txt)">
   <input type="checkbox" id="f-auto" checked style="width:auto"> Auto-approve tools (--auto)</label></div>
 </div>
 <label>Verifier prompt <span class="dim">(optional)</span></label>
 <input id="f-check" placeholder="Verify that ...">
 <div class="footer-actions"><button onclick="closeModal()">Cancel</button>
 <button class="primary" onclick="createJob()">Create Task</button></div>`);
 setKind(kind);
 const at=$('#f-at');const d=new Date(Date.now()+3600000);at.value=d.toISOString().slice(0,16);
}
function setKind(k){kind=k;for(const x of['cron','every','once']){$('#k-'+x)?.classList.toggle('on',x===k);$('#w-'+x)&&($('#w-'+x).style.display=x===k?'':'none')}}
async function createJob(){
 const name=$('#f-name').value.trim(),prompt=$('#f-prompt').value.trim();
 if(!name||!prompt)return toast('Name and prompt are required');
 let b={name,prompt,kind,auto:$('#f-auto').checked,timeout:+$('#f-timeout').value||600,
        check_prompt:$('#f-check').value};
 if(kind==='cron')b.schedule=$('#f-cron').value.trim();
 else if(kind==='every')b.every=$('#f-every').value.trim();
  else{
   const v=$('#f-at').value;if(!v)return toast('Pick a date/time');
   const d=new Date(v);
   b.kind='cron';b.schedule=`${d.getMinutes()} ${d.getHours()} ${d.getDate()} ${d.getMonth()+1} *`;
  }
 const r=await api('add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});
 toast(r.ok?'Task created':(r.out||'Failed'));
 if(r.ok){closeModal();refresh()}else alert(r.out);
}
async function showJob(id){
 const j=jobs.find(x=>x.id===id);if(!j)return;
 const runs=(await api('runs?job_id='+id)).runs.slice(0,10);
 openModal(`<h2>${esc(j.name)} <span class="x" onclick="closeModal()">×</span></h2>
 <div class="mono dim" style="margin-bottom:14px">#${j.id} · ${esc(j.schedule)} · ${j.auto_approve?'auto':'manual'} · timeout ${j.timeout_s}s</div>
 <label>Prompt</label><pre>${esc(j.prompt)}</pre>
 ${j.check_prompt?`<label>Verifier</label><pre>${esc(j.check_prompt)}</pre>`:''}
 <label>Last 10 runs</label><pre>${runs.map(r=>`${new Date(r.started_at*1000).toLocaleString()}  exit=${r.exit_code??'-'}  ${r.duration_s?Math.round(r.duration_s)+'s':''} ${r.check_passed===0?'CHECK FAILED':''}`).join('\n')||'no runs yet'}</pre>
 <div class="footer-actions">
  <button onclick="closeModal();showRuns(${id})">All runs</button>
  <button onclick="closeModal()">Close</button></div>`);
}
async function showRuns(jobId){
 document.querySelectorAll('.nav button')[1].click();
 await loadRuns();
}
async function loadRuns(){
 const d=await api('runs');
 $('#runsBody').innerHTML=d.runs.map(r=>`<tr>
  <td class="mono dim">${r.id}</td>
  <td class="name" onclick="showReport(${r.id})">${esc(r.job_name)}</td>
  <td class="mono">${new Date(r.started_at*1000).toLocaleString()}</td>
  <td class="mono">${r.duration_s!=null?Math.round(r.duration_s)+'s':'<span class="dim">…'}</span></td>
  <td>${r.status==='running'?pill('running'):r.exit_code===0?'<span class="pill okp">OK</span>':'<span class="pill errp">exit '+(r.exit_code??'?')+'</span>'}</td>
  <td>${r.check_passed===null||r.check_passed===undefined?'<span class="dim">—</span>':r.check_passed?'<span class="pill okp">pass</span>':'<span class="pill errp">fail</span>'}</td>
  <td class="mono">${r.report_score??'<span class="dim">—</span>'}</td>
  <td><button onclick="showReport(${r.id})">View</button></td></tr>`).join('')
  ||'<tr><td colspan="8" class="dim" style="text-align:center;padding:40px">No runs recorded</td></tr>';
}
async function showReport(id){
 const r=await api('report?id='+id);
 openModal(`<h2>Run #${r.id} — ${esc(r.status)} <span class="x" onclick="closeModal()">×</span></h2>
 <div class="mono dim" style="margin-bottom:12px">started ${new Date(r.started_at*1000).toLocaleString()}
  · exit ${r.exit_code??'-'} · score ${r.report_score??'-'}</div>
 <label>Agent output</label><pre>${esc((r.output_text||'(none)').slice(0,8000))}</pre>
 ${r.report_text?`<label>Report</label><pre>${esc(r.report_text.slice(0,8000))}</pre>`:''}`);
}
async function loadJournal(){
 const d=await api('journal');
 $('#journalBox').innerHTML=d.entries.map(e=>{
  const col={start:'var(--run)',finish_ok:'var(--ok)',finish_fail:'var(--err)',email_sent:'var(--acc)'};
  return `<div class="jl"><span class="dim">${esc(e.time)}</span>
   <span class="ev" style="color:${col[e.event]||'var(--txt)'}">${esc(e.event)}</span>
   <span class="dim">${esc(Object.entries(e).filter(([k])=>!['ts','time','event'].includes(k))
     .map(([k,v])=>k+'='+v).join('  '))}</span></div>`;
 }).join('')||'<div class="dim" style="padding:30px;text-align:center">No activity</div>';
}
$('#clock').textContent=new Date().toLocaleString();
setInterval(()=>$('#clock').textContent=new Date().toLocaleTimeString(),1000);
refresh();timer=setInterval(refresh,3000);
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeModal()});
</script></body></html>"""

if __name__ == "__main__":
    host = os.environ.get("WEBUI_HOST", "0.0.0.0")
    print(f"Scheduler console → http://{host}:{PORT}")
    ThreadingHTTPServer((host, PORT), Handler).serve_forever()
