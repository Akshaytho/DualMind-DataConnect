"""Web UI — simple query interface served at /ui.

Single-page HTML application with vanilla JS.
Calls the REST API endpoints with X-API-Key header.
No auth required to serve the page itself — API calls
are authenticated via the key entered in the UI.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from dataconnect.config import PROJECT_NAME

logger = logging.getLogger(__name__)

router = APIRouter(tags=["web"])


def _build_html() -> str:
    """Build the single-page HTML application.

    Returns:
        Complete HTML string with embedded CSS and JS.
    """
    return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{PROJECT_NAME}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:system-ui,-apple-system,sans-serif;background:#0f172a;
color:#e2e8f0;min-height:100vh;display:flex;flex-direction:column}}
header{{background:#1e293b;padding:1rem 2rem;border-bottom:1px solid #334155}}
header h1{{font-size:1.25rem;color:#38bdf8}}
header span{{font-size:.85rem;color:#94a3b8;margin-left:.5rem}}
.container{{max-width:900px;width:100%;margin:2rem auto;padding:0 1rem;
flex:1}}
.config{{display:grid;grid-template-columns:1fr 1fr;gap:.75rem;
margin-bottom:1.5rem}}
.config label{{font-size:.8rem;color:#94a3b8;display:block;
margin-bottom:.25rem}}
.config input,.config select{{width:100%;padding:.5rem;
background:#1e293b;border:1px solid #334155;border-radius:6px;
color:#e2e8f0;font-size:.9rem}}
.config input:focus,.config select:focus{{outline:none;
border-color:#38bdf8}}
.query-box{{display:flex;gap:.5rem;margin-bottom:1.5rem}}
.query-box input{{flex:1;padding:.75rem;background:#1e293b;
border:1px solid #334155;border-radius:6px;color:#e2e8f0;
font-size:1rem}}
.query-box button{{padding:.75rem 1.5rem;background:#0ea5e9;
color:#fff;border:none;border-radius:6px;font-weight:600;
cursor:pointer;font-size:.9rem;white-space:nowrap}}
.query-box button:hover{{background:#0284c7}}
.query-box button:disabled{{background:#475569;cursor:not-allowed}}
#status{{text-align:center;color:#94a3b8;padding:2rem;
font-size:.9rem}}
.result{{background:#1e293b;border-radius:8px;padding:1.5rem;
margin-bottom:1rem}}
.result h2{{font-size:1rem;color:#38bdf8;margin-bottom:.75rem}}
.sql-block{{background:#0f172a;padding:1rem;border-radius:6px;
font-family:monospace;font-size:.85rem;white-space:pre-wrap;
overflow-x:auto;margin-bottom:1rem;border:1px solid #334155}}
.confidence{{display:inline-block;padding:.25rem .75rem;
border-radius:12px;font-size:.8rem;font-weight:600}}
.conf-HIGH{{background:#065f46;color:#6ee7b7}}
.conf-MEDIUM{{background:#713f12;color:#fcd34d}}
.conf-LOW{{background:#7c2d12;color:#fdba74}}
.conf-UNVERIFIED{{background:#7f1d1d;color:#fca5a5}}
.checks{{margin-top:1rem}}
.check-item{{display:flex;gap:.5rem;padding:.35rem 0;
font-size:.85rem;border-bottom:1px solid #1e293b}}
.check-status{{font-weight:600;width:5rem;text-align:center;
flex-shrink:0}}
.st-passed{{color:#6ee7b7}}.st-warning{{color:#fcd34d}}
.st-failed{{color:#fca5a5}}.st-skipped{{color:#94a3b8}}
.meta{{display:flex;gap:1.5rem;font-size:.8rem;color:#94a3b8;
margin-top:.75rem;flex-wrap:wrap}}
.error{{background:#7f1d1d;color:#fca5a5;padding:1rem;
border-radius:8px;margin-bottom:1rem}}
footer{{text-align:center;padding:1rem;font-size:.75rem;color:#475569}}
</style>
</head>
<body>
<header>
<h1>{PROJECT_NAME}</h1>
<span>Query databases in plain English with verified SQL</span>
</header>
<div class="container">
<div class="config">
<div>
<label for="apiKey">Server API Key (X-API-Key)</label>
<input type="password" id="apiKey" placeholder="Enter server API key">
</div>
<div>
<label for="dbSelect">Database</label>
<select id="dbSelect"><option value="">— load databases —</option>
</select>
</div>
<div>
<label for="llmModel">LLM Model</label>
<input type="text" id="llmModel" placeholder="e.g. gpt-4o"
 value="gpt-4o">
</div>
<div>
<label for="llmKey">LLM API Key</label>
<input type="password" id="llmKey"
 placeholder="Your LLM provider API key">
</div>
</div>
<div class="query-box">
<input type="text" id="question"
 placeholder="Ask a question about your database..."
 autocomplete="off">
<button id="askBtn" onclick="askQuestion()">Ask</button>
</div>
<div id="status">Enter your API key and click a database to start.</div>
<div id="results"></div>
</div>
<footer>{PROJECT_NAME} &mdash; verified natural-language SQL</footer>
<script>
const API=window.location.origin;
function hdr(){{return{{"X-API-Key":document.getElementById("apiKey").value,
"Content-Type":"application/json"}}}}
async function loadDatabases(){{
const key=document.getElementById("apiKey").value;
if(!key)return;
try{{
const r=await fetch(API+"/databases",{{headers:hdr()}});
if(!r.ok)throw new Error((await r.json()).detail||r.statusText);
const d=await r.json();
const sel=document.getElementById("dbSelect");
sel.innerHTML="<option value=\\"\\">\u2014 select database \u2014</option>";
d.databases.forEach(function(name){{
const o=document.createElement("option");o.value=name;
o.textContent=name;sel.appendChild(o)}});
document.getElementById("status").textContent=
d.count+" database"+(d.count!==1?"s":"")+" available.";
}}catch(e){{showError(e.message)}}
}}
document.getElementById("apiKey").addEventListener("change",loadDatabases);
async function askQuestion(){{
const q=document.getElementById("question").value.trim();
const db=document.getElementById("dbSelect").value;
const model=document.getElementById("llmModel").value.trim();
const llmKey=document.getElementById("llmKey").value.trim();
const apiKey=document.getElementById("apiKey").value;
if(!apiKey)return showError("Server API key is required.");
if(!db)return showError("Select a database first.");
if(!q)return showError("Enter a question.");
if(!model)return showError("Enter an LLM model ID.");
if(!llmKey)return showError("Enter your LLM API key.");
const btn=document.getElementById("askBtn");
btn.disabled=true;btn.textContent="Working...";
document.getElementById("status").textContent="Generating SQL...";
document.getElementById("results").innerHTML="";
try{{
const r=await fetch(API+"/ask",{{method:"POST",headers:hdr(),
body:JSON.stringify({{question:q,database_name:db,model:model,
llm_api_key:llmKey,retry:true}})}});
if(!r.ok)throw new Error((await r.json()).detail||r.statusText);
const d=await r.json();renderResult(d);
document.getElementById("status").textContent="";
}}catch(e){{showError(e.message)}}
finally{{btn.disabled=false;btn.textContent="Ask"}}
}}
document.getElementById("question").addEventListener("keydown",
function(e){{if(e.key==="Enter")askQuestion()}});
function renderResult(d){{
const el=document.getElementById("results");
let checks="";d.checks.forEach(function(c){{
const cls="st-"+c.status;
checks+='<div class="check-item"><span class="check-status '+cls
+'">'+c.status.toUpperCase()+'</span><span>'+esc(c.check_name)
+(c.message?" \u2014 "+esc(c.message):"")+'</span></div>'}});
el.innerHTML='<div class="result"><h2>Generated SQL</h2>'
+'<div class="sql-block">'+esc(d.sql)+'</div>'
+'<span class="confidence conf-'+d.confidence_label+'">'
+d.confidence_label+" "+d.confidence_score.toFixed(0)+"%</span>"
+(d.is_verified?""
:' <span style="color:#fca5a5;font-size:.8rem">(unverified)</span>')
+'<div class="meta"><span>Tables: '+d.selected_tables.join(", ")
+'</span><span>Attempts: '+d.attempt_number
+'</span><span>Time: '+d.execution_time_ms.toFixed(0)+'ms</span>'
+'</div><div class="checks"><h2>Verification Checks</h2>'
+checks+'</div></div>'}}
function showError(msg){{
document.getElementById("status").textContent="";
document.getElementById("results").innerHTML=
'<div class="error">'+esc(msg)+'</div>'}}
function esc(s){{const d=document.createElement("div");
d.textContent=s;return d.innerHTML}}
</script>
</body>
</html>"""


@router.get(
    "/ui",
    response_class=HTMLResponse,
    summary="Web query interface.",
    include_in_schema=False,
)
def web_ui() -> HTMLResponse:
    """Serve the single-page web query interface.

    No authentication required — the page itself is public.
    API calls from the browser include X-API-Key header.
    """
    return HTMLResponse(content=_build_html())
