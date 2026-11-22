package api

import (
	"crypto/sha256"
	"encoding/base64"
)

// The WebSocket demo page (/ws-demo) sends notifications through the API and follows them live
// over /v1/ws (B-4). It is served from the API's origin, which the WebSocket origin check
// requires of browsers. The script is inline and allowed by its hash; nothing else may run.
const (
	demoScript = `
const $ = (id) => document.getElementById(id);
const rows = new Map(); // notification id -> {status, at}
let ws;

function log(kind, text) {
  const li = document.createElement("li");
  li.className = kind;
  li.textContent = new Date().toLocaleTimeString() + "  " + text;
  $("log").prepend(li);
}

function render(e) {
  const prev = rows.get(e.notificationId);
  if (prev && Date.parse(prev.at) > Date.parse(e.at)) return; // events may arrive out of order; keep the latest
  rows.set(e.notificationId, e);
  let tr = document.querySelector('tr[data-id="' + e.notificationId + '"]');
  if (!tr) {
    tr = document.createElement("tr");
    tr.dataset.id = e.notificationId;
    for (let i = 0; i < 5; i++) tr.appendChild(document.createElement("td"));
    $("rows").prepend(tr);
  }
  const c = tr.children;
  c[0].textContent = e.notificationId.slice(-12);
  c[0].title = e.notificationId;
  c[1].textContent = e.channel + " / " + e.priority;
  c[2].innerHTML = "";
  const badge = document.createElement("span");
  badge.className = "badge " + e.status;
  badge.textContent = e.status;
  c[2].appendChild(badge);
  c[3].textContent = e.attempts;
  c[4].textContent = e.lastError || "";
}

function connect() {
  ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/v1/ws");
  ws.onopen = () => { $("state").textContent = "connected"; $("state").className = "ok"; resubscribe(); };
  ws.onclose = (ev) => {
    $("state").textContent = "disconnected (" + ev.code + "), reconnecting…";
    $("state").className = "bad";
    setTimeout(connect, 1000);
  };
  ws.onmessage = (ev) => {
    const m = JSON.parse(ev.data);
    if (m.type === "status") { render(m.event); log("status", (m.snapshot ? "snapshot " : "event ") + m.event.notificationId.slice(-12) + " → " + m.event.status); }
    else if (m.type === "batch") log("batch", "batch " + m.batch.id.slice(-12) + " " + JSON.stringify(m.batch.statusCounts));
    else if (m.type === "subscribed") log("info", "following " + m.notificationIds.length + " notifications, " + m.batchIds.length + " batches");
    else if (m.type === "error") log("error", m.code + ": " + m.message + (m.id ? " (" + m.id + ")" : ""));
  };
}

const followed = { notificationIds: new Set(), batchIds: new Set() };
function subscribe(kind, id) {
  followed[kind].add(id);
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "subscribe", [kind]: [id] }));
}
function resubscribe() {
  const n = [...followed.notificationIds], b = [...followed.batchIds];
  if (n.length || b.length) ws.send(JSON.stringify({ type: "subscribe", notificationIds: n, batchIds: b }));
}

$("send").onclick = async () => {
  const body = { recipient: $("recipient").value, channel: $("channel").value, content: $("content").value, priority: $("priority").value };
  const res = await fetch("/v1/notifications", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const out = await res.json();
  if (!res.ok) { log("error", res.status + " " + JSON.stringify(out.error)); return; }
  log("info", "created " + out.id);
  subscribe("notificationIds", out.id);
};
$("follow").onclick = () => {
  const id = $("id").value.trim();
  if (id) subscribe($("kind").value, id);
};
$("channel").onchange = () => {
  $("recipient").value = { sms: "+905551234567", email: "user@example.com", push: "fcm-token-123456" }[$("channel").value];
};
connect();
`
	demoPage = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Live Status Demo</title>
  <style>
    :root { color-scheme: light dark; --fg: #1d1d1f; --muted: #6e6e73; --bg: #fafafa; --card: #fff; --line: #e5e5ea; }
    @media (prefers-color-scheme: dark) { :root { --fg: #f5f5f7; --muted: #a1a1a6; --bg: #111; --card: #1c1c1e; --line: #2c2c2e; } }
    body { margin: 0; font: 14px/1.45 system-ui, sans-serif; color: var(--fg); background: var(--bg); }
    main { max-width: 960px; margin: 0 auto; padding: 24px 16px; }
    h1 { font-size: 20px; margin: 0 0 4px; }
    p { color: var(--muted); margin: 0 0 16px; }
    section { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 14px; margin-bottom: 14px; }
    .row { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
    input, select, button { font: inherit; padding: 6px 8px; border-radius: 6px; border: 1px solid var(--line); background: var(--bg); color: var(--fg); }
    input.wide { flex: 1 1 220px; }
    button { cursor: pointer; background: #0a84ff; border-color: #0a84ff; color: #fff; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 6px 4px; border-bottom: 1px solid var(--line); font-variant-numeric: tabular-nums; }
    th { color: var(--muted); font-weight: 500; }
    .badge { padding: 2px 8px; border-radius: 999px; font-size: 12px; background: var(--line); }
    .sent { background: #30d15833; } .failed { background: #ff453a33; } .retrying { background: #ff9f0a33; }
    .processing, .queued { background: #0a84ff33; } .cancelled { background: #8e8e9333; }
    #state.ok { color: #30d158; } #state.bad { color: #ff453a; }
    ul { list-style: none; margin: 0; padding: 0; max-height: 260px; overflow: auto; font: 12px/1.6 ui-monospace, monospace; }
    li.error { color: #ff453a; } li.batch { color: #bf5af2; } li.info { color: var(--muted); }
  </style>
</head>
<body>
<main>
  <h1>Live notification status</h1>
  <p>WebSocket <code>/v1/ws</code>: <span id="state">connecting…</span></p>
  <section>
    <div class="row">
      <select id="channel"><option>sms</option><option>email</option><option>push</option></select>
      <input id="recipient" value="+905551234567" aria-label="recipient">
      <input id="content" class="wide" value="Hello from the live demo" aria-label="content">
      <select id="priority"><option>high</option><option selected>normal</option><option>low</option></select>
      <button id="send">Send &amp; follow</button>
    </div>
  </section>
  <section>
    <div class="row">
      <select id="kind"><option value="notificationIds">notification id</option><option value="batchIds">batch id</option></select>
      <input id="id" class="wide" placeholder="UUID" aria-label="id to follow">
      <button id="follow">Follow</button>
    </div>
  </section>
  <section>
    <table><thead><tr><th>Notification</th><th>Channel</th><th>Status</th><th>Attempts</th><th>Last error</th></tr></thead>
    <tbody id="rows"></tbody></table>
  </section>
  <section><ul id="log"></ul></section>
</main>
<script>` + demoScript + `</script>
</body>
</html>
`
)

var demoCSP = func() string {
	sum := sha256.Sum256([]byte(demoScript))
	return "default-src 'none'; script-src 'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'; " +
		"style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
}()
