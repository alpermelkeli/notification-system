// Notification pipeline playground. Plain JavaScript, no libraries: the map is hand-built SVG,
// positions come from the status events of /v1/ws, counters from /playground/api/stats.
"use strict";

const SVGNS = "http://www.w3.org/2000/svg";
const $ = (sel, root = document) => root.querySelector(sel);
const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const fmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const fmt1 = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });

const CHANNELS = ["sms", "email", "push"];
const PRIORITIES = ["high", "normal", "low"];
const CHANNEL_NAME = { sms: "SMS", email: "Email", push: "Push" };
const TERMINAL = new Set(["sent", "failed", "cancelled"]);

// ------------------------------------------------------------------ geometry
// Three channel lines share the trunk (API, PostgreSQL, dispatcher) a few pixels apart, split
// into their own lanes at Kafka and meet again at the provider, like lines on a transit map.
const TRUNK = 270;
const OFFSET = { sms: -9, email: 0, push: 9 };
const LANE = { sms: 110, email: 270, push: 430 };
const X = { api: 70, pg: 210, disp: 370, bend: 392, kafka: 615, worker: 745, drop: 812, limiter: 860, conv: 876, provider: 1040 };
const FAIL_Y = 515;
const DLQ_X = 940;
const CANCEL_Y = 50;
const KAFKA_COL = { high: -42, normal: 0, low: 42 };

function points(ch) {
  const t = TRUNK + OFFSET[ch];
  const lane = LANE[ch];
  const d = Math.abs(lane - t);
  return {
    api: [X.api, t], pg: [X.pg, t], disp: [X.disp, t], bend: [X.bend, t], k0: [X.bend + d, lane],
    kafka: [X.kafka, lane], worker: [X.worker, lane], drop: [X.drop, lane], limiter: [X.limiter, lane],
    conv: [X.conv, lane], c1: [X.conv + d, t], provider: [X.provider, t],
    failDown: [X.drop, FAIL_Y], failLeft: [X.pg, FAIL_Y], pgBelow: [X.pg, TRUNK + 34],
    dlq: [DLQ_X, FAIL_Y], cancel: [X.pg, CANCEL_Y], pgAbove: [X.pg, TRUNK - 30],
  };
}

// Where a notification in a status waits, as a station key; dots stack around its anchor.
function stationOf(n) {
  switch (n.status) {
    case "pending": case "retrying": return "pg";
    case "queued": return `kafka:${n.channel}:${n.priority}`;
    case "processing": return `limiter:${n.channel}`;
    case "sent": return "provider";
    case "failed": return "dlq";
    case "cancelled": return "cancel";
    default: return "pg";
  }
}

function anchorOf(key, ch) {
  const [kind, kch, pri] = key.split(":");
  const p = points(kch || ch);
  switch (kind) {
    case "pg": return [X.pg, TRUNK];
    case "kafka": return [X.kafka + KAFKA_COL[pri], LANE[kch] + 9];
    case "limiter": return p.limiter;
    case "provider": return [X.provider, TRUNK];
    case "dlq": return [DLQ_X, FAIL_Y];
    case "cancel": return [X.pg, CANCEL_Y];
    default: return p.api;
  }
}

// The path a notification takes for a status change, through the named points of its line.
// Changes can be skipped (an event lost or overtaken), so the path always runs along the line
// from where the dot is to where it goes, never straight across the map.
const MAIN = ["api", "pg", "disp", "bend", "k0", "kafka", "worker", "limiter"];
const MAIN_INDEX = { pending: 1, retrying: 1, queued: 5, processing: 7 };

function routeFor(prev, next, ch) {
  const p = points(ch);
  if (!prev) return next === "pending" ? [p.api, p.pg] : [];
  if (next === "pending" && prev === "failed") return [p.dlq, p.failLeft, p.pgBelow, p.pg];
  if (next === "cancelled") {
    const at = MAIN_INDEX[prev];
    if (at === undefined) return [p.cancel];
    return [...MAIN.slice(1, at + 1).reverse().map((k) => p[k]), p.pgAbove, p.cancel];
  }
  if (next === "queued" && prev === "processing") return [p.limiter, p.worker, p.kafka];
  const from = MAIN_INDEX[prev];
  if (from === undefined) return []; // from a terminus: jump
  const tails = {
    pending: [], retrying: [], queued: [], processing: [],
    sent: [p.conv, p.c1, p.provider],
    failed: [p.drop, p.failDown, p.dlq],
  };
  if (next === "retrying" && prev !== "pending" && prev !== "retrying") {
    return [...MAIN.slice(from, 8).map((k) => p[k]), p.drop, p.failDown, p.failLeft, p.pgBelow, p.pg];
  }
  const to = next in MAIN_INDEX ? MAIN_INDEX[next] : 7;
  if (!(next in tails)) return [];
  return [...MAIN.slice(from, Math.max(from, to) + 1).map((k) => p[k]), ...tails[next]];
}

// ------------------------------------------------------------------ svg helpers
function svg(tag, attrs = {}, parent = null, text = null) {
  const node = document.createElementNS(SVGNS, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, String(v));
  if (text !== null) node.textContent = text;
  if (parent) parent.appendChild(node);
  return node;
}
const pts = (list) => list.map(([x, y]) => `${x},${y}`).join(" ");

// ------------------------------------------------------------------ map drawing
const map = $("#map");
const layers = {};
const stationNodes = new Map(); // station info key -> <g>
const live = {}; // id -> <text> updated from metrics

function drawMap() {
  map.textContent = "";
  for (const name of ["tracks", "traces", "stations", "dots"]) layers[name] = svg("g", { class: name }, map);
  const tracks = layers.tracks;

  // Failure track: provider errors drop at the worker, then loop back left for a retry or run
  // right into the dead letters. Cancelled rows leave PostgreSQL upwards.
  svg("polyline", { class: "fail-track", points: pts([[X.drop, LANE.sms], [X.drop, FAIL_Y]]) }, tracks);
  svg("polyline", { class: "fail-track", points: pts([[X.pg, TRUNK + 34], [X.pg, FAIL_Y], [DLQ_X - 12, FAIL_Y]]) }, tracks);
  svg("polyline", { class: "fail-track", points: pts([[X.pg, TRUNK - 30], [X.pg, CANCEL_Y + 12]]) }, tracks);
  svg("polygon", { class: "arrow", points: pts([[X.pg - 6, TRUNK + 42], [X.pg + 6, TRUNK + 42], [X.pg, TRUNK + 31]]) }, tracks);
  svg("text", { class: "edge", x: X.drop + 10, y: 482 }, tracks, "provider error");
  svg("text", { class: "edge", x: (X.pg + X.drop) / 2, y: FAIL_Y - 10, "text-anchor": "middle" }, tracks, "retry after backoff, back to PostgreSQL");
  svg("text", { class: "edge", x: X.pg + 12, y: 148 }, tracks, "cancel");

  for (const ch of CHANNELS) {
    const p = points(ch);
    svg("polyline", {
      class: `line ${ch}`,
      points: pts([p.api, p.pg, p.disp, p.bend, p.k0, p.kafka, p.worker, p.limiter, p.conv, p.c1, p.provider]),
    }, tracks);
  }
  // Edge labels on the shared trunk and on the middle (straight) lane.
  svg("text", { class: "edge", x: (X.api + X.pg) / 2, y: TRUNK - 20, "text-anchor": "middle" }, tracks, "insert row");
  svg("text", { class: "edge", x: (X.pg + X.disp) / 2 + 10, y: TRUNK - 20, "text-anchor": "middle" }, tracks, "notify, poll");
  svg("text", { class: "edge", x: 482, y: LANE.email - 12, "text-anchor": "middle" }, tracks, "publish id");
  svg("text", { class: "edge", x: 962, y: LANE.email - 12, "text-anchor": "middle" }, tracks, "POST, 202");

  const st = layers.stations;
  // Trunk stations: interchange capsules spanning the three lines.
  trunkStation(st, "api", X.api, "API", "middle", [["api-rate", "0 created/s"]]);
  trunkStation(st, "pg", X.pg, "PostgreSQL", "start", [["pg-pending", "0 pending"], ["pg-retrying", "0 waiting to retry"]]);
  trunkStation(st, "disp", X.disp, "Dispatcher", "middle", [["disp-count", "relay and reaper"]]);
  trunkStation(st, "provider", X.provider, "Provider", "middle", [["provider-total", "0 delivered"]]);

  for (const ch of CHANNELS) {
    const lane = LANE[ch];
    // Kafka: one box per channel with a column per priority topic.
    const g = station(st, `kafka:${ch}`, `Kafka topics for ${CHANNEL_NAME[ch]}`);
    svg("rect", { class: "hit", x: X.kafka - 72, y: lane - 50, width: 144, height: 110 }, g);
    svg("rect", { class: "box", x: X.kafka - 64, y: lane - 28, width: 128, height: 56, rx: 9 }, g);
    pulseRing(g, X.kafka, lane, 48);
    svg("text", { class: "topic", x: X.kafka, y: lane - 37, "text-anchor": "middle" }, g, `notifications.${ch}.*`);
    for (const pri of PRIORITIES) {
      const x = X.kafka + KAFKA_COL[pri];
      svg("text", { class: "sub", x, y: lane - 11, "text-anchor": "middle" }, g, pri);
      if (pri !== "low") svg("line", { class: "lane-tick", x1: x + 21, x2: x + 21, y1: lane - 22, y2: lane + 22 }, g);
    }
    live[`lag-${ch}`] = svg("text", { class: "sub", x: X.kafka, y: lane + 46, "text-anchor": "middle" }, g, "backlog 0");

    // Worker: a stop on the lane, with its circuit breaker lamp.
    const w = station(st, `worker:${ch}`, `${CHANNEL_NAME[ch]} worker`);
    svg("rect", { class: "hit", x: X.worker - 40, y: lane - 40, width: 80, height: 80 }, w);
    svg("circle", { class: "stop", cx: X.worker, cy: lane, r: 10 }, w);
    pulseRing(w, X.worker, lane, 16);
    svg("text", { class: `line-name ${ch}`, x: X.worker, y: lane - 20, "text-anchor": "middle" }, w, `${CHANNEL_NAME[ch]} worker`);
    live[`lamp-${ch}`] = svg("circle", { class: "lamp", cx: X.worker - 38, cy: lane + 26, r: 5, "data-state": "closed" }, w);
    live[`breaker-${ch}`] = svg("text", { class: "sub", x: X.worker - 28, y: lane + 30 }, w, "breaker closed");

    // Rate limiter: a square gate in front of the provider.
    const r = station(st, `limiter:${ch}`, `Rate limiter for ${CHANNEL_NAME[ch]}`);
    svg("rect", { class: "hit", x: X.limiter - 40, y: lane - 40, width: 80, height: 80 }, r);
    svg("rect", { class: "stop", x: X.limiter - 9, y: lane - 9, width: 18, height: 18, rx: 3 }, r);
    pulseRing(r, X.limiter, lane, 16);
    svg("text", { class: "label", x: X.limiter, y: lane - 20, "text-anchor": "middle" }, r, "Limiter");
    live[`rate-${ch}`] = svg("text", { class: "sub", x: X.limiter, y: lane + 30, "text-anchor": "middle" }, r, "0 of 100/s");
  }

  // Termini.
  const dlq = station(st, "dlq", "Dead letters");
  svg("rect", { class: "hit", x: DLQ_X - 20, y: FAIL_Y - 24, width: 130, height: 48 }, dlq);
  svg("rect", { class: "terminus", x: DLQ_X - 9, y: FAIL_Y - 9, width: 18, height: 18, rx: 3 }, dlq);
  pulseRing(dlq, DLQ_X, FAIL_Y, 16);
  svg("text", { class: "label", x: DLQ_X + 18, y: FAIL_Y - 2 }, dlq, "Dead letters");
  live.dlq = svg("text", { class: "sub", x: DLQ_X + 18, y: FAIL_Y + 15 }, dlq, "0 published");

  const cancel = station(st, "cancel", "Cancelled");
  svg("rect", { class: "hit", x: X.pg - 20, y: CANCEL_Y - 22, width: 120, height: 44 }, cancel);
  svg("rect", { class: "terminus", x: X.pg - 9, y: CANCEL_Y - 9, width: 18, height: 18, rx: 3 }, cancel);
  pulseRing(cancel, X.pg, CANCEL_Y, 16);
  svg("text", { class: "label", x: X.pg + 18, y: CANCEL_Y + 5 }, cancel, "Cancelled");

  const fail = station(st, "fail", "Failure track");
  svg("rect", { class: "hit", x: X.drop - 8, y: 455, width: 120, height: 40 }, fail);
  svg("rect", { class: "hit", x: 330, y: FAIL_Y - 28, width: 300, height: 34 }, fail);
}

function station(parent, key, label) {
  const g = svg("g", { class: "station", tabindex: 0, role: "button", "aria-label": `${label}: show what it does`, "data-station": key }, parent);
  stationNodes.set(key, g);
  g.addEventListener("pointerenter", () => showStation(key));
  g.addEventListener("focus", () => showStation(key));
  g.addEventListener("click", () => showStation(key, true));
  g.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); showStation(key, true); } });
  return g;
}

function pulseRing(g, cx, cy, r) {
  svg("circle", { class: "pulse", cx, cy, r }, g);
}

function trunkStation(parent, key, x, label, anchor, subs) {
  const g = station(parent, key, label);
  const top = TRUNK + OFFSET.sms - 14;
  const height = OFFSET.push - OFFSET.sms + 28;
  svg("rect", { class: "hit", x: x - 30, y: top - 12, width: anchor === "start" ? 140 : 60, height: height + 90 }, g);
  svg("rect", { class: "capsule", x: x - 12, y: top, width: 24, height, rx: 12 }, g);
  pulseRing(g, x, TRUNK, 26);
  const tx = anchor === "start" ? x + 18 : x;
  const y0 = anchor === "start" ? TRUNK + 36 : TRUNK + 46;
  svg("text", { class: "label", x: tx, y: y0, "text-anchor": anchor }, g, label);
  subs.forEach(([id, text], i) => {
    live[id] = svg("text", { class: "sub", x: tx, y: y0 + 17 + i * 16, "text-anchor": anchor }, g, text);
  });
}

// ------------------------------------------------------------------ station info
const STATIONS = {
  api: {
    title: "API",
    text: "Validates the request (channel rules, idempotency key, template) and inserts the row in one transaction. It answers 202 once the row is committed. It never sends work to Kafka itself.",
    facts: () => ["POST /v1/notifications", "POST /v1/notifications/batch, up to 1000 at once", `${fmt1.format(stats.createdPerSecond || 0)} accepted per second now`],
  },
  pg: {
    title: "PostgreSQL",
    text: "The notifications table is the source of truth and the outbox. Scheduled rows and rows waiting for a retry stay here until they are due. Every status change is a guarded update that also writes a history row.",
    facts: () => ["pending", "queued", "processing", "retrying"].map((s) => `${fmt.format(stat(stats.notifications, s))} ${s}`),
  },
  disp: {
    title: "Dispatcher",
    text: "Wakes on a PostgreSQL NOTIFY, and polls as a fallback. It locks due rows with FOR UPDATE SKIP LOCKED and publishes their ids to the topic of their channel and priority. Its reaper recovers expired leases and lost messages.",
    facts: () => [`${fmt.format(stat(stats.instances, "dispatcher"))} replicas running`, "Several replicas share the work through SKIP LOCKED"],
  },
  kafka: {
    title: (ch) => `Kafka topics for ${CHANNEL_NAME[ch]}`,
    text: () => "Nine topics, one per channel and priority, carry only the notification id (a claim-check). The row in PostgreSQL stays the source of truth. The numbers are each topic's backlog.",
    facts: (ch) => PRIORITIES.map((p) => `notifications.${ch}.${p}: ${fmt.format(stat(stats.kafkaLag, `notifications.${ch}.${p}`))} waiting`),
  },
  worker: {
    title: (ch) => `${CHANNEL_NAME[ch]} worker`,
    text: () => "Reads the three priority topics into heaps and picks high priority first, with aging so low priority is never starved. It claims the row with a lease before sending, so a duplicate record is skipped. A circuit breaker stops calling a provider that keeps failing.",
    facts: (ch) => [`Circuit breaker ${(stats.breakers && stats.breakers[ch] || "closed").replace("_", "-")}`, `${fmt.format(stat(stats.instances, "worker"))} worker replicas running`],
  },
  limiter: {
    title: (ch) => `Rate limiter for ${CHANNEL_NAME[ch]}`,
    text: () => "A token bucket (GCRA) in Redis, shared by every worker replica, keeps each channel at 100 messages per second. Adding workers adds no extra load on the provider.",
    facts: (ch) => [`${fmt1.format(stat(stats.deliveredPerSecond, ch))} delivered per second now`, "Limit: 100 per second"],
  },
  provider: {
    title: "Provider",
    text: "A local stand-in for webhook.site. It answers 202 with a message id, and can be told to answer 500, 429 or 400 so retries, the circuit breaker and dead letters can be seen.",
    facts: () => CHANNELS.map((ch) => `${CHANNEL_NAME[ch]}: ${fmt.format(stat(stats.providerCalls && stats.providerCalls[ch], "delivered"))} delivered`),
  },
  fail: {
    title: "Failure track",
    text: "A 500 or 429 from the provider schedules a retry with exponential backoff and jitter, and the row goes back to PostgreSQL until it is due. A 400, or the last allowed attempt, ends as failed with a dead letter.",
    facts: () => ["Up to 5 attempts", "First retry after 2.5 to 5 seconds, doubling each time"],
  },
  dlq: {
    title: "Dead letters",
    text: "Failed notifications keep their last error, and a copy goes to the notifications.dlq topic. POST /v1/notifications/{id}/retry replays one with a fresh attempt budget.",
    facts: () => [`${fmt.format(stats.deadLetters || 0)} dead letters published since start`],
  },
  cancel: {
    title: "Cancelled",
    text: "A pending, queued or retrying notification can be cancelled. A worker that already holds its Kafka record sees the status when it claims the row and skips it.",
    facts: () => ["POST /v1/notifications/{id}/cancel"],
  },
};

let pinnedStation = null;
function showStation(key, pin = false) {
  if (pin) pinnedStation = key;
  else if (pinnedStation && pinnedStation !== key) return;
  const [kind, ch] = key.split(":");
  const info = STATIONS[kind];
  if (!info) return;
  const val = (v) => (typeof v === "function" ? v(ch) : v);
  const body = $("#station-body");
  body.textContent = "";
  const h = document.createElement("h3");
  h.textContent = val(info.title);
  const p = document.createElement("p");
  p.textContent = val(info.text);
  const ul = document.createElement("ul");
  ul.className = "facts";
  for (const f of info.facts(ch)) {
    const li = document.createElement("li");
    li.textContent = f;
    ul.appendChild(li);
  }
  body.append(h, p, ul);
  shownStation = key;
}
let shownStation = null;
map.addEventListener("pointerleave", () => { if (!pinnedStation) shownStation = null; });

// ------------------------------------------------------------------ notifications and dots
const notes = new Map(); // id -> notification state
const occupancy = new Map(); // station key -> Set of slot numbers in use
const listeners = new Set(); // called with every changed notification
const MAX_DOTS = 180;
let dotCount = 0;
let selectedId = null;

function takeSlot(key) {
  let used = occupancy.get(key);
  if (!used) occupancy.set(key, (used = new Set()));
  let i = 0;
  while (used.has(i)) i++;
  used.add(i);
  return i;
}
function freeSlot(key, i) {
  const used = occupancy.get(key);
  if (used) used.delete(i);
}

// Slot i of a station sits on a small golden-angle spiral around its anchor, so a backlog
// reads as a growing pile. Kafka columns are narrow: the pile grows downwards there.
function slotPosition(key, i, ch) {
  const [ax, ay] = anchorOf(key, ch);
  if (i === 0) return [ax, ay];
  if (key.startsWith("kafka")) {
    const col = i % 3, row = Math.floor(i / 3);
    return [ax + (col - 1) * 9, ay + Math.min(row, 2) * 7 - 7];
  }
  const r = 7 * Math.sqrt(i);
  const a = i * 2.39996;
  return [ax + r * Math.cos(a), ay + r * Math.sin(a)];
}

function track(nj, { dot = true } = {}) {
  let n = notes.get(nj.id);
  if (n) return n;
  n = {
    id: nj.id, batchId: nj.batchId || null, channel: nj.channel, priority: nj.priority,
    status: null, attempts: 0, at: 0, lastError: null, nextAttemptAt: null,
    recipient: nj.recipient, content: nj.content, scheduledAt: nj.scheduledAt || null,
    wantDot: dot, dot: null, label: nj.label || null,
  };
  notes.set(n.id, n);
  applyEvent({
    notificationId: nj.id, batchId: nj.batchId, channel: nj.channel, priority: nj.priority,
    status: nj.status, attempts: nj.attempts || 0, at: nj.updatedAt || nj.createdAt, lastError: nj.lastError,
    nextAttemptAt: nj.nextAttemptAt,
  });
  return n;
}

function applyEvent(e) {
  const n = notes.get(e.notificationId);
  if (!n) return; // not something this page follows
  const at = Date.parse(e.at) || Date.now();
  // Events may arrive out of order (ADR-0011): the latest transition time wins.
  if (n.status && (at < n.at || (at === n.at && e.status === n.status))) return;
  const prev = n.status;
  n.status = e.status;
  n.attempts = e.attempts;
  n.at = at;
  n.lastError = e.lastError || null;
  n.nextAttemptAt = e.nextAttemptAt || null;
  if (prev === n.status) return;
  n.seen = (n.seen || 0) + 1; // the step number of this change in the notification's history
  moveDot(n, prev);
  for (const fn of listeners) fn(n);
  if (n.id === selectedId) scheduleStepsRefresh();
  refreshFollowList();
}

function moveDot(n, prev) {
  if (!n.dot && n.wantDot && dotCount < MAX_DOTS && n.status && !(TERMINAL.has(n.status) && !prev)) {
    const [x, y] = prev ? anchorOf(stationOf({ ...n, status: prev }), n.channel) : points(n.channel).api;
    const el = svg("circle", { class: `dot ${n.channel}`, r: 5, cx: x, cy: y }, layers.dots);
    el.addEventListener("click", () => select(n.id));
    const title = svg("title", {}, el);
    n.dot = { el, title, x, y, queue: [], anim: null, key: null, slot: -1 };
    dotCount++;
  }
  if (!n.dot) return;
  const d = n.dot;
  d.el.setAttribute("class", `dot ${n.channel} ${n.status}${n.id === selectedId ? " selected" : ""}`);
  d.title.textContent = `${CHANNEL_NAME[n.channel]} ${n.priority}, ${n.status}`;
  if (d.key !== null) freeSlot(d.key, d.slot);
  d.key = stationOf(n);
  d.slot = takeSlot(d.key);
  const target = slotPosition(d.key, d.slot, n.channel);
  const route = routeFor(prev, n.status, n.channel);
  d.queue.push({ route: [...route, target], status: n.status, prev, no: n.seen, station: d.key, attempts: n.attempts, lastError: n.lastError, at: n.at });
  if (!d.anim) startNext(n);
}

const animating = new Set();
function startNext(n) {
  const d = n.dot;
  const step = d.queue.shift();
  if (!step) {
    d.anim = null;
    animating.delete(n);
    return;
  }
  const path = [[d.x, d.y], ...step.route];
  const lengths = [];
  let total = 0;
  for (let i = 1; i < path.length; i++) {
    const l = Math.hypot(path[i][0] - path[i - 1][0], path[i][1] - path[i - 1][1]);
    lengths.push(l);
    total += l;
  }
  // A dot that is behind (several changes queued) catches up faster. The followed dot moves
  // slower, so each step can be read as it happens.
  const followed = n.id === selectedId;
  const speed = (followed ? 0.35 : 0.5) * (1 + d.queue.length * (followed ? 0.25 : 1.5)); // px per ms
  const dur = reduceMotion ? 0 : Math.min(followed ? 2200 : 1600, Math.max(followed ? 900 : 350, total / speed));
  if (followed) narrate(n, step);
  d.anim = { path, lengths, total, start: performance.now(), dur, step };
  if (n.id === selectedId && !reduceMotion) drawTrace(n.channel, path);
  animating.add(n);
  if (!frameRequested) { frameRequested = true; requestAnimationFrame(frame); }
}

let frameRequested = false;
function frame(now) {
  frameRequested = false;
  for (const n of animating) {
    const d = n.dot;
    const a = d.anim;
    if (!a) continue;
    const t = a.dur === 0 ? 1 : Math.min(1, (now - a.start) / a.dur);
    const eased = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
    let dist = eased * a.total;
    let i = 0;
    while (i < a.lengths.length - 1 && dist > a.lengths[i]) { dist -= a.lengths[i]; i++; }
    const [x0, y0] = a.path[i];
    const [x1, y1] = a.path[i + 1] || a.path[i];
    const f = a.lengths[i] ? Math.min(1, dist / a.lengths[i]) : 1;
    d.x = x0 + (x1 - x0) * f;
    d.y = y0 + (y1 - y0) * f;
    d.el.setAttribute("cx", d.x.toFixed(1));
    d.el.setAttribute("cy", d.y.toFixed(1));
    if (n.id === selectedId) moveHalo(d.x, d.y);
    if (t >= 1) arrived(n, a.step);
  }
  if (animating.size) { frameRequested = true; requestAnimationFrame(frame); }
}

function arrived(n, step) {
  if (n.id === selectedId) {
    pulse(step.station);
    markCurrentStation(step.station);
  }
  // Delivered and cancelled dots fade out at their terminus after a moment.
  if ((step.status === "sent" || step.status === "cancelled") && !n.dot.queue.length) {
    const d = n.dot;
    setTimeout(() => {
      if (n.dot !== d || n.status !== step.status) return;
      if (n.id === selectedId) return; // keep the followed dot visible
      removeDot(n);
    }, step.status === "sent" ? 1400 : 4000);
  }
  startNext(n);
}

function removeDot(n) {
  const d = n.dot;
  if (!d) return;
  d.el.classList.add("gone");
  freeSlot(d.key, d.slot);
  n.dot = null;
  animating.delete(n);
  dotCount--;
  setTimeout(() => d.el.remove(), 650);
}

function stationGroupFor(key) {
  const [kind, ch] = key.split(":");
  return stationNodes.get(kind === "kafka" || kind === "limiter" || kind === "worker" ? `${kind}:${ch}` : kind);
}

function pulse(key) {
  const g = stationGroupFor(key);
  if (!g || reduceMotion) return;
  g.classList.remove("active");
  void g.getBBox(); // restart the animation
  g.classList.add("active");
}

function markCurrentStation(key) {
  for (const g of stationNodes.values()) g.classList.remove("current");
  const g = key && stationGroupFor(key);
  if (g) g.classList.add("current");
}

function drawTrace(ch, path) {
  const line = svg("polyline", { class: `trace ${ch}`, points: pts(path) }, layers.traces);
  setTimeout(() => line.classList.add("fade"), 900);
  setTimeout(() => line.remove(), 2500);
}

// The line above the map tells what the followed notification is doing right now, in step with
// its dot; the matching step in the list below is highlighted.
function narrate(n, step) {
  n.stepNo = step.no;
  const nj = { channel: n.channel, priority: n.priority, scheduledAt: n.scheduledAt, status: n.status, nextAttemptAt: n.nextAttemptAt };
  const { title } = describe({ to: step.status, from: step.prev, attempt: step.attempts, reason: step.lastError, at: new Date(step.at).toISOString() }, nj, false);
  const box = $("#now");
  box.className = `now active ${step.status}`;
  $("#now-step").textContent = `Step ${n.stepNo}`;
  $("#now-text").textContent = title;
  highlightStep(n.stepNo);
}

function highlightStep(no) {
  document.querySelectorAll("#steps li").forEach((li, i) => li.classList.toggle("playing", i === no - 1));
}

let halo = null;
function moveHalo(x, y) {
  if (!halo) halo = svg("circle", { class: "halo", r: 10 }, layers.dots);
  halo.setAttribute("cx", x.toFixed(1));
  halo.setAttribute("cy", y.toFixed(1));
}

// ------------------------------------------------------------------ live status stream
const subs = { notificationIds: new Set(), batchIds: new Set() };
let ws = null;
let wsOpen = false;

function connect() {
  ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/v1/ws`);
  setConn("connecting", "Connecting");
  ws.onopen = () => {
    wsOpen = true;
    setConn("open", "Connected");
    // Finished notifications will not change again; resubscribe only the others.
    sendSubscribe([...subs.notificationIds].filter((id) => !TERMINAL.has(notes.get(id)?.status)), []);
  };
  ws.onclose = () => {
    wsOpen = false;
    outbox.length = 0;
    setConn("closed", "Reconnecting");
    setTimeout(connect, 1500);
  };
  ws.onmessage = (msg) => {
    let m;
    try { m = JSON.parse(msg.data); } catch { return; }
    if (m.type === "status" && m.event) applyEvent(m.event);
    else if (m.type === "error") console.warn(`live updates: ${m.code}: ${m.message}`);
  };
}

function setConn(state, text) {
  const el = $("#conn");
  el.dataset.state = state;
  el.textContent = text;
}

// The server takes at most 100 ids per message and handles 10 messages per second, so messages
// leave through a queue paced at one every 120 ms.
const outbox = [];
let outboxTimer = null;
function sendSubscribe(ids, batchIds, type = "subscribe") {
  if (!wsOpen) return;
  const all = [...ids.map((id) => ["notificationIds", id]), ...batchIds.map((id) => ["batchIds", id])];
  for (let i = 0; i < all.length; i += 100) {
    const msg = { type, notificationIds: [], batchIds: [] };
    for (const [kind, id] of all.slice(i, i + 100)) msg[kind].push(id);
    outbox.push(JSON.stringify(msg));
  }
  flushOutbox();
}
function flushOutbox() {
  if (outboxTimer || !outbox.length) return;
  if (wsOpen) ws.send(outbox.shift());
  outboxTimer = setTimeout(() => { outboxTimer = null; flushOutbox(); }, 120);
}

function follow({ ids = [], batchIds = [] }) {
  ids.forEach((id) => subs.notificationIds.add(id));
  batchIds.forEach((id) => subs.batchIds.add(id));
  sendSubscribe(ids, batchIds);
  // Stay under the server's 1000 subscriptions per connection.
  if (subs.notificationIds.size > 800) {
    const old = [...subs.notificationIds].filter((id) => TERMINAL.has(notes.get(id)?.status)).slice(0, 200);
    old.forEach((id) => subs.notificationIds.delete(id));
    sendSubscribe(old, [], "unsubscribe");
  }
}

// ------------------------------------------------------------------ API
class APIError extends Error {
  constructor(status, body) {
    super(body?.error?.message || `request failed with ${status}`);
    this.status = status;
    this.code = body?.error?.code;
  }
}

async function api(method, path, body, headers = {}) {
  const res = await fetch(path, {
    method,
    headers: body ? { "Content-Type": "application/json", ...headers } : headers,
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = null; }
  if (!res.ok) throw new APIError(res.status, json);
  return { status: res.status, json, headers: res.headers };
}

const toEvent = (nj) => ({
  notificationId: nj.id, batchId: nj.batchId, channel: nj.channel, priority: nj.priority, status: nj.status,
  attempts: nj.attempts, at: nj.updatedAt, lastError: nj.lastError, nextAttemptAt: nj.nextAttemptAt,
});

// Status events are best effort, so anything a scenario waits on is also polled.
async function refreshNote(id) {
  try {
    const { json } = await api("GET", `/v1/notifications/${id}`);
    applyEvent(toEvent(json));
    return json;
  } catch {
    return null;
  }
}

async function send(body, headers) {
  const r = await api("POST", "/v1/notifications", body, headers);
  track(r.json);
  follow({ ids: [r.json.id] });
  return r;
}

// A batch is followed through the ids that get a dot, not through its batch id: the dispatcher
// publishes a whole batch within milliseconds, and a burst of 1000 events at once is more than
// the server queues per connection (ADR-0011 disconnects such a slow consumer). Progress comes
// from polling the batch instead.
async function sendBatch(items, dots = 60) {
  const r = await api("POST", "/v1/notifications/batch", { notifications: items });
  const visible = [];
  r.json.items.forEach((it, i) => {
    track(it.notification, { dot: i < dots });
    if (i < dots) visible.push(it.notification.id);
  });
  follow({ ids: visible });
  return r.json;
}

// sentTimes lists the delivery times of a batch's notifications, read page by page.
async function sentTimes(batchId) {
  const times = [];
  let cursor = "";
  do {
    const { json } = await api("GET", `/v1/notifications?batchId=${batchId}&status=sent&limit=500${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`);
    for (const n of json.items) if (n.sentAt) times.push(Date.parse(n.sentAt));
    cursor = json.nextCursor || "";
  } while (cursor);
  return times.sort((a, b) => a - b);
}

function waitFor(check, { timeout = 60000, poll = [], every = 1500, tick } = {}) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    let done = false;
    const finish = (fn, v) => { if (done) return; done = true; listeners.delete(onChange); clearInterval(timer); fn(v); };
    const test = () => {
      let v;
      try { v = check(); } catch (err) { finish(reject, err); return; }
      if (v) finish(resolve, v);
    };
    const onChange = () => test();
    listeners.add(onChange);
    let lastPoll = 0;
    const timer = setInterval(async () => {
      if (tick) tick(Date.now() - started);
      if (Date.now() - started > timeout) { finish(reject, new Error(`timed out after ${timeout / 1000} s`)); return; }
      if (Date.now() - lastPoll >= every) {
        lastPoll = Date.now();
        await Promise.all(poll.map((p) => (typeof p === "function" ? p() : refreshNote(p))));
      }
      test();
    }, 250);
    test();
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rand = () => Math.random().toString(36).slice(2, 8);
function recipientFor(ch, i = 0) {
  if (ch === "sms") return `+90555${String(1000000 + ((Date.now() + i) % 9000000)).slice(0, 7)}`;
  if (ch === "email") return `user${i}@example.com`;
  return `device-token-${rand()}${i}`;
}

// ------------------------------------------------------------------ metrics
let stats = {};
const stat = (m, k) => (m && m[k]) || 0;

async function pollStats() {
  try {
    const { json } = await api("GET", "/playground/api/stats");
    stats = json || {};
  } catch {
    stats = {};
  }
  renderStats();
  setTimeout(pollStats, 2000);
}

function renderStats() {
  const s = stats;
  if (!s.available) {
    $("#instances").textContent = s.error && s.error.includes("not set") ? "Metrics off" : "Metrics unavailable";
  } else {
    const inst = (job, one, many) => { const c = stat(s.instances, job); return `${c} ${c === 1 ? one : many}`; };
    $("#instances").textContent = `${inst("api", "API", "APIs")}, ${inst("dispatcher", "dispatcher", "dispatchers")}, ${inst("worker", "worker", "workers")}`;
  }
  const delivered = CHANNELS.reduce((sum, ch) => sum + stat(s.deliveredPerSecond, ch), 0);
  $("#created-rate").textContent = fmt1.format(s.createdPerSecond || 0);
  $("#delivered-rate").textContent = fmt1.format(delivered);

  setText("api-rate", `${fmt1.format(s.createdPerSecond || 0)} created/s`);
  setText("pg-pending", `${fmt.format(stat(s.notifications, "pending"))} pending`);
  setText("pg-retrying", `${fmt.format(stat(s.notifications, "retrying"))} waiting to retry`);
  const disp = stat(s.instances, "dispatcher");
  setText("disp-count", disp ? `${disp} ${disp === 1 ? "replica" : "replicas"}` : "relay and reaper");
  const total = CHANNELS.reduce((sum, ch) => sum + stat(s.providerCalls && s.providerCalls[ch], "delivered"), 0);
  setText("provider-total", `${fmt.format(total)} delivered`);
  setText("dlq", `${fmt.format(s.deadLetters || 0)} published`);
  for (const ch of CHANNELS) {
    const lag = PRIORITIES.reduce((sum, p) => sum + stat(s.kafkaLag, `notifications.${ch}.${p}`), 0);
    setText(`lag-${ch}`, `backlog ${fmt.format(lag)}`);
    setText(`rate-${ch}`, `${fmt.format(stat(s.deliveredPerSecond, ch))} of 100/s`);
    const b = (s.breakers && s.breakers[ch]) || "closed";
    live[`lamp-${ch}`].setAttribute("data-state", b);
    setText(`breaker-${ch}`, `breaker ${b.replace("_", "-")}`);
  }
  if (shownStation) showStation(shownStation, pinnedStation === shownStation);
}

function setText(id, text) {
  if (live[id] && live[id].textContent !== text) live[id].textContent = text;
}

// ------------------------------------------------------------------ following and steps
const followSelect = $("#follow-select");
let followListQueued = false;

function refreshFollowList() {
  if (followListQueued) return;
  followListQueued = true;
  setTimeout(() => {
    followListQueued = false;
    const recent = [...notes.values()].filter((n) => n.label || n.id === selectedId).slice(-25).reverse();
    followSelect.textContent = "";
    if (!recent.length) {
      followSelect.append(new Option("Nothing yet", ""));
      return;
    }
    for (const n of recent) {
      const o = new Option(`${n.label || `${CHANNEL_NAME[n.channel]} notification`}, ${n.status}`, n.id);
      o.selected = n.id === selectedId;
      followSelect.append(o);
    }
  }, 150);
}
followSelect.addEventListener("change", () => { if (followSelect.value) select(followSelect.value); });

function select(id) {
  const prev = selectedId && notes.get(selectedId);
  if (prev && prev.dot) prev.dot.el.classList.remove("selected");
  selectedId = id;
  const n = notes.get(id);
  if (halo) { halo.remove(); halo = null; }
  if (n && n.dot) {
    n.dot.el.classList.add("selected");
    layers.dots.appendChild(n.dot.el); // draw on top
    moveHalo(n.dot.x, n.dot.y);
    markCurrentStation(n.dot.key);
  }
  if (n && !n.label) n.label = `${CHANNEL_NAME[n.channel]} ${n.priority} from a batch`;
  $("#now").className = "now";
  $("#now-step").textContent = "";
  $("#now-text").textContent = n ? `Following: ${n.label}. Each step appears here as its dot moves.` : "";
  refreshFollowList();
  scheduleStepsRefresh(0);
}

let stepsTimer = null;
function scheduleStepsRefresh(delay = 150) {
  clearTimeout(stepsTimer);
  stepsTimer = setTimeout(renderSteps, delay);
}

const timeFmt = new Intl.DateTimeFormat("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit", fractionalSecondDigits: 3, hour12: false });

async function renderSteps() {
  const id = selectedId;
  if (!id) return;
  let detail, history;
  try {
    [detail, history] = await Promise.all([
      api("GET", `/v1/notifications/${id}`).then((r) => r.json),
      api("GET", `/v1/notifications/${id}/history`).then((r) => r.json),
    ]);
  } catch {
    return;
  }
  if (id !== selectedId) return;
  applyEvent(toEvent(detail));
  const n = notes.get(id);
  renderSummary(detail);
  const list = $("#steps");
  list.textContent = "";
  const changes = history.changes || [];
  $("#steps-empty").hidden = changes.length > 0;
  let prevAt = null;
  changes.forEach((c, i) => {
    const at = Date.parse(c.at);
    const latest = i === changes.length - 1;
    const { title, text } = describe(c, detail, latest);
    const li = document.createElement("li");
    li.className = `${c.to}${latest ? " latest" : ""}`;
    const t = document.createElement("span");
    t.className = "title";
    t.textContent = title;
    const time = document.createElement("span");
    time.className = "time";
    time.textContent = prevAt === null ? timeFmt.format(at) : `${timeFmt.format(at)}  +${fmtDelta(at - prevAt)}`;
    time.title = new Date(at).toISOString();
    const d = document.createElement("span");
    d.className = "detail";
    d.textContent = text;
    li.append(t, time, d);
    list.appendChild(li);
    prevAt = at;
  });
  if (n) {
    n.content = detail.content;
    if (n.stepNo) highlightStep(n.stepNo);
  }
}

function fmtDelta(ms) {
  if (ms < 1000) return `${Math.max(0, Math.round(ms))} ms`;
  if (ms < 60000) return `${fmt1.format(ms / 1000)} s`;
  return `${fmt1.format(ms / 60000)} min`;
}

function renderSummary(nj) {
  const box = $("#selected-summary");
  box.hidden = false;
  box.textContent = "";
  const badge = document.createElement("span");
  badge.className = `badge ${nj.status}`;
  badge.textContent = nj.status;
  const what = document.createElement("span");
  what.textContent = `${CHANNEL_NAME[nj.channel]}, ${nj.priority} priority, to ${nj.recipient}`;
  const attempts = document.createElement("span");
  attempts.textContent = `${nj.attempts} ${nj.attempts === 1 ? "attempt" : "attempts"}`;
  const id = document.createElement("span");
  id.className = "mono";
  id.textContent = nj.id;
  const content = document.createElement("span");
  content.className = "content";
  content.textContent = `“${nj.content}”`;
  box.append(badge, what, attempts, id, content);
}

const clock = (iso) => timeFmt.format(Date.parse(iso)).slice(0, 8);

// One sentence per status change: what the system did at this step and why.
function describe(c, nj, latest) {
  const ch = CHANNEL_NAME[nj.channel];
  const topic = `notifications.${nj.channel}.${nj.priority}`;
  switch (c.to) {
    case "pending":
      if (c.from === "failed") {
        return { title: "Replayed by POST /retry", text: "The failed notification got a fresh attempt budget and went back to PostgreSQL for the dispatcher to pick up." };
      }
      if (nj.scheduledAt && Date.parse(nj.scheduledAt) > Date.parse(c.at) + 500) {
        return { title: "API validated the request and stored it in PostgreSQL", text: `Scheduled for ${clock(nj.scheduledAt)}. The row waits in PostgreSQL until then; nothing is in Kafka yet.` };
      }
      return { title: "API validated the request and stored it in PostgreSQL", text: "The request returned 202 as soon as the row was committed. The row is the outbox: the dispatcher, not the API, hands it to Kafka." };
    case "queued":
      return {
        title: c.from === "retrying" ? `Due again: dispatcher published it to ${topic}` : c.from === "processing" ? `Released and published again to ${topic}` : `Dispatcher published it to ${topic}`,
        text: c.from === "processing"
          ? `${c.reason || "The worker gave the claim back"}. No attempt was used.`
          : "Only the id travels through Kafka (a claim-check). The topic is chosen by channel and priority, so high priority never waits behind a low-priority backlog.",
      };
    case "processing":
      return {
        title: `${ch} worker claimed it for attempt ${c.attempt}`,
        text: "A guarded update with a 30-second lease makes this worker the only sender. Then it takes a token from the Redis rate limiter (100 per second for the channel) and calls the provider.",
      };
    case "sent":
      return { title: "Provider accepted it with 202", text: `Delivered on attempt ${c.attempt}. A status event reached this page through Kafka and the WebSocket.` };
    case "retrying": {
      const next = latest && nj.status === "retrying" && nj.nextAttemptAt ? ` Next attempt at ${clock(nj.nextAttemptAt)}.` : "";
      return {
        title: `Attempt ${c.attempt} failed, retry scheduled`,
        text: `${c.reason || "The provider call failed"}. The row went back to PostgreSQL with exponential backoff and jitter.${next}`,
      };
    }
    case "failed":
      return {
        title: "Delivery failed, dead letter published",
        text: `${c.reason || "The provider rejected it"}. A permanent error or the last allowed attempt ends here. A copy went to notifications.dlq, and POST /retry can replay it.`,
      };
    case "cancelled":
      return { title: "Cancelled before delivery", text: "A guarded update moved it to cancelled. A worker that already holds its Kafka record will see the status and skip it." };
    default:
      return { title: c.to, text: c.reason || "" };
  }
}

// ------------------------------------------------------------------ send form
$("#send-channel").addEventListener("change", (e) => {
  $("#send-recipient").value = { sms: "+905551234567", email: "user@example.com", push: "device-token-123456" }[e.target.value];
});

$("#send-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const form = e.target;
  const out = $("#send-result");
  const data = Object.fromEntries(new FormData(form));
  const button = form.querySelector("button");
  button.disabled = true;
  out.className = "form-result";
  out.textContent = "Sending";
  try {
    const { json } = await send({ channel: data.channel, priority: data.priority, recipient: data.recipient.trim(), content: data.content });
    notes.get(json.id).label = `${CHANNEL_NAME[json.channel]} to ${json.recipient}`;
    select(json.id);
    out.textContent = "Sent to the API. Follow it on the map and in the steps below.";
  } catch (err) {
    out.className = "form-result error";
    out.textContent = err.message;
  } finally {
    button.disabled = false;
  }
});

// ------------------------------------------------------------------ provider faults
const NO_FAULTS = { failureRate: 0, rateLimitRate: 0, permanentRate: 0 };
let faultsAvailable = false;

async function getFaults() {
  try {
    const { json } = await api("GET", "/playground/api/faults");
    faultsAvailable = true;
    showFaults(json);
    return json;
  } catch {
    faultsAvailable = false;
    return null;
  }
}

async function setFaults(f) {
  const { json } = await api("PUT", "/playground/api/faults", { ...NO_FAULTS, ...f });
  showFaults(json);
  return json;
}

function showFaults(f) {
  const active = f && (f.failureRate || f.rateLimitRate || f.permanentRate);
  $("#faults-banner").hidden = !active;
  if (!active) return;
  const parts = [];
  if (f.failureRate) parts.push(`${Math.round(f.failureRate * 100)}% answer 500`);
  if (f.rateLimitRate) parts.push(`${Math.round(f.rateLimitRate * 100)}% answer 429`);
  if (f.permanentRate) parts.push(`${Math.round(f.permanentRate * 100)}% answer 400`);
  $("#faults-text").textContent = `The mock provider is failing on purpose: ${parts.join(", ")}.`;
}

$("#faults-reset").addEventListener("click", async () => {
  try { await setFaults(NO_FAULTS); } catch (err) { $("#faults-text").textContent = err.message; }
});

// ------------------------------------------------------------------ scenarios
const SCENARIOS = [
  {
    id: "single",
    title: "Send one notification",
    text: "A single SMS from the API to the provider, step by step.",
    async run(ctx) {
      const { json } = await send({ channel: "sms", priority: "normal", recipient: recipientFor("sms"), content: "Your order has shipped." });
      label(json.id, "One SMS");
      select(json.id);
      ctx.progress("Created. Watch it move along the SMS line.");
      const n = await waitFor(() => { const x = notes.get(json.id); return TERMINAL.has(x.status) && x; }, { poll: [json.id], timeout: 30000 });
      return n.status === "sent"
        ? pass(`Delivered in ${fmtDelta(n.at - Date.parse(json.createdAt))}, on attempt ${n.attempts}.`)
        : fail(`Ended as ${n.status}.`);
    },
  },
  {
    id: "burst",
    title: "Burst of 1000",
    text: "One batch of 1000 SMS. The rate limiter drains it at 100 per second.",
    async run(ctx) {
      const total = 1000;
      const items = Array.from({ length: total }, (_, i) => ({ channel: "sms", priority: "normal", recipient: recipientFor("sms", i), content: `Flash sale: 20% off today only (#${i + 1})` }));
      const res = await sendBatch(items, 80);
      const batchId = res.batch.id;
      ctx.progress("Batch accepted in one request. Rows are in PostgreSQL; the dispatcher is publishing them.", 0);
      let counts = {};
      await waitFor(() => (counts.sent || 0) >= total, {
        timeout: 60000, every: 1000,
        poll: [async () => { try { counts = (await api("GET", `/v1/batches/${batchId}`)).json.statusCounts; } catch { /* keep the last counts */ } }],
        tick: () => ctx.progress(`${fmt.format(counts.sent || 0)} of ${fmt.format(total)} delivered, ${fmt.format((counts.queued || 0) + (counts.pending || 0))} still waiting`, (counts.sent || 0) / total),
      });
      ctx.progress(`${fmt.format(total)} of ${fmt.format(total)} delivered. Measuring the rate.`, 1);
      const times = await sentTimes(batchId);
      const secs = (times[times.length - 1] - times[0]) / 1000;
      const rate = (times.length - 1) / secs;
      const msg = `All ${fmt.format(total)} delivered in ${fmt1.format(secs)} s, about ${fmt.format(rate)} per second. The limit is 100 per second.`;
      return rate <= 110 ? pass(msg) : fail(msg);
    },
  },
  {
    id: "priority",
    title: "High priority overtakes",
    text: "400 low-priority SMS, then 20 high-priority ones. The high ones finish first.",
    async run(ctx) {
      const low = await sendBatch(Array.from({ length: 400 }, (_, i) => ({ channel: "sms", priority: "low", recipient: recipientFor("sms", i), content: `Weekly newsletter (#${i + 1})` })), 40);
      const high = await sendBatch(Array.from({ length: 20 }, (_, i) => ({ channel: "sms", priority: "high", recipient: recipientFor("sms", 500 + i), content: `Your login code is ${100000 + i}` })), 20);
      const first = high.items[0].notification;
      label(first.id, "High-priority SMS");
      select(first.id);
      let lowCounts = {}, highCounts = {};
      const pollCounts = async () => {
        try {
          const [l, h] = await Promise.all([api("GET", `/v1/batches/${low.batch.id}`), api("GET", `/v1/batches/${high.batch.id}`)]);
          lowCounts = l.json.statusCounts;
          highCounts = h.json.statusCounts;
        } catch { /* keep the last counts */ }
      };
      await waitFor(() => (highCounts.sent || 0) >= 20, {
        timeout: 30000, every: 300, poll: [pollCounts],
        tick: () => ctx.progress(`High: ${highCounts.sent || 0} of 20 delivered. Low: ${lowCounts.sent || 0} of 400 delivered.`),
      });
      const lowLeft = 400 - (lowCounts.sent || 0);
      ctx.progress(`High priority done. Waiting for the remaining ${lowLeft} low-priority messages.`);
      await waitFor(() => (lowCounts.sent || 0) >= 400, { timeout: 60000, every: 1000, poll: [pollCounts] });
      return lowLeft >= 100
        ? pass(`All 20 high-priority messages were delivered while ${lowLeft} of the 400 low-priority ones were still waiting.`)
        : fail(`Only ${lowLeft} of the 400 low-priority messages were still waiting when the high-priority ones finished.`);
    },
  },
  {
    id: "scheduled",
    title: "Scheduled for later",
    text: "Due in 8 seconds. It waits in PostgreSQL, then goes out on time.",
    async run(ctx) {
      const due = Date.now() + 8000;
      const { json } = await send({ channel: "email", priority: "normal", recipient: recipientFor("email"), content: "Your appointment is tomorrow at 10:00.", scheduledAt: new Date(due).toISOString() });
      label(json.id, "Scheduled email");
      select(json.id);
      await waitFor(() => notes.get(json.id).status === "sent", {
        poll: [json.id], timeout: 30000,
        tick: () => {
          const left = Math.ceil((due - Date.now()) / 1000);
          ctx.progress(left > 0 ? `Waiting in PostgreSQL, due in ${left} s.` : "Due now: the dispatcher picks it up on its next poll.", Math.min(1, 1 - (due - Date.now()) / 8000));
        },
      });
      const detail = await refreshNote(json.id);
      const sentAt = Date.parse(detail.sentAt);
      const late = sentAt - due;
      if (late < -200) return fail(`Delivered ${fmtDelta(-late)} before its scheduled time.`);
      return late <= 3000
        ? pass(`Delivered ${fmtDelta(Math.max(0, late))} after its scheduled time.`)
        : fail(`Delivered ${fmtDelta(late)} after its scheduled time; the dispatcher polls every 500 ms.`);
    },
  },
  {
    id: "template",
    title: "Template with variables",
    text: "Creates a template, sends from it, and checks the rendered text.",
    async run(ctx) {
      const name = `playground-otp-${rand()}`;
      const body = "Your verification code is {{code}}. It expires in {{minutes}} minutes.";
      const tpl = (await api("POST", "/v1/templates", { name, channel: "sms", body })).json;
      ctx.progress(`Template ${name} created with variables ${tpl.variables.join(" and ")}.`);
      try {
        const { json } = await send({ channel: "sms", priority: "high", recipient: recipientFor("sms"), templateId: tpl.id, variables: { code: "482913", minutes: "5" } });
        label(json.id, "SMS from a template");
        select(json.id);
        const want = "Your verification code is 482913. It expires in 5 minutes.";
        if (json.content !== want) return fail(`Rendered "${json.content}".`);
        await waitFor(() => notes.get(json.id).status === "sent", { poll: [json.id], timeout: 30000 });
        return pass(`Rendered when created ("${want}") and delivered.`);
      } finally {
        api("DELETE", `/v1/templates/${tpl.id}`).catch(() => {});
      }
    },
  },
  {
    id: "cancel",
    title: "Cancel before sending",
    text: "Schedules a push for one minute ahead, then cancels it.",
    async run(ctx) {
      const { json } = await send({ channel: "push", priority: "normal", recipient: recipientFor("push"), content: "Your cart is waiting.", scheduledAt: new Date(Date.now() + 60000).toISOString() });
      label(json.id, "Push to cancel");
      select(json.id);
      ctx.progress("Scheduled. Cancelling now.");
      await sleep(1200);
      const r = await api("POST", `/v1/notifications/${json.id}/cancel`);
      applyEvent(toEvent(r.json));
      await sleep(1500);
      const after = await refreshNote(json.id);
      return after && after.status === "cancelled" && after.attempts === 0
        ? pass("Cancelled while still waiting in PostgreSQL. No attempt was made.")
        : fail(`Status is ${after && after.status}.`);
    },
  },
  {
    id: "idempotency",
    title: "Same request twice",
    text: "Two requests with the same Idempotency-Key create one notification.",
    async run(ctx) {
      const key = `playground-${rand()}`;
      const body = { channel: "sms", priority: "normal", recipient: recipientFor("sms"), content: "Payment received. Thank you." };
      const first = await send(body, { "Idempotency-Key": key });
      label(first.json.id, "SMS sent twice");
      select(first.json.id);
      const second = await send(body, { "Idempotency-Key": key });
      ctx.progress(`Second request answered ${second.status}, replayed: ${second.headers.get("Idempotent-Replayed") || "no"}.`);
      let conflict = null;
      try {
        await api("POST", "/v1/notifications", { ...body, content: "A different message" }, { "Idempotency-Key": key });
      } catch (err) {
        conflict = err;
      }
      await waitFor(() => notes.get(first.json.id).status === "sent", { poll: [first.json.id], timeout: 30000 });
      if (first.json.id !== second.json.id) return fail("The second request created another notification.");
      if (!conflict || conflict.status !== 409) return fail("Reusing the key with a different message was not rejected.");
      return pass("Both requests returned the same notification and it was delivered once. Reusing the key with different content got 409.");
    },
  },
  {
    id: "retry",
    title: "Retry with backoff",
    faults: true,
    text: "The provider answers 500 twice, then recovers. The retries wait longer each time.",
    async run(ctx) {
      await setFaults({ failureRate: 1 });
      const { json } = await send({ channel: "sms", priority: "high", recipient: recipientFor("sms"), content: "Your package is out for delivery." });
      label(json.id, "SMS that is retried");
      select(json.id);
      ctx.progress("The provider is answering 500 to everything.");
      await waitFor(() => { const n = notes.get(json.id); return n.status === "retrying" && n.attempts >= 1; }, { poll: [json.id], timeout: 30000 });
      ctx.progress("Attempt 1 failed. Back in PostgreSQL until the backoff ends.");
      await waitFor(() => { const n = notes.get(json.id); return n.status === "retrying" && n.attempts >= 2; }, { poll: [json.id], timeout: 40000 });
      await setFaults(NO_FAULTS);
      ctx.progress("Attempt 2 failed too. The provider is healthy again; the next attempt waits about twice as long.");
      const n = await waitFor(() => { const x = notes.get(json.id); return TERMINAL.has(x.status) && x; }, { poll: [json.id], timeout: 60000 });
      return n.status === "sent" && n.attempts >= 3
        ? pass(`Delivered on attempt ${n.attempts} after two provider errors.`)
        : fail(`Ended as ${n.status} after ${n.attempts} attempts.`);
    },
  },
  {
    id: "outage",
    title: "Provider outage",
    faults: true,
    text: "The provider fails everything. The circuit breaker opens, then lets traffic through again.",
    async run(ctx) {
      await setFaults({ failureRate: 1 });
      const res = await sendBatch(Array.from({ length: 10 }, (_, i) => ({ channel: "sms", priority: "normal", recipient: recipientFor("sms", i), content: `Order update (#${i + 1})` })), 10);
      const ids = res.items.map((it) => it.notification.id);
      label(ids[0], "SMS during an outage");
      select(ids[0]);
      ctx.progress("10 SMS sent while the provider answers 500 to everything.");
      let sawOpen = false;
      try {
        await waitFor(() => stats.breakers && stats.breakers.sms === "open", {
          timeout: 45000, tick: (ms) => ctx.progress(`Provider failing for ${Math.round(ms / 1000)} s. Waiting for the circuit breaker to open.`),
        });
        sawOpen = true;
      } catch { /* metrics may be off; delivery is still checked */ }
      await setFaults(NO_FAULTS);
      ctx.progress(sawOpen
        ? "Breaker open: the worker stops calling the provider for 30 s instead of burning attempts. The provider is healthy again."
        : "The provider is healthy again.");
      await waitFor(() => ids.every((id) => TERMINAL.has(notes.get(id).status)), {
        timeout: 150000, every: 3000, poll: ids,
        tick: () => {
          const sent = ids.filter((id) => notes.get(id).status === "sent").length;
          const b = (stats.breakers && stats.breakers.sms) || "closed";
          ctx.progress(`Breaker ${b.replace("_", "-")}. ${sent} of 10 delivered.`, sent / 10);
        },
      });
      const sent = ids.filter((id) => notes.get(id).status === "sent").length;
      if (sent !== 10) return fail(`${sent} of 10 delivered.`);
      return pass(sawOpen
        ? "The breaker opened after repeated 500s, paused calls, then closed again. All 10 were delivered and none failed."
        : "All 10 were delivered after the outage. The breaker state was not visible because metrics are off.");
    },
  },
  {
    id: "deadletter",
    title: "Dead letter and replay",
    faults: true,
    text: "The provider rejects an email with 400. It is not retried; it is dead-lettered, then replayed.",
    async run(ctx) {
      await setFaults({ permanentRate: 1 });
      const { json } = await send({ channel: "email", priority: "normal", recipient: recipientFor("email"), content: "Your invoice is attached." });
      label(json.id, "Email to replay");
      select(json.id);
      await waitFor(() => notes.get(json.id).status === "failed", { poll: [json.id], timeout: 30000 });
      await setFaults(NO_FAULTS);
      const failedAfter = notes.get(json.id).attempts; // the replay resets the attempts
      ctx.progress(`Failed after ${failedAfter} attempt: a 400 is permanent, so it went to the dead letters. Replaying it now.`);
      await sleep(2500);
      const r = await api("POST", `/v1/notifications/${json.id}/retry`);
      applyEvent(toEvent(r.json));
      await waitFor(() => notes.get(json.id).status === "sent", { poll: [json.id], timeout: 30000 });
      return failedAfter === 1
        ? pass("One attempt, no retries, a dead letter, then delivered after the replay.")
        : fail(`It took ${failedAfter} attempts before failing; a 400 should not be retried.`);
    },
  },
];

const pass = (why) => ({ ok: true, why });
const fail = (why) => ({ ok: false, why });
function label(id, text) {
  const n = notes.get(id);
  if (n) n.label = text;
}

let running = null;

function renderScenarios() {
  const list = $("#scenario-list");
  for (const s of SCENARIOS) {
    const li = document.createElement("li");
    li.id = `scenario-${s.id}`;
    const head = document.createElement("div");
    head.className = "scenario-head";
    const h = document.createElement("h3");
    h.textContent = s.title;
    const p = document.createElement("p");
    p.textContent = s.text;
    const b = document.createElement("button");
    b.type = "button";
    b.className = "quiet";
    b.textContent = "Run";
    b.setAttribute("aria-label", `Run: ${s.title}`);
    b.addEventListener("click", () => runScenario(s));
    head.append(h, b, p);
    const progress = document.createElement("p");
    progress.className = "scenario-progress";
    progress.setAttribute("role", "status");
    const bar = document.createElement("div");
    bar.className = "bar";
    bar.hidden = true;
    bar.appendChild(document.createElement("span"));
    const result = document.createElement("p");
    result.className = "scenario-result";
    li.append(head, progress, bar, result);
    list.appendChild(li);
    s.el = { li, button: b, progress, bar, result };
  }
}

function updateScenarioButtons() {
  for (const s of SCENARIOS) {
    const blocked = s.faults && !faultsAvailable;
    s.el.button.disabled = running !== null || blocked;
    if (blocked) s.el.button.title = "Needs the mock provider (DEMO_PROVIDER_ADMIN_URL)";
  }
}

async function runScenario(s) {
  if (running) return;
  running = s;
  updateScenarioButtons();
  const { li, progress, bar, result } = s.el;
  li.classList.add("running");
  result.textContent = "";
  result.className = "scenario-result";
  progress.textContent = "";
  bar.hidden = true;
  const ctx = {
    progress(text, fraction) {
      progress.textContent = text;
      if (typeof fraction === "number") {
        bar.hidden = false;
        bar.firstChild.style.width = `${Math.round(Math.max(0, Math.min(1, fraction)) * 100)}%`;
      }
    },
  };
  const started = Date.now();
  let outcome;
  try {
    outcome = await s.run(ctx);
  } catch (err) {
    outcome = fail(err.message);
  } finally {
    if (s.faults) {
      // The playground always leaves the provider healthy (all rates 0), whatever MOCK_* set.
      try { await setFaults(NO_FAULTS); } catch { /* the banner shows it */ }
    }
    li.classList.remove("running");
    running = null;
    updateScenarioButtons();
  }
  const mark = document.createElement("span");
  mark.textContent = outcome.ok ? "✓ Passed" : "✗ Failed";
  const why = document.createElement("span");
  why.className = "why";
  why.textContent = `${outcome.why} (${fmtDelta(Date.now() - started)})`;
  result.className = `scenario-result ${outcome.ok ? "pass" : "fail"}`;
  result.append(mark, why);
  bar.hidden = true;
}

// Closing or reloading the page in the middle of a failure scenario must not leave the
// provider failing for everyone.
window.addEventListener("pagehide", () => {
  if (!running || !running.faults) return;
  fetch("/playground/api/faults", { method: "PUT", keepalive: true, headers: { "Content-Type": "application/json" }, body: JSON.stringify(NO_FAULTS) });
});

// ------------------------------------------------------------------ start
drawMap();
renderScenarios();
connect();
pollStats();
getFaults().then(updateScenarioButtons);
updateScenarioButtons();
