"use strict";

const $ = (id) => document.getElementById(id);
let config = null;
let activeScenario = null;
const tasks = new Map();

async function boot() {
  config = await (await fetch("/api/config")).json();
  renderScenarios();
  renderWorkloads();
  selectScenario(config.scenarios[0].key);
  connect();
}

function renderScenarios() {
  const box = $("scenarios");
  box.innerHTML = "";
  for (const s of config.scenarios) {
    const el = document.createElement("div");
    el.className = "scenario";
    el.dataset.key = s.key;
    el.innerHTML =
      `<div class="t">${esc(s.title)}</div>` +
      `<div class="d">${esc(s.summary)}</div>` +
      `<div class="k">teaches: ${esc(s.teaches)}</div>`;
    el.onclick = () => selectScenario(s.key);
    box.appendChild(el);
  }
}

function renderWorkloads() {
  const sel = $("workload");
  sel.innerHTML = "";
  for (const w of ["reductions", "scale", "axpy", "compute_heavy", "stats"]) {
    const o = document.createElement("option");
    o.value = w;
    o.textContent = w;
    sel.appendChild(o);
  }
}

function selectScenario(key) {
  const s = config.scenarios.find((x) => x.key === key);
  if (!s) return;
  activeScenario = key;
  for (const el of document.querySelectorAll(".scenario")) {
    el.classList.toggle("active", el.dataset.key === key);
  }
  const p = s.params;
  $("workload").value = p.workload;
  $("spe").value = p.spe;
  $("elements").value = p.elements;
  $("chunk").value = p.chunk;
  $("store_bytes").value = p.store_bytes;
  $("dma_bw").value = p.dma_bw ?? "";
  updateDerived();
}

function currentParams() {
  const bw = $("dma_bw").value.trim();
  return {
    workload: $("workload").value,
    spe: Number($("spe").value),
    elements: Number($("elements").value),
    chunk: Number($("chunk").value),
    store_bytes: Number($("store_bytes").value),
    dma_bw: bw === "" ? null : Number(bw),
  };
}

function updateDerived() {
  const p = currentParams();
  const chunkBytes = p.chunk * 4;
  const tasks = Math.ceil(p.elements / p.chunk);
  const transport = chunkBytes > config.limits.inline_limit ? "Arrow DataRef" : "inline";
  const notes = [
    `tasks        : ${tasks}`,
    `chunk bytes  : ${chunkBytes.toLocaleString()}`,
    `transport    : ${transport} (limit ${(config.limits.inline_limit / 1048576).toFixed(0)} MiB)`,
  ];
  let warn = "";
  if (chunkBytes > p.store_bytes) {
    warn = `\nlocal store  : ${p.store_bytes.toLocaleString()} B is SMALLER than the chunk -> every task overflows`;
  } else if (chunkBytes > p.store_bytes * 0.8) {
    warn = `\nlocal store  : chunk is close to the store limit`;
  }
  $("derived").innerHTML =
    notes.map((n) => esc(n)).join("\n") +
    (warn ? "\n" + `<span class="warn">${esc(warn)}</span>` : "");
}

async function run() {
  $("error").textContent = "";
  tasks.clear();
  $("taskBody").innerHTML = "";
  $("run").disabled = true;
  try {
    const res = await fetch("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(currentParams()),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "run failed");
  } catch (e) {
    $("error").textContent = String(e.message || e);
    $("run").disabled = false;
  }
}

async function stop() {
  await fetch("/api/stop", { method: "POST" });
}

function connect() {
  const es = new EventSource("/api/stream");
  es.onmessage = (ev) => handle(JSON.parse(ev.data));
  es.onerror = () => {
    // EventSource reconnects on its own; surface it rather than hiding it.
    $("status").textContent = "reconnecting";
  };
}

function handle(msg) {
  switch (msg.type) {
    case "state":
      applyState(msg.state);
      break;
    case "status":
      setStatus(msg.status);
      if (msg.status !== "running") $("run").disabled = false;
      break;
    case "trace":
      applyTrace(msg.record);
      break;
    case "workers":
      break;
    case "done":
      setStatus(msg.result.ok ? "done" : "failed");
      $("run").disabled = false;
      renderResult(msg.result);
      break;
    case "error":
      $("error").textContent = msg.message;
      $("run").disabled = false;
      break;
  }
}

function applyState(state) {
  if (!state || state.status === "idle") return;
  setStatus(state.status);
  renderWorkers(state.workers || {});
  for (const t of Object.values(state.task_states || {})) tasks.set(t.task_id, t);
  renderTasks();
  if (state.counts) $("taskCounts").textContent = JSON.stringify(state.counts);
}

function applyTrace(rec) {
  if (rec.event === "worker_ready") {
    renderWorkers({ ...(currentWorkers()), [rec.worker]: { ...(currentWorkers()[rec.worker] || {}), ...rec, status: "idle" } });
  } else if (rec.event === "dispatch") {
    tasks.set(rec.task, {
      task_id: rec.task, index: rec.index, worker: rec.worker,
      state: "running", queue_ms: +(rec.queue_s * 1000).toFixed(2),
      input_bytes: rec.input_bytes,
    });
  } else if (rec.event === "collect") {
    const t = tasks.get(rec.task) || { task_id: rec.task, index: rec.index };
    t.state = "done";
    t.worker = rec.worker;
    t.exec_ms = +(rec.exec_s * 1000).toFixed(3);
    t.total_ms = +(rec.total_s * 1000).toFixed(3);
    t.output_bytes = rec.output_bytes;
    tasks.set(rec.task, t);
  } else if (["retry", "task_failed", "task_error"].includes(rec.event)) {
    const t = tasks.get(rec.task) || { task_id: rec.task };
    t.state = rec.event === "retry" ? "retry" : "failed";
    t.error = rec.error || rec.message;
    tasks.set(rec.task, t);
  }
  appendLog(rec);
  renderTasks();
}

function currentWorkers() {
  const map = {};
  for (const el of document.querySelectorAll(".spe")) {
    map[el.dataset.id] = {
      worker_id: el.dataset.id,
      status: el.classList.contains("busy") ? "busy" : "idle",
      current_task: el.dataset.task || null,
      tasks: Number(el.dataset.done || 0),
      tasks_dispatched: Number(el.dataset.tasks || 0),
      bytes_in: Number(el.dataset.in || 0),
      bytes_out: Number(el.dataset.out || 0),
      store_bytes: Number(el.dataset.store || 0),
      busy_ms: Number(el.dataset.busy || 0),
      pid: el.dataset.pid,
    };
  }
  return map;
}

function renderWorkers(workers) {
  const grid = $("speGrid");
  const ids = Object.keys(workers).sort();
  if (!ids.length) return;
  if (grid.childElementCount !== ids.length) {
    grid.innerHTML = ids
      .map(
        (id) =>
          `<div class="spe" data-id="${id}">
             <div class="spe-head"><span class="spe-id">${id}</span>
               <span class="badge idle">idle</span></div>
             <div class="spe-task">—</div>
             <div class="spe-stats">
               <span>tasks <b data-f="tasks">0</b></span>
               <span>busy <b data-f="busy">0</b> ms</span>
               <span>in <b data-f="in">0</b> B</span>
               <span>out <b data-f="out">0</b> B</span>
               <span>store <b data-f="store">—</b></span>
               <span>pid <b data-f="pid">—</b></span>
             </div>
             <div class="bar"><div></div></div>
           </div>`
      )
      .join("");
  }
  const maxTasks = Math.max(1, ...ids.map((i) => workers[i].tasks || 0));
  for (const id of ids) {
    const w = workers[id];
    const el = grid.querySelector(`.spe[data-id="${id}"]`);
    if (!el) continue;
    const busy = w.status === "busy";
    el.classList.toggle("busy", busy);
    el.dataset.task = w.current_task || "";
    el.dataset.tasks = w.tasks_dispatched ?? w.tasks ?? 0;
    el.dataset.done = w.tasks ?? 0;
    el.dataset.in = w.bytes_in || 0;
    el.dataset.out = w.bytes_out || 0;
    el.dataset.store = w.store_bytes || 0;
    el.dataset.busy = w.busy_ms || 0;
    el.dataset.pid = w.pid || "";
    const badge = el.querySelector(".badge");
    badge.textContent = busy ? "busy" : "idle";
    badge.className = "badge " + (busy ? "busy" : "idle");
    el.querySelector(".spe-task").textContent = w.current_task || "—";
    const dispatched = w.tasks_dispatched ?? w.tasks ?? 0;
    const done = w.tasks ?? 0;
    el.querySelector('[data-f="tasks"]').textContent = dispatched === done ? `${done}` : `${done}/${dispatched}`;
    el.querySelector('[data-f="busy"]').textContent = (w.busy_ms ?? 0).toFixed(1);
    el.querySelector('[data-f="in"]').textContent = fmtBytes(w.bytes_in || 0);
    el.querySelector('[data-f="out"]').textContent = fmtBytes(w.bytes_out || 0);
    el.querySelector('[data-f="store"]').textContent = fmtBytes(w.store_bytes || 0);
    el.querySelector('[data-f="pid"]').textContent = w.pid ?? "—";
    el.querySelector(".bar > div").style.width = ((w.tasks || 0) / maxTasks) * 100 + "%";
  }
}

function renderTasks() {
  const rows = [...tasks.values()].sort((a, b) => (a.index ?? 0) - (b.index ?? 0));
  $("taskCounts").textContent =
    rows.length ? `${rows.length} tasks` : "";
  $("taskBody").innerHTML = rows
    .map(
      (t) => `<tr class="${esc(t.state || "pending")}">
        <td>${t.index ?? ""}</td>
        <td>${esc(t.task_id || "")}</td>
        <td>${esc(t.worker || "")}</td>
        <td>${esc(t.state || "")}${t.error ? " " + esc(String(t.error).slice(0, 40)) : ""}</td>
        <td>${t.queue_ms ?? ""}</td>
        <td>${t.exec_ms ?? ""}</td>
        <td>${t.input_bytes != null ? fmtBytes(t.input_bytes) : ""}</td>
        <td>${t.output_bytes != null ? fmtBytes(t.output_bytes) : ""}</td>
      </tr>`
    )
    .join("");
}

function renderResult(r) {
  const cls = r.ok ? "good" : "bad";
  $("result").innerHTML =
    `<span class="${cls}">${r.ok ? "OK" : "FAILED"}</span> ` +
    `op=<span class="k">${esc(r.op)}</span> tasks=<span class="k">${r.tasks}</span> ` +
    `failed=<span class="k">${r.failed}</span> wall=<span class="k">${r.wall_ms} ms</span>\n` +
    `transport: <span class="k">${esc(JSON.stringify(r.by_transport))}</span>\n` +
    `bytes: <span class="k">${fmtBytes(r.total_bytes)}</span>  ` +
    `store peak: <span class="k">${fmtBytes(r.max_store_high_water || 0)}</span>\n` +
    `output[0:${r.output_len}]: ${esc((r.output_preview || []).join(", "))}\n` +
    (Object.keys(r.failures || {}).length
      ? `<span class="bad">failures: ${esc(JSON.stringify(r.failures))}</span>`
      : "");
}

function appendLog(rec) {
  const el = document.createElement("div");
  const extra = rec.task ? ` ${rec.task}` : rec.worker ? ` ${rec.worker}` : "";
  el.innerHTML = `<span class="tm">${(rec.t || 0).toFixed(3)}</span> ` +
                 `<span class="ev">${esc(rec.event)}</span>${esc(extra)}`;
  const log = $("log");
  log.appendChild(el);
  while (log.childElementCount > 300) log.removeChild(log.firstChild);
  log.scrollTop = log.scrollHeight;
}

function setStatus(s) {
  $("status").textContent = s;
  $("status").className = "status " + (s || "");
}

function fmtBytes(n) {
  if (!n) return "0 B";
  const u = ["B", "KiB", "MiB", "GiB"];
  let i = 0, v = n;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return `${i === 0 ? v : v.toFixed(1)} ${u[i]}`;
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

$("run").onclick = run;
$("stop").onclick = stop;
for (const id of ["workload", "spe", "elements", "chunk", "store_bytes", "dma_bw"]) {
  $(id).addEventListener("input", updateDerived);
}
boot();