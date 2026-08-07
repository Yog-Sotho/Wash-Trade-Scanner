/* Wash Trade Scanner panel — no dependencies, same-origin API only. */
"use strict";

/* ------------------------------------------------------------ helpers */

const $ = (sel) => document.querySelector(sel);

const usd = new Intl.NumberFormat("en-US", {
  style: "currency", currency: "USD", maximumFractionDigits: 0,
});
const usdCompact = new Intl.NumberFormat("en-US", {
  style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1,
});
const num = new Intl.NumberFormat("en-US");
const pct = (x) => `${(x * 100).toFixed(2)}%`;
const shortAddr = (a) => (a && a.length > 12 ? `${a.slice(0, 8)}…${a.slice(-4)}` : a || "—");
const nowTs = () => new Date().toLocaleTimeString();

const SEVERITY_COLOR = {
  MINIMAL: "var(--status-good)",
  LOW: "var(--status-good)",
  MEDIUM: "var(--status-warning)",
  HIGH: "var(--status-serious)",
  CRITICAL: "var(--status-critical)",
};

function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else node.setAttribute(k, v);
  }
  for (const child of children) node.appendChild(child);
  return node;
}

/* ------------------------------------------------------------ session */

const session = { authRequired: false };

async function api(path, options = {}) {
  const response = await fetch(path, { credentials: "same-origin", ...options });
  if (response.status === 401) {
    showLogin(true);
    throw new Error("unauthorized");
  }
  if (!response.ok) {
    let detail = `${response.status}`;
    try { detail = (await response.json()).detail || detail; } catch { /* ignore */ }
    throw new Error(detail);
  }
  return response.json();
}

function showLogin(show) {
  $("#login-overlay").classList.toggle("hidden", !show);
}

async function checkSession() {
  try {
    const data = await api("/panel/session");
    session.authRequired = data.auth_required;
    $("#conn-status").textContent = data.auth_required ? "authenticated" : "local mode";
    $("#conn-status").classList.add("ok");
    $("#logout-btn").classList.toggle("hidden", !data.auth_required);
    showLogin(false);
    return true;
  } catch {
    $("#conn-status").textContent = "signed out";
    $("#conn-status").classList.remove("ok");
    return false;
  }
}

$("#login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  $("#login-error").classList.add("hidden");
  const submitBtn = $("#login-form button[type='submit']");
  const originalText = submitBtn.textContent;
  submitBtn.disabled = true;
  submitBtn.textContent = "Signing in…";
  try {
    await api("/panel/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ api_key: $("#login-key").value }),
    });
    $("#login-key").value = "";
    if (await checkSession()) loadOverview();
  } catch {
    $("#login-error").classList.remove("hidden");
  } finally {
    submitBtn.disabled = false;
    submitBtn.textContent = originalText;
  }
});

$("#logout-btn").addEventListener("click", async () => {
  try { await api("/panel/logout", { method: "POST" }); } catch { /* ignore */ }
  monitor.stop();
  await checkSession();
  showLogin(true);
});

/* --------------------------------------------------------------- tabs */

function selectTab(button) {
  for (const tab of document.querySelectorAll(".tab")) {
    const isActive = tab === button;
    tab.classList.toggle("active", isActive);
    tab.setAttribute("aria-selected", isActive ? "true" : "false");
    tab.setAttribute("tabindex", isActive ? "0" : "-1");
  }
  for (const view of document.querySelectorAll(".view")) {
    view.classList.toggle("hidden", view.id !== `view-${button.dataset.view}`);
  }
  if (button.dataset.view === "overview") loadOverview();
}

$("#tabs").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-view]");
  if (button) selectTab(button);
});

$("#tabs").addEventListener("keydown", (event) => {
  const tabs = Array.from(document.querySelectorAll(".tab"));
  const activeIndex = tabs.indexOf(document.activeElement);
  if (activeIndex === -1) return;

  let newIndex = null;
  if (event.key === "ArrowRight" || event.key === "ArrowDown") {
    newIndex = (activeIndex + 1) % tabs.length;
  } else if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
    newIndex = (activeIndex - 1 + tabs.length) % tabs.length;
  } else if (event.key === "Home") {
    newIndex = 0;
  } else if (event.key === "End") {
    newIndex = tabs.length - 1;
  }

  if (newIndex !== null) {
    event.preventDefault();
    tabs[newIndex].focus();
    selectTab(tabs[newIndex]);
  }
});

/* ------------------------------------------------------------ tooltip */

const tooltip = $("#tooltip");

function bindTooltip(node, html) {
  const showTooltip = (x, y) => {
    tooltip.innerHTML = html;
    tooltip.classList.remove("hidden");
    const pad = 12;
    const px = Math.min(x + pad, window.innerWidth - tooltip.offsetWidth - pad);
    const py = Math.min(y + pad, window.innerHeight - tooltip.offsetHeight - pad);
    tooltip.style.left = `${px}px`;
    tooltip.style.top = `${py}px`;
  };

  node.addEventListener("mousemove", (event) => {
    showTooltip(event.clientX, event.clientY);
  });
  node.addEventListener("mouseleave", () => tooltip.classList.add("hidden"));

  node.addEventListener("focus", () => {
    const rect = node.getBoundingClientRect();
    const x = rect.left + 170;
    const y = rect.top + rect.height / 2;
    showTooltip(x, y);
  });
  node.addEventListener("blur", () => tooltip.classList.add("hidden"));
}

/* -------------------------------------------------------------- tiles */

function tile(label, value, sub, extra) {
  const card = el("div", { class: "tile" });
  card.appendChild(el("div", { class: "label", text: label }));
  const valueNode = el("div", { class: "value" });
  if (value instanceof Node) valueNode.appendChild(value); else valueNode.textContent = value;
  card.appendChild(valueNode);
  if (sub) card.appendChild(el("div", { class: "sub", text: sub }));
  if (extra) card.appendChild(extra);
  return card;
}

function severityBadge(severity) {
  const badge = el("span", { class: "severity", text: severity || "—" });
  badge.style.setProperty("--sev-color", SEVERITY_COLOR[severity] || "var(--muted)");
  return badge;
}

function proportionBar(ratio) {
  const track = el("div", { class: "proportion" });
  const fill = el("span");
  fill.style.width = `${Math.min(100, ratio * 100).toFixed(2)}%`;
  track.appendChild(fill);
  return track;
}

/* ---------------------------------------------------- bar chart (HTML) */

function renderBarChart(container, rows, formatValue) {
  container.textContent = "";
  if (!rows.length) {
    container.appendChild(el("div", { class: "empty", text: "No detections yet." }));
    return;
  }
  const max = Math.max(...rows.map((r) => r.value));
  for (const row of rows.sort((a, b) => b.value - a.value)) {
    // Cap at 80% of the track so the direct value label always fits.
    const width = max > 0 ? (row.value / max) * 80 : 0;
    const barRow = el("div", { class: "bar-row", tabindex: "0" });
    barRow.appendChild(el("div", { class: "bar-label", text: row.label, title: row.label }));
    const track = el("div", { class: "bar-track" });
    const bar = el("div", { class: "bar" });
    bar.style.width = `${Math.max(width, 0.5).toFixed(2)}%`;
    const value = el("span", { class: "bar-value", text: formatValue(row.value) });
    value.style.left = `calc(${Math.max(width, 0.5).toFixed(2)}% + 8px)`;
    track.appendChild(bar);
    track.appendChild(value);
    barRow.appendChild(track);
    bindTooltip(barRow,
      `<span class="t-label">${row.label}</span><br>` +
      `<span class="t-value">${formatValue(row.value)}</span>` +
      (row.sub ? `<br><span class="t-label">${row.sub}</span>` : ""));
    container.appendChild(barRow);
  }
}

/* ------------------------------------------------------------ overview */

async function loadOverview() {
  let stats;
  try { stats = await api("/api/v1/stats/overview"); } catch { return; }

  const washRatio = stats.total_volume_usd > 0 ? stats.wash_volume_usd / stats.total_volume_usd : 0;
  const tiles = $("#overview-tiles");
  tiles.textContent = "";
  tiles.append(
    tile("Total volume", usdCompact.format(stats.total_volume_usd), `${num.format(stats.total_trades)} trades`),
    tile("Wash volume", usdCompact.format(stats.wash_volume_usd), pct(washRatio) + " of volume", proportionBar(washRatio)),
    tile("Wash trades", num.format(stats.wash_trades), `${num.format(stats.total_trades)} analyzed`),
    tile("Pools tracked", num.format(stats.pools_tracked), `${num.format(stats.chains_active)} chains`),
  );

  renderBarChart(
    $("#method-chart"),
    Object.entries(stats.by_method).map(([method, entry]) => ({
      label: method, value: entry.volume_usd, sub: `${num.format(entry.trades)} trades`,
    })),
    (v) => usdCompact.format(v),
  );

  const chainBody = $("#chain-table tbody");
  chainBody.textContent = "";
  for (const [chain, entry] of Object.entries(stats.by_chain)) {
    const tr = el("tr");
    tr.append(
      el("td", { text: chain }),
      el("td", { class: "num", text: num.format(entry.trades) }),
      el("td", { class: "num", text: usdCompact.format(entry.volume_usd) }),
      el("td", { class: "num", text: num.format(entry.wash_trades) }),
      el("td", { class: "num", text: usdCompact.format(entry.wash_volume_usd) }),
    );
    chainBody.appendChild(tr);
  }

  const poolsBody = $("#top-pools-table tbody");
  poolsBody.textContent = "";
  for (const pool of stats.top_wash_pools) {
    const share = pool.volume_usd > 0 ? pool.wash_volume_usd / pool.volume_usd : 0;
    const tr = el("tr");
    tr.append(
      el("td", { class: "addr", text: shortAddr(pool.pool_address), title: pool.pool_address }),
      el("td", { text: String(pool.chain_id) }),
      el("td", { class: "num", text: num.format(pool.wash_trades) }),
      el("td", { class: "num", text: usdCompact.format(pool.wash_volume_usd) }),
      el("td", { class: "num", text: pct(share) }),
    );
    poolsBody.appendChild(tr);
  }

  const auditsBody = $("#recent-audits-table tbody");
  auditsBody.textContent = "";
  for (const log of stats.recent_audits) {
    const tr = el("tr");
    tr.append(
      el("td", { text: log.created_at ? new Date(log.created_at).toLocaleString() : "—" }),
      el("td", { class: "addr", text: shortAddr(log.pool_address), title: log.pool_address }),
      el("td", { class: "num", text: num.format(log.trades_processed) }),
      el("td", { class: "num", text: num.format(log.wash_trades_detected) }),
      el("td", { class: "num", text: log.duration_seconds ? `${log.duration_seconds.toFixed(1)}s` : "—" }),
    );
    auditsBody.appendChild(tr);
  }
}

/* -------------------------------------------------------- pool inspector */

const PAGE_SIZE = 25;
const poolState = { chain: 1, address: "", washOnly: false, page: 0 };

$("#pool-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  poolState.chain = Number($("#pool-chain").value);
  poolState.address = $("#pool-address").value.trim();
  poolState.washOnly = $("#pool-wash-only").checked;
  poolState.page = 0;
  $("#pool-error").classList.add("hidden");
  $("#pool-error").textContent = "";
  const submitBtn = $("#pool-submit-btn");
  const originalText = submitBtn.textContent;
  submitBtn.disabled = true;
  submitBtn.textContent = "Inspecting…";
  try {
    await loadPool();
  } finally {
    submitBtn.disabled = false;
    submitBtn.textContent = originalText;
  }
});
$("#pool-prev").addEventListener("click", () => {
  if (poolState.page > 0) { poolState.page -= 1; loadPoolTrades(); }
});
$("#pool-next").addEventListener("click", () => { poolState.page += 1; loadPoolTrades(); });

async function loadPool() {
  let report;
  try {
    report = await api(`/api/v1/pools/${poolState.chain}/${poolState.address}/report`);
  } catch (error) {
    $("#pool-error").textContent = `Report failed: ${error.message}`;
    $("#pool-error").classList.remove("hidden");
    $("#pool-results").classList.add("hidden");
    $("#pool-empty-state").classList.remove("hidden");
    return;
  }
  $("#pool-results").classList.remove("hidden");
  $("#pool-empty-state").classList.add("hidden");
  $("#pool-error").classList.add("hidden");

  const tiles = $("#pool-tiles");
  tiles.textContent = "";
  tiles.append(
    tile("Severity", severityBadge(report.severity), pct(report.wash_trade_volume_ratio) + " wash volume"),
    tile("Total volume", usdCompact.format(report.total_volume_usd), `${num.format(report.total_trades_analyzed)} trades`),
    tile("Wash volume", usdCompact.format(report.wash_trade_volume_usd),
      `${num.format(report.wash_trades_count)} flagged trades`,
      proportionBar(report.wash_trade_volume_ratio)),
    tile("Risk score", pct(report.overall_risk_score), "flagged / analyzed"),
  );

  renderBarChart(
    $("#pool-method-chart"),
    Object.entries(report.wash_volume_by_method).map(([method, volume]) => ({
      label: method, value: volume,
    })),
    (v) => usdCompact.format(v),
  );

  loadPoolTrades();
}

async function loadPoolTrades() {
  const offset = poolState.page * PAGE_SIZE;
  const query = `limit=${PAGE_SIZE}&offset=${offset}&wash_only=${poolState.washOnly}`;
  let data;
  try {
    data = await api(`/api/v1/pools/${poolState.chain}/${poolState.address}/trades?${query}`);
  } catch (error) {
    $("#pool-error").textContent = `Failed to load trades: ${error.message}`;
    $("#pool-error").classList.remove("hidden");
    return;
  }

  const body = $("#pool-trades-table tbody");
  body.textContent = "";
  for (const trade of data.trades) {
    const tr = el("tr");
    tr.append(
      el("td", { text: new Date(trade.block_timestamp).toLocaleString() }),
      el("td", { class: "addr", text: shortAddr(trade.sender), title: trade.sender }),
      el("td", { class: "addr", text: shortAddr(trade.recipient), title: trade.recipient }),
      el("td", { class: "num", text: trade.volume_usd == null ? "—" : usd.format(trade.volume_usd) }),
      el("td", trade.is_wash_trade
        ? { class: "flag", text: trade.detection_method || "wash" }
        : { class: "flag-ok", text: "clean" }),
      el("td", { class: "num", text: trade.is_wash_trade ? trade.wash_trade_score.toFixed(2) : "—" }),
    );
    body.appendChild(tr);
  }
  $("#pool-page").textContent = `page ${poolState.page + 1}`;
  $("#pool-prev").disabled = poolState.page === 0;
  $("#pool-next").disabled = data.count < PAGE_SIZE;
}

/* ---------------------------------------------------------- live monitor */

const monitor = {
  socket: null,
  alerts: 0,
  polls: 0,
  stop() {
    if (this.socket) { this.socket.close(); this.socket = null; }
    $("#monitor-toggle").textContent = "Start monitoring";
  },
};

function feedItem(kind, text) {
  const item = el("li");
  item.append(
    el("span", { class: "ts", text: nowTs() }),
    el("span", { class: `kind ${kind}`, text: kind }),
    el("span", { text }),
  );
  const feed = $("#monitor-feed");
  feed.prepend(item);
  while (feed.children.length > 200) feed.removeChild(feed.lastChild);
}

function renderMonitorTiles(state) {
  const tiles = $("#monitor-tiles");
  tiles.textContent = "";
  tiles.append(
    tile("Status", state, monitor.socket ? "websocket connected" : ""),
    tile("Alerts", num.format(monitor.alerts), "flagged this session"),
    tile("Detection passes", num.format(monitor.polls), "with activity"),
  );
}

$("#monitor-form").addEventListener("submit", (event) => {
  event.preventDefault();
  if (monitor.socket) {
    monitor.stop();
    renderMonitorTiles("stopped");
    feedItem("status", "monitoring stopped");
    return;
  }
  const chain = Number($("#monitor-chain").value);
  const address = $("#monitor-address").value.trim();
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${scheme}://${location.host}/api/v1/ws/monitor/${chain}/${address}`);
  monitor.socket = socket;
  monitor.alerts = 0;
  monitor.polls = 0;
  $("#monitor-toggle").textContent = "Stop monitoring";
  renderMonitorTiles("connecting…");

  socket.onmessage = (message) => {
    const event_ = JSON.parse(message.data);
    if (event_.type === "alert") {
      monitor.alerts += 1;
      const d = event_.data;
      feedItem("alert",
        `${d.detection_method} score ${Number(d.wash_trade_score).toFixed(2)} ` +
        `${shortAddr(d.sender)} → ${shortAddr(d.recipient)} ` +
        (d.volume_usd != null ? usd.format(d.volume_usd) : ""));
    } else if (event_.type === "stats") {
      monitor.polls += 1;
      feedItem("stats", `block ${event_.data.block}: ${event_.data.new_alerts} new alerts`);
    } else if (event_.type === "status") {
      feedItem("status", event_.data.state +
        (event_.data.from_block ? ` from block ${event_.data.from_block}` : ""));
      renderMonitorTiles(event_.data.state);
      return;
    } else if (event_.type === "error") {
      feedItem("error", event_.data.reason);
    }
    renderMonitorTiles(monitor.socket ? "monitoring" : "stopped");
  };
  socket.onclose = (closeEvent) => {
    if (closeEvent.code === 4401) showLogin(true);
    monitor.socket = null;
    $("#monitor-toggle").textContent = "Start monitoring";
    renderMonitorTiles("disconnected");
    feedItem("status", `connection closed (${closeEvent.code})`);
  };
  socket.onerror = () => feedItem("error", "websocket error");
});

/* --------------------------------------------------------------- audits */

function auditLogItem(kind, text) {
  const item = el("li");
  item.append(
    el("span", { class: "ts", text: nowTs() }),
    el("span", { class: `kind ${kind}`, text: kind }),
    el("span", { text }),
  );
  $("#audit-log").prepend(item);
}

$("#audit-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const payload = {
    chain_id: Number($("#audit-chain").value),
    pool_address: $("#audit-address").value.trim(),
    start_block: $("#audit-start").value ? Number($("#audit-start").value) : null,
    end_block: $("#audit-end").value ? Number($("#audit-end").value) : null,
    use_ml: $("#audit-ml").checked,
    use_heuristics: $("#audit-heuristics").checked,
  };
  const submitBtn = $("#audit-submit-btn");
  const originalText = submitBtn.textContent;
  submitBtn.disabled = true;
  submitBtn.textContent = "Starting…";
  let started;
  try {
    started = await api("/api/v1/audits", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (error) {
    auditLogItem("error", `failed to start: ${error.message}`);
    return;
  } finally {
    submitBtn.disabled = false;
    submitBtn.textContent = originalText;
  }
  auditLogItem("status", `task ${started.task_id} started for ${shortAddr(payload.pool_address)}`);
  pollAudit(started.task_id);
});

async function pollAudit(taskId) {
  for (let i = 0; i < 2880; i += 1) {           // up to ~4h at 5s cadence
    await new Promise((resolve) => setTimeout(resolve, 5000));
    let status;
    try { status = await api(`/api/v1/audits/${taskId}`); } catch { return; }
    if (status.status === "completed") {
      const metrics = status.result.risk_metrics || {};
      auditLogItem("status",
        `task ${taskId} completed: ${status.result.wash_trades_detected} wash trades, ` +
        `severity ${metrics.severity || "—"}, wash volume ` +
        `${metrics.wash_trade_volume_usd != null ? usd.format(metrics.wash_trade_volume_usd) : "—"}`);
      loadOverview();
      return;
    }
    if (status.status === "failed") {
      auditLogItem("error", `task ${taskId} failed: ${status.error}`);
      return;
    }
  }
  auditLogItem("error", `task ${taskId}: gave up polling`);
}

/* ---------------------------------------------------------------- boot */

(async function boot() {
  const ok = await checkSession();
  if (ok) loadOverview();
  else showLogin(true);
})();
