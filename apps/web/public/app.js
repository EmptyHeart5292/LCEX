/* LCEX 交易页 —— vanilla JS,数据全部来自 API 契约(packages/api-spec)。 */
"use strict";

const state = {
  symbol: localStorage.getItem("sym") || "BTC-USDT",
  side: "bid",
  lastDepthSeq: 0,
  ws: null,
  wsBackoff: 1000,
  candles: [],
};

const $ = (id) => document.getElementById(id);
const fmt = (v) => (v === "" || v === null || v === undefined) ? "--" : v;

// ---------- REST ----------

async function api(path, opts = {}) {
  const headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
  if (opts.auth) headers["X-User-Id"] = $("uid").value;
  const res = await fetch(path, Object.assign({}, opts, { headers }));
  const body = await res.json();
  if (body.code !== 0) throw Object.assign(new Error(body.message), { code: body.code });
  return body.data;
}

async function loadTickers() {
  const list = await api(`/api/v1/tickers?symbol=${state.symbol}`);
  const t = list[0];
  if (!t) return;
  $("tLast").textContent = t.last || "--";
  const chg = parseFloat(t.changePct24h || "0");
  $("tChg").textContent = (chg >= 0 ? "+" : "") + (t.changePct24h || "0") + "%";
  $("tChg").className = "chg " + (chg >= 0 ? "up" : "down");
  $("tHigh").textContent = fmt(t.high24h);
  $("tLow").textContent = fmt(t.low24h);
  $("tVol").textContent = fmt(t.volume24h);
}

async function loadDepthSnapshot() {
  const d = await api(`/api/v1/depth?symbol=${state.symbol}&limit=12`);
  state.lastDepthSeq = d.seq;
  renderDepth(d.bids, d.asks);
}

async function loadTrades() {
  const rows = await api(`/api/v1/trades?symbol=${state.symbol}&limit=20`);
  renderTrades(rows);
}

async function loadKlines() {
  const ks = await api(`/api/v1/klines?symbol=${state.symbol}&interval=1m&limit=60`);
  state.candles = ks.map((k) => ({
    start: +k[0], open: +k[1], high: +k[2], low: +k[3], close: +k[4], volume: +k[5],
  }));
  drawKline();
}

async function loadBalances() {
  const rows = await api("/api/v1/account/balances", { auth: true });
  const tb = $("balances").querySelector("tbody");
  tb.innerHTML = "";
  rows.forEach((b) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${b.currency}</td><td>${b.available || "0"}</td><td>${b.frozen || "0"}</td>`;
    tb.appendChild(tr);
  });
  const base = state.symbol.split("-")[0], quote = state.symbol.split("-")[1];
  const row = (c) => rows.find((r) => r.currency === c);
  $("avail").textContent = state.side === "bid" ? (row(quote)?.available ?? "0") : (row(base)?.available ?? "0");
  $("availCcy").textContent = state.side === "bid" ? quote : base;
}

async function loadOpenOrders() {
  const rows = await api(`/api/v1/orders/open?symbol=${state.symbol}`, { auth: true });
  const tb = $("openOrders").querySelector("tbody");
  tb.innerHTML = "";
  $("noOrders").style.display = rows.length ? "none" : "block";
  rows.forEach((o) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${(o.createdAt || "").slice(11, 19)}</td>
      <td class="${o.side === "bid" ? "bid" : "ask"}">${o.side === "bid" ? "买入" : "卖出"}</td>
      <td>${fmt(o.price)}</td><td>${o.qty}</td><td>${o.filledQty || "0"}</td>
      <td><button class="cancelBtn" data-id="${o.orderId}">撤单</button></td>`;
    tb.appendChild(tr);
  });
}

// ---------- 渲染 ----------

function renderDepth(bids, asks) {
  const row = (lv) => `<div class="row"><span>${lv[0]}</span><span class="qty">${lv[1]}</span></div>`;
  const mk = (levels, n) => {
    const el = document.createElement("div");
    el.className = "row";
    if (!levels.length) { el.innerHTML = `<span class="dim">--</span><span></span>`; return el; }
    el.innerHTML = row(levels[n - 1] || ["--", ""]);
    return el;
  };
  const asksEl = $("asks"); asksEl.innerHTML = "";
  [...asks].reverse().slice(0, 12).forEach((lv) => {
    const el = document.createElement("div");
    el.className = "row";
    el.innerHTML = `<span>${lv[0]}</span><span class="qty">${lv[1]}</span>`;
    asksEl.appendChild(el);
  });
  if (!asks.length) asksEl.innerHTML = `<div class="row dim"><span>暂无卖盘</span><span></span></div>`;
  $("mid").textContent = $("tLast").textContent;
  const bidsEl = $("bids"); bidsEl.innerHTML = "";
  (bids || []).slice(0, 12).forEach((lv) => {
    const el = document.createElement("div");
    el.className = "row";
    el.innerHTML = `<span>${lv[0]}</span><span class="qty">${lv[1]}</span>`;
    bidsEl.appendChild(el);
  });
  if (!(bids || []).length) bidsEl.innerHTML = `<div class="row dim"><span>暂无买盘</span><span></span></div>`;
  $("depthSeq").textContent = "seq " + state.lastDepthSeq;
}

function renderTrades(rows) {
  const tb = $("trades").querySelector("tbody");
  tb.innerHTML = "";
  rows.forEach((t) => {
    const tr = document.createElement("tr");
    tr.className = t.side; // taker 方向着色
    tr.innerHTML = `<td>${t.price}</td><td>${t.qty}</td><td>${new Date(t.ts).toLocaleTimeString()}</td>`;
    tb.appendChild(tr);
  });
}

function drawKline() {
  const cv = $("kline"), ctx = cv.getContext("2d");
  ctx.clearRect(0, 0, cv.width, cv.height);
  const cs = state.candles;
  if (!cs.length) return;
  const pad = 30, W = cv.width - pad, H = cv.height - 20;
  const hi = Math.max(...cs.map((c) => c.high)), lo = Math.min(...cs.map((c) => c.low));
  const span = hi - lo || 1;
  const y = (p) => 10 + (hi - p) / span * (H - 10);
  const bw = W / cs.length;
  // 网格与价格刻度
  ctx.strokeStyle = "#232a3a"; ctx.fillStyle = "#8b93a7"; ctx.font = "10px sans-serif";
  for (let i = 0; i <= 4; i++) {
    const p = lo + (span * i) / 4;
    ctx.beginPath(); ctx.moveTo(0, y(p)); ctx.lineTo(W, y(p)); ctx.stroke();
    ctx.fillText(p.toFixed(2), W + 2, y(p) + 3);
  }
  cs.forEach((c, i) => {
    const x = i * bw + bw / 2, up = c.close >= c.open;
    ctx.strokeStyle = ctx.fillStyle = up ? "#2ebd85" : "#f6465d";
    ctx.beginPath(); ctx.moveTo(x, y(c.high)); ctx.lineTo(x, y(c.low)); ctx.stroke();
    const top = y(Math.max(c.open, c.close)), h = Math.max(1, Math.abs(y(c.open) - y(c.close)));
    ctx.fillRect(x - bw * 0.3, top, bw * 0.6, h);
  });
}

function toast(msg, ok) {
  const t = $("toast");
  t.textContent = msg;
  t.style.display = "block";
  t.style.borderColor = ok ? "var(--up)" : "var(--down)";
  setTimeout(() => (t.style.display = "none"), 3000);
}

// ---------- WebSocket(协议见 packages/api-spec/ws/protocol.md)----------

function wsTopic(base) { return `${base}@${state.symbol}`; }

function connectWS() {
  if (state.ws) { try { state.ws.close(); } catch (_) {} }
  const proto = location.protocol === "https:" ? "wss://" : "ws://";
  const ws = new WebSocket(proto + location.host + "/stream");
  state.ws = ws;
  ws.onopen = () => {
    state.wsBackoff = 1000;
    ws.send(JSON.stringify({ op: "subscribe", args: [
      wsTopic("ticker"), wsTopic("depth") + "@50", wsTopic("trades"), wsTopic("kline") + "@1m",
    ]}));
  };
  ws.onmessage = (e) => {
    let m;
    try { m = JSON.parse(e.data); } catch (_) { return; }
    if (m.channel === "ping") { ws.send(JSON.stringify({ op: "pong" })); return; }
    const sym = (m.symbol || "").toUpperCase();
    if (sym !== state.symbol) return;
    if (m.channel === "ticker" && m.data) loadTickers();
    if (m.channel === "depth") {
      if (m.type === "snapshot") { state.lastDepthSeq = m.seq; api(`/api/v1/depth?symbol=${state.symbol}&limit=12`).then((d) => { state.lastDepthSeq = d.seq; renderDepth(d.bids, d.asks); }); return; }
      if (m.seq !== state.lastDepthSeq + 1) { loadDepthSnapshot(); return; } // seq 断档 → 重新拉快照
      state.lastDepthSeq = m.seq;
      // 增量应用:直接重拉太重,这里用本地聚合的简化策略 —— 有更新即拉一次快照(盘口 12 档开销可忽略)
      api(`/api/v1/depth?symbol=${state.symbol}&limit=12`).then((d) => { state.lastDepthSeq = d.seq; renderDepth(d.bids, d.asks); });
    }
    if (m.channel === "trades" && m.data) {
      const tb = $("trades").querySelector("tbody");
      const tr = document.createElement("tr");
      tr.className = m.data.side;
      tr.innerHTML = `<td>${m.data.price}</td><td>${m.data.qty}</td><td>${new Date(m.data.ts).toLocaleTimeString()}</td>`;
      tb.prepend(tr);
      while (tb.children.length > 20) tb.removeChild(tb.lastChild);
    }
    if (m.channel === "kline" && m.data) {
      const d = m.data;
      const c = { start: +d.start, open: +d.open, high: +d.high, low: +d.low, close: +d.close, volume: +d.volume };
      const arr = state.candles;
      const lastC = arr[arr.length - 1];
      if (lastC && lastC.start === c.start) arr[arr.length - 1] = c; else arr.push(c);
      if (arr.length > 60) arr.shift();
      drawKline();
    }
  };
  ws.onclose = () => {
    setTimeout(connectWS, state.wsBackoff);
    state.wsBackoff = Math.min(state.wsBackoff * 2, 30000);
  };
}

// ---------- 事件 ----------

$("symbolSel").value = state.symbol;
$("symbolSel").addEventListener("change", (e) => {
  state.symbol = e.target.value;
  localStorage.setItem("sym", state.symbol);
  state.lastDepthSeq = 0;
  refreshAll();
});

$("btnBuy").addEventListener("click", () => setSide("bid"));
$("btnSell").addEventListener("click", () => setSide("ask"));
function setSide(side) {
  state.side = side;
  $("btnBuy").classList.toggle("active", side === "bid");
  $("btnSell").classList.toggle("active", side === "ask");
  $("submit").className = "submit " + side;
  $("submit").textContent = (side === "bid" ? "买入 " : "卖出 ") + state.symbol.split("-")[0];
  loadBalances();
}

$("submit").addEventListener("click", async () => {
  const msg = $("formMsg");
  msg.textContent = "";
  try {
    const body = {
      symbol: state.symbol,
      clientOrderId: "web-" + Date.now() + "-" + Math.random().toString(36).slice(2, 8),
      side: state.side,
      type: "LIMIT",
      timeInForce: "GTC",
      price: $("price").value,
      qty: $("qty").value,
    };
    const d = await api("/api/v1/orders", { method: "POST", body: JSON.stringify(body), auth: true });
    toast(`下单成功 #${d.orderId}`, true);
    loadOpenOrders(); loadBalances();
  } catch (err) {
    msg.textContent = `错误 ${err.code || ""}: ${err.message}`;
    toast("下单失败: " + err.message, false);
  }
});

$("openOrders").addEventListener("click", async (e) => {
  const id = e.target.dataset && e.target.dataset.id;
  if (!id) return;
  try {
    await api(`/api/v1/orders/${id}`, { method: "DELETE", auth: true });
    toast(`撤单成功 #${id}`, true);
    loadOpenOrders(); loadBalances();
  } catch (err) {
    toast("撤单失败: " + err.message, false);
  }
});

// ---------- 初始化 ----------

function refreshAll() {
  loadTickers().catch(console.error);
  loadDepthSnapshot().catch(console.error);
  loadTrades().catch(console.error);
  loadKlines().catch(console.error);
  loadBalances().catch(console.error);
  loadOpenOrders().catch(console.error);
}

refreshAll();
connectWS();
setInterval(() => { loadOpenOrders().catch(() => {}); loadBalances().catch(() => {}); }, 4000);
setInterval(() => { loadKlines().catch(() => {}); }, 10000);
