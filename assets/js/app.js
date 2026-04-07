//Last Bid — realtime client
import { Socket } from "phoenix";

// ─── Boot ───────────────────────────────────────────────────────────
const config = window.DarkPool || {};

function boot(cfg) {
  if (!cfg.socketToken) {
    console.debug("[DarkPool] No socket token found — WebSocket not started.");
    return;
  }

  const socket = new Socket("/socket", {
    params: { token: cfg.socketToken },
  });

  socket.connect();

  const lobbyMgr = new LobbyManager(socket, cfg);
  lobbyMgr.init();

  if (cfg.matchId) {
    const matchMgr = new MatchManager(socket, cfg, lobbyMgr);
    matchMgr.init();
  }
}

// ─── Toast ──────────────────────────────────────────────────────────
const Toast = {
  show(msg, type = "info", duration = 4000) {
    const icons = {
      info: "ℹ",
      success: "✓",
      warning: "⚠",
      error: "✕",
    };

    let container = document.getElementById("toast-container");
    if (!container) {
      container = document.createElement("div");
      container.id = "toast-container";
      container.style.position = "fixed";
      container.style.top = "16px";
      container.style.right = "16px";
      container.style.zIndex = "9999";
      container.style.display = "flex";
      container.style.flexDirection = "column";
      container.style.gap = "8px";
      document.body.appendChild(container);
    }

    const el = document.createElement("div");
    el.className = `toast ${type}`;
    el.style.padding = "10px 12px";
    el.style.borderRadius = "10px";
    el.style.border = "1px solid rgba(255,255,255,.12)";
    el.style.background = "#111827";
    el.style.color = "#e5e7eb";
    el.style.fontSize = "13px";
    el.style.fontFamily = "ui-monospace, SFMono-Regular, Menlo, monospace";
    el.style.display = "flex";
    el.style.alignItems = "center";
    el.style.gap = "8px";
    el.style.cursor = "pointer";
    el.innerHTML = `
      <span style="flex-shrink:0">${icons[type] || "•"}</span>
      <span>${escapeHtml(msg)}</span>
    `;

    container.appendChild(el);

    const timer = setTimeout(() => el.remove(), duration);
    el.addEventListener("click", () => {
      clearTimeout(timer);
      el.remove();
    });
  },
};

// ─── Sparkline Chart ────────────────────────────────────────────────
class SparklineChart {
  constructor(containerId, options = {}) {
    this.container = document.getElementById(containerId);
    this.history = [];
    this.maxPoints = options.maxPoints || 20;
    this.w = options.width || 80;
    this.h = options.height || 28;
    if (this.container) this._createSvg();
  }

  _createSvg() {
    this.svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    this.svg.setAttribute("viewBox", `0 0 ${this.w} ${this.h}`);
    this.svg.setAttribute("width", this.w);
    this.svg.setAttribute("height", this.h);

    const gradId = `sg-${Math.random().toString(36).slice(2)}`;
    this.svg.innerHTML = `
      <defs>
        <linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="var(--spark-color,#10b981)" stop-opacity="0.35"/>
          <stop offset="100%" stop-color="var(--spark-color,#10b981)" stop-opacity="0"/>
        </linearGradient>
      </defs>
      <path class="area" fill="url(#${gradId})"></path>
      <polyline class="line" fill="none" stroke="var(--spark-color,#10b981)" stroke-width="2"></polyline>
    `;
    this.gradId = gradId;
    this.container.innerHTML = "";
    this.container.appendChild(this.svg);
  }

  reset(values = []) {
    this.history = [];
    values.forEach((v) => this.push(v));
  }

  push(price) {
    const p = parseFloat(price);
    if (Number.isNaN(p)) return;
    this.history.push(p);
    if (this.history.length > this.maxPoints) this.history.shift();
    this._render();
  }

  setColor(isUp) {
    if (!this.svg) return;
    const c = isUp ? "#10b981" : "#ef4444";
    this.svg.style.setProperty("--spark-color", c);
    const firstStop = this.svg.querySelector("stop");
    if (firstStop) firstStop.setAttribute("stop-color", c);
  }

  _render() {
    if (!this.svg || this.history.length === 0) return;

    const min = Math.min(...this.history);
    const max = Math.max(...this.history);
    const range = max - min || 1;
    const pad = 2;
    const step =
      this.history.length > 1
        ? (this.w - pad * 2) / (this.history.length - 1)
        : 0;

    const points = this.history.map((v, i) => {
      const x = pad + i * step;
      const y = pad + (1 - (v - min) / range) * (this.h - pad * 2);
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    });

    const line = this.svg.querySelector(".line");
    const area = this.svg.querySelector(".area");

    if (line) line.setAttribute("points", points.join(" "));
    if (area && points.length > 0) {
      const first = points[0].split(",");
      const last = points[points.length - 1].split(",");
      area.setAttribute(
        "d",
        `M${first[0]},${this.h - pad} L${points.join(" L")} L${last[0]},${this.h - pad} Z`,
      );
    }
  }
}

// ─── Large Detail Chart ─────────────────────────────────────────────
class DetailChart {
  constructor(canvasId) {
    this.canvas = document.getElementById(canvasId);
    this.ctx = this.canvas?.getContext("2d");
  }

  render(points) {
    if (!this.canvas || !this.ctx) return;

    const rect = this.canvas.getBoundingClientRect();
    const w = Math.max(420, Math.floor(rect.width || 420));
    const h = Math.max(260, Math.floor(rect.height || 260));
    this.canvas.width = w;
    this.canvas.height = h;

    const ctx = this.ctx;
    ctx.clearRect(0, 0, w, h);

    if (!points || points.length === 0) return;

    const values = points.map((p) => p.price);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = max - min || 1;
    const padL = 44;
    const padR = 18;
    const padT = 18;
    const padB = 30;

    ctx.strokeStyle = "rgba(255,255,255,0.08)";
    ctx.lineWidth = 1;

    for (let i = 0; i < 4; i++) {
      const y = padT + ((h - padT - padB) / 3) * i;
      ctx.beginPath();
      ctx.moveTo(padL, y);
      ctx.lineTo(w - padR, y);
      ctx.stroke();
    }

    ctx.beginPath();
    points.forEach((p, i) => {
      const x = padL + (i / Math.max(1, points.length - 1)) * (w - padL - padR);
      const y = h - padB - ((p.price - min) / range) * (h - padT - padB);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = "#10b981";
    ctx.lineWidth = 3;
    ctx.stroke();

    points.forEach((p, i) => {
      const x = padL + (i / Math.max(1, points.length - 1)) * (w - padL - padR);
      const y = h - padB - ((p.price - min) / range) * (h - padT - padB);
      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
      ctx.fillStyle = "#10b981";
      ctx.fill();
    });

    ctx.fillStyle = "#9ca3af";
    ctx.font = "12px ui-monospace, monospace";
    ctx.fillText(`$${max.toFixed(2)}`, 6, 14);
    ctx.fillText(`$${min.toFixed(2)}`, 6, h - 10);

    points.forEach((p, i) => {
      const x = padL + (i / Math.max(1, points.length - 1)) * (w - padL - padR);
      ctx.fillText(String(p.round), x - 3, h - 8);
    });
  }
}

// ─── LobbyManager ───────────────────────────────────────────────────
class LobbyManager {
  constructor(socket, cfg) {
    this.socket = socket;
    this.cfg = cfg;
    this.channel = null;
    this.joined = false;
  }

  init() {
    this.channel = this.socket.channel("lobby:general", {});

    this.channel.on("open_matches", (p) =>
      this._updateMatchList(p.matches || []),
    );
    this.channel.on("match_created", (match) => this._addOrUpdateMatch(match));
    this.channel.on("match_updated", (match) => this._addOrUpdateMatch(match));
    this.channel.on("match_started", (p) => this._onMatchStarted(p.match_id));
    this.channel.on("presence_state", (state) => this._onPresenceState(state));
    this.channel.on("presence_diff", (diff) => this._onPresenceDiff(diff));

    this.channel
      .join()
      .receive("ok", () => {
        this.joined = true;
        this._wireJoinButtons(document);
      })
      .receive("error", () => {
        this._wireJoinButtons(document);
      });
  }

  push(event, payload) {
    if (this.channel) return this.channel.push(event, payload);
  }

  _wireJoinButtons(root = document) {
    root
      .querySelectorAll(".join-match-btn:not([data-wired])")
      .forEach((btn) => {
        btn.dataset.wired = "1";
        btn.addEventListener("click", (e) => {
          e.preventDefault();
          const matchId =
            btn.dataset.matchId ||
            btn.closest("[data-match-id]")?.dataset.matchId;
          if (!matchId) return;
          this._joinMatch(matchId, btn);
        });
      });
  }

  _joinMatch(matchId, btn) {
    const original = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Joining...";

    this.channel
      .push("join_match", { match_id: matchId })
      .receive("ok", () => {
        window.location.href = `/matches/${matchId}`;
      })
      .receive("error", (err) => {
        btn.disabled = false;
        btn.textContent = original;
        Toast.show(
          `Could not join: ${err?.reason || "unknown error"}`,
          "error",
        );
      })
      .receive("timeout", () => {
        btn.disabled = false;
        btn.textContent = original;
        Toast.show("Join timed out. Please try again.", "warning");
      });
  }

  _addOrUpdateMatch(match) {
    const list = document.getElementById("open-matches-list");
    if (!list || !match) return;

    document.getElementById("no-matches-placeholder")?.remove();

    const existing = list.querySelector(`[data-match-id="${match.id}"]`);
    const card = this._buildMatchCard(match);

    if (existing) existing.replaceWith(card);
    else list.insertAdjacentElement("afterbegin", card);

    this._wireJoinButtons(list);
  }

  _buildMatchCard(match) {
    const div = document.createElement("div");
    div.className = "card card-hover p-4 match-card fade-in-up";
    div.dataset.matchId = match.id;

    const isFull = (match.player_count || 0) >= match.max_players;

    div.innerHTML = `
      <div class="flex items-center justify-between">
        <div class="flex items-center gap-3">
          <div class="w-9 h-9 rounded-lg bg-emerald-500/10 border border-emerald-500/20 flex items-center justify-center shrink-0">
            <svg class="w-4 h-4 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
              <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"></polyline>
            </svg>
          </div>
          <div>
            <div class="font-semibold text-sm text-white font-mono">${escapeHtml(match.name)}</div>
            <div class="text-xs text-gray-500 mt-0.5">
              hosted by <span class="text-gray-400">${escapeHtml(match.host_username || "—")}</span>
            </div>
          </div>
        </div>
        <div class="flex items-center gap-3">
          <div class="text-right">
            <div class="text-xs font-mono text-gray-400">
              <span class="text-white font-semibold">${match.player_count || 0}</span>/${match.max_players || 4}
            </div>
          </div>
          ${
            isFull
              ? `<span class="btn-ghost text-xs py-1.5 px-3 opacity-40 pointer-events-none">Full</span>`
              : `<button type="button" data-match-id="${escapeHtml(match.id)}" class="join-match-btn btn-primary text-xs py-1.5 px-4">Join</button>`
          }
        </div>
      </div>
    `;

    return div;
  }

  _updateMatchList(matches) {
    const list = document.getElementById("open-matches-list");
    if (!list) return;

    if (!matches.length) return;

    list.innerHTML = "";
    matches.forEach((m) => list.appendChild(this._buildMatchCard(m)));
    this._wireJoinButtons(list);
  }

  _onMatchStarted(matchId) {
    const card = document.querySelector(
      `#open-matches-list [data-match-id="${matchId}"]`,
    );
    if (card) card.remove();
  }

  _onPresenceState(state) {
    const list = document.getElementById("lobby-presence-list");
    const counter = document.getElementById("lobby-online-count");
    if (!list) return;

    const users = [];
    Object.values(state || {}).forEach((entry) => {
      const meta = entry.metas?.[0];
      if (meta?.username) users.push(meta.username);
    });

    if (counter) counter.textContent = String(users.length);
    this._renderPresenceList(list, users);
  }

  _onPresenceDiff(diff) {
    const list = document.getElementById("lobby-presence-list");
    if (!list) return;

    const current = new Map();
    list.querySelectorAll("[data-presence-user]").forEach((el) => {
      current.set(el.dataset.presenceUser, el);
    });

    Object.values(diff?.joins || {}).forEach((entry) => {
      const meta = entry.metas?.[0];
      if (!meta?.username || current.has(meta.username)) return;
      const el = this._buildPresenceRow(meta.username);
      list.appendChild(el);
      current.set(meta.username, el);
    });

    Object.values(diff?.leaves || {}).forEach((entry) => {
      const meta = entry.metas?.[0];
      if (!meta?.username) return;
      const el = current.get(meta.username);
      if (el) {
        el.remove();
        current.delete(meta.username);
      }
    });

    const counter = document.getElementById("lobby-online-count");
    if (counter) counter.textContent = String(current.size);

    if (current.size === 0) {
      list.innerHTML = `<p class="text-xs text-gray-600 font-mono">Nobody else online.</p>`;
    }
  }

  _renderPresenceList(list, users) {
    if (!users.length) {
      list.innerHTML = `<p class="text-xs text-gray-600 font-mono">Nobody else online.</p>`;
      return;
    }

    list.innerHTML = "";
    users.forEach((u) => list.appendChild(this._buildPresenceRow(u)));
  }

  _buildPresenceRow(username) {
    const el = document.createElement("div");
    el.className = "flex items-center gap-2 fade-in-up";
    el.dataset.presenceUser = username;
    el.innerHTML = `
      <div class="presence-dot"></div>
      <span class="text-xs font-mono text-gray-300">${escapeHtml(username)}</span>
    `;
    return el;
  }
}

// ─── MatchManager ───────────────────────────────────────────────────
class MatchManager {
  constructor(socket, cfg, lobbyMgr) {
    this.socket = socket;
    this.cfg = cfg;
    this.lobbyMgr = lobbyMgr;
    this.channel = null;
    this.players = [];
    this.companies = [];
    this.myState = null;
    this.phase = null;
    this.countdown = null;
    this.phaseOverlayTimer = null;

    this.prevPrices = {};
    this.sparklineCharts = {};
    this.priceHistory = {};
    this.selectedTicker = null;
    this.detailChart = new DetailChart("stock-detail-chart");

    this.onlineUsers = new Set();
  }

  init() {
    this.channel = this.socket.channel(`match:${this.cfg.matchId}`, {});

    this.channel.on("state_updated", (p) => this._onStateUpdated(p));
    this.channel.on("phase_changed", (p) => this._onPhaseChanged(p));
    this.channel.on("private_state", (p) => this._onPrivateState(p));
    this.channel.on("private_events", (p) => this._onPrivateEvents(p));
    this.channel.on("new_message", (m) => this._appendChat(m));
    this.channel.on("match_finished", (p) => this._onMatchFinished(p));
    this.channel.on("presence_state", (s) => this._onPresenceState(s));
    this.channel.on("presence_diff", (d) => this._onPresenceDiff(d));
    this.channel.on("waiting_room_updated", (p) =>
      this._onWaitingRoomUpdated(p),
    );

    this.channel
      .join()
      .receive("ok", () => {})
      .receive("error", ({ reason }) => {
        Toast.show(`Could not join match: ${reason}`, "error");
      });

    this._wireControls();
  }

  _wireControls() {
    const startBtn = document.getElementById("start-match-btn");
    if (startBtn) {
      startBtn.addEventListener("click", (e) => {
        e.preventDefault();
        startBtn.disabled = true;
        startBtn.textContent = "Starting...";

        this.lobbyMgr
          .push("start_match", { match_id: this.cfg.matchId })
          ?.receive("ok", () => {
            startBtn.remove();
          })
          ?.receive("error", (err) => {
            startBtn.disabled = false;
            startBtn.textContent = "Start Match";
            Toast.show(
              `Could not start: ${err?.reason || "unknown error"}`,
              "error",
            );
          });
      });
    }

    document.querySelectorAll(".action-btn").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        if (btn.disabled) return;
        this._openActionModal(btn.dataset);
      });
    });

    document
      .getElementById("copy-invite-btn")
      ?.addEventListener("click", (e) => {
        e.preventDefault();
        const url = window.location.href;
        navigator.clipboard
          ?.writeText(url)
          .then(() => Toast.show("Invite link copied", "success"))
          .catch(() => window.prompt("Copy this invite link:", url));
      });

    document.getElementById("modal-close")?.addEventListener("click", (e) => {
      e.preventDefault();
      this._closeModal();
    });

    document.getElementById("action-modal")?.addEventListener("click", (e) => {
      if (e.target === e.currentTarget) this._closeModal();
    });

    document.getElementById("modal-submit")?.addEventListener("click", (e) => {
      e.preventDefault();
      this._submitAction();
    });

    document
      .getElementById("input-quantity")
      ?.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          e.preventDefault();
          this._submitAction();
        }
      });

    const sendBtn = document.getElementById("chat-send");
    const chatInput = document.getElementById("chat-input");

    if (sendBtn && chatInput) {
      sendBtn.addEventListener("click", (e) => {
        e.preventDefault();
        this._sendChatMessage();
      });

      chatInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) {
          e.preventDefault();
          this._sendChatMessage();
        }
      });
    }
  }

  // ── state updates ────────────────────────────────────────────────

  _onStateUpdated(payload) {
    this.players = payload.players || [];
    this.companies = payload.companies || [];
    this.phase = payload.phase;

    this._renderRound(payload.round, payload.total_rounds);
    this._renderPhase(payload.phase);
    this._renderPlayers(payload.players || []);
    this._renderMarket(payload.companies || [], payload.round);
    this._renderActionPanel(payload.phase);
    const events = payload.public_events || [];
    if (events.length > 0) {
      this._renderEvents(events);
      this._renderNews(events);
    }
    this._updateChatIndicator(payload.phase);

    if (payload.phase === "negotiation" && payload.negotiation_deadline) {
      this._startCountdown(new Date(payload.negotiation_deadline));
    } else if (payload.phase !== "negotiation") {
      this._clearCountdown();
    }
  }

  _onPhaseChanged(payload) {
    this.phase = payload.phase;

    this._renderRound(payload.round, payload.total_rounds);
    this._renderPhase(payload.phase);
    this._renderActionPanel(payload.phase);
    this._updateChatIndicator(payload.phase);
    this._showPhaseOverlay(payload.phase);

    if (payload.phase === "action_submission") {
      this._clearCountdown();
      this._hide(document.getElementById("action-submitted-badge"));
      document.querySelectorAll(".action-btn").forEach((b) => {
        b.disabled = false;
        b.classList.remove("submitted");
      });
    }

    if (payload.phase === "negotiation" && payload.negotiation_deadline) {
      this._startCountdown(new Date(payload.negotiation_deadline));
    } else if (payload.phase !== "negotiation") {
      this._clearCountdown();
    }
  }

  _onPrivateState(state) {
    this.myState = state;
    this._renderMyStats(state);
  }

  _onPrivateEvents(payload) {
    (payload.events || []).forEach((ev) => {
      const msg = ev.message || ev.headline || ev.type || "Event";
      Toast.show(msg, ev.positive ? "success" : "info");
    });
  }

  _onMatchFinished(payload) {
    this._clearCountdown();
    this._renderLeaderboard(payload.leaderboard || []);
    this._showPhaseOverlay("finished");
    this._hide(document.getElementById("action-panel"));
    this._show(document.getElementById("leaderboard-panel"), "block");
    Toast.show("Match over! Final rankings are in.", "info", 8000);
  }

  // ── presence ─────────────────────────────────────────────────────

  _onPresenceState(state) {
    Object.entries(state || {}).forEach(([userId]) => {
      this._setPlayerOnline(userId, true);
    });
  }

  _onPresenceDiff(diff) {
    Object.entries(diff?.joins || {}).forEach(([userId]) =>
      this._setPlayerOnline(userId, true),
    );
    Object.entries(diff?.leaves || {}).forEach(([userId]) =>
      this._setPlayerOnline(userId, false),
    );
  }

  _onWaitingRoomUpdated(payload) {
    if (payload.players) {
      this.players = payload.players;
      this._renderPlayers(payload.players);
    }
  }

  _setPlayerOnline(userId, online) {
    if (online) this.onlineUsers.add(String(userId));
    else this.onlineUsers.delete(String(userId));
    this._applyOnlineState();
  }

  _applyOnlineState() {
    document.querySelectorAll("[data-player-id]").forEach((row) => {
      const dot = row.querySelector(".presence-dot");
      if (dot)
        dot.classList.toggle(
          "offline",
          !this.onlineUsers.has(row.dataset.playerId),
        );
    });
  }

  // ── rendering ────────────────────────────────────────────────────

  _renderRound(round, total) {
    const el = document.getElementById("current-round");
    if (el) el.textContent = round || "—";

    const lastRound = document.getElementById("detail-last-round");
    if (lastRound) lastRound.textContent = round || "1";
  }

  _renderPhase(phase) {
    const container = document.getElementById("phase-badge-container");
    if (!container) return;
    container.innerHTML = `<span class="phase-badge phase-${phase || "waiting"}">${phaseLabel(phase)}</span>`;
  }

  _renderActionPanel(phase) {
    const panel = document.getElementById("action-panel");
    if (!panel) return;

    if (phase === "action_submission") this._show(panel, "block");
    else this._hide(panel);
  }

  _renderPlayers(players) {
    const list = document.getElementById("players-list");
    if (!list || !players) return;

    list.innerHTML = "";

    players
      .slice()
      .sort((a, b) => (a.seat_number || 0) - (b.seat_number || 0))
      .forEach((p) => {
        const row = document.createElement("div");
        row.className = "flex items-center gap-2.5";
        row.dataset.playerId = p.user_id;

        row.innerHTML = `
          <div class="presence-dot offline"></div>
          <div class="flex-1 min-w-0">
            <span class="text-sm text-gray-300 font-mono truncate">${escapeHtml(p.username)}</span>
          </div>
          <span class="text-xs font-mono text-amber-400">${(p.regulatory_heat || 0) > 0 ? `⚖${p.regulatory_heat}` : ""}</span>
          <span class="text-xs font-mono text-blue-400">${p.liquidity_frozen ? "❄" : ""}</span>
          <span class="text-xs font-mono text-emerald-400">${p.has_submitted ? "✓" : ""}</span>
        `;

        list.appendChild(row);
      });

    this._applyOnlineState();
  }

  _renderMyStats(state) {
    this._show(document.getElementById("my-stats"), "block");

    const cash = parseFloat(state.cash || 0);
    const netWorth = parseFloat(state.net_worth || 0);

    const cashEl = document.getElementById("my-cash");
    const nwEl = document.getElementById("my-networth");
    const heatEl = document.getElementById("my-heat");

    if (cashEl) cashEl.textContent = `$${cash.toFixed(2)}`;
    if (nwEl) nwEl.textContent = `$${netWorth.toFixed(2)}`;
    if (heatEl) heatEl.textContent = state.regulatory_heat || 0;

    if (state.liquidity_frozen)
      this._show(document.getElementById("my-frozen-badge"), "block");
    else this._hide(document.getElementById("my-frozen-badge"));

    const holdingsEl = document.getElementById("my-holdings");
    if (holdingsEl) {
      holdingsEl.innerHTML = "";
      Object.entries(state.portfolio || {}).forEach(([ticker, qty]) => {
        const row = document.createElement("div");
        row.className = "flex justify-between text-xs font-mono";
        row.innerHTML = `<span class="text-gray-500">${escapeHtml(ticker)}</span><span class="text-gray-300">${qty} sh</span>`;
        holdingsEl.appendChild(row);
      });
    }
  }

  _renderMarket(companies, round) {
    const list = document.getElementById("companies-list");
    if (!list) return;

    list.innerHTML = "";

    companies.forEach((c) => {
      const price = parseFloat(c.price || 0);
      const prev = this.prevPrices[c.ticker];
      const isUp = prev === undefined ? true : price >= prev;
      this.prevPrices[c.ticker] = price;

      if (!this.priceHistory[c.ticker]) this.priceHistory[c.ticker] = [];
      const history = this.priceHistory[c.ticker];
      const last = history[history.length - 1];
      if (!last || last.round !== round || last.price !== price) {
        history.push({
          round: round || 1,
          price,
          regulatory_heat: c.regulatory_heat || 0,
        });
      }

      const row = document.createElement("div");
      row.className = `ticker-row ${this.selectedTicker === c.ticker ? "selected" : ""}`;
      row.dataset.ticker = c.ticker;

      const chartId = `chart-${c.ticker}`;
      const pct =
        prev === undefined || prev === 0
          ? ""
          : `${price >= prev ? "+" : ""}${(((price - prev) / prev) * 100).toFixed(2)}%`;

      row.innerHTML = `
        <span class="font-mono font-bold text-sm ticker-symbol">${escapeHtml(c.ticker)}</span>
        <span class="text-xs text-gray-500 truncate ticker-name">${escapeHtml(c.name || "")}</span>
        <div class="text-right">
          <div class="font-mono font-semibold text-sm ticker-price" style="color:${isUp ? "#10b981" : "#ef4444"}">$${price.toFixed(2)}</div>
          <div class="text-xs font-mono ticker-change mt-0.5" style="color:${isUp ? "#10b981" : "#ef4444"}">${pct}</div>
        </div>
        <div id="${chartId}" class="flex items-center justify-end" style="height:28px;width:80px"></div>
        <div></div>
      `;

      row.addEventListener("click", () => {
        this.selectedTicker = c.ticker;
        this._renderMarket(this.companies, round);
        this._renderStockDetail(c.ticker);
      });

      list.appendChild(row);

      requestAnimationFrame(() => {
        this.sparklineCharts[c.ticker] = new SparklineChart(chartId, {
          width: 80,
          height: 28,
        });
        this.sparklineCharts[c.ticker].reset(
          this.priceHistory[c.ticker].map((p) => p.price),
        );
        this.sparklineCharts[c.ticker].setColor(isUp);
      });
    });

    if (!this.selectedTicker && companies[0]) {
      this.selectedTicker = companies[0].ticker;
    }

    if (this.selectedTicker) {
      this._renderStockDetail(this.selectedTicker);
    }
  }

  _renderStockDetail(ticker) {
    const company = this.companies.find((c) => c.ticker === ticker);
    const history = this.priceHistory[ticker] || [];
    if (!company) return;

    this._hide(document.getElementById("stock-detail-empty"));
    this._show(document.getElementById("stock-detail-panel"), "block");

    const currentPrice = parseFloat(company.price || 0);
    const firstPrice = history[0]?.price ?? currentPrice;
    const lastPrice = history[history.length - 1]?.price ?? currentPrice;
    const pct = ((lastPrice - firstPrice) / Math.max(firstPrice, 0.0001)) * 100;

    const title = document.getElementById("selected-stock-title");
    const meta = document.getElementById("selected-stock-meta");
    const current = document.getElementById("detail-current-price");
    const change = document.getElementById("detail-round-change");
    const heat = document.getElementById("detail-reg-heat");
    const count = document.getElementById("detail-history-count");
    const lastRound = document.getElementById("detail-last-round");
    const summary = document.getElementById("detail-history-summary");

    if (title) title.textContent = `${ticker} · ${company.name}`;
    if (meta) meta.textContent = `Round-by-round movement for ${ticker}`;
    if (current) current.textContent = `$${currentPrice.toFixed(2)}`;
    if (change) {
      change.textContent = `${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`;
      change.style.color = pct >= 0 ? "#10b981" : "#ef4444";
    }
    if (heat) heat.textContent = String(company.regulatory_heat || 0);
    if (count) count.textContent = String(history.length);
    if (lastRound)
      lastRound.textContent = String(history[history.length - 1]?.round || 1);
    if (summary) summary.textContent = `${history.length} price points tracked`;

    const historyList = document.getElementById("stock-history-list");
    if (historyList) {
      historyList.innerHTML = "";

      if (!history.length) {
        historyList.innerHTML = `<p class="text-xs text-gray-600 font-mono text-center py-4">Waiting for price history…</p>`;
      } else {
        history.forEach((item, idx) => {
          const prev = idx > 0 ? history[idx - 1].price : item.price;
          const dir =
            item.price > prev ? "up" : item.price < prev ? "down" : "flat";
          const row = document.createElement("div");
          row.className = `history-item ${dir}`;
          row.innerHTML = `
            <div class="flex items-center justify-between">
              <span class="text-xs font-mono text-gray-400">Round ${item.round}</span>
              <span class="text-xs font-mono ${
                dir === "up"
                  ? "text-emerald-400"
                  : dir === "down"
                    ? "text-red-400"
                    : "text-gray-400"
              }">$${item.price.toFixed(2)}</span>
            </div>
          `;
          historyList.appendChild(row);
        });
      }
    }

    this.detailChart.render(history);
  }

  _renderEvents(events) {
    const box = document.getElementById("events-feed");
    if (!box) return;

    box.innerHTML = "";

    if (!events.length) {
      box.innerHTML = `<p class="text-xs text-gray-600 font-mono text-center py-2">No events yet.</p>`;
      return;
    }

    events.forEach((ev) => {
      const item = document.createElement("div");
      item.className = "history-item";

      if (ev.type === "news_event") {
        item.innerHTML = `
          <div class="text-xs font-mono text-emerald-400 mb-1">NEWS</div>
          <div class="text-xs text-gray-300">${escapeHtml(ev.headline || "")}</div>
        `;
      } else if (ev.type === "price_updated") {
        item.innerHTML = `
          <div class="text-xs font-mono text-cyan-400 mb-1">${escapeHtml(ev.ticker || "")}</div>
          <div class="text-xs text-gray-300">Price now $${parseFloat(ev.price || 0).toFixed(2)}</div>
        `;
      } else {
        item.innerHTML = `<div class="text-xs text-gray-300">${escapeHtml(ev.headline || JSON.stringify(ev))}</div>`;
      }

      box.appendChild(item);
    });
  }

  _renderNews(events) {
    const box = document.getElementById("news-feed");
    if (!box) return;

    const news = (events || []).filter(
      (ev) => ev.type === "news_event" || ev.type === "price_updated",
    );
    box.innerHTML = "";

    if (!news.length) {
      box.innerHTML = `<p class="text-xs text-gray-600 font-mono text-center py-4">No news yet.</p>`;
      return;
    }

    news.forEach((ev) => {
      const item = document.createElement("div");
      item.className = `news-item ${
        ev.type === "news_event"
          ? (ev.impact || 0) >= 0
            ? "positive"
            : "negative"
          : "neutral"
      }`;

      if (ev.type === "news_event") {
        item.innerHTML = `
          <div class="flex items-center justify-between mb-1">
            <span class="text-[10px] font-mono uppercase tracking-widest text-emerald-400">Round ${ev.round || "—"}</span>
            <span class="text-[10px] font-mono text-gray-500">${escapeHtml((ev.targets || []).join(", "))}</span>
          </div>
          <div class="text-sm text-gray-200">${escapeHtml(ev.headline || "")}</div>
        `;
      } else {
        item.innerHTML = `
          <div class="flex items-center justify-between">
            <span class="text-xs font-mono text-cyan-400">${escapeHtml(ev.ticker || "")}</span>
            <span class="text-xs font-mono text-white">$${parseFloat(ev.price || 0).toFixed(2)}</span>
          </div>
        `;
      }

      box.appendChild(item);
    });
  }

  _updateChatIndicator(phase) {
    const el = document.getElementById("chat-phase-indicator");
    if (!el) return;

    const open = ["negotiation", "disclosure", "finished"].includes(phase);
    el.textContent = open ? "Open" : "Closed";
    el.style.color = open ? "#10b981" : "#6b7280";
  }

  _showPhaseOverlay(phase) {
    const overlay = document.getElementById("phase-overlay");
    const badge = document.getElementById("phase-overlay-badge");
    const desc = document.getElementById("phase-overlay-desc");
    if (!overlay || !badge) return;

    const descs = {
      news: "Market news just landed.",
      action_submission: "Submit your move.",
      negotiation: "Chat is open.",
      resolution: "Resolving actions.",
      disclosure: "Results posted.",
      finished: "Final rankings ready.",
    };

    badge.textContent = phaseLabel(phase);
    badge.className = `phase-badge phase-${phase} text-2xl px-8 py-4 font-mono tracking-widest uppercase`;
    if (desc) desc.textContent = descs[phase] || "";

    this._show(overlay, "flex");
    clearTimeout(this.phaseOverlayTimer);
    if (phase !== "finished") {
      this.phaseOverlayTimer = setTimeout(() => this._hide(overlay), 2500);
    }
  }

  // ── countdown ────────────────────────────────────────────────────

  _startCountdown(deadline) {
    this._clearCountdown();

    const el = document.getElementById("countdown-timer");
    const valEl = document.getElementById("countdown-value");
    if (!el || !valEl) return;

    this._show(el, "flex");

    const tick = () => {
      const diff = Math.max(
        0,
        Math.floor((deadline.getTime() - Date.now()) / 1000),
      );
      const m = Math.floor(diff / 60);
      const s = diff % 60;
      valEl.textContent = `${m}:${String(s).padStart(2, "0")}`;
      el.classList.toggle("countdown-urgent", diff <= 10 && diff > 0);
      if (diff <= 0) this._clearCountdown();
    };

    tick();
    this.countdown = setInterval(tick, 1000);
  }

  _clearCountdown() {
    if (this.countdown) clearInterval(this.countdown);
    this.countdown = null;

    const el = document.getElementById("countdown-timer");
    if (el) {
      this._hide(el);
      el.classList.remove("countdown-urgent");
    }
  }

  // ── leaderboard ──────────────────────────────────────────────────

  _renderLeaderboard(leaderboard) {
    const list = document.getElementById("leaderboard-list");
    if (!list) return;

    list.innerHTML = "";
    leaderboard.forEach((entry, i) => {
      const div = document.createElement("div");
      div.className = "flex items-center gap-3 p-3 rounded-lg card fade-in-up";

      const medals = ["🥇", "🥈", "🥉"];
      div.innerHTML = `
        <span class="text-xl w-8 text-center">${medals[i] || `#${i + 1}`}</span>
        <span class="font-mono font-semibold text-sm text-white flex-1">${escapeHtml(entry.username)}</span>
        <span class="font-mono text-sm ${i === 0 ? "text-emerald-400 font-bold" : "text-gray-300"}">
          $${parseFloat(entry.net_worth || 0).toLocaleString("en-US", {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })}
        </span>
      `;
      list.appendChild(div);
    });
  }

  // ── action modal ─────────────────────────────────────────────────

  _openActionModal(dataset) {
    const modal = document.getElementById("action-modal");
    const titleEl = document.getElementById("modal-title");
    const descEl = document.getElementById("modal-desc");
    const errEl = document.getElementById("modal-error");
    const tickerDiv = document.getElementById("field-ticker");
    const qtyDiv = document.getElementById("field-quantity");
    const targetDiv = document.getElementById("field-target");
    const costDiv = document.getElementById("cost-estimate");
    const tickerBtns = document.getElementById("ticker-buttons");
    const targetBtns = document.getElementById("target-buttons");
    const qtyInput = document.getElementById("input-quantity");

    if (!modal) return;

    if (titleEl) titleEl.textContent = dataset.label || dataset.action || "";
    if (descEl) descEl.textContent = dataset.desc || "";
    if (errEl) {
      errEl.textContent = "";
      this._hide(errEl);
    }

    const needsTicker = dataset.needsTicker === "true";
    const needsQty = dataset.needsQuantity === "true";
    const needsTarget = dataset.needsTarget === "true";

    needsTicker ? this._show(tickerDiv, "block") : this._hide(tickerDiv);
    needsQty ? this._show(qtyDiv, "block") : this._hide(qtyDiv);
    needsTarget ? this._show(targetDiv, "block") : this._hide(targetDiv);
    this._hide(costDiv);

    if (needsTicker && tickerBtns) {
      tickerBtns.innerHTML = "";
      this.companies.forEach((c) => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "ticker-pill";
        btn.dataset.ticker = c.ticker;
        btn.innerHTML = `<span class="font-bold">${escapeHtml(c.ticker)}</span>
                         <span class="text-gray-500 text-xs ml-1">$${parseFloat(c.price).toFixed(2)}</span>`;
        btn.addEventListener("click", (e) => {
          e.preventDefault();
          tickerBtns
            .querySelectorAll(".ticker-pill")
            .forEach((b) => b.classList.remove("selected"));
          btn.classList.add("selected");
          if (needsQty)
            this._updateCostEstimate(dataset.action, c.ticker, qtyInput?.value);
        });
        tickerBtns.appendChild(btn);
      });
    }

    if (needsTarget && targetBtns) {
      targetBtns.innerHTML = "";
      this.players.forEach((p) => {
        if (String(p.user_id) === String(this.cfg.userId)) return;

        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "player-pill";
        btn.dataset.targetId = p.user_id;
        btn.innerHTML = `
          <div class="w-2 h-2 rounded-full ${p.liquidity_frozen ? "bg-blue-400" : "bg-emerald-400"}"></div>
          <span class="font-mono text-sm text-gray-200 flex-1">${escapeHtml(p.username)}</span>
          ${p.liquidity_frozen ? `<span class="text-xs text-blue-400 font-mono">❄ frozen</span>` : ""}
          ${p.regulatory_heat > 0 ? `<span class="text-xs text-amber-400 font-mono">⚖ ${p.regulatory_heat}</span>` : ""}
        `;
        btn.addEventListener("click", (e) => {
          e.preventDefault();
          targetBtns
            .querySelectorAll(".player-pill")
            .forEach((b) => b.classList.remove("selected"));
          btn.classList.add("selected");
        });
        targetBtns.appendChild(btn);
      });
    }

    if (qtyInput) {
      qtyInput.value = "";
      qtyInput.oninput = () => {
        const ticker = tickerBtns?.querySelector(".selected")?.dataset.ticker;
        this._updateCostEstimate(dataset.action, ticker, qtyInput.value);
      };
    }

    const form = document.getElementById("action-form");
    if (form) form.dataset.actionType = dataset.action;

    this._show(modal, "flex");
  }

  _closeModal() {
    this._hide(document.getElementById("action-modal"));
    this._hide(document.getElementById("modal-error"));
  }

  _updateCostEstimate(action, ticker, qty) {
    const costDiv = document.getElementById("cost-estimate");
    const costValEl = document.getElementById("cost-value");
    const cashRemEl = document.getElementById("cash-remaining");

    if (
      !costDiv ||
      !ticker ||
      !qty ||
      !["buy", "short", "acquire_stake"].includes(action)
    ) {
      this._hide(costDiv);
      return;
    }

    const company = this.companies.find((c) => c.ticker === ticker);
    if (!company) {
      this._hide(costDiv);
      return;
    }

    const price = parseFloat(company.price);
    const q = parseInt(qty, 10) || 0;
    const total = price * q;
    const myCash = this.myState ? parseFloat(this.myState.cash) : 0;

    this._show(costDiv, "block");

    if (costValEl) {
      costValEl.textContent = `$${total.toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })}`;
    }

    if (cashRemEl) {
      cashRemEl.textContent = `$${Math.max(0, myCash - total).toLocaleString(
        "en-US",
        {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        },
      )}`;
      cashRemEl.style.color = myCash - total < 0 ? "#ef4444" : "#d1d5db";
    }
  }

  _submitAction() {
    const form = document.getElementById("action-form");
    const submitBtn = document.getElementById("modal-submit");
    if (!form) return;

    const actionType = form.dataset.actionType;
    if (!actionType) return;

    const params = {};

    const selectedTicker = document.querySelector(
      "#ticker-buttons .ticker-pill.selected",
    );
    if (selectedTicker) params.ticker = selectedTicker.dataset.ticker;

    const qtyInput = document.getElementById("input-quantity");
    const qtyParent = document.getElementById("field-quantity");
    const qtyVisible = qtyParent && qtyParent.style.display !== "none";
    if (qtyVisible && qtyInput) {
      const q = parseInt(qtyInput.value, 10);
      if (!q || q <= 0) {
        this._showModalError("Please enter a valid quantity.");
        return;
      }
      params.quantity = q;
    }

    const selectedTarget = document.querySelector(
      "#target-buttons .player-pill.selected",
    );
    if (selectedTarget) params.target_user_id = selectedTarget.dataset.targetId;

    const tickerParent = document.getElementById("field-ticker");
    if (
      tickerParent &&
      tickerParent.style.display !== "none" &&
      !params.ticker
    ) {
      this._showModalError("Please select a company.");
      return;
    }

    const targetParent = document.getElementById("field-target");
    if (
      targetParent &&
      targetParent.style.display !== "none" &&
      !params.target_user_id
    ) {
      this._showModalError("Please select a target player.");
      return;
    }

    submitBtn.disabled = true;
    submitBtn.textContent = "Submitting...";

    this.channel
      .push("submit_action", { action_type: actionType, params })
      .receive("ok", () => {
        this._closeModal();
        submitBtn.disabled = false;
        submitBtn.textContent = "Confirm Action";

        document.querySelectorAll(".action-btn").forEach((b) => {
          b.disabled = true;
          b.classList.add("submitted");
        });

        this._show(document.getElementById("action-submitted-badge"), "flex");
        Toast.show("Action submitted! Waiting for other players.", "success");
      })
      .receive("error", (e) => {
        submitBtn.disabled = false;
        submitBtn.textContent = "Confirm Action";
        const reason =
          typeof e.reason === "string" ? e.reason : JSON.stringify(e.reason);
        this._showModalError("Rejected: " + reason);
      });
  }

  _showModalError(msg) {
    const el = document.getElementById("modal-error");
    if (!el) return;
    el.textContent = msg;
    this._show(el, "block");
  }

  _show(el, display = "block") {
    if (!el) return;
    el.classList.remove("hidden");
    el.style.display = display;
  }

  _hide(el) {
    if (!el) return;
    el.classList.add("hidden");
    el.style.display = "none";
  }

  // ── chat ─────────────────────────────────────────────────────────

  _sendChatMessage() {
    const input = document.getElementById("chat-input");
    if (!input) return;

    const content = input.value.trim();
    if (!content) return;

    this.channel
      .push("send_message", { content })
      .receive("ok", () => {
        input.value = "";
      })
      .receive("error", (e) => {
        Toast.show(`Message rejected: ${JSON.stringify(e.reason)}`, "error");
      });
  }

  _appendChat(msg) {
    const container = document.getElementById("chat-messages");
    if (!container) return;

    const placeholder = container.querySelector("p");
    if (placeholder && placeholder.classList.contains("text-center")) {
      placeholder.remove();
    }

    const el = document.createElement("div");
    el.className = "flex gap-1.5 fade-in-up";

    const time = msg.inserted_at
      ? new Date(msg.inserted_at).toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
        })
      : "";

    el.innerHTML = `
      <span class="text-gray-600 font-mono text-xs shrink-0 mt-0.5">${time}</span>
      <div class="min-w-0">
        <span class="font-mono text-xs font-semibold text-emerald-400">${escapeHtml(msg.username || "?")}:</span>
        <span class="text-gray-300 text-xs ml-1 break-words">${escapeHtml(msg.content)}</span>
      </div>
    `;

    container.appendChild(el);
    container.scrollTop = container.scrollHeight;
  }
}

// ─── Helpers ───────────────────────────────────────────────────────
function phaseLabel(phase) {
  const labels = {
    waiting: "WAITING",
    news: "NEWS",
    action_submission: "SUBMIT ACTION",
    negotiation: "NEGOTIATION",
    resolution: "RESOLUTION",
    disclosure: "DISCLOSURE",
    finished: "FINISHED",
  };

  return (
    labels[phase] ||
    String(phase || "—")
      .replaceAll("_", " ")
      .toUpperCase()
  );
}

function escapeHtml(str) {
  return String(str ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

// Start only after the whole file has loaded
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => boot(config), {
    once: true,
  });
} else {
  boot(config);
}
