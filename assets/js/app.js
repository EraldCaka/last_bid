// Dark Pool — complete game client
// Handles lobby real-time updates, match gameplay, charts, and rich UI.

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
    const container = document.getElementById("toast-container");
    if (!container) return;

    const el = document.createElement("div");
    el.className = `toast ${type}`;
    el.innerHTML = `
      <span class="text-base leading-none" style="flex-shrink:0">${icons[type] || "•"}</span>
      <span class="flex-1 text-gray-200">${escapeHtml(msg)}</span>
    `;
    container.appendChild(el);

    setTimeout(() => {
      el.style.animation = "slide-out-right 0.25s ease forwards";
      el.addEventListener("animationend", () => el.remove(), { once: true });
    }, duration);

    el.addEventListener("click", () => el.remove());
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
    this.svg.classList.add("sparkline");

    const gradId = `sg-${Math.random().toString(36).slice(2)}`;
    this.svg.innerHTML = `
      <defs>
        <linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="var(--spark-color,#10b981)" stop-opacity="0.4"/>
          <stop offset="100%" stop-color="var(--spark-color,#10b981)" stop-opacity="0"/>
        </linearGradient>
      </defs>
      <path class="area" fill="url(#${gradId})"/>
      <polyline class="line" style="stroke:var(--spark-color,#10b981)"/>
    `;
    this._gradId = gradId;
    this.container.appendChild(this.svg);
  }

  push(price) {
    const p = parseFloat(price);
    if (isNaN(p)) return;
    this.history.push(p);
    if (this.history.length > this.maxPoints) this.history.shift();
    this._render();
  }

  setColor(isUp) {
    if (!this.svg) return;
    const c = isUp ? "#10b981" : "#ef4444";
    this.svg.style.setProperty("--spark-color", c);
    const stop = this.svg.querySelector(`#${this._gradId} stop`);
    if (stop) stop.setAttribute("stop-color", c);
  }

  _render() {
    if (!this.svg || this.history.length < 2) return;
    const pts = this.history;
    const min = Math.min(...pts);
    const max = Math.max(...pts);
    const range = max - min || 1;
    const pad = 2;
    const xStep = (this.w - pad * 2) / (pts.length - 1);

    const points = pts.map((v, i) => {
      const x = pad + i * xStep;
      const y = pad + (1 - (v - min) / range) * (this.h - pad * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    });

    const polyline = this.svg.querySelector("polyline.line");
    const area = this.svg.querySelector("path.area");
    if (polyline) polyline.setAttribute("points", points.join(" "));
    if (area) {
      const first = points[0].split(",");
      const last = points[points.length - 1].split(",");
      const d = `M${first[0]},${this.h - pad} L${points.join(" L")} L${last[0]},${this.h - pad} Z`;
      area.setAttribute("d", d);
    }
  }
}

// ─── LobbyManager ───────────────────────────────────────────────────
class LobbyManager {
  constructor(socket, cfg) {
    this.socket = socket;
    this.cfg = cfg;
    this.channel = null;
    this.presence = null;
    this.joined = false;
  }

  init() {
    this.channel = this.socket.channel("lobby:general", {});

    this.channel.on("open_matches", (p) => this._updateMatchList(p.matches));
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
      .receive("error", (e) => {
        console.warn("[Lobby] join error", e);
        this._wireJoinButtons(document);
      });
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
    btn.disabled = true;
    const origText = btn.textContent;
    btn.textContent = "Joining...";

    // Phoenix channels buffer pushes while joining — no need to gate on this.joined.
    // If the channel is not yet open, the push will be flushed once it connects.
    this.channel
      .push("join_match", { match_id: matchId })
      .receive("ok", () => {
        window.location = `/matches/${matchId}`;
      })
      .receive("error", (err) => {
        btn.disabled = false;
        btn.textContent = origText;
        const reason = err?.reason || JSON.stringify(err);
        Toast.show("Could not join: " + reason, "error");
      })
      .receive("timeout", () => {
        btn.disabled = false;
        btn.textContent = origText;
        Toast.show("Join timed out. Please try again.", "warning");
      });
  }

  _addOrUpdateMatch(match) {
    const list = document.getElementById("open-matches-list");
    if (!list) return;

    // Remove placeholder
    const placeholder = document.getElementById("no-matches-placeholder");
    if (placeholder) placeholder.remove();

    const existing = list.querySelector(`[data-match-id="${match.id}"]`);
    const card = this._buildMatchCard(match);

    if (existing) {
      existing.replaceWith(card);
    } else {
      list.insertAdjacentElement("afterbegin", card);
    }

    this._wireJoinButtons(list);
  }

  _buildMatchCard(match) {
    const div = document.createElement("div");
    div.className = "card card-hover p-4 match-card fade-in-up";
    div.dataset.matchId = match.id;

    const isFull = match.player_count >= match.max_players;
    const isMyMatch = match.host_username === this._myUsername();

    div.innerHTML = `
      <div class="flex items-center justify-between">
        <div class="flex items-center gap-3">
          <div class="w-9 h-9 rounded-lg bg-emerald-500/10 border border-emerald-500/20 flex items-center justify-center shrink-0">
            <svg class="w-4 h-4 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
              <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/>
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
              <span class="text-white font-semibold">${match.player_count}</span>/${match.max_players}
            </div>
          </div>
          ${
            isFull
              ? `<span class="btn-ghost text-xs py-1.5 px-3 opacity-40 pointer-events-none">Full</span>`
              : `<button type="button" data-match-id="${escapeHtml(match.id)}" class="join-match-btn btn-primary text-xs py-1.5 px-4">
                 ${isMyMatch ? "Enter" : "Join"}
               </button>`
          }
        </div>
      </div>
    `;
    return div;
  }

  _updateMatchList(matches) {
    const list = document.getElementById("open-matches-list");
    if (!list) return;
    if (!matches || matches.length === 0) return;

    list.innerHTML = "";
    matches.forEach((m) => list.appendChild(this._buildMatchCard(m)));
    this._wireJoinButtons(list);
  }

  _onMatchStarted(matchId) {
    // Remove the card from the open-matches list since it's no longer waiting
    const card = document.querySelector(
      `#open-matches-list [data-match-id="${matchId}"]`,
    );
    if (card) {
      card.style.animation = "slide-out-right 0.3s ease forwards";
      card.addEventListener("animationend", () => card.remove(), {
        once: true,
      });
    }
  }

  _onPresenceState(state) {
    const list = document.getElementById("lobby-presence-list");
    const counter = document.getElementById("lobby-online-count");
    if (!list) return;

    const users = [];
    Object.values(state).forEach((entry) => {
      const meta = entry.metas?.[0];
      if (meta?.username) users.push(meta.username);
    });

    if (counter) counter.textContent = `${users.length} online`;
    this._renderPresenceList(list, users);
  }

  _onPresenceDiff(diff) {
    const list = document.getElementById("lobby-presence-list");
    if (!list) return;

    // Collect existing from DOM
    const current = new Map();
    list.querySelectorAll("[data-presence-user]").forEach((el) => {
      current.set(el.dataset.presenceUser, el);
    });

    // Add joins
    Object.values(diff.joins || {}).forEach((entry) => {
      const meta = entry.metas?.[0];
      if (!meta?.username) return;
      if (current.has(meta.username)) return;

      const el = this._buildPresenceRow(meta.username);
      list.appendChild(el);
      current.set(meta.username, el);
    });

    // Remove leaves
    Object.values(diff.leaves || {}).forEach((entry) => {
      const meta = entry.metas?.[0];
      if (!meta?.username) return;
      const el = current.get(meta.username);
      if (el) {
        el.remove();
        current.delete(meta.username);
      }
    });

    const counter = document.getElementById("lobby-online-count");
    if (counter) counter.textContent = `${current.size} online`;

    if (current.size === 0) {
      list.innerHTML = `<p class="text-xs text-gray-600 font-mono">Nobody else online.</p>`;
    }
  }

  _renderPresenceList(list, users) {
    if (users.length === 0) {
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

  _myUsername() {
    const nav = document.querySelector("header nav .font-mono");
    return nav ? nav.textContent.trim() : null;
  }

  // Called by MatchManager to send lobby events from the match page
  push(event, payload) {
    if (this.channel && this.joined) {
      return this.channel.push(event, payload);
    }
  }
}

// ─── MatchManager ───────────────────────────────────────────────────
class MatchManager {
  constructor(socket, cfg, lobbyMgr) {
    this.socket = socket;
    this.cfg = cfg;
    this.lobbyMgr = lobbyMgr;
    this.channel = null;
    this.charts = {}; // ticker → SparklineChart
    this.prevPrices = {}; // ticker → last known price
    this.players = []; // current players list from public state
    this.companies = []; // current companies list from public state
    this.myState = null; // my private state
    this.phase = null;
    this.countdown = null; // setInterval handle
    this.phaseOverlayTimer = null;
  }

  init() {
    this.channel = this.socket.channel(`match:${this.cfg.matchId}`, {});

    this.channel.on("state_updated", (p) => this._onStateUpdated(p));
    this.channel.on("phase_changed", (p) => this._onPhaseChanged(p));
    this.channel.on("private_state", (p) => this._onPrivateState(p));
    this.channel.on("private_events", (p) => this._onPrivateEvents(p));
    this.channel.on("new_message", (m) => this._appendChat(m));
    this.channel.on("new_whisper", (m) => this._appendChat(m, true));
    this.channel.on("match_finished", (p) => this._onMatchFinished(p));
    this.channel.on("presence_state", (s) => this._onPresenceState(s));
    this.channel.on("presence_diff", (d) => this._onPresenceDiff(d));

    this.channel
      .join()
      .receive("ok", () => console.debug("[Match] joined"))
      .receive("error", ({ reason }) => {
        Toast.show("Could not join match: " + reason, "error");
      });

    this._wireControls();
  }

  // ── Wire static controls ──────────────────────────────────────────

  _wireControls() {
    const startBtn = document.getElementById("start-match-btn");
    if (startBtn) {
      startBtn.addEventListener("click", (e) => {
        e.preventDefault();
        startBtn.disabled = true;
        startBtn.textContent = "Starting...";
        this.lobbyMgr
          .push("start_match", { match_id: this.cfg.matchId })
          ?.receive("ok", () => startBtn.remove())
          ?.receive("error", (err) => {
            startBtn.disabled = false;
            startBtn.textContent = "Start Match";
            const reason = err?.reason || JSON.stringify(err);
            Toast.show("Could not start: " + reason, "error");
          });
      });
    }

    document.querySelectorAll(".action-btn").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
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
          .then(() => Toast.show("Invite link copied to clipboard!", "success"))
          .catch(() => {
            window.prompt("Copy this invite link:", url);
          });
      });

    document.getElementById("modal-close")?.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      this._closeModal();
    });

    document.getElementById("action-modal")?.addEventListener("click", (e) => {
      if (e.target === e.currentTarget) this._closeModal();
    });

    document.getElementById("modal-submit")?.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
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

  // ── State updates ─────────────────────────────────────────────────

  _onStateUpdated(payload) {
    this.players = payload.players || [];
    this.companies = payload.companies || [];
    this.phase = payload.phase;

    this._renderPhase(payload.phase, payload.round);
    this._renderRound(payload.round, payload.total_rounds);
    this._renderMarket(payload.companies);
    this._renderPlayers(payload.players);
    this._renderActionPanel(payload.phase);

    // Start negotiation countdown if applicable
    if (payload.phase === "negotiation" && payload.negotiation_deadline) {
      this._startCountdown(new Date(payload.negotiation_deadline));
    }
  }

  _onPhaseChanged(payload) {
    this.phase = payload.phase;
    this._renderPhase(payload.phase, payload.round);
    this._renderRound(payload.round);
    this._renderActionPanel(payload.phase);
    this._showPhaseOverlay(payload.phase);
    this._updateChatIndicator(payload.phase);

    if (payload.phase === "action_submission") {
      this._clearCountdown();
      // Reset action submitted state
      document
        .getElementById("action-submitted-badge")
        ?.classList.add("hidden");
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
      Toast.show(
        ev.message || ev.type || "Event",
        ev.positive ? "success" : "warning",
      );
    });
  }

  _onMatchFinished(payload) {
    this._clearCountdown();
    this._renderLeaderboard(payload.leaderboard);
    this._showPhaseOverlay("finished");
    document.getElementById("action-panel")?.classList.add("hidden");
    document.getElementById("leaderboard-panel")?.classList.remove("hidden");
    Toast.show("Match over! Final rankings are in.", "info", 8000);
  }

  // ── Presence ──────────────────────────────────────────────────────

  _onPresenceState(state) {
    const onlineIds = new Set();
    Object.entries(state).forEach(([userId, entry]) => {
      onlineIds.add(userId);
      const meta = entry.metas?.[0];
      this._setPlayerOnline(userId, meta?.username, true);
    });
  }

  _onPresenceDiff(diff) {
    Object.entries(diff.joins || {}).forEach(([userId, entry]) => {
      const meta = entry.metas?.[0];
      this._setPlayerOnline(userId, meta?.username, true);
    });
    Object.entries(diff.leaves || {}).forEach(([userId]) => {
      this._setPlayerOnline(userId, null, false);
    });
  }

  _setPlayerOnline(userId, _username, online) {
    const row = document.querySelector(`[data-player-id="${userId}"]`);
    if (!row) return;
    const dot = row.querySelector(".presence-dot");
    if (dot) dot.classList.toggle("offline", !online);
  }

  // ── Market rendering ──────────────────────────────────────────────

  _renderMarket(companies) {
    const list = document.getElementById("companies-list");
    if (!list || !companies) return;

    companies.forEach((c) => {
      const prev = this.prevPrices[c.ticker];
      const price = parseFloat(c.price);
      const isUp = prev !== undefined ? price >= prev : true;
      this.prevPrices[c.ticker] = price;

      let row = list.querySelector(`[data-ticker="${c.ticker}"]`);
      if (!row) {
        row = this._createMarketRow(c);
        list.appendChild(row);
      }

      this._updateMarketRow(row, c, price, prev, isUp);
    });
  }

  _createMarketRow(c) {
    const row = document.createElement("div");
    row.className = "ticker-row";
    row.dataset.ticker = c.ticker;

    const chartId = `chart-${c.ticker}`;
    row.innerHTML = `
      <span class="font-mono font-bold text-sm ticker-symbol"></span>
      <span class="text-xs text-gray-500 truncate ticker-name"></span>
      <div class="text-right">
        <div class="font-mono font-semibold text-sm ticker-price"></div>
        <div class="text-xs font-mono ticker-change mt-0.5"></div>
      </div>
      <div id="${chartId}" class="flex items-center justify-end" style="height:28px;width:80px"></div>
    `;

    // Create sparkline
    setTimeout(() => {
      this.charts[c.ticker] = new SparklineChart(chartId, {
        width: 80,
        height: 28,
      });
      this.charts[c.ticker].push(parseFloat(c.price));
    }, 0);

    return row;
  }

  _updateMarketRow(row, c, price, prev, isUp) {
    const priceEl = row.querySelector(".ticker-price");
    const changeEl = row.querySelector(".ticker-change");
    const symEl = row.querySelector(".ticker-symbol");
    const nameEl = row.querySelector(".ticker-name");

    if (symEl) symEl.textContent = c.ticker;
    if (nameEl) nameEl.textContent = c.name || "";

    if (priceEl) {
      const oldPrice = priceEl.textContent;
      const newText = `$${price.toFixed(2)}`;
      if (oldPrice !== newText) {
        priceEl.textContent = newText;
        priceEl.classList.remove("price-up", "price-down");
        void priceEl.offsetWidth; // reflow
        priceEl.classList.add(isUp ? "price-up" : "price-down");
        priceEl.style.color = isUp ? "#10b981" : "#ef4444";

        row.classList.remove("row-flash-green", "row-flash-red");
        void row.offsetWidth;
        row.classList.add(isUp ? "row-flash-green" : "row-flash-red");
      }
    }

    if (changeEl && prev !== undefined) {
      const pct = ((price - prev) / prev) * 100;
      if (Math.abs(pct) > 0.01) {
        changeEl.textContent = (pct >= 0 ? "+" : "") + pct.toFixed(2) + "%";
        changeEl.style.color = pct >= 0 ? "#10b981" : "#ef4444";
      } else {
        changeEl.textContent = "";
      }
    }

    // Heat indicator
    if (c.regulatory_heat > 0) {
      let heatEl = row.querySelector(".heat-badge");
      if (!heatEl) {
        heatEl = document.createElement("span");
        heatEl.className = "heat-badge absolute right-0 top-0";
        // We'll just add a textual indicator in the row
      }
    }

    // Update sparkline
    if (this.charts[c.ticker]) {
      this.charts[c.ticker].push(price);
      this.charts[c.ticker].setColor(isUp);
    }
  }

  // ── Players rendering ─────────────────────────────────────────────

  _renderPlayers(players) {
    const list = document.getElementById("players-list");
    if (!list || !players) return;

    // Update submit status for known player rows
    players.forEach((p) => {
      const row = list.querySelector(`[data-player-id="${p.user_id}"]`);
      if (!row) return;

      let submitBadge = row.querySelector(".submit-badge");
      if (!submitBadge) {
        submitBadge = document.createElement("span");
        submitBadge.className = "submit-badge text-xs font-mono ml-auto";
        row.appendChild(submitBadge);
      }
      submitBadge.textContent = p.has_submitted ? "✓" : "";
      submitBadge.style.color = "#10b981";

      let frozenBadge = row.querySelector(".frozen-badge");
      if (p.liquidity_frozen) {
        if (!frozenBadge) {
          frozenBadge = document.createElement("span");
          frozenBadge.className =
            "frozen-badge text-xs font-mono text-blue-400";
          row.appendChild(frozenBadge);
        }
        frozenBadge.textContent = "❄";
      } else if (frozenBadge) {
        frozenBadge.remove();
      }

      // Heat indicator
      let heatEl = row.querySelector(".heat-indicator");
      if (p.regulatory_heat > 0) {
        if (!heatEl) {
          heatEl = document.createElement("span");
          heatEl.className = "heat-indicator text-xs font-mono text-amber-500";
          row.appendChild(heatEl);
        }
        heatEl.textContent = `⚖${p.regulatory_heat}`;
      } else if (heatEl) {
        heatEl.remove();
      }
    });
  }

  // ── My stats ──────────────────────────────────────────────────────

  _renderMyStats(state) {
    const panel = document.getElementById("my-stats");
    this._show(panel, "block");

    const cash = parseFloat(state.cash);
    const nw = parseFloat(state.net_worth);

    const cashEl = document.getElementById("my-cash");
    const nwEl = document.getElementById("my-networth");
    const heatEl = document.getElementById("my-heat");
    const frzEl = document.getElementById("my-frozen-badge");

    if (cashEl) {
      cashEl.textContent =
        "$" +
        cash.toLocaleString("en-US", {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        });
    }

    if (nwEl) {
      nwEl.textContent =
        "$" +
        nw.toLocaleString("en-US", {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        });
    }

    if (heatEl) heatEl.textContent = state.regulatory_heat || 0;

    if (frzEl) {
      if (state.liquidity_frozen) this._show(frzEl, "block");
      else this._hide(frzEl);
    }

    const holdingsEl = document.getElementById("my-holdings");
    if (holdingsEl && state.portfolio) {
      holdingsEl.innerHTML = "";
      Object.entries(state.portfolio).forEach(([ticker, qty]) => {
        if (qty <= 0) return;
        const div = document.createElement("div");
        div.className = "flex justify-between text-xs font-mono";
        div.innerHTML = `<span class="text-gray-500">${escapeHtml(ticker)}</span><span class="text-gray-300">${qty} sh</span>`;
        holdingsEl.appendChild(div);
      });
    }
  }

  // ── Phase UI ──────────────────────────────────────────────────────

  _renderPhase(phase, round) {
    const container = document.getElementById("phase-badge-container");
    if (!container) return;
    const label = phaseLabel(phase);
    container.innerHTML = `<span class="phase-badge phase-${phase || "waiting"}">${label}</span>`;
  }

  _renderRound(round, total) {
    const el = document.getElementById("current-round");
    if (el && round) el.textContent = round;
  }

  _renderActionPanel(phase) {
    const panel = document.getElementById("action-panel");
    if (!panel) return;

    if (phase === "action_submission") {
      this._show(panel, "block");
    } else {
      this._hide(panel);
    }
  }
  _updateChatIndicator(phase) {
    const el = document.getElementById("chat-phase-indicator");
    if (!el) return;
    const map = {
      negotiation: "Open",
      action_submission: "Closed",
      news: "Closed",
      resolution: "Closed",
      disclosure: "Open",
      finished: "Open",
    };
    el.textContent = map[phase] || "—";
    el.style.color =
      phase === "negotiation" || phase === "disclosure" || phase === "finished"
        ? "#10b981"
        : "#6b7280";
  }

  _showPhaseOverlay(phase) {
    const overlay = document.getElementById("phase-overlay");
    const badge = document.getElementById("phase-overlay-badge");
    const desc = document.getElementById("phase-overlay-desc");
    if (!overlay || !badge) return;

    const descs = {
      news: "A market event has occurred. Analyse it carefully.",
      action_submission: "Choose your action wisely. No one else can see it.",
      negotiation: "Deals, deceptions, and alliances. Chat is open.",
      resolution: "Actions are resolving. Brace for impact.",
      disclosure: "Round complete. Results are in.",
      finished: "The market closes. Final rankings revealed.",
    };

    badge.className = `phase-badge phase-${phase} text-lg px-6 py-3 font-mono tracking-widest uppercase`;
    badge.textContent = phaseLabel(phase);
    if (desc) desc.textContent = descs[phase] || "";

    this._show(overlay, "flex");

    if (this.phaseOverlayTimer) clearTimeout(this.phaseOverlayTimer);
    if (phase !== "finished") {
      this.phaseOverlayTimer = setTimeout(() => this._hide(overlay), 3000);
    }
  }

  // ── Countdown timer ───────────────────────────────────────────────

  _startCountdown(deadline) {
    this._clearCountdown();
    const el = document.getElementById("countdown-timer");
    const valEl = document.getElementById("countdown-value");
    if (!el || !valEl) return;

    this._show(el, "flex");

    const tick = () => {
      const diff = Math.max(0, Math.floor((deadline - Date.now()) / 1000));
      const m = Math.floor(diff / 60);
      const s = diff % 60;
      valEl.textContent = `${m}:${String(s).padStart(2, "0")}`;

      if (diff <= 10) el.classList.add("countdown-urgent");
      else el.classList.remove("countdown-urgent");

      if (diff === 0) this._clearCountdown();
    };

    tick();
    this.countdown = setInterval(tick, 1000);
  }

  _clearCountdown() {
    if (this.countdown) {
      clearInterval(this.countdown);
      this.countdown = null;
    }

    const el = document.getElementById("countdown-timer");
    if (el) {
      this._hide(el);
      el.classList.remove("flex", "countdown-urgent");
    }
  }

  // ── Leaderboard ───────────────────────────────────────────────────

  _renderLeaderboard(leaderboard) {
    const list = document.getElementById("leaderboard-list");
    if (!list || !leaderboard) return;

    list.innerHTML = "";
    leaderboard.forEach((entry, i) => {
      const div = document.createElement("div");
      div.className = "flex items-center gap-3 p-3 rounded-lg card fade-in-up";
      div.style.animationDelay = `${i * 80}ms`;

      const medals = ["🥇", "🥈", "🥉"];
      div.innerHTML = `
        <span class="text-xl w-8 text-center">${medals[i] || `#${i + 1}`}</span>
        <span class="font-mono font-semibold text-sm text-white flex-1">${escapeHtml(entry.username)}</span>
        <span class="font-mono text-sm ${i === 0 ? "text-emerald-400 font-bold" : "text-gray-300"}">
          $${parseFloat(entry.net_worth).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </span>
      `;
      list.appendChild(div);
    });
  }

  // ── Action Modal ──────────────────────────────────────────────────

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

    setTimeout(() => {
      const focusEl =
        (needsTicker && tickerBtns?.querySelector(".ticker-pill")) ||
        (needsQty && qtyInput) ||
        (needsTarget && targetBtns?.querySelector(".player-pill")) ||
        document.getElementById("modal-close");

      focusEl?.focus?.();
    }, 20);
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

        const badge = document.getElementById("action-submitted-badge");
        this._show(badge, "flex");

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

  // ── Chat ──────────────────────────────────────────────────────────

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
      .receive("error", (e) =>
        Toast.show("Message rejected: " + JSON.stringify(e.reason), "error"),
      );
  }

  _appendChat(msg, isWhisper = false) {
    const container = document.getElementById("chat-messages");
    if (!container) return;

    // Clear placeholder
    const placeholder = container.querySelector("p");
    if (placeholder && placeholder.classList.contains("text-center"))
      placeholder.remove();

    const el = document.createElement("div");
    el.className = `flex gap-1.5 fade-in-up ${isWhisper ? "pl-2 border-l-2 border-purple-500/30" : ""}`;

    const time = msg.inserted_at
      ? new Date(msg.inserted_at).toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
        })
      : "";

    const typeClass = isWhisper ? "text-purple-400" : "text-emerald-400";

    el.innerHTML = `
      <span class="text-gray-600 font-mono text-xs shrink-0 mt-0.5">${time}</span>
      <div class="min-w-0">
        <span class="font-mono text-xs font-semibold ${typeClass}">${escapeHtml(msg.username || "?")}${isWhisper ? " →whisper" : ""}:</span>
        <span class="text-gray-300 text-xs ml-1 break-words">${escapeHtml(msg.content)}</span>
      </div>
    `;
    container.appendChild(el);
    container.scrollTop = container.scrollHeight;
  }
}

// ─── Helpers ────────────────────────────────────────────────────────

function phaseLabel(phase) {
  const labels = {
    waiting: "WAITING",
    news: "NEWS",
    action_submission: "SUBMIT ACTION",
    negotiation: "NEGOTIATION",
    resolution: "RESOLVING",
    disclosure: "DISCLOSURE",
    finished: "FINISHED",
  };
  return (
    labels[phase] || (phase ? phase.replace(/_/g, " ").toUpperCase() : "—")
  );
}

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => boot(config), {
    once: true,
  });
} else {
  boot(config);
}
