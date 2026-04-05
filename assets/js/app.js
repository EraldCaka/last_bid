// Dark Pool — minimal game client
// Connects to Phoenix channels and drives the match UI.

import { Socket } from "phoenix";

let socket = null;
let matchChannel = null;
let lobbyChannel = null;

const config = window.DarkPool || {};

// Connect the socket if we have a token
if (config && config.socketToken) {
  socket = new Socket("/socket", {
    params: { token: config.socketToken },
    logger: (kind, msg, data) => {
      if (process.env.NODE_ENV === "development") {
        console.log(`${kind}: ${msg}`, data);
      }
    },
  });

  socket.connect();

  // Join match channel if we're on a match page
  if (config.matchId) {
    matchChannel = socket.channel(`match:${config.matchId}`, {});

    matchChannel.on("state_updated", (payload) => {
      updateMarket(payload.companies);
      updatePlayers(payload.players);
      updatePhase(payload.phase, payload.round);
    });

    matchChannel.on("phase_changed", (payload) => {
      updatePhase(payload.phase, payload.round);
      showPhaseNotice(payload.phase);

      const actionPanel = document.getElementById("action-panel");
      if (actionPanel) {
        actionPanel.classList.toggle(
          "hidden",
          payload.phase !== "action_submission",
        );
      }
    });

    matchChannel.on("new_message", (msg) => appendChatMessage(msg));
    matchChannel.on("new_whisper", (msg) => appendChatMessage(msg, true));

    matchChannel.on("match_finished", (payload) => {
      showLeaderboard(payload.leaderboard);
    });

    matchChannel.on("presence_state", (state) => {
      Object.entries(state).forEach(([userId, { metas }]) => {
        markOnline(userId, metas?.[0]?.username);
      });
    });

    matchChannel
      .join()
      .receive("ok", () => console.log("Joined match channel"))
      .receive("error", ({ reason }) =>
        console.error("Failed to join match channel:", reason),
      );

    // Start match button
    const startBtn = document.getElementById("start-match-btn");
    if (startBtn) {
      startBtn.addEventListener("click", () => {
        if (!lobbyChannel) {
          alert("Realtime lobby not connected");
          return;
        }

        lobbyChannel
          .push("start_match", { match_id: config.matchId })
          .receive("ok", () => startBtn.remove())
          .receive("error", (e) =>
            alert("Could not start: " + JSON.stringify(e)),
          );
      });
    }

    // Action buttons
    document.querySelectorAll(".action-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const actionType = btn.dataset.action;
        const ticker = prompt("Ticker (e.g. APEX):");
        if (!ticker) return;

        const params = { ticker };

        if (["buy", "short", "acquire_stake"].includes(actionType)) {
          const qty = parseInt(prompt("Quantity:"), 10);
          if (!qty || qty <= 0) return;
          params.quantity = qty;
        } else if (
          ["freeze_liquidity", "report_to_regulator", "leak"].includes(
            actionType,
          )
        ) {
          const targetId = prompt("Target user ID:");
          if (!targetId) return;
          params.target_user_id = targetId;
        }

        matchChannel
          .push("submit_action", { action_type: actionType, params })
          .receive("ok", () => (btn.disabled = true))
          .receive("error", (e) =>
            alert("Action rejected: " + JSON.stringify(e.reason)),
          );
      });
    });

    // Chat
    const sendBtn = document.getElementById("chat-send");
    const chatInput = document.getElementById("chat-input");

    if (sendBtn && chatInput) {
      sendBtn.addEventListener("click", sendMessage);
      chatInput.addEventListener("keypress", (e) => {
        if (e.key === "Enter") sendMessage();
      });
    }

    function sendMessage() {
      const content = chatInput.value.trim();
      if (!content) return;

      matchChannel
        .push("send_message", { content })
        .receive("ok", () => {
          chatInput.value = "";
        })
        .receive("error", (e) =>
          alert("Message rejected: " + JSON.stringify(e.reason)),
        );
    }
  }

  // Always join lobby channel for real-time match list updates
  lobbyChannel = socket.channel("lobby:general", {});

  // Helper: extract match id from an element
  function getMatchIdFromElement(el) {
    if (!el) return null;
    // prefer data-match-id
    if (el.dataset && el.dataset.matchId) return String(el.dataset.matchId);
    // if element is an anchor with href like /matches/:id
    if (el.tagName === "A") {
      const href = el.getAttribute("href") || "";
      const parts = href.split("/").filter(Boolean);
      if (parts.length > 0) return parts[parts.length - 1];
    }
    // try to find nearest ancestor with data-match-id
    const ancestor = el.closest && el.closest("[data-match-id]");
    if (ancestor && ancestor.dataset) return String(ancestor.dataset.matchId);
    return null;
  }

  // Wire up any join controls (buttons or anchors). Works idempotently.
  function wireJoinControls(root = document) {
    // select elements that are intended to be join controls:
    // - elements with class 'join-match-btn'
    // - anchors that look like /matches/<id> and have visible text 'Join'
    const selectors = [
      ".join-match-btn",
      'a[href^="/matches/"]',
      "[data-match-id]",
    ];
    const els = Array.from(root.querySelectorAll(selectors.join(",")));

    els.forEach((el) => {
      // ignore if already wired
      if (el.dataset && el.dataset.joinHandlerBound) return;

      // We want to handle both <a> and <button> or any clickable element.
      const clickHandler = (e) => {
        // If it's a form submit or other default behavior, prevent it.
        if (e) e.preventDefault();

        const matchId = getMatchIdFromElement(el);
        if (!matchId) {
          // fallback: if it's an anchor with href, navigate
          if (el.tagName === "A" && el.href) {
            window.location = el.href;
            return;
          }
          alert("Could not determine match id for join action.");
          return;
        }

        // If lobby channel isn't ready, navigate or alert
        if (!lobbyChannel || !lobbyChannel.joined) {
          // attempt direct navigation (may fail authorization)
          window.location = `/matches/${matchId}`;
          return;
        }

        // give immediate UI feedback
        el.setAttribute("disabled", "true");
        el.classList && el.classList.add("opacity-50", "pointer-events-none");

        lobbyChannel
          .push("join_match", { match_id: matchId })
          .receive("ok", () => {
            // only navigate after server confirms join
            window.location = `/matches/${matchId}`;
          })
          .receive("error", (err) => {
            // restore UI state
            el.removeAttribute("disabled");
            el.classList &&
              el.classList.remove("opacity-50", "pointer-events-none");
            console.warn("Join failed:", err);
            const reason =
              err && (err.reason || err.error || JSON.stringify(err));
            alert("Could not join match: " + reason);
          });
      };

      // Bind handler
      el.addEventListener("click", clickHandler);
      // mark as wired
      el.dataset.joinHandlerBound = "1";
    });
  }

  // Watch for DOM changes and wire new controls (use MutationObserver to catch dynamic updates)
  const observer = new MutationObserver((mutations) => {
    // if nodes added, attempt to wire join controls under those nodes
    for (const m of mutations) {
      if (m.addedNodes && m.addedNodes.length > 0) {
        m.addedNodes.forEach((node) => {
          if (node.nodeType === Node.ELEMENT_NODE) {
            wireJoinControls(node);
          }
        });
      }
    }
  });

  // Start the observer to watch the body for changes
  if (typeof document !== "undefined" && document.body) {
    observer.observe(document.body, { childList: true, subtree: true });
  }

  // Join lobby channel and wire controls once connected
  lobbyChannel
    .join()
    .receive("ok", () => {
      console.log("Joined lobby channel");
      // mark joined to allow early checks
      lobbyChannel.joined = true;
      wireJoinControls(document);
    })
    .receive("error", (e) => {
      console.warn("Could not join lobby:", e);
      // still attempt to wire controls so UI doesn't break — joins will navigate directly
      wireJoinControls(document);
    });

  // Re-wire on relevant lobby events (DOM might have been updated by server-rendered templates)
  lobbyChannel.on("match_created", () => wireJoinControls(document));
  lobbyChannel.on("match_updated", () => wireJoinControls(document));
  lobbyChannel.on("open_matches", () => wireJoinControls(document));
}

// DOM helpers

function updateMarket(companies) {
  const list = document.getElementById("companies-list");
  if (!list || !companies) return;

  list.innerHTML = companies
    .map(
      (c) => `
    <div class="flex items-center justify-between py-1 border-b border-gray-800">
      <div>
        <span class="font-mono text-emerald-400 text-sm">${escapeHtml(c.ticker)}</span>
        <span class="ml-2 text-gray-400 text-xs">${escapeHtml(c.name)}</span>
      </div>
      <div class="text-right">
        <span class="text-white font-semibold text-sm">$${parseFloat(c.price).toFixed(2)}</span>
        ${c.regulatory_heat > 0 ? `<span class="ml-1 text-xs text-red-400">⚠ ${escapeHtml(String(c.regulatory_heat))}</span>` : ""}
      </div>
    </div>
  `,
    )
    .join("");
}

function updatePlayers(players) {
  const list = document.getElementById("players-list");
  if (!list || !players) return;

  list.innerHTML = players
    .map(
      (p) => `
    <div class="flex items-center gap-2">
      <div class="w-2 h-2 rounded-full ${p.liquidity_frozen ? "bg-red-500" : "bg-emerald-500"}"></div>
      <span class="text-sm text-gray-300">${escapeHtml(p.username)}</span>
      ${p.has_submitted ? '<span class="ml-auto text-xs text-emerald-400">✓</span>' : ""}
    </div>
  `,
    )
    .join("");
}

function updatePhase(phase, round) {
  const el = document.getElementById("current-phase");
  const roundEl = document.getElementById("current-round");
  if (el) el.textContent = phase ? phase.replace("_", " ") : "";
  if (roundEl && round) roundEl.textContent = round;
}

function showPhaseNotice(phase) {
  const notices = {
    news: "📰 New market event!",
    action_submission: "🎯 Submit your action now.",
    negotiation: "💬 Negotiation phase — chat is open.",
    resolution: "⚡ Resolving round...",
    disclosure: "📊 Round resolved. Check the market.",
    finished: "🏁 Match over!",
  };
  if (notices[phase]) {
    appendSystemMessage(notices[phase]);
  }
}

function appendChatMessage(msg, isWhisper = false) {
  const container = document.getElementById("chat-messages");
  if (!container) return;

  const el = document.createElement("div");
  el.className = isWhisper ? "text-purple-300" : "text-gray-200";
  el.innerHTML = `<span class="font-medium text-emerald-400">${escapeHtml(msg.username || "?")}</span>: ${escapeHtml(msg.content)}`;
  container.appendChild(el);
  container.scrollTop = container.scrollHeight;
}

function appendSystemMessage(text) {
  const container = document.getElementById("chat-messages");
  if (!container) return;

  const el = document.createElement("div");
  el.className = "text-yellow-500 text-xs italic";
  el.textContent = text;
  container.appendChild(el);
  container.scrollTop = container.scrollHeight;
}

function showLeaderboard(leaderboard) {
  appendSystemMessage("=== FINAL STANDINGS ===");
  leaderboard.forEach((entry, i) => {
    appendSystemMessage(
      `${i + 1}. ${escapeHtml(entry.username)} — $${parseFloat(entry.net_worth).toFixed(2)}`,
    );
  });
}

function markOnline(userId, username) {
  // Update player dot to green — could be enhanced with a proper presence list
  // This function is intentionally left simple for now.
}

function escapeHtml(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
