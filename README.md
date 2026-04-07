# Last Bid

A server-authoritative multiplayer market-warfare game built with Elixir/Phoenix.

Players manipulate prices, control narratives, and outmaneuver rivals across 8 rounds to finish with the highest net worth.

---

## Stack

| Layer | Tech |
|---|---|
| Language | Elixir 1.15+ |
| Web | Phoenix 1.7 + Channels + Presence |
| DB | PostgreSQL + Ecto |
| Auth | Custom session auth (bcrypt) |
| Background jobs | Oban |
| Runtime concurrency | GenServer + DynamicSupervisor + Registry |
| Testing | ExUnit + ExMachina |
| Quality | Credo + Dialyxir |

---

## Quick start

### 1. Start PostgreSQL

```bash
docker compose up -d
```

### 2. Install dependencies and set up the database

```bash
mix setup
```

This runs:
- `mix deps.get`
- `mix ecto.create`
- `mix ecto.migrate`
- `mix run priv/repo/seeds.exs`
- Asset compilation (esbuild + tailwind)

### 3. Run the server

```bash
mix phx.server
```

Visit [http://localhost:4000](http://localhost:4000).

**Seed accounts:**
- `alice@example.com` / `supersecret123!`
- `bob@example.com` / `supersecret123!`

---

## Running tests

```bash
mix test
```

Run a specific test file:

```bash
mix test test/last_bid/match_engine/domain/resolver_test.exs
```

---

## Architecture overview

```
lib/
├── last_bid/
│   ├── accounts/           # User registration, auth, session tokens
│   ├── lobby/              # Match lifecycle (create/join/leave)
│   ├── market/             # Company schema and price persistence
│   ├── match_engine/
│   │   ├── domain/         # Pure game logic (no side effects)
│   │   │   ├── match_state.ex
│   │   │   ├── player_state.ex
│   │   │   ├── company_state.ex
│   │   │   ├── action_validator.ex
│   │   │   ├── phase_machine.ex
│   │   │   ├── event_generator.ex
│   │   │   ├── resolver.ex
│   │   │   └── serializer.ex
│   │   ├── runtime/        # GenServer + Supervisor (side-effecting)
│   │   │   ├── match_server.ex
│   │   │   └── match_supervisor.ex
│   │   └── match_engine.ex # Public context API
│   ├── chat/               # Chat messages (public + whisper)
│   ├── audit/              # Append-only event log
│   └── moderation/         # Content validation hooks
└── last_bid_web/
    ├── channels/
    │   ├── user_socket.ex   # Token-based WS auth
    │   ├── match_channel.ex # Game actions + chat
    │   └── lobby_channel.ex # Match creation/joining
    ├── controllers/         # Auth + lobby + match UI
    ├── components/          # Layouts + core components
    └── presence.ex          # Safe presence metadata
```

### Key design decisions

**Server authoritative**: All game state mutations happen inside `MatchServer`. The client submits commands; the server validates and applies them. Client state is never trusted.

**Information isolation**: Two serializers — `public_match_view/1` (broadcast to all) and `private_player_view/2` (pushed only to the owner). Raw internal structs are never sent to clients.

**Pure domain logic**: `Domain.*` modules are pure functions. They take state in, return state out. Side effects (DB, broadcasts) live in `MatchServer` and the context layer.

**One process per match**: Each active match has a `MatchServer` GenServer owned by `MatchSupervisor`. Lookup by match_id via `Registry`.

---

## Round phases

| Phase | Description |
|---|---|
| `news` | A random public event affects company prices |
| `action_submission` | Players secretly submit one action each |
| `negotiation` | 2-minute public chat window |
| `resolution` | Server applies actions in deterministic order |
| `disclosure` | Resolved prices and events broadcast to all |

**Resolution order**: event effects → liquidity freezes → buys/shorts → leaks/hype → stake acquisitions → regulator heat → end-of-round cleanup.

---

## Player actions

| Action | Effect |
|---|---|
| `buy` | Purchase shares at current price |
| `short` | Open a short position |
| `hype` | Push a ticker's price up ~2% |
| `leak` | Push a ticker's price down ~3% |
| `freeze_liquidity` | Block a target player for one round |
| `report_to_regulator` | Add regulatory heat to a target (3 = frozen) |
| `acquire_stake` | Large buy with stake registration |

---

## WebSocket protocol

Connect at `ws://localhost:4000/socket` with `token=<user_auth_token>`.

The auth token is generated server-side and embedded in the match HTML page.

**Match channel** (`match:{match_id}`):

| Event (client → server) | Payload |
|---|---|
| `submit_action` | `{action_type, params}` |
| `send_message` | `{content}` |
| `send_whisper` | `{content, recipient_id}` |
| `get_private_state` | `{}` |

| Event (server → client) | Payload |
|---|---|
| `state_updated` | Public match state |
| `phase_changed` | `{phase, round}` |
| `private_state` | Per-player private state |
| `new_message` | Chat message |
| `new_whisper` | Whisper (sender + recipient only) |
| `match_finished` | Final leaderboard |

---

## Security checklist

- [x] Passwords hashed with bcrypt
- [x] Session tokens stored in signed cookie (not plaintext)
- [x] WebSocket auth via Phoenix.Token (signed, time-limited)
- [x] Channel join requires verified match membership
- [x] Private player state pushed to individual socket only
- [x] Other players' portfolios/orders never sent
- [x] Chat content sanitized (HTML stripped)
- [x] Chat messages size-bounded (500 chars)
- [x] Action payloads size-bounded (10 keys max)
- [x] CSRF protection on all browser forms
- [x] DB constraints back up schema validations
- [x] Audit log for all match events
- [x] Only host can start a match
- [x] `report_to_regulator` uses server-side player state, not client claims
- [ ] Rate limiting (TODO: add via plug or separate middleware)
- [ ] IP-based abuse protection (TODO: reverse proxy config)

---

## Development commands

```bash
# Format code
mix format

# Run linter
mix credo

# Run type checker (first run is slow)
mix dialyzer

# Open interactive shell with app started
iex -S mix phx.server

# Reset database
mix ecto.reset

# Generate a secret key base
mix phx.gen.secret
```
