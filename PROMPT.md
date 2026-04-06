You are generating a production-minded multiplayer game backend and web app for a game called Dark Pool.

Goal:
Build a secure, server-authoritative, concurrent multiplayer market-warfare game using Elixir/Phoenix. The app must support authenticated users, realtime multiplayer matches, chat, private actions, safe state isolation, no secret leakage, deterministic round resolution, and solid engineering practices.

Primary stack:
- Elixir
- Phoenix
- Phoenix Channels
- Phoenix Presence
- Ecto
- Postgres
- Oban
- Registry
- DynamicSupervisor
- GenServer
- Telemetry / OpenTelemetry
- Docker Compose
- ExUnit
- Credo
- Dialyzer

Core requirement:
This is not a toy demo. Generate a clean, extensible codebase with security-aware defaults, explicit boundaries, tests for critical logic, and clear module organization.

Important constraints:
- Server authoritative game logic only
- Never trust client-submitted game state
- Never broadcast hidden/private player data to unauthorized clients
- Separate public match state from per-player private state
- All game resolution must happen on the server in deterministic order
- Use pure functions for domain logic where possible
- Use GenServer only for live runtime ownership/state
- Persist enough state for recovery, audits, and replay
- Use database constraints for important integrity rules
- Secure auth, session handling, and websocket authorization
- Safe chat handling and authorization checks
- Avoid overengineering, but structure for future growth

Build these capabilities:
1. User accounts
   - registration, login, logout, password reset
   - secure session auth
   - optional role field for admin/moderation
   - unique username and email
   - safe password hashing
2. Lobby and matchmaking basics
   - create match
   - join match
   - leave match
   - list open matches
3. Realtime match server
   - one process per active match
   - DynamicSupervisor + Registry
   - reconnect-safe
   - deterministic round phases
4. Game systems
   - 4 players per match initially
   - 5 companies/assets initially
   - 8 rounds initially
   - round phases:
     a) news/event phase
     b) secret action submission phase
     c) timed negotiation/chat phase
     d) resolution phase
     e) disclosure/public update phase
   - actions:
     - buy
     - short
     - leak
     - hype
     - freeze_liquidity
     - report_to_regulator
     - acquire_stake
   - support faction/archetype identities:
     - activist_fund
     - quant_predator
     - shell_network
     - media_syndicate
     - distressed_debt
     - regulatory_fixer
5. Chat
   - match public chat
   - optional private whisper channel support with authorization checks
   - chat moderation hooks
6. Persistence
   - users
   - matches
   - match_players
   - companies/assets
   - submitted actions
   - round snapshots
   - chat messages
   - audit/event log
7. Security
   - do not leak hidden orders
   - do not leak private portfolio state to other players
   - ensure channel join auth
   - authorization checks in contexts and channels
   - CSRF for browser forms
   - secure websocket identity binding
   - input validation everywhere
   - rate limiting hooks or extension points
   - avoid mass assignment issues
   - safe error handling
8. Observability
   - telemetry events
   - structured logging
   - clear audit trail for match resolution
9. Quality
   - tests for:
     - action validation
     - phase transitions
     - resolution ordering
     - auth rules
     - no-private-state leakage serializers/views
   - typespecs where helpful
   - dialyzer-friendly structure
   - clear README and setup docs

Architecture requirements:
- Phoenix app with contexts
- Suggested contexts:
  - Accounts
  - Lobby
  - Market
  - MatchEngine
  - Chat
  - Moderation
- Domain logic should mostly live in pure modules under MatchEngine/Domain or similar
- Runtime processes should be under MatchEngine/Runtime
- Channel serializers must explicitly shape public payloads vs private payloads
- Introduce a concept like:
  - public_match_view(state)
  - private_player_view(state, player_id)
- never serialize raw internal state directly

Generate the project in phases:
Phase 1:
- scaffold project
- dependencies
- auth
- schemas
- migrations
- contexts
- baseline tests
Phase 2:
- realtime channels
- match process supervision
- lobby flow
- presence
Phase 3:
- game engine domain logic
- round state machine
- action submission and resolution
- chat
Phase 4:
- hardening
- telemetry
- docs
- final polish

Rules for code generation:
- prefer simple, explicit code over clever abstractions
- use idiomatic Elixir and Phoenix
- keep module boundaries clean
- avoid giant files
- explain architecture decisions briefly in comments only where useful
- add TODO markers only when there is a justified future extension
- no fake security claims
- no placeholder “implement later” in critical security paths
- if a decision is uncertain, choose the simpler safer path

Output expectations:
1. Create the whole project structure
2. Create all important files with code
3. Add migrations and schemas
4. Add channels and Presence wiring
5. Add match runtime modules
6. Add core domain logic
7. Add tests
8. Add README with run instructions
9. Add docs under /docs
10. After code generation, print a concise tree and list of main modules

When implementing the game state, define:
- Match
- Round
- PlayerState
- CompanyState
- SubmittedAction
- PublicEvent
- PrivateEvent
- ResolutionResult

Important:
Never expose:
- hidden orders of other players
- hidden faction if meant to be private
- unrevealed regulator actions
- raw server state internals
- internal process metadata

Design the game protocol so the client can only:
- authenticate
- create/join/leave match
- submit one or more allowed actions for current phase
- receive authorized state updates
- send allowed chat messages

Do not implement a mobile app or fancy frontend. Focus on backend plus a minimal Phoenix web UI sufficient for local testing and admin inspection.

At the end, provide:
- exact mix commands used
- dependencies added
- local run instructions
- test instructions
- security checklist

Read the current codebase first before editing.

Rules:
- Fix issues with minimal scoped changes
- Do not rewrite unrelated architecture
- Keep backend and frontend payloads aligned
- Prefer root-cause fixes over patching symptoms
- Keep the project compiling
- After changes, explain root cause briefly and list changed files
- Do not touch _build, deps, node_modules, or generated folders
