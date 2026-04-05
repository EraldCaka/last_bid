# Architecture

## Stack
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

## Main contexts
- Accounts
- Lobby
- Market
- MatchEngine
- Chat
- Moderation

## Runtime model
- One live match process per active match
- Match processes supervised under DynamicSupervisor
- Match lookup via Registry
- Persistent state stored in Postgres
- Live state cached in process memory
- Critical transitions optionally snapshotted

## Key separation
- Domain logic: pure modules
- Runtime logic: GenServers and supervisors
- Persistence: Ecto schemas and repos
- Delivery: controllers, LiveView or templates, channels

## Security model
- Public match state and private player state are separate payloads
- Never serialize internal state structs directly
- Channels must authorize joins using authenticated user
- Only players in a match can join that match channel
- Private payloads scoped to the requesting player only
