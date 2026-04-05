Project instructions for Claude Code:

Priorities:
1. Security and information isolation
2. Deterministic server-authoritative multiplayer logic
3. Clean OTP design
4. Maintainability and testability
5. Minimal but solid web UI

Coding rules:
- Do not expose private state in channels
- Do not trust client data beyond validated commands
- Use pure functions for game rules
- Use GenServer for live match state only
- Keep runtime and domain logic separate
- Keep files focused and reasonably small
- Prefer explicit authorization checks
- Use transactions where consistency matters
- Use DB constraints for integrity
- Add tests for critical invariants

Architecture:
- Contexts for boundaries
- Match runtime process per active match
- Public and private serializers separated
- Replay/audit event log for important match changes
- Channels authorize every join
- Presence only exposes safe metadata

Quality bar:
- The generated code should compile
- Add migrations, tests, docs, and README
- Avoid stubs in critical paths
- If a feature is deferred, do it only for non-critical polish

When generating files:
- show full file paths
- create the files directly
- avoid giant monolithic modules
- keep naming consistent and idiomatic

Ignore generated and dependency folders unless explicitly needed for debugging or fixing compilation:
- `_build/`
- `deps/`
- `.elixir_ls/`
- `node_modules/`
- `priv/static/assets/`
- `assets/node_modules/`
- `coverage/`
- `.git/`
- `tmp/`

Rules for ignored folders:
- Do not read, modify, or plan around files inside those folders
- Do not waste tokens scanning generated artifacts or vendored dependencies
- Focus on project-owned source, config, migrations, tests, docs, and assets
- If a build error references generated files, trace back to the project source and fix that
- Inspect ignored folders only as a last resort and minimally
