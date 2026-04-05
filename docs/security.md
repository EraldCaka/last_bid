# Security requirements

## Non-negotiable
- No hidden action leakage
- No unauthorized match access
- No unauthorized whisper/chat access
- No trust in client-side game state
- No raw state dumps over channels

## Auth
- Use Phoenix auth generator or equivalent secure approach
- Secure password hashing
- Secure session management
- Safe websocket auth binding to current user

## Authorization
- Validate user membership before channel join
- Validate player can only act in their own match seat
- Validate phase before accepting actions
- Validate action schema and allowed parameters

## Data exposure
- Public serializer for match-wide info
- Private serializer for per-player info
- Never expose other players' positions if not public by rules
- Never expose hidden roles if those become private game data

## Persistence and integrity
- DB constraints for uniqueness and foreign keys
- Transactions for important state changes
- Audit log for action submissions and resolution results

## Abuse resistance
- Add hooks for rate limiting
- Sanitize chat payloads
- Bound message sizes
- Bound action payload sizes
