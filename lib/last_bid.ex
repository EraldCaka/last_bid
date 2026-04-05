defmodule LastBid do
  @moduledoc """
  LastBid — Dark Pool multiplayer market-warfare game.

  This module is the OTP application entry point alias.
  See `LastBid.Application` for the supervision tree.

  Key contexts:
  - `LastBid.Accounts`    — user registration and auth
  - `LastBid.Lobby`       — match lifecycle (create/join/leave)
  - `LastBid.Market`      — company price management
  - `LastBid.MatchEngine` — active match runtime
  - `LastBid.Chat`        — in-match messaging
  - `LastBid.Audit`       — append-only event log
  - `LastBid.Moderation`  — content validation hooks
  """
end
