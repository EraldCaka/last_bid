defmodule LastBid.MatchEngine do
  @moduledoc """
  MatchEngine context — public API for the match runtime.

  This module bridges Lobby, Market, and the Runtime layer.
  All game mutations go through here.
  """

  alias LastBid.MatchEngine.Runtime.{MatchServer, MatchSupervisor}
  alias LastBid.{Audit, Lobby}

  @doc """
  Start a match. Transitions it to :active and spawns the runtime process.

  Returns {:ok, match} or {:error, reason}.
  """
  def start_match(match_id) do
    match = Lobby.get_match!(match_id)

    unless match.status == "waiting" do
      {:error, :match_not_waiting}
    else
      Lobby.start_match!(match)

      case MatchSupervisor.start_match(match_id) do
        {:ok, _pid} ->
          Audit.log(:match_started, %{}, match_id: match_id)
          LastBid.Telemetry.match_started(match_id)
          {:ok, Lobby.get_match!(match_id)}

        {:error, reason} ->
          # Roll back status if process failed to start
          Lobby.abandon_match!(Lobby.get_match!(match_id))
          {:error, reason}
      end
    end
  end

  @doc "Submit an action for a player in an active match."
  def submit_action(match_id, user_id, action_type, params)
      when is_binary(match_id) and is_binary(user_id) do
    if MatchServer.alive?(match_id) do
      result = MatchServer.submit_action(match_id, user_id, action_type, params)
      LastBid.Telemetry.action_submitted(match_id, user_id, action_type)
      result
    else
      {:error, :match_not_active}
    end
  end

  @doc "Get public match state (safe for all players)."
  def get_public_state(match_id) do
    if MatchServer.alive?(match_id) do
      MatchServer.get_public_state(match_id)
    else
      {:error, :match_not_active}
    end
  end

  @doc "Get private player state (only for the requesting player)."
  def get_private_state(match_id, user_id) do
    if MatchServer.alive?(match_id) do
      MatchServer.get_private_state(match_id, user_id)
    else
      {:error, :match_not_active}
    end
  end

  @doc "Manually advance the phase (admin/debug only)."
  def advance_phase(match_id) do
    if MatchServer.alive?(match_id) do
      MatchServer.advance_phase(match_id)
    else
      {:error, :match_not_active}
    end
  end

  @doc "Check if a match has a live server process."
  def active?(match_id), do: MatchServer.alive?(match_id)
end
