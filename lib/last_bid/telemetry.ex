defmodule LastBid.Telemetry do
  @moduledoc """
  Telemetry event definitions for the Dark Pool application.

  Events emitted:
  - [:last_bid, :match, :started]
  - [:last_bid, :match, :finished]
  - [:last_bid, :round, :phase_changed]
  - [:last_bid, :round, :resolved]
  - [:last_bid, :action, :submitted]
  - [:last_bid, :chat, :message_sent]
  """

  @doc "Emit a match started event."
  def match_started(match_id, metadata \\ %{}) do
    :telemetry.execute([:last_bid, :match, :started], %{count: 1}, Map.put(metadata, :match_id, match_id))
  end

  @doc "Emit a match finished event."
  def match_finished(match_id, metadata \\ %{}) do
    :telemetry.execute([:last_bid, :match, :finished], %{count: 1}, Map.put(metadata, :match_id, match_id))
  end

  @doc "Emit a round phase changed event."
  def phase_changed(match_id, round, phase, metadata \\ %{}) do
    :telemetry.execute(
      [:last_bid, :round, :phase_changed],
      %{count: 1},
      metadata |> Map.put(:match_id, match_id) |> Map.put(:round, round) |> Map.put(:phase, phase)
    )
  end

  @doc "Emit a round resolved event."
  def round_resolved(match_id, round, metadata \\ %{}) do
    :telemetry.execute(
      [:last_bid, :round, :resolved],
      %{count: 1},
      metadata |> Map.put(:match_id, match_id) |> Map.put(:round, round)
    )
  end

  @doc "Emit an action submitted event."
  def action_submitted(match_id, user_id, action_type, metadata \\ %{}) do
    :telemetry.execute(
      [:last_bid, :action, :submitted],
      %{count: 1},
      metadata
      |> Map.put(:match_id, match_id)
      |> Map.put(:user_id, user_id)
      |> Map.put(:action_type, action_type)
    )
  end

  @doc "Emit a chat message sent event."
  def chat_message_sent(match_id, user_id, type, metadata \\ %{}) do
    :telemetry.execute(
      [:last_bid, :chat, :message_sent],
      %{count: 1},
      metadata |> Map.put(:match_id, match_id) |> Map.put(:user_id, user_id) |> Map.put(:type, type)
    )
  end
end
