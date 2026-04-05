defmodule LastBid.Audit do
  @moduledoc """
  Audit context.

  Records all important match events for compliance, replay, and debugging.
  Audit events are append-only and must never be updated or deleted.
  """

  import Ecto.Query
  alias LastBid.Repo
  alias LastBid.Audit.Event

  @doc "Log a match event. Returns :ok on success, logs an error otherwise."
  def log(event_type, payload, opts \\ []) do
    attrs = %{
      event_type: to_string(event_type),
      payload: payload,
      match_id: opts[:match_id],
      user_id: opts[:user_id],
      round_number: opts[:round_number]
    }

    case %Event{} |> Event.create_changeset(attrs) |> Repo.insert() do
      {:ok, _event} ->
        :ok

      {:error, changeset} ->
        require Logger
        Logger.error("Audit log failed", changeset: inspect(changeset), event_type: event_type)
        :ok
    end
  end

  @doc "Returns events for a match, ordered by insertion time."
  def events_for_match(match_id) do
    Repo.all(
      from e in Event,
        where: e.match_id == ^match_id,
        order_by: [asc: e.inserted_at]
    )
  end

  @doc "Returns events of a given type for a match."
  def events_for_match_by_type(match_id, event_type) do
    Repo.all(
      from e in Event,
        where: e.match_id == ^match_id and e.event_type == ^to_string(event_type),
        order_by: [asc: e.inserted_at]
    )
  end
end
