defmodule LastBidWeb.LobbyChannel do
  @moduledoc """
  Realtime channel for the game lobby.

  All authenticated users may join this channel.
  Handles match creation, joining, and listing.

  Topic: "lobby:general"
  """

  use LastBidWeb, :channel
  require Logger

  alias LastBid.{Lobby, MatchEngine}
  alias LastBidWeb.Presence

  @impl true
  def join("lobby:general", _params, socket) do
    send(self(), :after_join)
    {:ok, socket}
  end

  @impl true
  def handle_info(:after_join, socket) do
    user_id = socket.assigns.user_id
    user = LastBid.Accounts.get_user!(user_id)

    Presence.track_user(socket, user_id, %{username: user.username})
    push(socket, "presence_state", Presence.list(socket))

    push(socket, "open_matches", %{matches: list_open_matches()})

    {:noreply, socket}
  end

  @impl true
  def handle_info(%{event: "presence_diff", payload: diff}, socket) do
    push(socket, "presence_diff", diff)
    {:noreply, socket}
  end

  def handle_info(_msg, socket), do: {:noreply, socket}

  @impl true
  def handle_in("create_match", params, socket) do
    user = LastBid.Accounts.get_user!(socket.assigns.user_id)
    attrs = Map.take(params, ["name"])

    case Lobby.create_match(user, attrs) do
      {:ok, match} ->
        # Reload with preloads before broadcasting so format_match has full data
        full_match = Lobby.get_match_with_players!(match.id)
        broadcast!(socket, "match_created", format_match(full_match))
        {:reply, {:ok, %{match_id: match.id}}, socket}

      {:error, changeset} ->
        errors = format_errors(changeset)
        {:reply, {:error, %{errors: errors}}, socket}
    end
  end

  @impl true
  def handle_in("join_match", %{"match_id" => match_id}, socket) do
    user = LastBid.Accounts.get_user!(socket.assigns.user_id)

    Logger.debug("lobby:join_match request", user_id: user.id, match_id: match_id)

    case Lobby.join_match(user, match_id) do
      {:ok, _match_player} ->
        match = Lobby.get_match_with_players!(match_id)
        broadcast!(socket, "match_updated", format_match(match))
        Logger.debug("lobby:join_match success", user_id: user.id, match_id: match_id)
        {:reply, {:ok, %{match_id: match_id}}, socket}

      {:error, reason} ->
        Logger.debug("lobby:join_match failure",
          user_id: user.id,
          match_id: match_id,
          reason: inspect(reason)
        )

        {:reply, {:error, %{reason: inspect(reason)}}, socket}
    end
  end

  @impl true
  def handle_in("leave_match", %{"match_id" => match_id}, socket) do
    user = LastBid.Accounts.get_user!(socket.assigns.user_id)

    case Lobby.leave_match(user, match_id) do
      {:ok, :left} ->
        match = Lobby.get_match!(match_id)
        if match, do: broadcast!(socket, "match_updated", format_match(match))
        {:reply, :ok, socket}

      {:error, reason} ->
        {:reply, {:error, %{reason: inspect(reason)}}, socket}
    end
  end

  @impl true
  def handle_in("start_match", %{"match_id" => match_id}, socket) do
    user_id = socket.assigns.user_id
    match = Lobby.get_match!(match_id)

    if match && match.host_id == user_id do
      case MatchEngine.start_match(match_id) do
        {:ok, _match} ->
          broadcast!(socket, "match_started", %{match_id: match_id})
          {:reply, :ok, socket}

        {:error, reason} ->
          {:reply, {:error, %{reason: inspect(reason)}}, socket}
      end
    else
      {:reply, {:error, %{reason: "not_the_host"}}, socket}
    end
  end

  @impl true
  def handle_in("list_matches", _params, socket) do
    {:reply, {:ok, %{matches: list_open_matches()}}, socket}
  end

  # Helpers

  defp list_open_matches do
    Lobby.list_open_matches()
    |> Enum.map(&format_match/1)
  end

  defp format_match(match) do
    player_count =
      case match.match_players do
        %Ecto.Association.NotLoaded{} -> 0
        players -> length(players)
      end

    host_username =
      case match.host do
        %Ecto.Association.NotLoaded{} -> nil
        nil -> nil
        host -> host.username
      end

    %{
      id: match.id,
      name: match.name,
      status: match.status,
      max_players: match.max_players,
      player_count: player_count,
      host_username: host_username
    }
  end

  defp format_errors(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
      Regex.replace(~r"%{(\w+)}", msg, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end
end
