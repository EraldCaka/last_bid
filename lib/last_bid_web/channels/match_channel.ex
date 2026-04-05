defmodule LastBidWeb.MatchChannel do
  @moduledoc """
  Realtime channel for an active match.

  Security:
  - join/3 verifies the user is a player in the match
  - Actions are validated on the server; client data is never trusted
  - Private state is pushed only to the requesting socket
  - Broadcasts contain only public state
  - Whisper messages are authorized before persistence

  Topic format: "match:{match_id}"
  """

  use LastBidWeb, :channel

  alias LastBid.{Chat, Lobby, MatchEngine, Moderation}
  alias LastBidWeb.Presence

  @impl true
  def join("match:" <> match_id, _params, socket) do
    user_id = socket.assigns.user_id

    if Lobby.player_in_match?(user_id, match_id) do
      socket = assign(socket, :match_id, match_id)

      send(self(), :after_join)
      {:ok, socket}
    else
      {:error, %{reason: "unauthorized"}}
    end
  end

  @impl true
  def handle_info(:after_join, socket) do
    user_id = socket.assigns.user_id
    match_id = socket.assigns.match_id

    # Track presence (safe meta only)
    user = LastBid.Accounts.get_user!(user_id)
    Presence.track_user(socket, user_id, %{username: user.username})

    # Push full presence list to the joining user
    push(socket, "presence_state", Presence.list(socket))

    # Push private state to this specific player
    case MatchEngine.get_private_state(match_id, user_id) do
      {:ok, private_state} -> push(socket, "private_state", private_state)
      _ -> :ok
    end

    # Push public state to this specific player
    case MatchEngine.get_public_state(match_id) do
      {:ok, public_state} -> push(socket, "state_updated", public_state)
      _ -> :ok
    end

    {:noreply, socket}
  end

  @impl true
  def handle_info(%{event: "presence_diff", payload: diff}, socket) do
    push(socket, "presence_diff", diff)
    {:noreply, socket}
  end

  def handle_info(_msg, socket), do: {:noreply, socket}

  # --- Client messages ---

  @impl true
  def handle_in("submit_action", %{"action_type" => action_type_str, "params" => params}, socket) do
    user_id = socket.assigns.user_id
    match_id = socket.assigns.match_id

    action_type =
      case action_type_str do
        s when is_binary(s) -> String.to_existing_atom(s)
        _ -> nil
      end

    if is_nil(action_type) do
      {:reply, {:error, %{reason: "invalid_action_type"}}, socket}
    else
      case MatchEngine.submit_action(match_id, user_id, action_type, params) do
        :ok ->
          {:reply, :ok, socket}

        {:error, reason} ->
          {:reply, {:error, %{reason: inspect(reason)}}, socket}
      end
    end
  rescue
    ArgumentError ->
      {:reply, {:error, %{reason: "invalid_action_type"}}, socket}
  end

  @impl true
  def handle_in("send_message", %{"content" => content}, socket) do
    user_id = socket.assigns.user_id
    match_id = socket.assigns.match_id

    with :ok <- Moderation.validate_chat_message(content),
         {:ok, msg} <-
           Chat.post_public_message(%{
             match_id: match_id,
             sender_id: user_id,
             content: content
           }) do
      broadcast!(socket, "new_message", format_message(msg, socket))
      {:reply, :ok, socket}
    else
      {:error, :message_too_long} ->
        {:reply, {:error, %{reason: "message_too_long"}}, socket}

      {:error, _} ->
        {:reply, {:error, %{reason: "message_rejected"}}, socket}
    end
  end

  @impl true
  def handle_in("send_whisper", %{"content" => content, "recipient_id" => recipient_id}, socket) do
    user_id = socket.assigns.user_id
    match_id = socket.assigns.match_id

    # Verify recipient is also in this match
    unless Lobby.player_in_match?(recipient_id, match_id) do
      {:reply, {:error, %{reason: "recipient_not_in_match"}}, socket}
    else
      with :ok <- Moderation.validate_chat_message(content),
           {:ok, msg} <-
             Chat.post_whisper(%{
               match_id: match_id,
               sender_id: user_id,
               recipient_id: recipient_id,
               content: content
             }) do
        # Push only to sender and recipient, NOT broadcast
        push(socket, "new_whisper", format_whisper(msg))

        Phoenix.PubSub.broadcast(
          LastBid.PubSub,
          "user:#{recipient_id}",
          {:new_whisper, format_whisper(msg)}
        )

        {:reply, :ok, socket}
      else
        {:error, _} -> {:reply, {:error, %{reason: "message_rejected"}}, socket}
      end
    end
  end

  @impl true
  def handle_in("get_private_state", _params, socket) do
    user_id = socket.assigns.user_id
    match_id = socket.assigns.match_id

    case MatchEngine.get_private_state(match_id, user_id) do
      {:ok, private_state} -> {:reply, {:ok, private_state}, socket}
      {:error, reason} -> {:reply, {:error, %{reason: inspect(reason)}}, socket}
    end
  end

  # --- Helpers ---

  defp format_message(msg, _socket) do
    %{
      id: msg.id,
      content: msg.content,
      sender_id: msg.sender_id,
      # Only expose username, not internal user data
      username: msg.sender && msg.sender.username,
      inserted_at: DateTime.to_iso8601(msg.inserted_at),
      type: "public"
    }
  end

  defp format_whisper(msg) do
    %{
      id: msg.id,
      content: msg.content,
      sender_id: msg.sender_id,
      recipient_id: msg.recipient_id,
      inserted_at: DateTime.to_iso8601(msg.inserted_at),
      type: "whisper"
    }
  end
end
