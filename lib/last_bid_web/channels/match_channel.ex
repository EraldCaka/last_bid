defmodule LastBidWeb.MatchChannel do
  use LastBidWeb, :channel

  alias LastBid.{Chat, Lobby, MatchEngine, Moderation}
  alias LastBidWeb.Presence

  @allowed_actions ~w(
    buy
    sell
    short
    leak
    hype
    freeze_liquidity
    report_to_regulator
    acquire_stake
  )

  @impl true
  def join("match:" <> match_id, _params, socket) do
    user_id = socket.assigns.user_id

    if Lobby.player_in_match?(user_id, match_id) do
      socket =
        socket
        |> assign(:match_id, match_id)
        |> assign(:private_topic, "match_private:#{match_id}:#{user_id}")

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
    user = LastBid.Accounts.get_user!(user_id)

    Presence.track_user(socket, user_id, %{username: user.username})
    Phoenix.PubSub.subscribe(LastBid.PubSub, socket.assigns.private_topic)

    push(socket, "presence_state", Presence.list(socket))

    case MatchEngine.get_private_state(match_id, user_id) do
      {:ok, private_state} -> push(socket, "private_state", private_state)
      _ -> :ok
    end

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

  def handle_info({:private_state, payload}, socket) do
    push(socket, "private_state", payload)
    {:noreply, socket}
  end

  def handle_info({:private_events, payload}, socket) do
    push(socket, "private_events", payload)
    {:noreply, socket}
  end

  def handle_info(%{event: "waiting_room_updated", payload: payload}, socket) do
    push(socket, "waiting_room_updated", payload)
    {:noreply, socket}
  end

  def handle_info(_msg, socket), do: {:noreply, socket}

  @impl true
  def handle_in("submit_action", %{"action_type" => action_type_str, "params" => params}, socket) do
    user_id = socket.assigns.user_id
    match_id = socket.assigns.match_id

    with {:ok, action_type} <- parse_action_type(action_type_str) do
      case MatchEngine.submit_action(match_id, user_id, action_type, params || %{}) do
        :ok -> {:reply, :ok, socket}
        {:error, reason} -> {:reply, {:error, %{reason: inspect(reason)}}, socket}
      end
    else
      {:error, reason} ->
        {:reply, {:error, %{reason: reason}}, socket}
    end
  end

  @impl true
  def handle_in("send_message", %{"content" => content}, socket) do
    user_id = socket.assigns.user_id
    match_id = socket.assigns.match_id

    with :ok <- Moderation.validate_chat_message(content),
         {:ok, msg} <-
           Chat.post_public_message(%{match_id: match_id, sender_id: user_id, content: content}) do
      broadcast!(socket, "new_message", %{
        id: msg.id,
        content: msg.content,
        sender_id: msg.sender_id,
        username: msg.sender && msg.sender.username,
        inserted_at: DateTime.to_iso8601(msg.inserted_at),
        type: "public"
      })

      {:reply, :ok, socket}
    else
      {:error, _} -> {:reply, {:error, %{reason: "message_rejected"}}, socket}
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

  defp parse_action_type(action_type) when is_binary(action_type) do
    if action_type in @allowed_actions do
      {:ok, String.to_atom(action_type)}
    else
      {:error, "invalid_action_type"}
    end
  end

  defp parse_action_type(_), do: {:error, "invalid_action_type"}
end
