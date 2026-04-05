defmodule LastBidWeb.UserSocket do
  use Phoenix.Socket

  ## Channels
  channel "match:*", LastBidWeb.MatchChannel
  channel "lobby:general", LastBidWeb.LobbyChannel

  @doc """
  Connect using a signed user token.

  The token is generated server-side on page load and embedded in the HTML.
  It is signed with Phoenix.Token so the server can verify identity without
  trusting any client-supplied user_id.
  """
  @impl true
  def connect(%{"token" => token}, socket, _connect_info) do
    case Phoenix.Token.verify(socket, "user_auth", token, max_age: 86_400) do
      {:ok, user_id} ->
        {:ok, assign(socket, :user_id, user_id)}

      {:error, _reason} ->
        :error
    end
  end

  def connect(_params, _socket, _connect_info), do: :error

  @doc "Unique socket ID per user — allows targeted disconnect."
  @impl true
  def id(socket), do: "user_socket:#{socket.assigns.user_id}"
end
