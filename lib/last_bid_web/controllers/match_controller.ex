defmodule LastBidWeb.MatchController do
  use LastBidWeb, :controller

  alias LastBid.Lobby
  alias LastBid.Lobby.Match

  def index(conn, _params) do
    open_matches = Lobby.list_open_matches()
    my_matches = Lobby.list_matches_for_user(conn.assigns.current_user.id)
    render(conn, :index, open_matches: open_matches, my_matches: my_matches)
  end

  def new(conn, _params) do
    changeset = Lobby.Match.create_changeset(%Match{}, %{})
    render(conn, :new, changeset: changeset)
  end

  def create(conn, %{"match" => match_params}) do
    user = conn.assigns.current_user

    case Lobby.create_match(user, match_params) do
      {:ok, match} ->
        conn
        |> put_flash(:info, "Match created! Share the link with friends.")
        |> redirect(to: ~p"/matches/#{match.id}")

      {:error, %Ecto.Changeset{} = changeset} ->
        render(conn, :new, changeset: changeset)
    end
  end

  def show(conn, %{"id" => match_id}) do
    user = conn.assigns.current_user

    unless Lobby.player_in_match?(user.id, match_id) do
      conn
      |> put_flash(:error, "You are not in this match.")
      |> redirect(to: ~p"/lobby")
    else
      match = Lobby.get_match_with_players!(match_id)
      # Generate a WebSocket auth token for this user
      socket_token =
        Phoenix.Token.sign(LastBidWeb.Endpoint, "user_auth", user.id)

      render(conn, :show,
        match: match,
        socket_token: socket_token,
        current_user: user
      )
    end
  end
end
