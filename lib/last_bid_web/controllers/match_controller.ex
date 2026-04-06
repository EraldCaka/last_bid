defmodule LastBidWeb.MatchController do
  use LastBidWeb, :controller

  alias LastBid.Lobby
  alias LastBid.Lobby.Match

  def index(conn, _params) do
    user = conn.assigns.current_user
    open_matches = Lobby.list_open_matches()
    my_matches = Lobby.list_matches_for_user(user.id)
    socket_token = Phoenix.Token.sign(LastBidWeb.Endpoint, "user_auth", user.id)

    render(conn, :index,
      open_matches: open_matches,
      my_matches: my_matches,
      socket_token: socket_token
    )
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
        |> put_status(:see_other)
        |> redirect(to: ~p"/matches/#{match.id}")

      {:error, %Ecto.Changeset{} = changeset} ->
        render(conn, :new, changeset: changeset)
    end
  end

  def show(conn, %{"id" => match_id}) do
    user = conn.assigns.current_user
    match = Lobby.get_match_with_players!(match_id)
    is_player = Lobby.player_in_match?(user.id, match_id)
    socket_token = Phoenix.Token.sign(LastBidWeb.Endpoint, "user_auth", user.id)

    render(conn, :show,
      match: match,
      socket_token: socket_token,
      current_user: user,
      is_player: is_player
    )
  end

  def join(conn, %{"id" => match_id}) do
    user = conn.assigns.current_user

    case Lobby.join_match(user, match_id) do
      {:ok, _match_player} ->
        conn
        |> put_flash(:info, "You joined the match!")
        |> put_status(:see_other)
        |> redirect(to: ~p"/matches/#{match_id}")

      {:error, reason} ->
        conn
        |> put_flash(:error, "Could not join: #{format_join_error(reason)}")
        |> put_status(:see_other)
        |> redirect(to: ~p"/lobby")
    end
  end

  defp format_join_error(:match_full), do: "the match is full."
  defp format_join_error(:match_not_open), do: "the match is no longer open."
  defp format_join_error(:match_not_found), do: "match not found."
  defp format_join_error(:already_joined), do: "you already joined this match."
  defp format_join_error(other), do: inspect(other)
end
