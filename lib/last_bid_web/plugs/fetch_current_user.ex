defmodule LastBidWeb.Plugs.FetchCurrentUser do
  @moduledoc """
  Plug that loads the current user from the session and assigns it to conn.

  Assigns:
  - `:current_user` — the authenticated User struct or nil
  """

  import Plug.Conn
  alias LastBid.Accounts

  def init(opts), do: opts

  def call(conn, _opts) do
    {user_token, conn} = ensure_user_token(conn)

    current_user = user_token && Accounts.get_user_by_session_token(user_token)
    assign(conn, :current_user, current_user)
  end

  defp ensure_user_token(conn) do
    if token = get_session(conn, :user_token) do
      {token, conn}
    else
      conn = fetch_cookies(conn, signed: ~w(_last_bid_remember_me))

      if token = conn.cookies["_last_bid_remember_me"] do
        {token, put_session(conn, :user_token, token)}
      else
        {nil, conn}
      end
    end
  end
end
