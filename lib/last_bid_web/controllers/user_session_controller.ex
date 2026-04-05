defmodule LastBidWeb.UserSessionController do
  use LastBidWeb, :controller

  alias LastBid.Accounts

  def new(conn, _params) do
    render(conn, :new, error_message: nil)
  end

  def create(conn, %{"user" => %{"email" => email, "password" => password}}) do
    case Accounts.get_user_by_email_and_password(email, password) do
      user when not is_nil(user) ->
        token = Accounts.generate_user_session_token(user)

        conn
        |> put_session(:user_token, token)
        |> put_flash(:info, "Welcome back, #{user.username}!")
        |> redirect(to: ~p"/lobby")

      nil ->
        render(conn, :new, error_message: "Invalid email or password.")
    end
  end

  def delete(conn, _params) do
    if token = get_session(conn, :user_token) do
      Accounts.delete_user_session_token(token)
    end

    conn
    |> delete_session(:user_token)
    |> put_flash(:info, "Logged out successfully.")
    |> redirect(to: ~p"/")
  end
end
