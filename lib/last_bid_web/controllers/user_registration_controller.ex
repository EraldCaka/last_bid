defmodule LastBidWeb.UserRegistrationController do
  use LastBidWeb, :controller

  alias LastBid.Accounts
  alias LastBid.Accounts.User

  def new(conn, _params) do
    changeset = Accounts.change_user_registration(%User{})
    render(conn, :new, changeset: changeset)
  end

  def create(conn, %{"user" => user_params}) do
    case Accounts.register_user(user_params) do
      {:ok, _user} ->
        conn
        |> put_flash(:info, "Account created! You can now log in.")
        |> redirect(to: ~p"/users/login")

      {:error, %Ecto.Changeset{} = changeset} ->
        render(conn, :new, changeset: changeset)
    end
  end
end
