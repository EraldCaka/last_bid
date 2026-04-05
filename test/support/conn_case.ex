defmodule LastBidWeb.ConnCase do
  @moduledoc """
  Test case template for controller tests.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      use LastBidWeb, :verified_routes
      import Plug.Conn
      import Phoenix.ConnTest
      import LastBidWeb.ConnCase
      alias LastBidWeb.Router.Helpers, as: Routes

      @endpoint LastBidWeb.Endpoint
    end
  end

  setup tags do
    LastBid.DataCase.setup_sandbox(tags)
    {:ok, conn: Phoenix.ConnTest.build_conn()}
  end

  @doc "Log in a user and return a conn with session set."
  def log_in_user(conn, user) do
    token = LastBid.Accounts.generate_user_session_token(user)

    conn
    |> Phoenix.ConnTest.init_test_session(%{})
    |> Plug.Conn.put_session(:user_token, token)
  end
end
