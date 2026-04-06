defmodule LastBidWeb.Router do
  use LastBidWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {LastBidWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    # Load current_user from session token
    plug LastBidWeb.Plugs.FetchCurrentUser
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  ## Public routes

  scope "/", LastBidWeb do
    pipe_through :browser

    get "/", PageController, :home

    get "/users/register", UserRegistrationController, :new
    post "/users/register", UserRegistrationController, :create

    get "/users/login", UserSessionController, :new
    post "/users/login", UserSessionController, :create

    delete "/users/logout", UserSessionController, :delete
  end

  ## Authenticated routes

  scope "/", LastBidWeb do
    pipe_through [:browser, :require_auth]

    get "/lobby", MatchController, :index
    get "/matches/new", MatchController, :new
    post "/matches", MatchController, :create
    get "/matches/:id", MatchController, :show
    post "/matches/:id/join", MatchController, :join
  end

  ## Admin / development tools

  if Application.compile_env(:last_bid, :dev_routes) do
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through :browser
      live_dashboard "/dashboard", metrics: LastBidWeb.Telemetry
    end
  end

  ## Auth pipeline plug

  pipeline :require_auth do
    plug :require_authenticated_user
  end

  defp require_authenticated_user(conn, _opts) do
    if conn.assigns[:current_user] do
      conn
    else
      conn
      |> Phoenix.Controller.put_flash(:error, "You must log in to access this page.")
      |> Phoenix.Controller.redirect(to: "/users/login")
      |> Plug.Conn.halt()
    end
  end
end
