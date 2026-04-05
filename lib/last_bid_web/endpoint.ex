defmodule LastBidWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :last_bid

  # The session will be stored in the cookie and signed,
  # this means its contents can be read but not tampered with.
  @session_options [
    store: :cookie,
    key: "_last_bid_key",
    signing_salt: "dark_pool_salt",
    same_site: "Lax",
    max_age: 60 * 60 * 24 * 60
  ]

  # WebSocket for Phoenix channels — uses token-based auth, not cookies.
  socket "/socket", LastBidWeb.UserSocket,
    websocket: [timeout: 45_000],
    longpoll: false

  # LiveView socket
  socket "/live", Phoenix.LiveView.Socket,
    websocket: [connect_info: [session: @session_options]]

  plug Plug.Static,
    at: "/",
    from: :last_bid,
    gzip: false,
    only: LastBidWeb.static_paths()

  if code_reloading? do
    plug Phoenix.CodeReloader
    plug Phoenix.Ecto.CheckRepoStatus, otp_app: :last_bid
  end

  plug Phoenix.LiveDashboard.RequestLogger,
    param_key: "request_logger",
    cookie_key: "request_logger"

  plug Plug.RequestId
  plug Plug.Telemetry, event_prefix: [:phoenix, :endpoint]

  plug Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()

  plug Plug.MethodOverride
  plug Plug.Head
  plug Plug.Session, @session_options
  plug LastBidWeb.Router
end
