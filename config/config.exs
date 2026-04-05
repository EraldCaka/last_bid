import Config

config :last_bid,
  ecto_repos: [LastBid.Repo],
  generators: [timestamp_type: :utc_datetime, binary_id: true]

config :last_bid, LastBidWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: LastBidWeb.ErrorHTML, json: LastBidWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: LastBid.PubSub,
  live_view: [signing_salt: "dark_pool_lv"]

config :last_bid, LastBid.Mailer, adapter: Swoosh.Adapters.Local

config :swoosh, :api_client, false

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id, :match_id, :user_id]

config :phoenix, :json_library, Jason

config :last_bid, Oban,
  repo: LastBid.Repo,
  queues: [default: 10, match_events: 5]

config :esbuild,
  version: "0.17.11",
  last_bid: [
    args: ~w(js/app.js --bundle --target=es2017 --outdir=../priv/static/assets),
    cd: Path.expand("../assets", __DIR__),
    env: %{"NODE_PATH" => Path.expand("../deps", __DIR__)}
  ]

config :tailwind,
  version: "3.4.0",
  last_bid: [
    args: ~w(
      --config=tailwind.config.js
      --input=css/app.css
      --output=../priv/static/assets/app.css
    ),
    cd: Path.expand("../assets", __DIR__)
  ]

import_config "#{config_env()}.exs"
