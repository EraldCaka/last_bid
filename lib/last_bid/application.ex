defmodule LastBid.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Database
      LastBid.Repo,
      # PubSub for Phoenix channels and Presence
      {Phoenix.PubSub, name: LastBid.PubSub},
      # Telemetry supervisor (metrics + poller)
      LastBidWeb.Telemetry,
      # HTTP client for Swoosh mailer
      {Finch, name: LastBid.Finch},
      # Background job processing
      {Oban, Application.fetch_env!(:last_bid, Oban)},
      # Match engine: Registry + DynamicSupervisor for active matches
      LastBid.MatchEngine.Runtime.MatchSupervisor,
      # Phoenix Presence (tracks connected players per match/lobby)
      LastBidWeb.Presence,
      # Phoenix endpoint (HTTP + WS) — must be last
      LastBidWeb.Endpoint
    ]

    opts = [strategy: :one_for_one, name: LastBid.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def config_change(changed, _new, removed) do
    LastBidWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
