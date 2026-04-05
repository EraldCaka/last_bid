defmodule LastBid.Repo do
  use Ecto.Repo,
    otp_app: :last_bid,
    adapter: Ecto.Adapters.Postgres
end
