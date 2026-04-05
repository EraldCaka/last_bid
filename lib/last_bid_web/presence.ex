defmodule LastBidWeb.Presence do
  @moduledoc """
  Phoenix Presence for tracking connected users in lobby and matches.

  Only safe metadata is exposed (username, user_id, online_at).
  Never expose session tokens, IP addresses, or internal state.
  """

  use Phoenix.Presence,
    otp_app: :last_bid,
    pubsub_server: LastBid.PubSub

  @doc "Track a user joining a topic (lobby or match channel)."
  def track_user(socket, user_id, meta) do
    track(socket, user_id, safe_meta(meta))
  end

  @doc "Update user presence metadata."
  def update_user(socket, user_id, meta) do
    update(socket, user_id, safe_meta(meta))
  end

  # Only expose safe fields — never session tokens, IPs, etc.
  defp safe_meta(meta) do
    %{
      username: meta[:username] || meta["username"],
      online_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }
  end
end
