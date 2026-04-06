defmodule LastBid.MatchEngine.Domain.Serializer do
  @moduledoc """
  Explicit serialization of match state for client delivery.
  """

  alias LastBid.MatchEngine.Domain.{MatchState, PlayerState, CompanyState}

  def public_match_view(%MatchState{} = state) do
    %{
      match_id: state.match_id,
      round: state.round,
      total_rounds: state.total_rounds,
      phase: state.phase,
      players: Enum.map(state.players, fn {_id, p} -> public_player_summary(p) end),
      companies: Enum.map(state.companies, fn {_ticker, c} -> public_company_view(c) end),
      public_events: Enum.map(state.public_events || [], &public_event_view/1),
      negotiation_deadline:
        state.negotiation_deadline && DateTime.to_iso8601(state.negotiation_deadline)
    }
  end

  def private_player_view(%MatchState{} = state, player_id) do
    case Map.get(state.players, player_id) do
      nil ->
        {:error, :not_a_player}

      player ->
        {:ok,
         %{
           user_id: player.user_id,
           username: player.username,
           faction: player.faction,
           seat_number: player.seat_number,
           cash: Decimal.to_string(player.cash),
           portfolio: player.portfolio,
           short_positions: player.short_positions,
           regulatory_heat: player.regulatory_heat,
           liquidity_frozen: player.liquidity_frozen,
           has_submitted: player.has_submitted,
           net_worth:
             player
             |> PlayerState.net_worth(CompanyState.price_map(state.companies))
             |> Decimal.to_string()
         }}
    end
  end

  def public_event_view(%{type: :news_event, event: event}) do
    %{
      type: "news_event",
      round: event.round,
      headline: event.description,
      targets: event.targets,
      impact: event.price_delta
    }
  end

  def public_event_view(%{
        type: :price_updated,
        ticker: ticker,
        price: price,
        delta_pct: delta_pct
      }) do
    %{
      type: "price_updated",
      ticker: ticker,
      price: Decimal.to_string(price),
      delta_pct: delta_pct
    }
  end

  def public_event_view(%{type: :player_joined, username: username}) do
    %{type: "player_joined", headline: "#{username} joined the match."}
  end

  def public_event_view(%{type: :player_left, username: username}) do
    %{type: "player_left", headline: "#{username} left the match."}
  end

  def public_event_view(event) when is_map(event), do: event

  def leaderboard(%MatchState{players: players, companies: companies}) do
    price_map = CompanyState.price_map(companies)

    players
    |> Enum.map(fn {_id, player} ->
      %{
        username: player.username,
        seat_number: player.seat_number,
        net_worth: player |> PlayerState.net_worth(price_map) |> Decimal.to_string()
      }
    end)
    |> Enum.sort_by(fn row -> Decimal.new(row.net_worth) end, :desc)
  end

  defp public_player_summary(%PlayerState{} = p) do
    %{
      user_id: p.user_id,
      username: p.username,
      seat_number: p.seat_number,
      faction: p.faction,
      regulatory_heat: p.regulatory_heat,
      liquidity_frozen: p.liquidity_frozen,
      has_submitted: p.has_submitted
    }
  end

  defp public_company_view(%CompanyState{} = c) do
    %{
      ticker: c.ticker,
      name: c.name,
      price: Decimal.to_string(c.price),
      volatility: c.volatility,
      regulatory_heat: c.regulatory_heat
    }
  end
end
