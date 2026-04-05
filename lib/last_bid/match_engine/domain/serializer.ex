defmodule LastBid.MatchEngine.Domain.Serializer do
  @moduledoc """
  Explicit serialization of match state for client delivery.

  Two views:
  - public_match_view/1  — safe for broadcast to all players in the match
  - private_player_view/2 — scoped to a single player only

  Never serialize raw internal state structs directly.
  Never expose other players' portfolio details, hidden orders, or process metadata.
  """

  alias LastBid.MatchEngine.Domain.{MatchState, PlayerState, CompanyState}

  @doc """
  Build the public match state that can be safely broadcast to all players.

  Includes: round, phase, company prices, player usernames + heat (no portfolios).
  """
  def public_match_view(%MatchState{} = state) do
    %{
      match_id: state.match_id,
      round: state.round,
      total_rounds: state.total_rounds,
      phase: state.phase,
      players: Enum.map(state.players, fn {_id, p} -> public_player_summary(p) end),
      companies: Enum.map(state.companies, fn {_ticker, c} -> public_company_view(c) end),
      # Negotiation deadline as ISO8601 string if set
      negotiation_deadline:
        state.negotiation_deadline &&
          DateTime.to_iso8601(state.negotiation_deadline)
    }
  end

  @doc """
  Build the private view for a single player. Includes their own portfolio and cash.
  Must NEVER be sent to any other player.
  """
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

  @doc "Serialize a public event for broadcast."
  def public_event_view(event) when is_map(event), do: event

  @doc "Serialize a list of resolution results (prices only, no actions)."
  def resolution_summary(%{public_events: events, state: state}) do
    %{
      round: state.round,
      phase: :disclosure,
      events: events,
      leaderboard: leaderboard(state)
    }
  end

  @doc "Compute public leaderboard — net worth ordered, no portfolio detail."
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
    |> Enum.sort_by(& &1.net_worth, :desc)
  end

  # Private helpers

  defp public_player_summary(%PlayerState{} = p) do
    %{
      user_id: p.user_id,
      username: p.username,
      seat_number: p.seat_number,
      # Faction is public knowledge in this MVP
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
