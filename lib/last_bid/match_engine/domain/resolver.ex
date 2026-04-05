defmodule LastBid.MatchEngine.Domain.Resolver do
  @moduledoc """
  Pure deterministic round resolver.

  Applies actions in a strict, deterministic order:
    1. Event effects (news price deltas)
    2. Liquidity freeze effects
    3. Buy/sell price movements
    4. Short effects
    5. Leak / hype effects
    6. Stake acquisitions
    7. Regulator heat
    8. End-of-round cleanup

  All inputs are pure — this function must be side-effect-free.
  Persistence happens in MatchEngine.match_engine.ex after resolution.
  """

  alias LastBid.MatchEngine.Domain.{MatchState, CompanyState}

  @type resolution_result :: %{
          state: MatchState.t(),
          public_events: list(),
          private_events: %{String.t() => list()}
        }

  @doc "Resolve all pending actions for the current round."
  def resolve(%MatchState{} = state, news_event) do
    private_events = Map.new(Map.keys(state.players), fn uid -> {uid, []} end)

    {state, private_events} =
      state
      |> apply_news_event(news_event)
      |> apply_freeze_liquidity(state.pending_actions, private_events)
      |> apply_buy_short(state.pending_actions, private_events)
      |> apply_leaks_hype(state.pending_actions, private_events)
      |> apply_stake_acquisitions(state.pending_actions, private_events)
      |> apply_regulator_actions(state.pending_actions, private_events)

    public_events = build_public_events(state, news_event)

    %{state: state, public_events: public_events, private_events: private_events}
  end

  # Step 1: Apply news/event price deltas
  defp apply_news_event({state, private_events}, news_event) do
    companies =
      Enum.reduce(news_event.targets, state.companies, fn ticker, acc ->
        case Map.get(acc, ticker) do
          nil -> acc
          company -> Map.put(acc, ticker, CompanyState.apply_price_delta(company, news_event.price_delta))
        end
      end)

    {%{state | companies: companies}, private_events}
  end

  defp apply_news_event(state, news_event) when is_map(state) do
    apply_news_event({state, %{}}, news_event)
  end

  # Step 2: Apply liquidity freeze effects
  defp apply_freeze_liquidity({state, private_events}, actions, _pev) do
    freezes = Enum.filter(actions, &(&1.action_type == "freeze_liquidity"))

    players =
      Enum.reduce(freezes, state.players, fn action, acc ->
        target_id = get_in(action.payload, ["target_user_id"])

        if target_id && Map.has_key?(acc, target_id) do
          Map.update!(acc, target_id, &%{&1 | liquidity_frozen: true})
        else
          acc
        end
      end)

    freeze_events =
      Enum.map(freezes, fn action ->
        target_id = get_in(action.payload, ["target_user_id"])
        {action.user_id, {:freeze_applied, target_id}}
      end)

    updated_private = Enum.reduce(freeze_events, private_events, fn {uid, event}, acc ->
      Map.update(acc, uid, [event], &[event | &1])
    end)

    {%{state | players: players}, updated_private}
  end

  # Step 3: Apply buy and short actions
  defp apply_buy_short({state, private_events}, actions, _pev) do
    buys = Enum.filter(actions, &(&1.action_type == "buy"))
    shorts = Enum.filter(actions, &(&1.action_type == "short"))

    {state, buy_events} = Enum.reduce(buys, {state, []}, &apply_buy/2)
    {state, short_events} = Enum.reduce(shorts, {state, []}, &apply_short/2)

    updated_private =
      Enum.reduce(buy_events ++ short_events, private_events, fn {uid, event}, acc ->
        Map.update(acc, uid, [event], &[event | &1])
      end)

    {state, updated_private}
  end

  defp apply_buy(action, {state, events}) do
    ticker = get_in(action.payload, ["ticker"])
    quantity = get_in(action.payload, ["quantity"]) || 0
    user_id = action.user_id

    with player when not is_nil(player) <- Map.get(state.players, user_id),
         company when not is_nil(company) <- Map.get(state.companies, ticker),
         false <- player.liquidity_frozen do
      cost = Decimal.mult(company.price, Decimal.new(quantity))

      if Decimal.compare(player.cash, cost) != :lt do
        new_cash = Decimal.sub(player.cash, cost)
        new_portfolio = Map.update(player.portfolio, ticker, quantity, &(&1 + quantity))
        new_player = %{player | cash: new_cash, portfolio: new_portfolio}
        new_state = put_in(state.players[user_id], new_player)
        event = {user_id, {:buy_executed, ticker, quantity, company.price}}
        {new_state, [event | events]}
      else
        {state, events}
      end
    else
      _ -> {state, events}
    end
  end

  defp apply_short(action, {state, events}) do
    ticker = get_in(action.payload, ["ticker"])
    quantity = get_in(action.payload, ["quantity"]) || 0
    user_id = action.user_id

    with player when not is_nil(player) <- Map.get(state.players, user_id),
         company when not is_nil(company) <- Map.get(state.companies, ticker),
         false <- player.liquidity_frozen do
      new_shorts = Map.update(player.short_positions, ticker, quantity, &(&1 + quantity))
      new_player = %{player | short_positions: new_shorts}
      new_state = put_in(state.players[user_id], new_player)
      event = {user_id, {:short_opened, ticker, quantity}}
      {new_state, [event | events]}
    else
      _ -> {state, events}
    end
  end

  # Step 4: Apply leak/hype — modifies pending effects on companies
  defp apply_leaks_hype({state, private_events}, actions, _pev) do
    leaks = Enum.filter(actions, &(&1.action_type == "leak"))
    hypes = Enum.filter(actions, &(&1.action_type == "hype"))

    companies =
      Enum.reduce(leaks, state.companies, fn action, acc ->
        ticker = get_in(action.payload, ["ticker"])
        if ticker && Map.has_key?(acc, ticker) do
          Map.update!(acc, ticker, &%{&1 | pending_leak: &1.pending_leak + 1})
        else
          acc
        end
      end)

    companies =
      Enum.reduce(hypes, companies, fn action, acc ->
        ticker = get_in(action.payload, ["ticker"])
        if ticker && Map.has_key?(acc, ticker) do
          Map.update!(acc, ticker, &%{&1 | pending_hype: &1.pending_hype + 1})
        else
          acc
        end
      end)

    # Each hype: +2% per hype, each leak: -3% per leak
    companies =
      Map.new(companies, fn {ticker, company} ->
        delta = company.pending_hype * 0.02 - company.pending_leak * 0.03
        {ticker, if(delta != 0, do: CompanyState.apply_price_delta(company, delta), else: company)}
      end)

    {%{state | companies: companies}, private_events}
  end

  # Step 5: Stake acquisitions (buy with large position bonus)
  defp apply_stake_acquisitions({state, private_events}, actions, _pev) do
    stakes = Enum.filter(actions, &(&1.action_type == "acquire_stake"))

    {state, stake_events} = Enum.reduce(stakes, {state, []}, fn action, {st, evs} ->
      # Treat as a large buy — reuse buy logic
      {new_st, new_evs} = apply_buy(action, {st, []})
      {new_st, evs ++ new_evs}
    end)

    updated_private =
      Enum.reduce(stake_events, private_events, fn {uid, event}, acc ->
        Map.update(acc, uid, [event], &[event | &1])
      end)

    {state, updated_private}
  end

  # Step 6: Regulator heat adjustments
  defp apply_regulator_actions({state, private_events}, actions, _pev) do
    reports = Enum.filter(actions, &(&1.action_type == "report_to_regulator"))

    players =
      Enum.reduce(reports, state.players, fn action, acc ->
        target_id = get_in(action.payload, ["target_user_id"])

        if target_id && Map.has_key?(acc, target_id) do
          Map.update!(acc, target_id, &%{&1 | regulatory_heat: &1.regulatory_heat + 1})
        else
          acc
        end
      end)

    # Freeze players with heat >= 3
    players =
      Map.new(players, fn {id, player} ->
        if player.regulatory_heat >= 3 do
          {id, %{player | liquidity_frozen: true}}
        else
          {id, player}
        end
      end)

    {%{state | players: players}, private_events}
  end

  # Build public events list after all resolution
  defp build_public_events(state, news_event) do
    price_changes =
      Enum.map(state.companies, fn {ticker, company} ->
        %{type: :price_updated, ticker: ticker, price: company.price}
      end)

    [%{type: :news_event, event: news_event} | price_changes]
  end
end
