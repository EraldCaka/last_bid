defmodule LastBid.MatchEngine.Domain.Resolver do
  @moduledoc """
  Pure deterministic round resolver.

  Applies actions in a strict, deterministic order:
    1. Event effects (news price deltas)
    2. Liquidity freeze effects
    3. Buy / short market impact
    4. Leak / hype market impact
    5. Stake acquisitions
    6. Regulator heat
    7. Public event generation
  """

  alias LastBid.MatchEngine.Domain.{MatchState, CompanyState}

  @type resolution_result :: %{
          state: MatchState.t(),
          public_events: list(),
          private_events: %{String.t() => list()}
        }

  def resolve(%MatchState{} = state, news_event) do
    original_prices = CompanyState.price_map(state.companies)
    private_events = Map.new(Map.keys(state.players), fn uid -> {uid, []} end)

    {state, private_events} =
      state
      |> apply_news_event(news_event, private_events)
      |> apply_freeze_liquidity(state.pending_actions)
      |> apply_buy_short(state.pending_actions)
      |> apply_leaks_hype(state.pending_actions)
      |> apply_stake_acquisitions(state.pending_actions)
      |> apply_regulator_actions(state.pending_actions)

    public_events = build_public_events(state, news_event, original_prices)

    %{
      state: %{state | public_events: public_events},
      public_events: public_events,
      private_events: private_events
    }
  end

  defp apply_news_event(%MatchState{} = state, news_event, private_events) do
    companies =
      Enum.reduce(news_event.targets, state.companies, fn ticker, acc ->
        case Map.get(acc, ticker) do
          nil ->
            acc

          company ->
            Map.put(acc, ticker, CompanyState.apply_price_delta(company, news_event.price_delta))
        end
      end)

    {%{state | companies: companies}, private_events}
  end

  defp apply_freeze_liquidity({state, private_events}, actions) do
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

    updated_private =
      Enum.reduce(freezes, private_events, fn action, acc ->
        target_id = get_in(action.payload, ["target_user_id"])
        event = {:freeze_applied, target_id}
        Map.update(acc, action.user_id, [event], &[event | &1])
      end)

    {%{state | players: players}, updated_private}
  end

  defp apply_buy_short({state, private_events}, actions) do
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
    quantity = normalize_qty(get_in(action.payload, ["quantity"]))
    user_id = action.user_id

    with player when not is_nil(player) <- Map.get(state.players, user_id),
         company when not is_nil(company) <- Map.get(state.companies, ticker),
         false <- player.liquidity_frozen do
      cost = Decimal.mult(company.price, Decimal.new(quantity))

      if Decimal.compare(player.cash, cost) != :lt do
        new_cash = Decimal.sub(player.cash, cost)
        new_portfolio = Map.update(player.portfolio, ticker, quantity, &(&1 + quantity))
        new_player = %{player | cash: new_cash, portfolio: new_portfolio}
        new_company = CompanyState.apply_price_delta(company, impact_for_buy(quantity))

        new_state =
          state
          |> put_in([Access.key(:players), user_id], new_player)
          |> put_in([Access.key(:companies), ticker], new_company)

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
    quantity = normalize_qty(get_in(action.payload, ["quantity"]))
    user_id = action.user_id

    with player when not is_nil(player) <- Map.get(state.players, user_id),
         company when not is_nil(company) <- Map.get(state.companies, ticker),
         false <- player.liquidity_frozen do
      new_shorts = Map.update(player.short_positions, ticker, quantity, &(&1 + quantity))
      new_player = %{player | short_positions: new_shorts}
      new_company = CompanyState.apply_price_delta(company, -impact_for_short(quantity))

      new_state =
        state
        |> put_in([Access.key(:players), user_id], new_player)
        |> put_in([Access.key(:companies), ticker], new_company)

      event = {user_id, {:short_opened, ticker, quantity}}
      {new_state, [event | events]}
    else
      _ -> {state, events}
    end
  end

  defp apply_leaks_hype({state, private_events}, actions) do
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

    companies =
      Map.new(companies, fn {ticker, company} ->
        delta = company.pending_hype * 0.02 - company.pending_leak * 0.03

        {ticker,
         if(delta != 0, do: CompanyState.apply_price_delta(company, delta), else: company)}
      end)

    {%{state | companies: companies}, private_events}
  end

  defp apply_stake_acquisitions({state, private_events}, actions) do
    stakes = Enum.filter(actions, &(&1.action_type == "acquire_stake"))

    {state, stake_events} =
      Enum.reduce(stakes, {state, []}, fn action, {st, evs} ->
        ticker = get_in(action.payload, ["ticker"])
        quantity = normalize_qty(get_in(action.payload, ["quantity"]))
        user_id = action.user_id

        with player when not is_nil(player) <- Map.get(st.players, user_id),
             company when not is_nil(company) <- Map.get(st.companies, ticker),
             false <- player.liquidity_frozen do
          cost = Decimal.mult(company.price, Decimal.new(quantity))

          if Decimal.compare(player.cash, cost) != :lt do
            new_cash = Decimal.sub(player.cash, cost)
            new_portfolio = Map.update(player.portfolio, ticker, quantity, &(&1 + quantity))
            new_player = %{player | cash: new_cash, portfolio: new_portfolio}
            new_company = CompanyState.apply_price_delta(company, impact_for_stake(quantity))

            new_state =
              st
              |> put_in([Access.key(:players), user_id], new_player)
              |> put_in([Access.key(:companies), ticker], new_company)

            event = {user_id, {:stake_acquired, ticker, quantity}}
            {new_state, [event | evs]}
          else
            {st, evs}
          end
        else
          _ -> {st, evs}
        end
      end)

    updated_private =
      Enum.reduce(stake_events, private_events, fn {uid, event}, acc ->
        Map.update(acc, uid, [event], &[event | &1])
      end)

    {state, updated_private}
  end

  defp apply_regulator_actions({state, private_events}, actions) do
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

  defp build_public_events(state, news_event, original_prices) do
    price_changes =
      Enum.reduce(state.companies, [], fn {ticker, company}, acc ->
        previous = Map.get(original_prices, ticker, company.price)

        if Decimal.compare(previous, company.price) == :eq do
          acc
        else
          pct_change = pct_change(previous, company.price)

          [
            %{
              type: :price_updated,
              ticker: ticker,
              price: company.price,
              previous_price: previous,
              pct_change: pct_change
            }
            | acc
          ]
        end
      end)
      |> Enum.reverse()

    [%{type: :news_event, event: news_event} | price_changes]
  end

  defp impact_for_buy(qty), do: min(0.005 + qty / 10_000, 0.08)
  defp impact_for_short(qty), do: min(0.005 + qty / 10_000, 0.08)
  defp impact_for_stake(qty), do: min(0.02 + qty / 5_000, 0.12)

  defp normalize_qty(qty) when is_integer(qty), do: max(qty, 0)

  defp normalize_qty(qty) when is_binary(qty) do
    case Integer.parse(qty) do
      {int, _} -> max(int, 0)
      :error -> 0
    end
  end

  defp normalize_qty(_), do: 0

  defp pct_change(previous, current) do
    prev = Decimal.to_float(previous)
    curr = Decimal.to_float(current)

    if prev == 0 do
      0.0
    else
      (curr - prev) / prev * 100.0
    end
  end
end
