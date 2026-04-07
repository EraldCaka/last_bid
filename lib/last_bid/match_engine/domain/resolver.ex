defmodule LastBid.MatchEngine.Domain.Resolver do
  @moduledoc false

  alias LastBid.MatchEngine.Domain.MatchState

  def resolve(%MatchState{} = state, news_event) do
    {state, news_public_events} = apply_news_event(state, news_event)

    {state, public_events, private_events} =
      state.pending_actions
      |> Enum.reverse()
      |> Enum.reduce({state, news_public_events, %{}}, fn action,
                                                          {acc_state, acc_public, acc_private} ->
        {next_state, new_public, new_private} = apply_action(acc_state, action)

        merged_private =
          Map.merge(acc_private, new_private, fn _user_id, left, right ->
            left ++ right
          end)

        {next_state, acc_public ++ new_public, merged_private}
      end)

    price_events =
      Enum.map(state.companies, fn {ticker, company} ->
        %{
          type: :price_updated,
          ticker: ticker,
          price: company.price,
          delta_pct: 0.0
        }
      end)

    %{
      state: state,
      public_events: public_events ++ price_events,
      private_events: private_events
    }
  end

  defp apply_news_event(state, nil), do: {state, []}

  defp apply_news_event(state, %{targets: targets, price_delta: delta} = event)
       when is_list(targets) and is_number(delta) do
    new_companies =
      Enum.reduce(targets, state.companies, fn ticker, companies ->
        update_in(companies[ticker], fn
          nil -> nil
          company -> %{company | price: adjust_price(company.price, delta)}
        end)
      end)

    public_event = %{
      type: :news_event,
      round: event.round,
      headline: event.description,
      targets: targets,
      impact: delta
    }

    {%{state | companies: new_companies}, [public_event]}
  end

  defp apply_news_event(state, _event), do: {state, []}

  defp apply_action(state, %{action_type: "buy", user_id: user_id, payload: payload}) do
    ticker = payload[:ticker] || payload["ticker"]
    requested_qty = payload[:quantity] || payload["quantity"] || 0
    player = state.players[user_id]
    company = state.companies[ticker]

    max_affordable =
      if to_float(company.price) <= 0 do
        0
      else
        floor(to_float(player.cash) / to_float(company.price))
      end

    qty = min(requested_qty, max_affordable)

    if qty <= 0 do
      {state, [], %{user_id => [%{message: "Buy failed: not enough cash.", positive: false}]}}
    else
      cost = to_float(company.price) * qty
      new_cash = money(to_float(player.cash) - cost)
      old_owned = Map.get(player.portfolio, ticker, 0)
      new_owned = old_owned + qty
      new_price = adjust_price(company.price, min(0.12, qty * 0.002))

      new_state =
        state
        |> put_in([Access.key!(:players), user_id, Access.key!(:cash)], new_cash)
        |> put_in([Access.key!(:players), user_id, Access.key!(:portfolio), ticker], new_owned)
        |> put_in([Access.key!(:companies), ticker, Access.key!(:price)], new_price)

      public_event = %{headline: "#{player.username} bought #{qty} #{ticker}.", type: :trade}

      private_event = %{
        message: "Bought #{qty} shares of #{ticker} for $#{Float.round(cost, 2)}.",
        positive: true
      }

      {new_state, [public_event], %{user_id => [private_event]}}
    end
  end

  defp apply_action(state, %{action_type: "sell", user_id: user_id, payload: payload}) do
    ticker = payload[:ticker] || payload["ticker"]
    requested_qty = payload[:quantity] || payload["quantity"] || 0
    player = state.players[user_id]
    company = state.companies[ticker]

    owned = Map.get(player.portfolio, ticker, 0)
    qty = min(requested_qty, owned)

    if qty <= 0 do
      {state, [],
       %{user_id => [%{message: "Sell failed: you do not own shares to sell.", positive: false}]}}
    else
      proceeds = to_float(company.price) * qty
      new_cash = money(to_float(player.cash) + proceeds)
      remaining = max(owned - qty, 0)
      new_price = adjust_price(company.price, -min(0.12, qty * 0.002))

      new_portfolio =
        if remaining == 0 do
          Map.delete(player.portfolio, ticker)
        else
          Map.put(player.portfolio, ticker, remaining)
        end

      new_state =
        state
        |> put_in([Access.key!(:players), user_id, Access.key!(:cash)], new_cash)
        |> put_in([Access.key!(:players), user_id, Access.key!(:portfolio)], new_portfolio)
        |> put_in([Access.key!(:companies), ticker, Access.key!(:price)], new_price)

      public_event = %{headline: "#{player.username} sold #{qty} #{ticker}.", type: :trade}

      private_event = %{
        message: "Sold #{qty} shares of #{ticker} for $#{Float.round(proceeds, 2)}.",
        positive: true
      }

      {new_state, [public_event], %{user_id => [private_event]}}
    end
  end

  defp apply_action(state, %{action_type: "short", user_id: user_id, payload: payload}) do
    ticker = payload[:ticker] || payload["ticker"]
    requested_qty = payload[:quantity] || payload["quantity"] || 0
    player = state.players[user_id]
    company = state.companies[ticker]

    qty = max(requested_qty, 0)

    if qty <= 0 do
      {state, [], %{user_id => [%{message: "Short failed: invalid quantity.", positive: false}]}}
    else
      old_short = Map.get(player.short_positions, ticker, 0)
      new_short = old_short + qty
      credit = to_float(company.price) * qty * 0.25
      new_cash = money(to_float(player.cash) + credit)
      new_price = adjust_price(company.price, -min(0.18, qty * 0.003))

      new_state =
        state
        |> put_in([Access.key!(:players), user_id, Access.key!(:cash)], new_cash)
        |> put_in(
          [Access.key!(:players), user_id, Access.key!(:short_positions), ticker],
          new_short
        )
        |> put_in([Access.key!(:companies), ticker, Access.key!(:price)], new_price)

      public_event = %{
        headline: "#{player.username} increased a short position in #{ticker}.",
        type: :trade
      }

      private_event = %{message: "Shorted #{qty} shares of #{ticker}.", positive: true}

      {new_state, [public_event], %{user_id => [private_event]}}
    end
  end

  defp apply_action(state, %{action_type: "acquire_stake", user_id: user_id, payload: payload}) do
    ticker = payload[:ticker] || payload["ticker"]
    requested_qty = payload[:quantity] || payload["quantity"] || 0
    player = state.players[user_id]
    company = state.companies[ticker]

    price_per_share = to_float(company.price) * 1.1

    max_affordable =
      if price_per_share <= 0, do: 0, else: floor(to_float(player.cash) / price_per_share)

    qty = min(requested_qty, max_affordable)

    if qty <= 0 do
      {state, [],
       %{user_id => [%{message: "Acquire stake failed: not enough cash.", positive: false}]}}
    else
      cost = price_per_share * qty
      new_cash = money(to_float(player.cash) - cost)
      old_owned = Map.get(player.portfolio, ticker, 0)
      new_owned = old_owned + qty
      new_price = adjust_price(company.price, min(0.25, qty * 0.004))

      new_state =
        state
        |> put_in([Access.key!(:players), user_id, Access.key!(:cash)], new_cash)
        |> put_in([Access.key!(:players), user_id, Access.key!(:portfolio), ticker], new_owned)
        |> put_in([Access.key!(:companies), ticker, Access.key!(:price)], new_price)

      public_event = %{
        headline: "#{player.username} acquired a strategic stake in #{ticker}.",
        type: :trade
      }

      private_event = %{message: "Acquired #{qty} shares of #{ticker}.", positive: true}

      {new_state, [public_event], %{user_id => [private_event]}}
    end
  end

  defp apply_action(state, %{action_type: "hype", user_id: user_id, payload: payload}) do
    ticker = payload[:ticker] || payload["ticker"]
    player = state.players[user_id]
    company = state.companies[ticker]
    new_price = adjust_price(company.price, 0.02)

    new_state = put_in(state, [Access.key!(:companies), ticker, Access.key!(:price)], new_price)

    public_event = %{
      headline: "#{player.username} pumped positive sentiment around #{ticker}.",
      type: :news
    }

    {new_state, [public_event], %{}}
  end

  defp apply_action(state, %{action_type: "leak", user_id: user_id, payload: payload}) do
    ticker = payload[:ticker] || payload["ticker"]
    target_user_id = payload[:target_user_id] || payload["target_user_id"]
    player = state.players[user_id]
    target = state.players[target_user_id]
    company = state.companies[ticker]
    new_price = adjust_price(company.price, -0.03)

    new_state = put_in(state, [Access.key!(:companies), ticker, Access.key!(:price)], new_price)

    public_event = %{
      headline: "#{player.username} leaked damaging intel about #{ticker}.",
      type: :news
    }

    private_event = %{
      message: "You were targeted by a leak operation from #{player.username}.",
      positive: false
    }

    {new_state, [public_event],
     %{
       target_user_id => [private_event],
       user_id => [%{message: "Leak executed against #{target.username}.", positive: true}]
     }}
  end

  defp apply_action(state, %{action_type: "freeze_liquidity", user_id: user_id, payload: payload}) do
    target_user_id = payload[:target_user_id] || payload["target_user_id"]
    player = state.players[user_id]
    target = state.players[target_user_id]

    new_state =
      put_in(state, [Access.key!(:players), target_user_id, Access.key!(:liquidity_frozen)], true)

    public_event = %{
      headline: "#{player.username} froze liquidity for #{target.username}.",
      type: :control
    }

    private_events = %{
      target_user_id => [%{message: "Your liquidity was frozen for this round.", positive: false}],
      user_id => [%{message: "Liquidity freeze applied to #{target.username}.", positive: true}]
    }

    {new_state, [public_event], private_events}
  end

  defp apply_action(state, %{
         action_type: "report_to_regulator",
         user_id: user_id,
         payload: payload
       }) do
    target_user_id = payload[:target_user_id] || payload["target_user_id"]
    player = state.players[user_id]
    target = state.players[target_user_id]

    current_heat = target.regulatory_heat || 0
    new_heat = current_heat + 1

    target_player =
      state.players[target_user_id]
      |> Map.put(:regulatory_heat, new_heat)
      |> maybe_freeze_from_heat()

    new_state = put_in(state, [Access.key!(:players), target_user_id], target_player)

    public_event = %{
      headline: "#{player.username} reported #{target.username} to regulators.",
      type: :control
    }

    private_events = %{
      target_user_id => [%{message: "Regulatory heat increased to #{new_heat}.", positive: false}],
      user_id => [%{message: "Report filed against #{target.username}.", positive: true}]
    }

    {new_state, [public_event], private_events}
  end

  defp apply_action(state, _action), do: {state, [], %{}}

  defp maybe_freeze_from_heat(player) do
    if (player.regulatory_heat || 0) >= 3 do
      %{player | liquidity_frozen: true}
    else
      player
    end
  end

  defp adjust_price(decimal_price, pct_delta) do
    base = to_float(decimal_price)
    next = max(1.0, base * (1.0 + pct_delta))
    money(next)
  end

  defp to_float(%Decimal{} = d), do: Decimal.to_float(d)
  defp to_float(v) when is_number(v), do: v
  defp to_float(_), do: 0.0

  defp money(value) when is_number(value) do
    rounded = Float.round(value, 2)
    Decimal.new(:erlang.float_to_binary(rounded, decimals: 2))
  end
end
