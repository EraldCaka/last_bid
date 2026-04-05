defmodule LastBid.MatchEngine.Domain.ResolverTest do
  use ExUnit.Case, async: true

  alias LastBid.MatchEngine.Domain.{
    Resolver,
    MatchState,
    PlayerState,
    CompanyState
  }

  defp base_state do
    player1 = %PlayerState{
      user_id: "u1",
      username: "alice",
      seat_number: 1,
      cash: Decimal.new("10000"),
      portfolio: %{},
      short_positions: %{},
      regulatory_heat: 0,
      liquidity_frozen: false,
      has_submitted: true
    }

    player2 = %PlayerState{
      user_id: "u2",
      username: "bob",
      seat_number: 2,
      cash: Decimal.new("10000"),
      portfolio: %{},
      short_positions: %{},
      regulatory_heat: 0,
      liquidity_frozen: false,
      has_submitted: true
    }

    company = %CompanyState{
      ticker: "APEX",
      name: "Apex Industries",
      price: Decimal.new("100"),
      base_price: Decimal.new("100"),
      volatility: 0.1,
      pending_hype: 0,
      pending_leak: 0
    }

    %MatchState{
      match_id: "m1",
      round: 1,
      phase: :resolution,
      total_rounds: 8,
      players: %{"u1" => player1, "u2" => player2},
      companies: %{"APEX" => company},
      pending_actions: [],
      public_events: []
    }
  end

  defp news_event do
    %{
      type: :earnings_beat,
      description: "APEX reports earnings beat.",
      targets: ["APEX"],
      price_delta: 0.10,
      round: 1,
      public: true
    }
  end

  describe "resolve/2" do
    test "applies news event price delta" do
      state = base_state()
      %{state: new_state} = Resolver.resolve(state, news_event())
      price = new_state.companies["APEX"].price
      # 100 * 1.10 = 110
      assert Decimal.compare(price, Decimal.new("110")) == :eq
    end

    test "buy action deducts cash and adds portfolio" do
      actions = [
        %{
          user_id: "u1",
          action_type: "buy",
          payload: %{"action" => "buy", "ticker" => "APEX", "quantity" => 5},
          round_number: 1
        }
      ]

      state = %{base_state() | pending_actions: actions}
      %{state: new_state} = Resolver.resolve(state, %{news_event() | price_delta: 0.0})

      player = new_state.players["u1"]
      # bought 5 shares at 100 = 500 deducted
      assert Decimal.compare(player.cash, Decimal.new("9500")) == :eq
      assert player.portfolio["APEX"] == 5
    end

    test "frozen player cannot execute buy" do
      actions = [
        %{
          user_id: "u1",
          action_type: "buy",
          payload: %{"ticker" => "APEX", "quantity" => 5},
          round_number: 1
        }
      ]

      state = %{base_state() | pending_actions: actions}
      state = put_in(state.players["u1"].liquidity_frozen, true)

      %{state: new_state} = Resolver.resolve(state, %{news_event() | price_delta: 0.0})

      # cash should not change for frozen player
      assert Decimal.compare(new_state.players["u1"].cash, Decimal.new("10000")) == :eq
    end

    test "freeze_liquidity action freezes target" do
      actions = [
        %{
          user_id: "u1",
          action_type: "freeze_liquidity",
          payload: %{"target_user_id" => "u2"},
          round_number: 1
        }
      ]

      state = %{base_state() | pending_actions: actions}
      %{state: new_state} = Resolver.resolve(state, %{news_event() | price_delta: 0.0})

      assert new_state.players["u2"].liquidity_frozen == true
    end

    test "report_to_regulator increases target regulatory_heat" do
      actions = [
        %{
          user_id: "u1",
          action_type: "report_to_regulator",
          payload: %{"target_user_id" => "u2"},
          round_number: 1
        }
      ]

      state = %{base_state() | pending_actions: actions}
      %{state: new_state} = Resolver.resolve(state, %{news_event() | price_delta: 0.0})

      assert new_state.players["u2"].regulatory_heat == 1
    end

    test "player is frozen when regulatory_heat reaches 3" do
      state = put_in(base_state().players["u2"].regulatory_heat, 2)

      actions = [
        %{
          user_id: "u1",
          action_type: "report_to_regulator",
          payload: %{"target_user_id" => "u2"},
          round_number: 1
        }
      ]

      state = %{state | pending_actions: actions}
      %{state: new_state} = Resolver.resolve(state, %{news_event() | price_delta: 0.0})

      assert new_state.players["u2"].regulatory_heat == 3
      assert new_state.players["u2"].liquidity_frozen == true
    end

    test "hype raises price by 2%" do
      actions = [
        %{
          user_id: "u1",
          action_type: "hype",
          payload: %{"ticker" => "APEX"},
          round_number: 1
        }
      ]

      state = %{base_state() | pending_actions: actions}
      %{state: new_state} = Resolver.resolve(state, %{news_event() | price_delta: 0.0})

      price = new_state.companies["APEX"].price
      # 100 * 1.02 = 102
      assert Decimal.compare(price, Decimal.new("102")) == :eq
    end

    test "resolution returns non-empty public_events" do
      state = base_state()
      %{public_events: events} = Resolver.resolve(state, news_event())
      assert length(events) > 0
    end
  end
end
