defmodule LastBid.MatchEngine.Domain.ActionValidatorTest do
  use ExUnit.Case, async: true

  alias LastBid.MatchEngine.Domain.{ActionValidator, MatchState, PlayerState, CompanyState}

  # Build a minimal MatchState for testing
  defp base_state do
    player = %PlayerState{
      user_id: "user1",
      username: "alice",
      seat_number: 1,
      cash: Decimal.new("10000"),
      portfolio: %{},
      short_positions: %{},
      regulatory_heat: 0,
      liquidity_frozen: false,
      has_submitted: false
    }

    company = %CompanyState{
      ticker: "APEX",
      name: "Apex Industries",
      price: Decimal.new("100"),
      base_price: Decimal.new("100"),
      volatility: 0.1
    }

    %MatchState{
      match_id: "match1",
      round: 1,
      phase: :action_submission,
      total_rounds: 8,
      players: %{"user1" => player},
      companies: %{"APEX" => company},
      pending_actions: [],
      public_events: []
    }
  end

  describe "validate/4 - phase check" do
    test "accepts actions in action_submission phase" do
      state = base_state()
      assert {:ok, _} = ActionValidator.validate(state, "user1", :buy, %{"ticker" => "APEX", "quantity" => 5})
    end

    test "rejects actions in wrong phase" do
      state = %{base_state() | phase: :news}
      assert {:error, {:wrong_phase, :news}} = ActionValidator.validate(state, "user1", :buy, %{"ticker" => "APEX", "quantity" => 5})
    end
  end

  describe "validate/4 - player checks" do
    test "rejects unknown player" do
      state = base_state()
      assert {:error, :not_a_player} = ActionValidator.validate(state, "unknown", :buy, %{"ticker" => "APEX", "quantity" => 5})
    end

    test "rejects duplicate submission" do
      state = %{base_state() | players: %{"user1" => %{base_state().players["user1"] | has_submitted: true}}}
      assert {:error, :already_submitted} = ActionValidator.validate(state, "user1", :buy, %{"ticker" => "APEX", "quantity" => 5})
    end

    test "rejects action when liquidity frozen" do
      state = %{base_state() | players: %{"user1" => %{base_state().players["user1"] | liquidity_frozen: true}}}
      assert {:error, :liquidity_frozen} = ActionValidator.validate(state, "user1", :buy, %{"ticker" => "APEX", "quantity" => 5})
    end
  end

  describe "validate/4 - buy" do
    test "accepts buy with sufficient cash" do
      state = base_state()
      assert {:ok, %{action: :buy, ticker: "APEX", quantity: 5}} =
               ActionValidator.validate(state, "user1", :buy, %{"ticker" => "APEX", "quantity" => 5})
    end

    test "rejects buy with insufficient cash" do
      state = %{base_state() | players: %{"user1" => %{base_state().players["user1"] | cash: Decimal.new("10")}}}
      assert {:error, :insufficient_cash} =
               ActionValidator.validate(state, "user1", :buy, %{"ticker" => "APEX", "quantity" => 5})
    end

    test "rejects buy for unknown ticker" do
      state = base_state()
      assert {:error, {:unknown_ticker, "FAKE"}} =
               ActionValidator.validate(state, "user1", :buy, %{"ticker" => "FAKE", "quantity" => 5})
    end

    test "rejects invalid action type" do
      state = base_state()
      assert {:error, _} = ActionValidator.validate(state, "user1", :steal_money, %{})
    end
  end

  describe "validate/4 - short" do
    test "accepts short within limits" do
      state = base_state()
      assert {:ok, %{action: :short}} =
               ActionValidator.validate(state, "user1", :short, %{"ticker" => "APEX", "quantity" => 10})
    end
  end

  describe "validate/4 - hype" do
    test "accepts hype with valid ticker" do
      state = base_state()
      assert {:ok, %{action: :hype, ticker: "APEX"}} =
               ActionValidator.validate(state, "user1", :hype, %{"ticker" => "APEX"})
    end
  end

  describe "validate/4 - freeze_liquidity" do
    test "requires target_user_id" do
      state = base_state()
      assert {:error, :missing_target_user_id} =
               ActionValidator.validate(state, "user1", :freeze_liquidity, %{})
    end
  end
end
