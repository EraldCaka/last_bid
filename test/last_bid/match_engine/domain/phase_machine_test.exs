defmodule LastBid.MatchEngine.Domain.PhaseMachineTest do
  use ExUnit.Case, async: true

  alias LastBid.MatchEngine.Domain.{PhaseMachine, MatchState}

  defp state_at(phase, round \\ 1, total \\ 8) do
    %MatchState{
      match_id: "m1",
      round: round,
      phase: phase,
      total_rounds: total,
      players: %{},
      companies: %{},
      pending_actions: [],
      public_events: []
    }
  end

  describe "advance/1" do
    test "news -> action_submission" do
      {:ok, next} = PhaseMachine.advance(state_at(:news))
      assert next.phase == :action_submission
    end

    test "action_submission -> negotiation" do
      {:ok, next} = PhaseMachine.advance(state_at(:action_submission))
      assert next.phase == :negotiation
    end

    test "negotiation -> resolution" do
      {:ok, next} = PhaseMachine.advance(state_at(:negotiation))
      assert next.phase == :resolution
    end

    test "resolution -> disclosure" do
      {:ok, next} = PhaseMachine.advance(state_at(:resolution))
      assert next.phase == :disclosure
    end

    test "disclosure -> news (next round)" do
      {:ok, next} = PhaseMachine.advance(state_at(:disclosure, 1, 8))
      assert next.phase == :news
      assert next.round == 2
    end

    test "disclosure at last round -> finished" do
      {:ok, next} = PhaseMachine.advance(state_at(:disclosure, 8, 8))
      assert next.phase == :finished
    end

    test "finished stays finished" do
      {:ok, next} = PhaseMachine.advance(state_at(:finished))
      assert next.phase == :finished
    end
  end

  describe "next_phase_atom/1" do
    test "maps each phase to the next" do
      assert PhaseMachine.next_phase_atom(state_at(:news)) == :action_submission
      assert PhaseMachine.next_phase_atom(state_at(:action_submission)) == :negotiation
      assert PhaseMachine.next_phase_atom(state_at(:negotiation)) == :resolution
      assert PhaseMachine.next_phase_atom(state_at(:resolution)) == :disclosure
      assert PhaseMachine.next_phase_atom(state_at(:disclosure, 1, 8)) == :news
      assert PhaseMachine.next_phase_atom(state_at(:disclosure, 8, 8)) == :finished
    end
  end
end
