defmodule LastBid.MatchEngine.Domain.PhaseMachine do
  @moduledoc """
  Pure state machine for match phase transitions.

  Phase order per round:
    news → action_submission → negotiation → resolution → disclosure → (next round or finished)
  """

  alias LastBid.MatchEngine.Domain.MatchState

  @negotiation_duration_seconds 120

  def advance(%MatchState{} = state) do
    case next_phase(state) do
      {:ok, new_state} -> {:ok, new_state}
      {:error, _} = err -> err
    end
  end

  def next_phase_atom(%MatchState{phase: phase, round: round, total_rounds: total}) do
    case phase do
      :news -> :action_submission
      :action_submission -> :negotiation
      :negotiation -> :resolution
      :resolution -> :disclosure
      :disclosure when round >= total -> :finished
      :disclosure -> :news
      :finished -> :finished
    end
  end

  defp next_phase(%MatchState{phase: :disclosure, round: round, total_rounds: total} = state)
       when round >= total do
    {:ok, %{state | phase: :finished, negotiation_deadline: nil}}
  end

  defp next_phase(%MatchState{phase: :disclosure} = state) do
    next_state =
      state
      |> advance_round()
      |> clear_round_state()
      |> Map.put(:phase, :news)
      |> Map.put(:negotiation_deadline, nil)

    {:ok, next_state}
  end

  defp next_phase(%MatchState{phase: :finished} = state) do
    {:ok, state}
  end

  defp next_phase(%MatchState{phase: :action_submission} = state) do
    deadline = DateTime.add(DateTime.utc_now(), @negotiation_duration_seconds, :second)
    {:ok, %{state | phase: :negotiation, negotiation_deadline: deadline}}
  end

  defp next_phase(%MatchState{phase: :negotiation} = state) do
    {:ok, %{state | phase: :resolution, negotiation_deadline: nil}}
  end

  defp next_phase(%MatchState{phase: current} = state) do
    transition = %{
      news: :action_submission,
      resolution: :disclosure
    }

    case Map.get(transition, current) do
      nil -> {:error, {:no_transition_from, current}}
      next -> {:ok, %{state | phase: next}}
    end
  end

  defp advance_round(%MatchState{round: round} = state), do: %{state | round: round + 1}

  defp clear_round_state(%MatchState{} = state) do
    players =
      Map.new(state.players, fn {id, player} ->
        {id, LastBid.MatchEngine.Domain.PlayerState.reset_submitted(player)}
      end)

    companies =
      Map.new(state.companies, fn {ticker, company} ->
        {ticker, LastBid.MatchEngine.Domain.CompanyState.clear_pending(company)}
      end)

    %{state | players: players, companies: companies, pending_actions: [], public_events: []}
  end
end
