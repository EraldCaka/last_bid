defmodule LastBid.MatchEngine.Runtime.MatchServer do
  use GenServer, restart: :transient

  require Logger

  alias LastBid.MatchEngine.Domain.{
    ActionValidator,
    EventGenerator,
    MatchState,
    PhaseMachine,
    Serializer
  }

  alias LastBid.{Audit, Lobby}
  alias LastBid.MatchEngine.RoundSnapshot
  alias LastBid.Repo
  alias LastBidWeb.Endpoint

  @registry LastBid.MatchEngine.Registry
  @negotiation_timeout_ms 125_000
  @news_display_ms 6_000

  def start_link(match_id) do
    GenServer.start_link(__MODULE__, match_id, name: via(match_id))
  end

  def get_public_state(match_id), do: GenServer.call(via(match_id), :get_public_state)

  def get_private_state(match_id, user_id),
    do: GenServer.call(via(match_id), {:get_private_state, user_id})

  def submit_action(match_id, user_id, action_type, params),
    do: GenServer.call(via(match_id), {:submit_action, user_id, action_type, params})

  def advance_phase(match_id), do: GenServer.call(via(match_id), :advance_phase)

  def alive?(match_id) do
    case Registry.lookup(@registry, match_id) do
      [{_pid, _}] -> true
      [] -> false
    end
  end

  @impl true
  def init(match_id) do
    Logger.metadata(match_id: match_id)

    case load_match_state(match_id) do
      {:ok, state} ->
        Audit.log(:round_started, %{round: state.round, phase: state.phase},
          match_id: match_id,
          round_number: state.round
        )

        {:ok, state, {:continue, :enter_initial_phase}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:enter_initial_phase, state) do
    handle_phase_entry(state)
    {:noreply, state}
  end

  @impl true
  def handle_call(:get_public_state, _from, state) do
    {:reply, {:ok, Serializer.public_match_view(state)}, state}
  end

  @impl true
  def handle_call({:get_private_state, user_id}, _from, state) do
    {:reply, Serializer.private_player_view(state, user_id), state}
  end

  @impl true
  def handle_call({:submit_action, user_id, action_type, params}, _from, state) do
    case ActionValidator.validate(state, user_id, action_type, params) do
      {:ok, normalized} ->
        action_record = %{
          user_id: user_id,
          action_type: Atom.to_string(action_type),
          payload: normalized,
          round_number: state.round
        }

        players =
          Map.update!(
            state.players,
            user_id,
            &LastBid.MatchEngine.Domain.PlayerState.mark_submitted/1
          )

        new_state = %{
          state
          | players: players,
            pending_actions: [action_record | state.pending_actions]
        }

        persist_submitted_action(state.match_id, action_record)
        broadcast_public_state(new_state)
        broadcast_private_states(new_state)

        if MatchState.all_submitted?(new_state), do: send(self(), :advance_to_negotiation)

        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:advance_phase, _from, state) do
    case PhaseMachine.advance(state) do
      {:ok, new_state} ->
        handle_phase_entry(new_state)
        {:reply, {:ok, PhaseMachine.next_phase_atom(state)}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(:advance_news, %{phase: :news} = state) do
    case PhaseMachine.advance(state) do
      {:ok, new_state} ->
        handle_phase_entry(new_state)
        {:noreply, new_state}

      {:error, _} ->
        {:noreply, state}
    end
  end

  def handle_info(:advance_news, state), do: {:noreply, state}

  @impl true
  def handle_info(:advance_to_negotiation, state) do
    case PhaseMachine.advance(state) do
      {:ok, new_state} ->
        handle_phase_entry(new_state)
        {:noreply, new_state}

      {:error, _} ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:negotiation_timeout, %{phase: :negotiation} = state) do
    case PhaseMachine.advance(state) do
      {:ok, new_state} ->
        handle_phase_entry(new_state)
        {:noreply, new_state}

      {:error, _} ->
        {:noreply, state}
    end
  end

  def handle_info(:negotiation_timeout, state), do: {:noreply, state}

  @impl true
  def handle_info(:resolve_round, %{phase: :resolution} = state) do
    news_event = EventGenerator.generate(state, state.round)
    result = LastBid.MatchEngine.Domain.Resolver.resolve(state, news_event)

    resolved_state = %{result.state | public_events: result.public_events}

    persist_round_snapshot(resolved_state, result)
    broadcast_private_events(resolved_state, result.private_events)
    broadcast_private_states(resolved_state)

    case PhaseMachine.advance(resolved_state) do
      {:ok, disclosure_state} ->
        disclosure_state = %{disclosure_state | public_events: result.public_events}
        handle_phase_entry(disclosure_state)

        Audit.log(:round_resolved, %{round: state.round},
          match_id: state.match_id,
          round_number: state.round
        )

        {:noreply, disclosure_state}

      {:error, _} ->
        {:noreply, resolved_state}
    end
  end

  def handle_info(:resolve_round, state), do: {:noreply, state}

  @impl true
  def handle_info(:finish_match, state) do
    broadcast_match_finished(state)

    match = Lobby.get_match!(state.match_id)
    Lobby.finish_match!(match)

    Audit.log(:match_finished, %{round: state.round}, match_id: state.match_id)
    {:stop, :normal, state}
  end

  @impl true
  def handle_info(:advance_disclosure, %{phase: :disclosure} = state) do
    case PhaseMachine.advance(state) do
      {:ok, new_state} ->
        handle_phase_entry(new_state)
        {:noreply, new_state}

      {:error, _} ->
        {:noreply, state}
    end
  end

  def handle_info(:advance_disclosure, state), do: {:noreply, state}

  defp handle_phase_entry(%{phase: :news} = state) do
    broadcast_phase_change(state, :news)
    broadcast_public_state(state)
    broadcast_private_states(state)
    Process.send_after(self(), :advance_news, @news_display_ms)
  end

  defp handle_phase_entry(%{phase: :action_submission} = state) do
    broadcast_phase_change(state, :action_submission)
    broadcast_public_state(state)
    broadcast_private_states(state)
  end

  defp handle_phase_entry(%{phase: :negotiation} = state) do
    broadcast_phase_change(state, :negotiation)
    broadcast_public_state(state)
    broadcast_private_states(state)
    Process.send_after(self(), :negotiation_timeout, @negotiation_timeout_ms)
  end

  defp handle_phase_entry(%{phase: :resolution} = state) do
    broadcast_phase_change(state, :resolution)
    broadcast_public_state(state)
    send(self(), :resolve_round)
  end

  defp handle_phase_entry(%{phase: :disclosure} = state) do
    broadcast_phase_change(state, :disclosure)
    broadcast_public_state(state)
    broadcast_private_states(state)
    Process.send_after(self(), :advance_disclosure, 10_000)
  end

  defp handle_phase_entry(%{phase: :finished}) do
    send(self(), :finish_match)
  end

  defp handle_phase_entry(state) do
    broadcast_public_state(state)
    broadcast_private_states(state)
  end

  defp broadcast_public_state(state) do
    Endpoint.broadcast(
      "match:#{state.match_id}",
      "state_updated",
      Serializer.public_match_view(state)
    )
  end

  defp broadcast_phase_change(state, phase) do
    Endpoint.broadcast("match:#{state.match_id}", "phase_changed", %{
      phase: phase,
      round: state.round,
      total_rounds: state.total_rounds,
      match_id: state.match_id,
      negotiation_deadline:
        state.negotiation_deadline && DateTime.to_iso8601(state.negotiation_deadline)
    })
  end

  defp broadcast_private_states(state) do
    Enum.each(Map.keys(state.players), fn user_id ->
      case Serializer.private_player_view(state, user_id) do
        {:ok, private_state} ->
          Phoenix.PubSub.broadcast(
            LastBid.PubSub,
            "match_private:#{state.match_id}:#{user_id}",
            {:private_state, private_state}
          )

        _ ->
          :ok
      end
    end)
  end

  defp broadcast_private_events(state, private_events) do
    Enum.each(private_events, fn {user_id, events} ->
      Phoenix.PubSub.broadcast(
        LastBid.PubSub,
        "match_private:#{state.match_id}:#{user_id}",
        {:private_events, %{match_id: state.match_id, round: state.round, events: events}}
      )
    end)
  end

  defp broadcast_match_finished(state) do
    Endpoint.broadcast("match:#{state.match_id}", "match_finished", %{
      match_id: state.match_id,
      leaderboard: Serializer.leaderboard(state)
    })
  end

  defp persist_submitted_action(match_id, action_record) do
    attrs = Map.put(action_record, :match_id, match_id)

    %LastBid.MatchEngine.SubmittedAction{}
    |> LastBid.MatchEngine.SubmittedAction.submit_changeset(attrs)
    |> Repo.insert()
    |> case do
      {:ok, _} -> :ok
      {:error, _} -> :ok
    end
  end

  defp persist_round_snapshot(state, _result) do
    public_state = Serializer.public_match_view(state)
    full_state = snapshot_state(state)

    %RoundSnapshot{}
    |> RoundSnapshot.create_changeset(%{
      match_id: state.match_id,
      round_number: state.round,
      public_state: public_state,
      full_state: Jason.encode!(full_state)
    })
    |> Repo.insert()
    |> case do
      {:ok, _} -> :ok
      {:error, _} -> :ok
    end
  end

  defp snapshot_state(state) do
    %{
      match_id: state.match_id,
      round: state.round,
      phase: state.phase,
      total_rounds: state.total_rounds,
      negotiation_deadline:
        state.negotiation_deadline && DateTime.to_iso8601(state.negotiation_deadline),
      players:
        Map.new(state.players, fn {id, player} ->
          {id,
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
             has_submitted: player.has_submitted
           }}
        end),
      companies:
        Map.new(state.companies, fn {ticker, company} ->
          {ticker,
           %{
             ticker: company.ticker,
             name: company.name,
             price: Decimal.to_string(company.price),
             base_price: Decimal.to_string(company.base_price),
             volatility: company.volatility,
             regulatory_heat: company.regulatory_heat,
             pending_hype: company.pending_hype,
             pending_leak: company.pending_leak
           }}
        end),
      pending_actions: state.pending_actions,
      public_events: state.public_events
    }
  end

  defp load_match_state(match_id) do
    match = Lobby.get_match!(match_id) |> Repo.preload(match_players: [:user])
    companies = LastBid.Market.list_match_companies(match_id)
    players = Enum.map(match.match_players, fn mp -> {mp, mp.user} end)
    {:ok, MatchState.build(match, players, companies)}
  end

  defp via(match_id), do: {:via, Registry, {@registry, match_id}}
end
