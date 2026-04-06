defmodule LastBid.MatchEngine.Runtime.MatchServer do
  @moduledoc """
  GenServer that owns the live in-memory state for a single active match.

  One process per match, started on demand and registered in the Registry.
  State is periodically snapshotted to Postgres for recovery.

  The server is the single source of truth during an active match.
  All game mutations go through handle_call/handle_cast here.
  """

  use GenServer, restart: :transient

  require Logger

  alias LastBid.MatchEngine.Domain.{
    ActionValidator,
    EventGenerator,
    MatchState,
    PhaseMachine,
    Serializer
  }

  alias LastBid.{Audit, Lobby, Market}
  alias LastBid.MatchEngine.RoundSnapshot
  alias LastBid.Repo
  alias LastBidWeb.Endpoint

  @registry LastBid.MatchEngine.Registry
  @negotiation_timeout_ms 125_000
  # How long players see the news headline before action_submission opens
  @news_display_ms 6_000

  # --- Public API ---

  def start_link(match_id) do
    GenServer.start_link(__MODULE__, match_id, name: via(match_id))
  end

  @doc "Get the current public state of a match."
  def get_public_state(match_id) do
    GenServer.call(via(match_id), :get_public_state)
  end

  @doc "Get the private state for a specific player."
  def get_private_state(match_id, user_id) do
    GenServer.call(via(match_id), {:get_private_state, user_id})
  end

  @doc "Submit an action for a player."
  def submit_action(match_id, user_id, action_type, params) do
    GenServer.call(via(match_id), {:submit_action, user_id, action_type, params})
  end

  @doc "Advance to the next phase (called by timer or explicit trigger)."
  def advance_phase(match_id) do
    GenServer.call(via(match_id), :advance_phase)
  end

  @doc "Check if the server for a match is alive."
  def alive?(match_id) do
    case Registry.lookup(@registry, match_id) do
      [{_pid, _}] -> true
      [] -> false
    end
  end

  # --- GenServer callbacks ---

  @impl true
  def init(match_id) do
    Logger.metadata(match_id: match_id)
    Logger.info("MatchServer starting for match #{match_id}")

    case load_match_state(match_id) do
      {:ok, state} ->
        Audit.log(:round_started, %{round: state.round, phase: state.phase},
          match_id: match_id,
          round_number: state.round
        )

        # Use handle_continue to broadcast initial phase AFTER init returns,
        # so the process is fully registered before any broadcast is sent.
        {:ok, state, {:continue, :enter_initial_phase}}

      {:error, reason} ->
        Logger.error("MatchServer failed to load state: #{inspect(reason)}")
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

        players = Map.update!(state.players, user_id, &LastBid.MatchEngine.Domain.PlayerState.mark_submitted/1)
        new_state = %{state | players: players, pending_actions: [action_record | state.pending_actions]}

        persist_submitted_action(state.match_id, action_record)
        broadcast_public_state(new_state)

        # Auto-advance if all players submitted
        if MatchState.all_submitted?(new_state) do
          send(self(), :advance_to_negotiation)
        end

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
    Logger.info("Negotiation timeout for match #{state.match_id}, advancing to resolution")

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

    result =
      LastBid.MatchEngine.Domain.Resolver.resolve(state, news_event)

    new_state = result.state

    # Persist snapshot
    persist_round_snapshot(new_state, result)

    # Emit private events to each player
    broadcast_private_events(new_state, result.private_events)

    # Advance to disclosure
    case PhaseMachine.advance(new_state) do
      {:ok, disclosure_state} ->
        handle_phase_entry(disclosure_state)

        Audit.log(:round_resolved, %{round: state.round},
          match_id: state.match_id,
          round_number: state.round
        )

        LastBid.Telemetry.round_resolved(state.match_id, state.round)

        {:noreply, disclosure_state}

      {:error, _} ->
        {:noreply, new_state}
    end
  end

  def handle_info(:resolve_round, state), do: {:noreply, state}

  @impl true
  def handle_info(:finish_match, state) do
    Logger.info("Match #{state.match_id} finished")
    broadcast_match_finished(state)

    match = Lobby.get_match!(state.match_id)
    Lobby.finish_match!(match)

    Audit.log(:match_finished, %{round: state.round}, match_id: state.match_id)
    LastBid.Telemetry.match_finished(state.match_id)

    {:stop, :normal, state}
  end

  # Advance from disclosure to next news phase (or handle finish)
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

  # --- Phase entry side effects ---

  defp handle_phase_entry(%{phase: :negotiation} = state) do
    broadcast_phase_change(state, :negotiation)
    Process.send_after(self(), :negotiation_timeout, @negotiation_timeout_ms)
  end

  defp handle_phase_entry(%{phase: :resolution} = state) do
    broadcast_phase_change(state, :resolution)
    send(self(), :resolve_round)
  end

  defp handle_phase_entry(%{phase: :disclosure} = state) do
    broadcast_phase_change(state, :disclosure)
    broadcast_public_state(state)
    # Auto-advance to next round's news after a brief disclosure window
    Process.send_after(self(), :advance_disclosure, 10_000)
  end

  defp handle_phase_entry(%{phase: :finished}) do
    send(self(), :finish_match)
  end

  defp handle_phase_entry(%{phase: :news} = state) do
    broadcast_phase_change(state, :news)
    broadcast_public_state(state)
    # Auto-advance to action_submission after players have seen the news headline
    Process.send_after(self(), :advance_news, @news_display_ms)
  end

  defp handle_phase_entry(%{phase: :action_submission} = state) do
    broadcast_phase_change(state, :action_submission)
    broadcast_public_state(state)
  end

  defp handle_phase_entry(state), do: broadcast_public_state(state)

  # --- Broadcast helpers ---

  defp broadcast_public_state(state) do
    Endpoint.broadcast("match:#{state.match_id}", "state_updated", Serializer.public_match_view(state))
  end

  defp broadcast_phase_change(state, phase) do
    Endpoint.broadcast("match:#{state.match_id}", "phase_changed", %{
      phase: phase,
      round: state.round,
      match_id: state.match_id
    })

    LastBid.Telemetry.phase_changed(state.match_id, state.round, phase)
  end

  defp broadcast_private_events(state, private_events) do
    Enum.each(private_events, fn {user_id, events} ->
      Endpoint.broadcast("user:#{user_id}", "private_events", %{
        match_id: state.match_id,
        round: state.round,
        events: events
      })
    end)
  end

  defp broadcast_match_finished(state) do
    Endpoint.broadcast("match:#{state.match_id}", "match_finished", %{
      match_id: state.match_id,
      leaderboard: Serializer.leaderboard(state)
    })
  end

  # --- Persistence helpers ---

  defp persist_submitted_action(match_id, action_record) do
    attrs = Map.put(action_record, :match_id, match_id)

    %LastBid.MatchEngine.SubmittedAction{}
    |> LastBid.MatchEngine.SubmittedAction.submit_changeset(attrs)
    |> Repo.insert()
    |> case do
      {:ok, _} -> :ok
      {:error, cs} -> Logger.warning("Failed to persist action: #{inspect(cs)}")
    end
  end

  defp persist_round_snapshot(state, _result) do
    public_state = Serializer.public_match_view(state)
    full_state = %{players: state.players, companies: state.companies}

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
      {:error, cs} -> Logger.warning("Failed to persist snapshot: #{inspect(cs)}")
    end
  end

  # --- State loading ---

  defp load_match_state(match_id) do
    match = Lobby.get_match_with_players!(match_id)
    companies = Market.list_companies(match_id)
    match_players_with_users = Enum.map(match.match_players, &{&1, &1.user})

    state = MatchState.build(match, match_players_with_users, companies)
    {:ok, state}
  rescue
    e ->
      {:error, e}
  end

  # --- Registry via tuple ---

  defp via(match_id) do
    {:via, Registry, {@registry, match_id}}
  end
end
