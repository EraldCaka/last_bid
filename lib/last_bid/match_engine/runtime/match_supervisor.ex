defmodule LastBid.MatchEngine.Runtime.MatchSupervisor do
  @moduledoc """
  DynamicSupervisor that owns all active MatchServer processes.

  Each match gets one child process.
  The Registry provides lookup by match_id.
  """

  use Supervisor

  alias LastBid.MatchEngine.Runtime.MatchServer

  @registry LastBid.MatchEngine.Registry
  @dynamic_sup LastBid.MatchEngine.DynamicSupervisor

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @dynamic_sup}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  @doc "Start a MatchServer for the given match_id. Idempotent."
  def start_match(match_id) do
    case DynamicSupervisor.start_child(@dynamic_sup, {MatchServer, match_id}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Stop and remove a MatchServer for the given match_id."
  def stop_match(match_id) do
    case Registry.lookup(@registry, match_id) do
      [{pid, _}] ->
        DynamicSupervisor.terminate_child(@dynamic_sup, pid)

      [] ->
        {:error, :not_found}
    end
  end

  @doc "Returns all running match_ids."
  def running_match_ids do
    Registry.select(@registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  end
end
