defmodule LastBidWeb.Telemetry do
  use Supervisor
  import Telemetry.Metrics

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children = [
      # Telemetry poller for periodic VM + DB measurements
      {:telemetry_poller, measurements: periodic_measurements(), period: 10_000}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def metrics do
    [
      # Phoenix metrics
      summary("phoenix.endpoint.start.system_time", unit: {:native, :millisecond}),
      summary("phoenix.endpoint.stop.duration", unit: {:native, :millisecond}),
      summary("phoenix.router_dispatch.stop.duration",
        tags: [:route],
        unit: {:native, :millisecond}
      ),

      # Database metrics
      summary("last_bid.repo.query.total_time", unit: {:native, :millisecond}),
      summary("last_bid.repo.query.decode_time", unit: {:native, :millisecond}),
      summary("last_bid.repo.query.query_time", unit: {:native, :millisecond}),
      summary("last_bid.repo.query.queue_time", unit: {:native, :millisecond}),
      summary("last_bid.repo.query.idle_time", unit: {:native, :millisecond}),

      # VM metrics
      summary("vm.memory.total", unit: {:byte, :kilobyte}),
      summary("vm.total_run_queue_lengths.total"),
      summary("vm.total_run_queue_lengths.cpu"),
      summary("vm.total_run_queue_lengths.io"),

      # Game domain metrics
      counter("last_bid.match.started.count"),
      counter("last_bid.match.finished.count"),
      counter("last_bid.round.resolved.count"),
      counter("last_bid.action.submitted.count"),
      counter("last_bid.chat.message_sent.count")
    ]
  end

  defp periodic_measurements do
    [
      {LastBidWeb.Telemetry, :dispatch_vm_stats, []}
    ]
  end

  @doc false
  def dispatch_vm_stats do
    :telemetry.execute([:vm, :memory], Map.new(:erlang.memory()), %{})
    run_queue = :erlang.statistics(:run_queue_lengths)

    :telemetry.execute(
      [:vm, :total_run_queue_lengths],
      %{
        total: Enum.sum(run_queue),
        cpu: Enum.sum(Enum.drop(run_queue, -1)),
        io: List.last(run_queue)
      },
      %{}
    )
  end
end
