# Storage-backend benchmark shaped after a real high-volume `:telemetry` user (an
# RTB bidder), because the synthetic "N counters over 2 tags maps" shape hides
# most of what decides backend cost:
#
#   - the hottest event is a *wide, untagged* batch: 13 distributions on one
#     event, all sharing the empty tags map, emitted once per bid request
#   - tag values are atoms and small integers. Not binaries: the codebase this is
#     modelled on has none in tags, and a binary is the one shape where a decoded
#     backend pays a memcmp instead of a word compare
#   - one metric carries four tags at high cardinality (`line_item_id` is a
#     pos_integer), so the series count is large and the insert slow path gets
#     exercised, not just the warm lookup
#   - the rest are small events with one or two integer/atom tags
#
# Two families of scenario, answering different questions:
#
#   storage  - the batches `Peep.EventHandler.handle_event_batched/4` would build,
#              fed straight to the backend. This is where a storage change shows
#              up undiluted.
#   handler  - the same workload through `:telemetry.execute/3`, so the storage
#              number can be read against what a bid request actually costs.
#
# Every reporter is started in `before_scenario` and stopped in `after_scenario`.
# That is not tidiness: every live reporter is attached to these events, so two
# running at once would make each `handler` scenario pay for the other backend.
#
#     elixir bench/bidder_shaped_bench.exs [--parallel N]
#
# `--parallel N` runs N concurrent processes per scenario. To compare
# revisions, run each with `--save PATH --tag NAME`, then run the last one with
# `--load PATH` as well.

Mix.install([{:peep_rs, path: Path.expand("..", __DIR__)}, {:benchee, "~> 1.5"}])

{opts, _, _} =
  OptionParser.parse(System.argv(),
    strict: [parallel: :integer, save: :string, tag: :string, load: :string]
  )

defmodule Workload do
  import Telemetry.Metrics

  @stages ~w(ready parse extract geo augment filter lenses db optout pacing bid encode log)a
  @exchanges [2, 3, 5, 8, 13]
  @schemas ~w(line_item creative deal audiences)a
  @filters ~w(geo daypart domain device os browser)a
  @tracker_types ~w(click impression win)a
  @optout_types ~w(ccpa gdpr lgpd)a

  def metrics do
    stage_dists =
      for stage <- @stages do
        distribution("bidder.stages.completed.#{stage}",
          event_name: [:bidder, :stages, :completed],
          measurement: stage,
          unit: {:native, :microsecond},
          reporter_options: [max_value: 1_000_000]
        )
      end

    bid_req_counters =
      for key <- ~w(app site user_matched)a do
        counter("exchange.bid_req.#{key}.count",
          event_name: [:exchange, :bid_req, key],
          tags: [:exchange_id]
        )
      end

    stage_dists ++
      bid_req_counters ++
      [
        distribution("exchange.bid_req.duration",
          tags: [:exchange_id],
          reporter_options: [max_value: 1_000_000]
        ),
        counter("filters.tracing.drops.count",
          tags: [:schema, :filter, :line_item_id, :extra_id]
        ),
        counter("filters.tracing.pass.count", tags: [:schema, :line_item_id, :extra_id]),
        counter("optouts.count", tags: [:exchange_id, :type]),
        distribution("bidder.tracker.url", unit: :byte, tags: [:type])
      ]
  end

  # One key per stage, the way `Bidder.Stages` accumulates durations and emits
  # them all in a single execute.
  def durations, do: Map.new(@stages, fn stage -> {stage, 40_000} end)

  # The nine events one bid request produces, as {event_name, measurements}.
  def events do
    [
      {[:bidder, :stages, :completed], durations()},
      {[:exchange, :bid_req], %{duration: 812}},
      {[:exchange, :bid_req, :app], %{}},
      {[:exchange, :bid_req, :site], %{}},
      {[:exchange, :bid_req, :user_matched], %{}},
      {[:filters, :tracing, :pass], %{}},
      {[:filters, :tracing, :drops], %{}},
      {[:optouts], %{count: 1}},
      {[:bidder, :tracker], %{url: 212}}
    ]
  end

  # Metadata pools, built once per scenario. `cardinality` sets how many distinct
  # `line_item_id`s the two `filters.tracing` metrics cycle through, which is what
  # drives the total series count. Nothing is generated inside a measured call.
  def metadata_pools(cardinality) do
    pick = fn list, i -> Enum.at(list, rem(i, length(list))) end

    %{
      [:bidder, :stages, :completed] => {%{}},
      [:exchange, :bid_req] => pool(5, &%{exchange_id: pick.(@exchanges, &1)}),
      [:exchange, :bid_req, :app] => pool(5, &%{exchange_id: pick.(@exchanges, &1)}),
      [:exchange, :bid_req, :site] => pool(5, &%{exchange_id: pick.(@exchanges, &1)}),
      [:exchange, :bid_req, :user_matched] => pool(5, &%{exchange_id: pick.(@exchanges, &1)}),
      [:filters, :tracing, :pass] =>
        pool(cardinality, fn i ->
          %{schema: pick.(@schemas, i), line_item_id: i + 1, extra_id: rem(i, 7)}
        end),
      [:filters, :tracing, :drops] =>
        pool(cardinality, fn i ->
          %{
            schema: pick.(@schemas, i),
            filter: pick.(@filters, i),
            line_item_id: i + 1,
            extra_id: rem(i, 7)
          }
        end),
      [:optouts] =>
        pool(15, fn i -> %{exchange_id: pick.(@exchanges, i), type: pick.(@optout_types, i)} end),
      [:bidder, :tracker] => pool(3, fn i -> %{type: pick.(@tracker_types, i)} end)
    }
  end

  defp pool(n, fun), do: 0..(n - 1) |> Enum.map(fun) |> List.to_tuple()
end

# Replicating what the handler hands the backend, so the storage scenarios carry
# no telemetry dispatch or tag computation.
defmodule Batches do
  require Peep.Persistent

  def build(name) do
    Peep.Persistent.persistent(events_to_metrics: e2m) = Peep.Persistent.fetch(name)

    for {event, measurements} <- Workload.events() do
      metrics = Map.fetch!(e2m, event)

      # Every event in this workload has one distinct tags map across its
      # metrics, so `tag_idx` is 0 throughout - exactly what `Config.new/3`
      # assigns when metrics share a tags/tag_values configuration.
      batch = for {metric, id} <- metrics, do: {id, metric, value(metric, measurements), 0}
      {event, batch}
    end
  end

  # Mirrors `Peep.EventHandler.fetch_measurement/3`. `unit: {:native, :microsecond}`
  # compiles `measurement` into a conversion function rather than leaving it a key,
  # which is what the workload being modelled actually configures.
  defp value(%Telemetry.Metrics.Counter{}, _measurements), do: 1
  defp value(%{measurement: fun}, measurements) when is_function(fun, 1), do: fun.(measurements)

  defp value(%{measurement: fun}, measurements) when is_function(fun, 2),
    do: fun.(measurements, %{})

  defp value(%{measurement: key}, measurements), do: Map.fetch!(measurements, key)
end

defmodule Drive do
  # `resolve/1` is called once per event, inside the measured call, exactly where
  # the handler calls it. Hoisting it out would pin every write to one shard and
  # manufacture locality the real handler never has.

  # One `insert_metrics/3` per event, the shape `handle_event_batched/4` uses.
  def batched([], _mod, _state, _pools, _i), do: :ok

  def batched([{event, batch} | rest], mod, state, pools, i) do
    pool = Map.fetch!(pools, event)
    mod.insert_metrics(mod.resolve(state), {elem(pool, rem(i, tuple_size(pool)))}, batch)
    batched(rest, mod, state, pools, i)
  end

  # One `insert_metric/5` per metric, the shape `handle_event/4` uses.
  def per_metric([], _mod, _state, _pools, _i), do: :ok

  def per_metric([{event, batch} | rest], mod, state, pools, i) do
    pool = Map.fetch!(pools, event)
    write(batch, mod, mod.resolve(state), elem(pool, rem(i, tuple_size(pool))))
    per_metric(rest, mod, state, pools, i)
  end

  defp write([], _mod, _resolved, _tags), do: :ok

  defp write([{id, metric, value, _idx} | rest], mod, resolved, tags) do
    mod.insert_metric(resolved, id, metric, value, tags)
    write(rest, mod, resolved, tags)
  end

  def handler([], _pools, _i), do: :ok

  def handler([{event, measurements} | rest], pools, i) do
    pool = Map.fetch!(pools, event)
    :telemetry.execute(event, measurements, elem(pool, rem(i, tuple_size(pool))))
    handler(rest, pools, i)
  end
end

defmodule Scenario do
  # Started here rather than up front so that exactly one reporter is attached to
  # these events while its own scenario runs.
  def setup(storage, cardinality) do
    name = :"bench_#{:erlang.unique_integer([:positive])}"
    {:ok, _} = Peep.start_link(name: name, metrics: Workload.metrics(), storage: storage)
    {mod, state} = Peep.Persistent.storage(name)

    # Resolved here, not per iteration: which shape a backend takes is a
    # property of the scenario, and it is the same test `Peep.EventHandler`
    # makes at attach time.
    drive =
      if function_exported?(mod, :insert_metrics, 3) do
        &Drive.batched/5
      else
        &Drive.per_metric/5
      end

    ctx = %{
      name: name,
      mod: mod,
      state: state,
      drive: drive,
      pools: Workload.metadata_pools(cardinality),
      events: Workload.events(),
      batches: Batches.build(name),
      counter: :atomics.new(1, signed: false)
    }

    # Create every series up front, so the measured calls take the warm path.
    for i <- 0..(cardinality - 1), do: Drive.handler(ctx.events, ctx.pools, i)

    ctx
  end

  def teardown(%{name: name}), do: :ok = GenServer.stop(name)

  def index(%{counter: counter}), do: :atomics.add_get(counter, 1, 1)
end

backends = [{"striped", :striped}, {"rustler", {Peep.Storage.RustNIF, []}}]

storage_scenarios =
  for {label, storage} <- backends, into: %{} do
    {"storage #{label}",
     {
       fn ctx -> ctx.drive.(ctx.batches, ctx.mod, ctx.state, ctx.pools, Scenario.index(ctx)) end,
       before_scenario: fn cardinality -> Scenario.setup(storage, cardinality) end,
       after_scenario: &Scenario.teardown/1
     }}
  end

handler_scenarios =
  for {label, storage} <- backends, into: %{} do
    {"handler #{label}",
     {
       fn ctx -> Drive.handler(ctx.events, ctx.pools, Scenario.index(ctx)) end,
       before_scenario: fn cardinality -> Scenario.setup(storage, cardinality) end,
       after_scenario: &Scenario.teardown/1
     }}
  end

parallel = Keyword.get(opts, :parallel, 1)

comparison =
  Enum.flat_map(opts, fn
    {:save, path} -> [save: [path: path, tag: Keyword.get(opts, :tag, "run")]]
    {:load, path} -> [load: path]
    _ -> []
  end)

IO.puts("""
bidder-shaped workload: 20 metrics, 9 events per bid request, parallel: #{parallel}
  [:bidder, :stages, :completed] -> 13 untagged distributions sharing one %{}
  tag values are atoms and integers; the input is the line_item_id cardinality,
  which sets the series count (~2 per id, plus 51 fixed)
one iteration = one bid request
""")

Benchee.run(
  Map.merge(storage_scenarios, handler_scenarios),
  [
    inputs: %{
      "1 series" => 1,
      "2k series" => 1_000,
      "100k series" => 50_000
    },
    warmup: 2,
    time: 6,
    parallel: parallel,
    # Reductions would undercount: a NIF bumps the reduction counter by a fixed
    # amount regardless of how much work it does, so the two backends are not
    # comparable on that axis.
    print: [fast_warning: false]
  ] ++ comparison
)
