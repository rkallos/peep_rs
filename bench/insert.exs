# Insert cost for Peep.Storage.RustNIF.
#
# Two events, each a Benchee input:
#
#   mixed - a counter, a sum, a last_value and a distribution sharing one
#           three-key tags map, picked at random from `--series` tags maps
#   wide  - 13 distributions sharing the empty tags map, like a request's
#           per-stage timings
#
# One iteration inserts 1,000 events built beforehand, so a single insert's
# time stays well above the timer's resolution. Divide by 1,000 for one event.
#
# `--parallel N` runs the job in N processes at once, so they contend for the
# storage; Benchee's figures are then per process. With `--scrape`, another
# process scrapes throughout, so inserts also contend with scrapes.
#
#     elixir bench/insert.exs [--series N] [--parallel N] [--scrape]
#
# To compare revisions, run each with `--save PATH --tag NAME`, then run the
# last one with `--load PATH` as well.

Mix.install([{:peep_rs, path: Path.expand("..", __DIR__)}, {:benchee, "~> 1.5"}])

alias Telemetry.Metrics

{opts, _, _} =
  OptionParser.parse(System.argv(),
    strict: [
      series: :integer,
      parallel: :integer,
      scrape: :boolean,
      save: :string,
      tag: :string,
      load: :string
    ]
  )

series = Keyword.get(opts, :series, 200)
events = 1_000
:rand.seed(:exsss, 42)

mixed_metrics = [
  Metrics.counter("bench.insert.counter"),
  Metrics.sum("bench.insert.sum"),
  Metrics.last_value("bench.insert.gauge"),
  Metrics.distribution("bench.insert.dist", reporter_options: [max_value: 1000])
]

wide_metrics =
  for stage <- 1..13 do
    Metrics.distribution("bench.insert.stage.#{stage}",
      reporter_options: [max_value: 1_000_000]
    )
  end

name = :insert_bench

{:ok, _} =
  Peep.start_link(
    name: name,
    storage: {Peep.Storage.RustNIF, []},
    metrics: mixed_metrics ++ wide_metrics
  )

{mod, storage} = Peep.Persistent.storage(name)

{mixed, wide} =
  name
  |> Peep.Persistent.fetch()
  |> Peep.Persistent.ids_to_metrics()
  |> Tuple.to_list()
  |> Enum.with_index()
  |> Enum.split(length(mixed_metrics))

tags_maps =
  List.to_tuple(for i <- 1..series, do: %{method: :get, route: "/items/#{i}", status: rem(i, 2)})

mixed_events =
  for _ <- 1..events do
    tags_map = elem(tags_maps, :rand.uniform(series) - 1)
    {{tags_map}, for({metric, id} <- mixed, do: {id, metric, :rand.uniform(1000), 0})}
  end

wide_events =
  for _ <- 1..events do
    {{%{}}, for({metric, id} <- wide, do: {id, metric, :rand.uniform(1_000_000), 0})}
  end

if opts[:scrape] do
  spawn(fn -> Stream.repeatedly(fn -> Peep.get_all_metrics(name) end) |> Stream.run() end)
end

comparison =
  Enum.flat_map(opts, fn
    {:save, path} -> [save: [path: path, tag: Keyword.get(opts, :tag, "run")]]
    {:load, path} -> [load: path]
    _ -> []
  end)

Benchee.run(
  %{
    "1,000 events" => fn events ->
      Enum.each(events, fn {tags, batch} ->
        mod.insert_metrics(mod.resolve(storage), tags, batch)
      end)
    end
  },
  [
    inputs: %{"mixed" => mixed_events, "wide" => wide_events},
    parallel: Keyword.get(opts, :parallel, 1),
    warmup: 1,
    time: 3
  ] ++ comparison
)
