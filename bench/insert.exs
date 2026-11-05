# Insert cost for Peep.Storage.RustNIF.
#
# Each event inserts a counter, a sum, a last_value and a distribution sharing
# one three-key tags map, picked at random from `--series` tags maps.
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

metrics = [
  Metrics.counter("bench.insert.counter"),
  Metrics.sum("bench.insert.sum"),
  Metrics.last_value("bench.insert.gauge"),
  Metrics.distribution("bench.insert.dist", reporter_options: [max_value: 1000])
]

name = :insert_bench
{:ok, _} = Peep.start_link(name: name, storage: {Peep.Storage.RustNIF, []}, metrics: metrics)
{mod, storage} = Peep.Persistent.storage(name)

batch =
  name
  |> Peep.Persistent.fetch()
  |> Peep.Persistent.ids_to_metrics()
  |> Tuple.to_list()
  |> Enum.with_index(fn metric, id -> {id, metric, 27, 0} end)

tags =
  List.to_tuple(
    for i <- 1..series, do: {%{method: :get, route: "/items/#{i}", status: rem(i, 2)}}
  )

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
    "insert_metrics/3" => fn tags_map ->
      mod.insert_metrics(mod.resolve(storage), tags_map, batch)
    end
  },
  [
    before_each: fn _ -> elem(tags, :rand.uniform(series) - 1) end,
    parallel: Keyword.get(opts, :parallel, 1),
    warmup: 2,
    time: 5
  ] ++ comparison
)
