# Scrape cost for Peep.Storage.RustNIF.
#
# A counter, a sum, a last_value and two distributions share `--series` tags
# maps, each inserted from every scheduler. One distribution uses
# Peep.Buckets.PowersOfTen (11 keys per bucket map), the other the default
# exponential buckets up to 1000 (38 keys). Erlang builds maps of up to 32 keys
# differently from larger ones.
#
#     mix run bench/scrape.exs [--series N]
#
# To compare revisions, run each with `--save PATH --tag NAME`, then run the
# last one with `--load PATH` as well.

alias Telemetry.Metrics

{opts, _, _} =
  OptionParser.parse(System.argv(),
    strict: [series: :integer, save: :string, tag: :string, load: :string]
  )

series = Keyword.get(opts, :series, 5000)

metrics = [
  Metrics.counter("bench.scrape.counter", tags: [:route]),
  Metrics.sum("bench.scrape.sum", tags: [:route]),
  Metrics.last_value("bench.scrape.gauge", tags: [:route]),
  Metrics.distribution("bench.scrape.small",
    tags: [:route],
    reporter_options: [peep_bucket_calculator: Peep.Buckets.PowersOfTen]
  ),
  Metrics.distribution("bench.scrape.large", tags: [:route], reporter_options: [max_value: 1000])
]

name = :scrape_bench
{:ok, _} = Peep.start_link(name: name, storage: {Peep.Storage.RustNIF, []}, metrics: metrics)
{mod, storage} = Peep.Persistent.storage(name)

batch =
  name
  |> Peep.Persistent.fetch()
  |> Peep.Persistent.ids_to_metrics()
  |> Tuple.to_list()
  |> Enum.with_index(fn metric, id -> {id, metric, 27, 0} end)

# One process bound to each scheduler inserts every series, so each scheduler
# thread's shard holds them all.
# The `:scheduler` process flag is undocumented but stable.
1..System.schedulers_online()
|> Enum.map(fn scheduler ->
  Task.async(fn ->
    :erlang.process_flag(:scheduler, scheduler)

    for i <- 1..series do
      :ok = mod.insert_metrics(mod.resolve(storage), {%{route: "/items/#{i}"}}, batch)
    end
  end)
end)
|> Task.await_many(:infinity)

comparison =
  Enum.flat_map(opts, fn
    {:save, path} -> [save: [path: path, tag: Keyword.get(opts, :tag, "run")]]
    {:load, path} -> [load: path]
    _ -> []
  end)

Benchee.run(
  %{"get_all_metrics/1" => fn -> Peep.get_all_metrics(name) end},
  [warmup: 2, time: 10] ++ comparison
)
