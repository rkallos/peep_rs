# Insert throughput for Peep.Storage.RustNIF.
#
# Each event inserts a counter, a sum, a last_value and a distribution sharing
# one three-key tags map, cycling through `--series` tags maps. Reports:
#
#   * single: ns per event from one process
#   * parallel: million events per second from one process per scheduler
#   * shared: as parallel, but every process reuses one `resolve/1` result
#
# With `--scrape`, a separate process scrapes continuously during the parallel
# runs, so inserts contend with scrapes.
#
#     MIX_ENV=prod mix run bench/insert.exs [--events N] [--series N] [--scrape]
#
# Compare medians across interleaved runs of the revisions being compared.

alias Telemetry.Metrics

{opts, _, _} =
  OptionParser.parse(System.argv(),
    strict: [events: :integer, series: :integer, scrape: :boolean]
  )

events = Keyword.get(opts, :events, 2_000_000)
series = Keyword.get(opts, :series, 200)
scrape? = Keyword.get(opts, :scrape, false)

defmodule InsertBench do
  def run(0, _mod, _storage, _tags, _batch, _resolved), do: :ok

  def run(n, mod, storage, tags, batch, resolved) do
    tags_map = elem(tags, rem(n, tuple_size(tags)))
    :ok = mod.insert_metrics(resolved || mod.resolve(storage), tags_map, batch)
    run(n - 1, mod, storage, tags, batch, resolved)
  end
end

metrics = [
  Metrics.counter("bench.insert.counter"),
  Metrics.sum("bench.insert.sum"),
  Metrics.last_value("bench.insert.gauge"),
  Metrics.distribution("bench.insert.dist", reporter_options: [max_value: 1000])
]

tags =
  List.to_tuple(
    for i <- 1..series, do: {%{method: :get, route: "/items/#{i}", status: rem(i, 2)}}
  )

start = fn ->
  name = :"insert_bench_#{System.unique_integer([:positive])}"
  {:ok, _} = Peep.start_link(name: name, storage: {Peep.Storage.RustNIF, []}, metrics: metrics)

  persistent = Peep.Persistent.fetch(name)
  {mod, storage} = Peep.Persistent.storage(name)

  batch =
    persistent
    |> Peep.Persistent.ids_to_metrics()
    |> Tuple.to_list()
    |> Enum.with_index(fn metric, id -> {id, metric, 27, 0} end)

  {name, mod, storage, batch}
end

with_scraper = fn name, fun ->
  if scrape? do
    scraper =
      spawn_link(fn ->
        Stream.repeatedly(fn -> Peep.get_all_metrics(name) end) |> Stream.run()
      end)

    result = fun.()
    Process.unlink(scraper)
    Process.exit(scraper, :kill)
    result
  else
    fun.()
  end
end

single = fn ->
  {_name, mod, storage, batch} = start.()
  InsertBench.run(200_000, mod, storage, tags, batch, nil)
  {us, :ok} = :timer.tc(fn -> InsertBench.run(events, mod, storage, tags, batch, nil) end)
  us * 1000 / events
end

parallel = fn shared? ->
  {name, mod, storage, batch} = start.()
  resolved = if shared?, do: mod.resolve(storage)
  procs = System.schedulers_online()
  per_proc = div(events, 2)
  parent = self()

  with_scraper.(name, fn ->
    {us, _} =
      :timer.tc(fn ->
        for _ <- 1..procs do
          spawn_link(fn ->
            InsertBench.run(per_proc, mod, storage, tags, batch, resolved)
            send(parent, :done)
          end)
        end

        for _ <- 1..procs, do: receive(do: (:done -> :ok))
      end)

    procs * per_proc / us
  end)
end

IO.puts(
  :io_lib.format(
    "single ~.1f ns/event | parallel ~.2f M events/s | shared ~.2f M events/s~s",
    [single.(), parallel.(false), parallel.(true), if(scrape?, do: " (scraping)", else: "")]
  )
)
