defmodule Peep.Storage.RustNIFTest.DescendingBuckets do
  @behaviour Peep.Buckets

  @impl true
  def config(_), do: %{}

  @impl true
  def boundaries(_), do: [1000, 100, 10]

  @impl true
  def bucket_for(_, _), do: 0

  @impl true
  def upper_bound(idx, _), do: Enum.at([1000.0, 100.0, 10.0], idx, :infinity)

  @impl true
  def number_of_buckets(_), do: 3
end

defmodule Peep.Storage.RustNIFTest.StringBuckets do
  @behaviour Peep.Buckets

  @impl true
  def config(_), do: %{}

  @impl true
  def boundaries(_), do: [10, 100]

  @impl true
  def bucket_for(_, _), do: 0

  @impl true
  def upper_bound(idx, _), do: Enum.at(["10", "100"], idx, :infinity)

  @impl true
  def number_of_buckets(_), do: 2
end

defmodule Peep.Storage.RustNIFTest do
  # Backend-specific behaviour: errors the NIF raises that the ETS backends have
  # no equivalent for, and the tag-value round-trips that back the claim this
  # backend stores the same terms they do.
  use ExUnit.Case, async: true

  import Bitwise

  alias Telemetry.Metrics

  @storage {Peep.Storage.RustNIF, []}

  # Tags are copied into a process-independent environment rather than decoded
  # into Rust, so the set of storable terms is whatever `enif_make_copy`
  # accepts, and these round-trip byte for byte.
  for {label, tags} <- [
        {"list", %{tag: ["a", "b"]}},
        {"tuple", %{tag: {1, 2}}},
        {"map", %{tag: %{a: 1}}},
        {"nested", %{tag: [%{a: {1, [2]}}, :b]}},
        {"pid", %{tag: :self}},
        {"reference", %{tag: :make_ref}},
        {"bignum", %{tag: 1 <<< 70}},
        {"bitstring", %{tag: <<1::size(3)>>}},
        {"charlist", %{tag: ~c"abc"}},
        {"empty", %{}}
      ] do
    test "a #{label} tag value round-trips" do
      counter = Metrics.counter("rustler.test.counter")
      tags = resolve_tags(unquote(Macro.escape(tags)))

      check(
        metrics: [counter],
        ops: [insert: {counter, 1, tags}, insert: {counter, 1, tags}],
        exp_metrics: %{counter => %{tags => 2}}
      )
    end
  end

  test "a non-atom tag key round-trips" do
    counter = Metrics.counter("rustler.test.counter")
    tags = %{"str" => 1, 2 => :two}

    check(
      metrics: [counter],
      ops: [insert: {counter, 1, tags}],
      exp_metrics: %{counter => %{tags => 1}}
    )
  end

  test "terms that are == but not =:= stay distinct" do
    counter = Metrics.counter("rustler.test.counter")
    tags = [%{tag: [1]}, %{tag: [1.0]}, %{tag: {1}}, %{tag: {1.0}}]

    check(
      metrics: [counter],
      ops: for(t <- tags, do: {:insert, {counter, 1, t}}),
      exp_metrics: %{counter => Map.new(tags, &{&1, 1})}
    )
  end

  test "an integer tag value does not merge into an equal float one" do
    # 16 is the smallest N whose two tags maps share their top 7 hash bits, so
    # hashbrown's control byte does not tell them apart and enif_is_identical
    # has to.
    counter = Metrics.counter("rustler.test.counter")

    check(
      metrics: [counter],
      ops: [insert: {counter, 1, %{code: 16.0}}, insert: {counter, 1, %{code: 16}}],
      exp_metrics: %{counter => %{%{code: 16.0} => 1, %{code: 16} => 1}}
    )
  end

  test "maps built in different orders are one series" do
    counter = Metrics.counter("rustler.test.counter")
    wide = Map.new(1..40, fn n -> {n, n} end)
    reversed = Map.new(Enum.reverse(Map.to_list(wide)))

    check(
      metrics: [counter],
      ops: [insert: {counter, 1, wide}, insert: {counter, 1, reversed}],
      exp_metrics: %{counter => %{wide => 2}}
    )
  end

  test "prune_tags matches an exotic tag value" do
    counter = Metrics.counter("rustler.test.counter")

    check(
      metrics: [counter],
      ops: [
        insert: {counter, 1, %{tag: ["a", "b"]}},
        insert: {counter, 1, %{tag: ["c"]}},
        prune: [%{tag: ["a", "b"]}]
      ],
      exp_metrics: %{counter => %{%{tag: ["c"]} => 1}}
    )
  end

  test "prune_tags rejects a non-map pattern" do
    counter = Metrics.counter("rustler.test.counter")

    check(metrics: [counter], ops: [prune: [:not_a_map]], exp_errors: [:bad_tags_map])
  end

  test "pruning keeps every metric's surviving series" do
    counter = Metrics.counter("rustler.test.counter")
    sum = Metrics.sum("rustler.test.sum")

    check(
      metrics: [counter, sum],
      ops: [
        insert: {counter, 1, %{route: "a"}},
        insert: {sum, 3, %{route: "a"}},
        insert: {counter, 1, %{route: "b"}},
        insert: {sum, 3, %{route: "b"}},
        prune: [%{route: "a"}]
      ],
      exp_metrics: %{counter => %{%{route: "b"} => 1}, sum => %{%{route: "b"} => 3}}
    )
  end

  test "pruning every tags map leaves storage as it was at registration" do
    counter = Metrics.counter("rustler.test.counter")
    sum = Metrics.sum("rustler.test.sum")

    check(
      metrics: [counter, sum],
      ops: [
        insert: {counter, 1, %{route: "a"}},
        insert: {sum, 3, %{route: "b"}},
        prune: [%{route: "a"}, %{route: "b"}]
      ]
    )
  end

  test "a pruned tags map can be inserted again" do
    counter = Metrics.counter("rustler.test.counter")

    check(
      metrics: [counter],
      ops: [
        insert: {counter, 1, %{route: "a"}},
        prune: [%{route: "a"}],
        insert: {counter, 1, %{route: "a"}}
      ],
      exp_metrics: %{counter => %{%{route: "a"} => 1}}
    )
  end

  test "a float sum value raises bad_measurement and stores nothing" do
    sum = Metrics.sum("rustler.test.sum")

    check(
      metrics: [sum],
      ops: [insert: {sum, 1.5, %{new: :tags}}],
      exp_errors: [:bad_measurement]
    )
  end

  test "a non-numeric last_value raises bad_measurement and stores nothing" do
    gauge = Metrics.last_value("rustler.test.gauge")

    check(
      metrics: [gauge],
      ops: [insert: {gauge, :nope, %{new: :tags}}],
      exp_errors: [:bad_measurement]
    )
  end

  test "an integer last_value stays an integer" do
    gauge = Metrics.last_value("rustler.test.gauge")

    check(
      metrics: [gauge],
      ops: [insert: {gauge, 10, %{}}],
      exp_metrics: %{gauge => %{%{} => 10}}
    )
  end

  test "an integer distribution value adds to the sum exactly" do
    dist = powers_of_ten("rustler.test.dist")
    value = (1 <<< 53) + 1

    check(
      metrics: [dist],
      ops: [insert: {dist, value, %{}}],
      exp_metrics: %{dist => %{%{} => buckets(dist, %{infinity: 1}, value)}}
    )
  end

  test "a calculator whose upper bounds aren't floats fails registration" do
    Process.flag(:trap_exit, true)

    dist =
      Metrics.distribution("rustler.test.dist",
        reporter_options: [peep_bucket_calculator: __MODULE__.StringBuckets]
      )

    assert {:error, {{:peep_storage_error, :bad_argument, _}, _}} =
             Peep.start_link(name: Peep.Test.fresh_id(), storage: @storage, metrics: [dist])
  end

  for value <- [1.0e19, -1.0e19, 1 <<< 64] do
    test "a distribution value of #{value} raises bad_measurement and stores nothing" do
      dist = powers_of_ten("rustler.test.dist")

      check(
        metrics: [dist],
        ops: [insert: {dist, unquote(value), %{new: :tags}}],
        exp_errors: [:bad_measurement]
      )
    end
  end

  test "a calculator's descending boundaries still bucket samples by bound" do
    dist =
      Metrics.distribution("rustler.test.dist",
        reporter_options: [peep_bucket_calculator: __MODULE__.DescendingBuckets]
      )

    check(
      metrics: [dist],
      ops: for(value <- [5, 50, 500, 5000], do: {:insert, {dist, value, %{}}}),
      exp_metrics: %{
        dist => %{%{} => %{10.0 => 1, 100.0 => 1, 1000.0 => 1, :infinity => 1, :sum => 5555}}
      }
    )
  end

  test "series inserted on every scheduler merge into one per tags map" do
    counter = Metrics.counter("rustler.test.counter")
    sum = Metrics.sum("rustler.test.sum")
    name = Peep.Test.start_peep!(storage: @storage, metrics: [counter, sum])
    procs = System.schedulers_online() * 4

    1..procs
    |> Enum.map(fn _ ->
      Task.async(fn ->
        for route <- 1..10, metric <- [counter, sum], do: insert(name, metric, 2, %{route: route})
      end)
    end)
    |> Task.await_many()

    expected = fn per_insert -> Map.new(1..10, &{%{route: &1}, procs * per_insert}) end

    assert Peep.get_all_metrics(name) == %{counter => expected.(1), sum => expected.(2)}
  end

  # A stress test, nondeterministic by design: it runs inserts, prunes and
  # scrapes against each other for a fixed time, with random tags, and checks
  # that none of them fails and that pruning everything leaves nothing behind.
  # A prune that interleaves with an insert between interning a tags map and
  # storing its samples leaves an orphaned or duplicate series, which this
  # catches.
  test "stress: inserts, prunes and scrapes can run concurrently" do
    counter = Metrics.counter("rustler.test.counter")
    sum = Metrics.sum("rustler.test.sum")
    name = Peep.Test.start_peep!(storage: @storage, metrics: [counter, sum])
    empty = Peep.storage_size(name)
    deadline = System.monotonic_time(:millisecond) + 300

    until_deadline = fn fun ->
      fn ->
        Stream.repeatedly(fun)
        |> Stream.take_while(fn _ -> System.monotonic_time(:millisecond) < deadline end)
        |> Stream.run()
      end
    end

    inserters =
      for _ <- 1..System.schedulers_online() do
        until_deadline.(fn ->
          tags = %{route: Enum.random(1..20)}
          for metric <- [counter, sum], do: :ok = insert(name, metric, 1, tags)
        end)
      end

    pruner = until_deadline.(fn -> Peep.prune_tags(name, [%{route: Enum.random(1..20)}]) end)
    scraper = until_deadline.(fn -> Peep.get_all_metrics(name) end)

    [pruner, scraper | inserters]
    |> Enum.map(&Task.async/1)
    |> Task.await_many(10_000)

    Peep.prune_tags(name, for(route <- 1..20, do: %{route: route}))

    assert Peep.get_all_metrics(name) == %{}
    assert Peep.storage_size(name) == empty
  end

  # Starts a Peep over `metrics`, runs `ops` in order, and checks the reasons of
  # the errors they raised and the whole scrape. Storage holding no series must
  # be the size it was at registration, so a rejected sample stored nothing,
  # not even its tags.
  #
  # Ops are `insert: {metric, value, tags}` and `prune: patterns`.
  defp check(opts) do
    name = Peep.Test.start_peep!(storage: @storage, metrics: Keyword.fetch!(opts, :metrics))
    empty = Peep.storage_size(name)
    exp_metrics = Keyword.get(opts, :exp_metrics, %{})

    errors = for op <- Keyword.get(opts, :ops, []), reason = run(name, op), do: reason

    assert errors == Keyword.get(opts, :exp_errors, [])
    assert Peep.get_all_metrics(name) == exp_metrics

    if exp_metrics == %{} do
      assert Peep.storage_size(name) == empty
    end
  end

  # The reason of the storage error the op raised, or nil.
  defp run(name, op) do
    case op do
      {:insert, {metric, value, tags}} -> insert(name, metric, value, tags)
      {:prune, patterns} -> Peep.prune_tags(name, patterns)
    end

    nil
  rescue
    error in ErlangError ->
      {:peep_storage_error, reason, _detail} = error.original
      reason
  end

  # Calls the backend directly, unlike `Peep.Test.insert_metric/4`, so that a
  # test can hand it values Peep's own guards drop.
  defp insert(name, metric, value, tags) do
    {mod, storage} = Peep.Persistent.storage(name)
    ids = name |> Peep.Persistent.fetch() |> Peep.Persistent.ids_to_metrics() |> Tuple.to_list()
    id = Enum.find_index(ids, &(&1 == metric))

    mod.insert_metrics(mod.resolve(storage), {tags}, [{id, metric, value, 0}])
  end

  defp powers_of_ten(name) do
    Metrics.distribution(name,
      reporter_options: [peep_bucket_calculator: Peep.Buckets.PowersOfTen]
    )
  end

  # A scraped distribution series: every bucket of `dist`, keyed by its
  # upper bound, with `counts` replacing zeros.
  defp buckets(dist, counts, sum) do
    {mod, config} = Peep.Buckets.config(dist)

    mod.boundaries(config)
    |> Enum.with_index(fn _bound, idx -> {mod.upper_bound(idx, config), 0} end)
    |> Map.new()
    |> Map.merge(%{infinity: 0, sum: sum})
    |> Map.merge(counts)
  end

  # `Macro.escape/1` cannot carry a live pid or reference into the generated test.
  defp resolve_tags(%{tag: :self}), do: %{tag: self()}
  defp resolve_tags(%{tag: :make_ref}), do: %{tag: make_ref()}
  defp resolve_tags(tags), do: tags
end
