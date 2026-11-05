defmodule Peep.Storage.RustNIF do
  use Rustler, otp_app: :peep_rs, crate: "peep_storage_rustler"

  @behaviour Peep.Storage

  @impl true
  def new(_), do: :erlang.nif_error(:nif_not_loaded)

  @impl true
  def register_metrics(_, _), do: :erlang.nif_error(:nif_not_loaded)

  @impl true
  def storage_size(_), do: :erlang.nif_error(:nif_not_loaded)

  # Amortize the NIF call overhead by batching inserts
  @impl true
  def insert_metrics(_resolved, _tag_results, _batch), do: :erlang.nif_error(:nif_not_loaded)

  @impl true
  def get_all_metrics(storage, persistent) do
    nif_get_all_metrics(storage, Peep.Persistent.ids_to_metrics(persistent))
  end

  def nif_get_all_metrics(_storage, _ids_to_metrics), do: :erlang.nif_error(:nif_not_loaded)

  @impl true
  def prune_tags(_, _), do: :erlang.nif_error(:nif_not_loaded)

  # `insert_metrics/3` picks its shard from the calling scheduler thread.
  @impl true
  def resolve(storage), do: storage
end
