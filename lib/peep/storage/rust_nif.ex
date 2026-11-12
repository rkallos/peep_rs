defmodule Peep.Storage.RustNIF do
  version = Mix.Project.config()[:version]

  # Downloads the NIF built for this platform by .github/workflows/release.yml.
  # PEEP_RS_BUILD=1 builds it from source instead, which needs Cargo.
  use RustlerPrecompiled,
    otp_app: :peep_rs,
    crate: "peep_storage_rustler",
    version: version,
    base_url: "https://github.com/rkallos/peep_rs/releases/download/v#{version}",
    force_build: System.get_env("PEEP_RS_BUILD") in ["1", "true"],
    # enif_term_size requires OTP 29 / NIF 2.18.
    nif_versions: ["2.18"],
    targets: ~w(
      aarch64-apple-darwin
      aarch64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      x86_64-apple-darwin
      x86_64-unknown-linux-gnu
      x86_64-unknown-linux-musl
    )

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
