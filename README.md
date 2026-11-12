# Peep.Storage.RustNIF

A [Peep](https://github.com/rkallos/peep) storage backend implemented as a Rust
NIF via [Rustler](https://github.com/rusterlium/rustler).

`Peep.Storage.RustNIF` implements the `Peep.Storage` behaviour, so it can be
passed as the `:storage` option to `Peep.start_link/1`:

```elixir
Peep.start_link(
  name: :my_peep,
  metrics: metrics,
  storage: {Peep.Storage.RustNIF, []}
)
```

## Installation

Add `peep_rs` to your dependencies:

```elixir
{:peep_rs, "~> 0.1"}
```

It requires OTP 29. Compiling it downloads a NIF built for your platform from
the project's GitHub releases, so no Rust toolchain is needed. Precompiled NIFs
are published for x86_64 and aarch64 Linux (glibc and musl) and macOS.

To build the NIF from source instead, which needs Cargo, add
`{:rustler, "~> 0.38"}` to your dependencies and set `PEEP_RS_BUILD=1`.

## Development

`mix compile` builds the Rust crate under `native/peep_storage_rustler` when
`PEEP_RS_BUILD=1` is set. It requires Cargo and OTP 29 (`enif_term_size`, NIF
2.18). The Nix flake supplies Erlang, Elixir and Rust, and sets `PEEP_RS_BUILD`:

    direnv allow   # or: nix develop

Tests include Peep's shared storage suite and
`test/peep/storage/rust_nif_test.exs`:

    mix test

## Releasing

`scripts/release.sh` tags the version in `mix.exs`, waits for
`.github/workflows/release.yml` to build a NIF for each platform, publishes the
GitHub release, writes the checksum file and publishes to Hex. Bump the version
in `mix.exs` and `native/peep_storage_rustler/Cargo.toml` first.

    scripts/release.sh --dry-run   # print the commands a release would run
    scripts/release.sh
