defmodule Peep.Storage.RustNIF.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/rkallos/peep_rs"

  def project do
    [
      app: :peep_rs,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "A Peep storage backend implemented as a Rust NIF.",
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:peep, "~> 6.0"},
      {:rustler_precompiled, "~> 0.10.1"},
      # Only needed to build the NIF from source (PEEP_RS_BUILD=1).
      {:rustler, "~> 0.38", optional: true, runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["Richard Kallos"],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(
        lib
        native/peep_storage_rustler/src
        native/peep_storage_rustler/Cargo.toml
        .cargo
        Cargo.toml
        Cargo.lock
        checksum-*.exs
        .formatter.exs
        mix.exs
        README.md
        LICENSE
      )
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      source_url: @source_url,
      source_ref: "v#{@version}"
    ]
  end
end
