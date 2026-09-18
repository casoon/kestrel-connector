# kestrel-connector

Provider-specific [`DataFeedAdapter`](https://github.com/casoon/kestrel-chartkit) implementations
for [`kestrel-chartkit`](https://github.com/casoon/kestrel-chartkit) — one module per market-data
provider, each behind its own Cargo feature so a consumer only pulls in what it needs.

`kestrel-chartkit` itself stays vendor-neutral by design (no broker/exchange-specific code, no
HTTP client). This crate is where that provider-specific glue lives instead, without pulling it
into the core library.

## Modules

| Feature | Module | Status |
|---|---|---|
| `eodhd` | `eodhd` | implemented (EOD, intraday, delayed real-time poll) |
| `capitalcom` | `capitalcom` | implemented (session login, historical prices w/ pagination, latest-bar poll) |
| `massive` | `massive` | implemented (exchange volume from futures, front-month roll by volume; needs a futures entitlement) |
| `ib` | `ib` | scaffold only, not implemented (deferred — needs a running TWS/Gateway process, see plan/status.md) |

## Usage

```toml
[dependencies]
kestrel-connector = { git = "https://github.com/casoon/kestrel-connector.git", tag = "v0.5.0", features = ["eodhd"] }
```

Pin the tag. Without one the dependency follows the default branch, and this crate re-exports
`kestrel-chartkit` types — a moving `Bar` on one side of a build is how two incompatible versions
of the same type end up in one dependency graph. Every consumer in the family must resolve to the
same `kestrel-chartkit` version this crate pins (`v0.15.0`).

## Scope

This crate contains no local archive, no cached market data, and no API keys. Instrument
selection, symbol mapping, and archival storage live in the (private) `kestrel-marketdata` crate
that consumes this one — see that project, or `kestrel-chartkit`'s
`plan/market-data-connector-und-anbieterstufen.md`, for the overall architecture.

Credentials for exercising these adapters against a real provider belong in a separate, private
test project (`.env`, gitignored), never in this repo.
