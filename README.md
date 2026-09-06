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
| `eodhd` | `eodhd` | scaffold only, not implemented |
| `capitalcom` | `capitalcom` | scaffold only, not implemented |
| `ib` | `ib` | scaffold only, not implemented |

## Usage

```toml
[dependencies]
kestrel-connector = { git = "https://github.com/casoon/kestrel-connector.git", features = ["eodhd"] }
```

## Scope

This crate contains no local archive, no cached market data, and no API keys. Instrument
selection, symbol mapping, and archival storage live in the (private) `kestrel-marketdata` crate
that consumes this one — see that project, or `kestrel-chartkit`'s
`plan/market-data-connector-und-anbieterstufen.md`, for the overall architecture.

Credentials for exercising these adapters against a real provider belong in a separate, private
test project (`.env`, gitignored), never in this repo.
