//! Provider-specific [`kestrel_chartkit::DataFeedAdapter`] implementations, one module per
//! provider, each behind its own feature flag so a consumer only pulls in what it needs.
//!
//! `kestrel-chartkit` itself stays vendor-neutral; this crate is where the provider-specific
//! HTTP/auth/session details live instead. See the crate README for scope and status.

#[cfg(any(feature = "eodhd", feature = "capitalcom"))]
mod civil_date;

#[cfg(feature = "eodhd")]
pub mod eodhd;

#[cfg(feature = "capitalcom")]
pub mod capitalcom;

#[cfg(feature = "ib")]
pub mod ib;
