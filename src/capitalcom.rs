//! Capital.com historical-bars adapter.
//!
//! Scaffold only — not yet implemented. Capital.com's session/token/streaming lifecycle for
//! trading itself is out of scope here; that stays in each consumer's own broker connector
//! (`kestrel`'s `Connector` trait). This module covers only the historical-bars side needed for
//! [`kestrel_chartkit::DataFeedAdapter`].

use kestrel_chartkit::{Bar, DataFeedAdapter, Timeframe};

/// [`DataFeedAdapter`] backed by the Capital.com REST API. Requires API key, identifier, and
/// password for session creation.
pub struct CapitalComAdapter {
    pub api_key: String,
}

impl CapitalComAdapter {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
        }
    }
}

impl DataFeedAdapter for CapitalComAdapter {
    type Error = String;

    fn fetch_historical(
        &mut self,
        _symbol: &str,
        _timeframe: Timeframe,
        _from: i64,
        _to: i64,
    ) -> Result<Vec<Bar>, Self::Error> {
        todo!("Capital.com REST client not yet implemented")
    }

    fn subscribe_live(&mut self, _symbol: &str, _timeframe: Timeframe) -> Result<(), Self::Error> {
        todo!("Capital.com live streaming not yet implemented")
    }

    fn poll_live(&mut self) -> Result<Vec<Bar>, Self::Error> {
        todo!("Capital.com live streaming not yet implemented")
    }
}
