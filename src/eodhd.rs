//! EODHD (<https://eodhd.com>) historical/intraday data adapter.
//!
//! Scaffold only — not yet implemented. Intended tier: "EOD+Intraday — All World Extended"
//! (see `kestrel-chartkit`'s `plan/market-data-connector-und-anbieterstufen.md`).

use kestrel_chartkit::{Bar, DataFeedAdapter, Timeframe};

/// [`DataFeedAdapter`] backed by the EODHD REST API. Requires an API key.
pub struct EodhdAdapter {
    pub api_key: String,
}

impl EodhdAdapter {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
        }
    }
}

impl DataFeedAdapter for EodhdAdapter {
    type Error = String;

    fn fetch_historical(
        &mut self,
        _symbol: &str,
        _timeframe: Timeframe,
        _from: i64,
        _to: i64,
    ) -> Result<Vec<Bar>, Self::Error> {
        todo!("EODHD REST client not yet implemented")
    }

    fn subscribe_live(&mut self, _symbol: &str, _timeframe: Timeframe) -> Result<(), Self::Error> {
        todo!("EODHD has no push feed; poll_live should hit the 15min-delayed live endpoint")
    }

    fn poll_live(&mut self) -> Result<Vec<Bar>, Self::Error> {
        todo!("EODHD REST client not yet implemented")
    }
}
