//! Interactive Brokers historical-bars adapter (TWS/Gateway or Client Portal API).
//!
//! Scaffold only — not yet implemented, and not currently planned for near-term use (see
//! `kestrel-chartkit`'s `plan/market-data-connector-und-anbieterstufen.md`, "Anbieter-
//! Alternativen"): relevant only if deeper pre-October-2020 European intraday history is needed,
//! or if an IBKR account exists for other reasons. Requires a running Gateway/TWS process, not a
//! plain REST call.

use kestrel_chartkit::{Bar, DataFeedAdapter, Timeframe};

/// [`DataFeedAdapter`] backed by an Interactive Brokers Gateway/TWS connection.
pub struct IbAdapter {
    pub host: String,
    pub port: u16,
}

impl IbAdapter {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }
}

impl DataFeedAdapter for IbAdapter {
    type Error = String;

    fn fetch_historical(
        &mut self,
        _symbol: &str,
        _timeframe: Timeframe,
        _from: i64,
        _to: i64,
    ) -> Result<Vec<Bar>, Self::Error> {
        todo!("IB Gateway/TWS client not yet implemented")
    }

    fn subscribe_live(&mut self, _symbol: &str, _timeframe: Timeframe) -> Result<(), Self::Error> {
        todo!("IB Gateway/TWS client not yet implemented")
    }

    fn poll_live(&mut self) -> Result<Vec<Bar>, Self::Error> {
        todo!("IB Gateway/TWS client not yet implemented")
    }
}
