//! EODHD (<https://eodhd.com>) historical/intraday data adapter.
//!
//! Implements [`DataFeedAdapter`] against three EODHD REST endpoints:
//! - `/api/eod/{symbol}` for `Timeframe::Day(1)`/`Week(1)`/`Month(1)` (split-/dividend-adjusted).
//! - `/api/intraday/{symbol}` for `Timeframe::Minute(1)`/`Minute(5)`/`Hour(1)` (not adjusted).
//! - `/api/real-time/{symbol}` (15-20min-delayed) for `poll_live` — EODHD has no push feed.
//!
//! Intended tier: "EOD+Intraday — All World Extended" (see `kestrel-chartkit`'s
//! `plan/market-data-connector-und-anbieterstufen.md`).

use std::collections::HashMap;

use kestrel_chartkit::{Bar, DataFeedAdapter, Timeframe};
use serde::Deserialize;

use crate::civil_date::{civil_from_days, days_from_civil};

const DEFAULT_BASE_URL: &str = "https://eodhd.com";

/// [`DataFeedAdapter`] backed by the EODHD REST API. Requires an API key.
pub struct EodhdAdapter {
    api_key: String,
    base_url: String,
    subscriptions: Vec<String>,
    live_cursor: HashMap<String, i64>,
}

impl EodhdAdapter {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_base_url(api_key, DEFAULT_BASE_URL)
    }

    /// Points at a custom base URL instead of the real EODHD host. Used by tests to talk to a
    /// mock server.
    fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
            subscriptions: Vec::new(),
            live_cursor: HashMap::new(),
        }
    }

    fn fetch_eod(
        &self,
        symbol: &str,
        period: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<Bar>, String> {
        let url = format!("{}/api/eod/{symbol}", self.base_url);
        let bars: Vec<EodBar> = ureq::get(&url)
            .query("api_token", self.api_key.as_str())
            .query("fmt", "json")
            .query("period", period)
            .query("from", format_date(from).as_str())
            .query("to", format_date(to).as_str())
            .call()
            .map_err(|e| format!("EODHD EOD request for '{symbol}' failed: {e}"))?
            .body_mut()
            .read_json()
            .map_err(|e| format!("EODHD EOD response for '{symbol}' could not be parsed: {e}"))?;

        bars.into_iter()
            .map(|b| {
                // EODHD liefert nur `adjusted_close` bereinigt, OHLC bleibt roh.
                // Ohne Skalierung erscheint ein Split als echter Kurssturz —
                // Amazons 20:1 im Juni 2022 als -95 % an einem Tag. Der Faktor
                // ist fuer jede Zeile derselbe wie fuer ihren Schluss, also
                // traegt er auch Open/High/Low.
                let factor = if b.close > 0.0 {
                    b.adjusted_close / b.close
                } else {
                    1.0
                };
                Ok(Bar {
                    timestamp: parse_eod_date(&b.date)?,
                    open: b.open * factor,
                    high: b.high * factor,
                    low: b.low * factor,
                    // `b.close * factor` statt `b.adjusted_close` direkt: rechnerisch
                    // dasselbe, aber dieselbe Operation wie fuer OHLC. Sonst faellt
                    // ein Schluss, der im Rohkurs exakt auf dem Tief liegt, um ein
                    // ULP darunter — und die Bar widerspraeche sich selbst.
                    close: b.close * factor,
                    volume: b.volume,
                })
            })
            .collect()
    }

    fn fetch_intraday(
        &self,
        symbol: &str,
        interval: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<Bar>, String> {
        let url = format!("{}/api/intraday/{symbol}", self.base_url);
        let bars: Vec<IntradayBar> = ureq::get(&url)
            .query("api_token", self.api_key.as_str())
            .query("fmt", "json")
            .query("interval", interval)
            .query("from", from.to_string().as_str())
            .query("to", to.to_string().as_str())
            .call()
            .map_err(|e| format!("EODHD intraday request for '{symbol}' failed: {e}"))?
            .body_mut()
            .read_json()
            .map_err(|e| {
                format!("EODHD intraday response for '{symbol}' could not be parsed: {e}")
            })?;

        Ok(bars
            .into_iter()
            .map(|b| Bar {
                timestamp: b.timestamp,
                open: b.open,
                high: b.high,
                low: b.low,
                close: b.close,
                // `null` wird zu `0.0`, und das ist bewusst die schlechtere
                // von zwei schlechten Möglichkeiten: die Bar zu verwerfen
                // nähme auch ihren Preis mit, und der ist da. Wer aus so
                // einer Reihe auf "führt Volumen" schließt, muss deshalb die
                // Mehrheit der Bars ansehen, nicht das Vorhandensein der
                // Spalte.
                volume: b.volume.unwrap_or(0.0),
            })
            .collect())
    }

    fn fetch_real_time(&self, symbol: &str) -> Result<Bar, String> {
        let url = format!("{}/api/real-time/{symbol}", self.base_url);
        let quote: RealTimeQuote = ureq::get(&url)
            .query("api_token", self.api_key.as_str())
            .query("fmt", "json")
            .call()
            .map_err(|e| format!("EODHD real-time request for '{symbol}' failed: {e}"))?
            .body_mut()
            .read_json()
            .map_err(|e| {
                format!("EODHD real-time response for '{symbol}' could not be parsed: {e}")
            })?;

        Ok(Bar {
            timestamp: quote.timestamp,
            open: quote.open,
            high: quote.high,
            low: quote.low,
            close: quote.close,
            volume: quote.volume,
        })
    }
}

impl DataFeedAdapter for EodhdAdapter {
    type Error = String;

    fn fetch_historical(
        &mut self,
        symbol: &str,
        timeframe: Timeframe,
        from: i64,
        to: i64,
    ) -> Result<Vec<Bar>, Self::Error> {
        match map_timeframe(timeframe)? {
            RequestKind::Eod { period } => self.fetch_eod(symbol, period, from, to),
            RequestKind::Intraday { interval } => self.fetch_intraday(symbol, interval, from, to),
        }
    }

    fn subscribe_live(&mut self, symbol: &str, _timeframe: Timeframe) -> Result<(), Self::Error> {
        if !self.subscriptions.iter().any(|s| s == symbol) {
            self.subscriptions.push(symbol.to_string());
            self.live_cursor.insert(symbol.to_string(), i64::MIN);
        }
        Ok(())
    }

    /// EODHD has no push feed. Each poll hits the 15-20min-delayed `/api/real-time/{symbol}`
    /// endpoint for every subscribed symbol and returns only bars whose timestamp is newer than
    /// the last one returned for that symbol — cursor logic analogous to
    /// `InMemoryDataFeed::poll_live` in `kestrel-chartkit`, adapted to a single always-current
    /// quote instead of an appended bar list.
    fn poll_live(&mut self) -> Result<Vec<Bar>, Self::Error> {
        let mut new_bars = Vec::new();
        for symbol in &self.subscriptions {
            let bar = self.fetch_real_time(symbol)?;
            let cursor = self.live_cursor.get(symbol).copied().unwrap_or(i64::MIN);
            if bar.timestamp > cursor {
                self.live_cursor.insert(symbol.clone(), bar.timestamp);
                new_bars.push(bar);
            }
        }
        Ok(new_bars)
    }
}

/// Which EODHD REST endpoint a [`Timeframe`] maps to.
enum RequestKind {
    Eod { period: &'static str },
    Intraday { interval: &'static str },
}

fn map_timeframe(timeframe: Timeframe) -> Result<RequestKind, String> {
    match timeframe {
        Timeframe::Day(1) => Ok(RequestKind::Eod { period: "d" }),
        Timeframe::Week(1) => Ok(RequestKind::Eod { period: "w" }),
        Timeframe::Month(1) => Ok(RequestKind::Eod { period: "m" }),
        Timeframe::Minute(1) => Ok(RequestKind::Intraday { interval: "1m" }),
        Timeframe::Minute(5) => Ok(RequestKind::Intraday { interval: "5m" }),
        Timeframe::Hour(1) => Ok(RequestKind::Intraday { interval: "1h" }),
        other => Err(format!(
            "EODHD does not support timeframe {other} (supported: Day(1)/Week(1)/Month(1) via \
             the EOD endpoint, Minute(1)/Minute(5)/Hour(1) via the intraday endpoint)"
        )),
    }
}

#[derive(Deserialize)]
struct EodBar {
    date: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    adjusted_close: f64,
    volume: f64,
}

#[derive(Deserialize)]
struct IntradayBar {
    timestamp: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    /// EODHD liefert auf dem Intraday-Endpunkt `null`, wo keine Menge
    /// vorliegt — bei Randbars einer Sitzung regelmäßig. Als `f64`
    /// deklariert ließ das die **ganze Serie** am Deserialisieren scheitern:
    /// ein fehlendes Feld in einer von tausend Bars, und der Abruf gibt
    /// einen Parserfehler statt Daten.
    volume: Option<f64>,
}

#[derive(Deserialize)]
struct RealTimeQuote {
    timestamp: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

/// Parses a `YYYY-MM-DD` date, as returned by the EOD endpoint, into a Unix timestamp at
/// midnight UTC.
fn parse_eod_date(date: &str) -> Result<i64, String> {
    fn parse_part<T: std::str::FromStr>(part: Option<&str>, date: &str) -> Result<T, String> {
        part.and_then(|s| s.parse().ok())
            .ok_or_else(|| format!("invalid EOD date '{date}'"))
    }

    let mut parts = date.splitn(3, '-');
    let year: i32 = parse_part(parts.next(), date)?;
    let month: u32 = parse_part(parts.next(), date)?;
    let day: u32 = parse_part(parts.next(), date)?;
    Ok(days_from_civil(year, month, day) * 86_400)
}

/// Formats a Unix timestamp as a `YYYY-MM-DD` date (UTC) for the EOD endpoint's `from`/`to`
/// parameters. The EOD endpoint has daily granularity, so any time-of-day component of the
/// input timestamp is dropped.
fn format_date(unix_ts: i64) -> String {
    let (year, month, day) = civil_from_days(unix_ts.div_euclid(86_400));
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_timeframe_is_rejected() {
        let mut adapter = EodhdAdapter::with_base_url("token", "http://127.0.0.1:0");
        let err = adapter
            .fetch_historical("AAPL.US", Timeframe::Minute(3), 0, 100)
            .unwrap_err();
        assert!(err.contains("does not support"));
    }

    #[test]
    fn fetch_historical_parses_eod_bars() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/api/eod/AAPL.US")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"date":"2024-01-02","open":295.05,"high":297.28,"low":295.05,"close":297.04,"adjusted_close":279.9221,"volume":4458400}]"#,
            )
            .create();

        let mut adapter = EodhdAdapter::with_base_url("token", server.url());
        let bars = adapter
            .fetch_historical("AAPL.US", Timeframe::Day(1), 0, 2_000_000_000)
            .unwrap();

        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].timestamp, days_from_civil(2024, 1, 2) * 86_400);
        // Bereinigt, nicht roh: `close` ist `adjusted_close`, und OHLC traegt
        // denselben Faktor, sonst waere die Bar in sich widerspruechlich.
        assert_eq!(bars[0].close, 279.9221);
        let factor = 279.9221 / 297.04;
        assert!((bars[0].open - 295.05 * factor).abs() < 1e-9);
        assert!((bars[0].high - 297.28 * factor).abs() < 1e-9);
        assert!((bars[0].low - 295.05 * factor).abs() < 1e-9);
        assert!(bars[0].low <= bars[0].close && bars[0].close <= bars[0].high);
        assert_eq!(bars[0].volume, 4_458_400.0);
    }

    /// Ein Split darf keine Kursluecke erzeugen: vor und nach Amazons 20:1
    /// im Juni 2022 muessen die bereinigten Schlusskurse stetig sein.
    #[test]
    fn split_does_not_produce_a_price_gap() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/api/eod/AMZN.US")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"date":"2022-06-03","open":2450.0,"high":2460.0,"low":2440.0,"close":2447.0,"adjusted_close":122.35,"volume":1.0},
                    {"date":"2022-06-06","open":125.0,"high":126.0,"low":124.0,"close":124.79,"adjusted_close":124.79,"volume":2.0}]"#,
            )
            .create();

        let mut adapter = EodhdAdapter::with_base_url("token", server.url());
        let bars = adapter
            .fetch_historical("AMZN.US", Timeframe::Day(1), 0, 2_000_000_000)
            .unwrap();

        let jump = (bars[1].close - bars[0].close).abs() / bars[0].close;
        assert!(jump < 0.05, "Split als Kurssprung durchgeschlagen: {jump}");
    }

    #[test]
    fn fetch_historical_parses_intraday_bars() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/api/intraday/AAPL.US")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"timestamp":1700058600,"gmtoffset":0,"datetime":"2023-11-15 14:30:00","open":187.845001,"high":188.699996,"low":187.800003,"close":188.149993,"volume":11861855},
                    {"timestamp":1700062200,"gmtoffset":0,"datetime":"2023-11-15 15:30:00","open":188.1,"high":188.4,"low":188.0,"close":188.2,"volume":null}]"#,
            )
            .create();

        let mut adapter = EodhdAdapter::with_base_url("token", server.url());
        let bars = adapter
            .fetch_historical("AAPL.US", Timeframe::Hour(1), 0, 2_000_000_000)
            .unwrap();

        // Zwei Bars, obwohl die zweite `volume: null` trägt: eine fehlende
        // Menge darf nicht die ganze Serie am Deserialisieren scheitern
        // lassen — genau das tat sie, bevor das Feld `Option` wurde.
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].timestamp, 1_700_058_600);
        assert_eq!(bars[0].volume, 11_861_855.0);
        assert_eq!(bars[1].volume, 0.0);
        assert_eq!(bars[1].close, 188.2);
    }

    #[test]
    fn http_error_is_surfaced() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/api/eod/AAPL.US")
            .match_query(mockito::Matcher::Any)
            .with_status(429)
            .create();

        let mut adapter = EodhdAdapter::with_base_url("token", server.url());
        let err = adapter
            .fetch_historical("AAPL.US", Timeframe::Day(1), 0, 100)
            .unwrap_err();
        assert!(err.contains("429"));
    }

    #[test]
    fn poll_live_returns_each_quote_only_once() {
        let mut server = mockito::Server::new();
        let _m = server
            .mock("GET", "/api/real-time/AAPL.US")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"code":"AAPL.US","timestamp":1700000000,"gmtoffset":0,"open":1,"high":2,"low":0.5,"close":1.5,"volume":100,"previousClose":1,"change":0.5,"change_p":50}"#,
            )
            .create();

        let mut adapter = EodhdAdapter::with_base_url("token", server.url());
        adapter
            .subscribe_live("AAPL.US", Timeframe::Minute(1))
            .unwrap();

        let first = adapter.poll_live().unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].timestamp, 1_700_000_000);

        let second = adapter.poll_live().unwrap();
        assert!(second.is_empty());
    }
}
