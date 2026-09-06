//! Capital.com (<https://capital.com>) broker REST adapter — historical/live prices only.
//!
//! Implements [`DataFeedAdapter`] against Capital.com's session-based REST API:
//! - `POST /api/v1/session` for auth (`CST` + `X-SECURITY-TOKEN` response headers, ~10min TTL).
//! - `GET /api/v1/prices/{epic}` for historical bars, paginated backward via the `to` cursor
//!   when a range exceeds the API's 1000-bars-per-request cap.
//! - The same endpoint with `max=1` for `poll_live`. Capital.com does have a push WebSocket
//!   stream, but wiring it up would pull an async runtime into this otherwise-sync adapter crate
//!   (see `kestrel-chartkit`'s `plan/market-data-connector-und-anbieterstufen.md`,
//!   "Connector-Design") — a REST poll for the latest bar is a deliberate, simpler substitute.
//!
//! Endpoint shape, field names and the session lifecycle come from Capital.com's own Postman
//! collection (`capital-com-sv/capital-api-postman`) and the already-production-verified
//! Capital.com client in `kestrel`'s `crates/capital` (see comments there, "verified against the
//! live demo API").

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kestrel_chartkit::{Bar, DataFeedAdapter, Timeframe};
use serde::{Deserialize, Serialize};

use crate::civil_date::{civil_from_days, days_from_civil};

/// Capital.com sessions expire 10 minutes after last use; refresh a bit earlier so an in-flight
/// request never races the expiry (same margin `kestrel`'s `crates/capital` uses).
const SESSION_TTL: Duration = Duration::from_secs(480);
/// Hard API limit: `max=1001` returns `error.invalid.max` (confirmed against the live demo
/// endpoint by `kestrel`'s `crates/capital`, see its `history.rs`).
const MAX_BARS_PER_REQUEST: usize = 1000;

struct CapitalSession {
    cst: String,
    security_token: String,
    obtained_at: Instant,
}

#[derive(Serialize)]
struct CreateSessionBody<'a> {
    identifier: &'a str,
    password: &'a str,
}

/// [`DataFeedAdapter`] backed by the Capital.com REST API. Requires an API key plus
/// identifier/password login credentials. `base_url` distinguishes demo
/// (`https://demo-api-capital.backend-capital.com`) from live
/// (`https://api-capital.backend-capital.com`).
pub struct CapitalComAdapter {
    base_url: String,
    api_key: String,
    identifier: String,
    password: String,
    session: Option<CapitalSession>,
    subscriptions: Vec<(String, Timeframe)>,
    live_cursor: HashMap<String, i64>,
}

impl CapitalComAdapter {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        identifier: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
            identifier: identifier.into(),
            password: password.into(),
            session: None,
            subscriptions: Vec::new(),
            live_cursor: HashMap::new(),
        }
    }

    fn authenticate(&self) -> Result<CapitalSession, String> {
        let url = format!("{}/api/v1/session", self.base_url);
        let response = ureq::post(&url)
            .header("X-CAP-API-KEY", self.api_key.as_str())
            .send_json(&CreateSessionBody {
                identifier: &self.identifier,
                password: &self.password,
            })
            .map_err(|e| format!("Capital.com login failed: {e}"))?;

        let cst = header_value(&response, "CST")?;
        let security_token = header_value(&response, "X-SECURITY-TOKEN")?;
        Ok(CapitalSession {
            cst,
            security_token,
            obtained_at: Instant::now(),
        })
    }

    fn ensure_session(&mut self) -> Result<(String, String), String> {
        let needs_refresh = match &self.session {
            Some(s) => s.obtained_at.elapsed() >= SESSION_TTL,
            None => true,
        };
        if needs_refresh {
            self.session = Some(self.authenticate()?);
        }
        let session = self.session.as_ref().expect("just populated above");
        Ok((session.cst.clone(), session.security_token.clone()))
    }

    fn request_prices(
        &self,
        symbol: &str,
        resolution: &str,
        to: i64,
        max: usize,
        cst: &str,
        security_token: &str,
    ) -> Result<ureq::http::Response<ureq::Body>, ureq::Error> {
        let url = format!("{}/api/v1/prices/{symbol}", self.base_url);
        ureq::get(&url)
            .header("X-CAP-API-KEY", self.api_key.as_str())
            .header("CST", cst)
            .header("X-SECURITY-TOKEN", security_token)
            .query("resolution", resolution)
            .query("max", max.to_string().as_str())
            .query("to", format_datetime(to).as_str())
            .call()
    }

    /// Fetches one page (up to `max` bars, ending at `to`), transparently re-authenticating once
    /// if the session expired server-side before our local TTL guess noticed.
    fn fetch_prices_page(
        &mut self,
        symbol: &str,
        resolution: &str,
        to: i64,
        max: usize,
    ) -> Result<Vec<PricePoint>, String> {
        let (cst, security_token) = self.ensure_session()?;
        let result = self.request_prices(symbol, resolution, to, max, &cst, &security_token);

        let mut response = match result {
            Ok(response) => response,
            Err(ureq::Error::StatusCode(401)) => {
                self.session = None;
                let (cst, security_token) = self.ensure_session()?;
                self.request_prices(symbol, resolution, to, max, &cst, &security_token)
                    .map_err(|e| format!("Capital.com prices request for '{symbol}' failed: {e}"))?
            }
            Err(e) => {
                return Err(format!(
                    "Capital.com prices request for '{symbol}' failed: {e}"
                ));
            }
        };

        let parsed: PricesResponse = response.body_mut().read_json().map_err(|e| {
            format!("Capital.com prices response for '{symbol}' could not be parsed: {e}")
        })?;
        Ok(parsed.prices)
    }

    /// Fetches all bars in `[from, to]`, paginating backward via the `to` cursor whenever the
    /// range needs more than [`MAX_BARS_PER_REQUEST`] bars — the same backward-pagination shape
    /// `kestrel`'s `crates/capital` uses (see its `history.rs`), adapted to the fixed `[from, to]`
    /// range [`DataFeedAdapter::fetch_historical`] expects instead of a bar count.
    fn fetch_prices(
        &mut self,
        symbol: &str,
        resolution: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<Bar>, String> {
        let mut collected: Vec<Bar> = Vec::new();
        let mut cursor_to = to;
        loop {
            let page =
                self.fetch_prices_page(symbol, resolution, cursor_to, MAX_BARS_PER_REQUEST)?;
            if page.is_empty() {
                break; // no more history available (e.g. instrument inception reached)
            }
            let mut page_bars = page
                .into_iter()
                .map(PricePoint::into_bar)
                .collect::<Result<Vec<Bar>, String>>()?;
            let Some(earliest) = page_bars.first().map(|b| b.timestamp) else {
                break;
            };
            page_bars.append(&mut collected);
            collected = page_bars;
            if earliest <= from {
                break;
            }
            cursor_to = earliest - 1;
        }
        collected.retain(|b| b.timestamp >= from && b.timestamp <= to);
        Ok(collected)
    }
}

impl DataFeedAdapter for CapitalComAdapter {
    type Error = String;

    fn fetch_historical(
        &mut self,
        symbol: &str,
        timeframe: Timeframe,
        from: i64,
        to: i64,
    ) -> Result<Vec<Bar>, Self::Error> {
        let resolution = map_resolution(timeframe)?;
        self.fetch_prices(symbol, resolution, from, to)
    }

    fn subscribe_live(&mut self, symbol: &str, timeframe: Timeframe) -> Result<(), Self::Error> {
        if !self.subscriptions.iter().any(|(s, _)| s == symbol) {
            self.subscriptions.push((symbol.to_string(), timeframe));
            self.live_cursor.insert(symbol.to_string(), i64::MIN);
        }
        Ok(())
    }

    /// Polls the latest bar (`max=1`) for every subscribed symbol and returns only bars whose
    /// timestamp is newer than the last one returned for that symbol — cursor logic analogous to
    /// `InMemoryDataFeed::poll_live` in `kestrel-chartkit`, adapted to a single always-current
    /// bar instead of an appended bar list (see the module-level docs for why this polls REST
    /// instead of using Capital.com's push WebSocket stream).
    fn poll_live(&mut self) -> Result<Vec<Bar>, Self::Error> {
        let subscriptions = self.subscriptions.clone();
        let now = current_unix_time();
        let mut new_bars = Vec::new();
        for (symbol, timeframe) in &subscriptions {
            let resolution = map_resolution(*timeframe)?;
            let Some(point) = self
                .fetch_prices_page(symbol, resolution, now, 1)?
                .into_iter()
                .next_back()
            else {
                continue;
            };
            let bar = point.into_bar()?;
            let cursor = self.live_cursor.get(symbol).copied().unwrap_or(i64::MIN);
            if bar.timestamp > cursor {
                self.live_cursor.insert(symbol.clone(), bar.timestamp);
                new_bars.push(bar);
            }
        }
        Ok(new_bars)
    }
}

fn current_unix_time() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn header_value(response: &ureq::http::Response<ureq::Body>, name: &str) -> Result<String, String> {
    response
        .headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| format!("Capital.com login response is missing the '{name}' header"))
}

/// Maps a [`Timeframe`] to Capital.com's `resolution` wire values, as used by `GET
/// /api/v1/prices/{epic}` (verified against the live API by `kestrel`'s
/// `crates/capital`/`kestrel-report`'s `capitalcom` datasource, both of which use the same list).
fn map_resolution(timeframe: Timeframe) -> Result<&'static str, String> {
    match timeframe {
        Timeframe::Minute(1) => Ok("MINUTE"),
        Timeframe::Minute(5) => Ok("MINUTE_5"),
        Timeframe::Minute(15) => Ok("MINUTE_15"),
        Timeframe::Minute(30) => Ok("MINUTE_30"),
        Timeframe::Hour(1) => Ok("HOUR"),
        Timeframe::Hour(4) => Ok("HOUR_4"),
        Timeframe::Day(1) => Ok("DAY"),
        Timeframe::Week(1) => Ok("WEEK"),
        other => Err(format!(
            "Capital.com does not support timeframe {other} (supported: Minute(1/5/15/30), \
             Hour(1/4), Day(1), Week(1))"
        )),
    }
}

#[derive(Deserialize)]
struct PricesResponse {
    prices: Vec<PricePoint>,
}

#[derive(Deserialize)]
struct PricePoint {
    #[serde(rename = "snapshotTimeUTC")]
    snapshot_time_utc: String,
    #[serde(rename = "openPrice")]
    open_price: PriceLevel,
    #[serde(rename = "highPrice")]
    high_price: PriceLevel,
    #[serde(rename = "lowPrice")]
    low_price: PriceLevel,
    #[serde(rename = "closePrice")]
    close_price: PriceLevel,
    #[serde(rename = "lastTradedVolume")]
    last_traded_volume: Option<f64>,
}

#[derive(Deserialize)]
struct PriceLevel {
    bid: Option<f64>,
    ask: Option<f64>,
}

impl PriceLevel {
    /// Capital.com quotes bid/ask, not a single trade price. Mid is the least-biased single
    /// number when both sides are present; either side alone is used as a fallback for
    /// instruments that only quote one (matches `kestrel-report`'s `capitalcom` datasource).
    fn mid(&self) -> f64 {
        match (self.bid, self.ask) {
            (Some(b), Some(a)) => (b + a) / 2.0,
            (Some(b), None) => b,
            (None, Some(a)) => a,
            (None, None) => 0.0,
        }
    }
}

impl PricePoint {
    fn into_bar(self) -> Result<Bar, String> {
        Ok(Bar {
            timestamp: parse_capital_timestamp(&self.snapshot_time_utc)?,
            open: self.open_price.mid(),
            high: self.high_price.mid(),
            low: self.low_price.mid(),
            close: self.close_price.mid(),
            volume: self.last_traded_volume.unwrap_or(0.0),
        })
    }
}

/// Parses a `snapshotTimeUTC` value (`YYYY-MM-DDTHH:MM:SS`, per Capital.com's Postman
/// collection) into a Unix timestamp.
fn parse_capital_timestamp(value: &str) -> Result<i64, String> {
    fn parse_num<T: std::str::FromStr>(part: &str, whole: &str) -> Result<T, String> {
        part.parse()
            .map_err(|_| format!("invalid Capital.com timestamp '{whole}'"))
    }

    let (date_part, time_part) = value
        .split_once('T')
        .ok_or_else(|| format!("invalid Capital.com timestamp '{value}'"))?;

    let mut date_parts = date_part.splitn(3, '-');
    let year: i32 = parse_num(date_parts.next().unwrap_or(""), value)?;
    let month: u32 = parse_num(date_parts.next().unwrap_or(""), value)?;
    let day: u32 = parse_num(date_parts.next().unwrap_or(""), value)?;

    let mut time_parts = time_part.splitn(3, ':');
    let hour: i64 = parse_num(time_parts.next().unwrap_or(""), value)?;
    let minute: i64 = parse_num(time_parts.next().unwrap_or(""), value)?;
    let second: i64 = parse_num(time_parts.next().unwrap_or(""), value)?;

    Ok(days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second)
}

/// Formats a Unix timestamp as `YYYY-MM-DDTHH:MM:SS` (UTC) for the `from`/`to` query parameters.
fn format_datetime(unix_ts: i64) -> String {
    let days = unix_ts.div_euclid(86_400);
    let secs_of_day = unix_ts.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Matcher;

    fn mock_session(server: &mut mockito::Server) -> mockito::Mock {
        server
            .mock("POST", "/api/v1/session")
            .with_status(200)
            .with_header("CST", "cst-token")
            .with_header("X-SECURITY-TOKEN", "sec-token")
            .with_body("{}")
            .create()
    }

    #[test]
    fn unsupported_timeframe_is_rejected() {
        let mut adapter = CapitalComAdapter::new("http://127.0.0.1:0", "key", "id", "pw");
        let err = adapter
            .fetch_historical("GERMANY40", Timeframe::Minute(3), 0, 100)
            .unwrap_err();
        assert!(err.contains("does not support"));
    }

    #[test]
    fn fetch_historical_parses_a_single_page() {
        let mut server = mockito::Server::new();
        let _session = mock_session(&mut server);
        let _prices = server
            .mock("GET", "/api/v1/prices/GERMANY40")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("resolution".into(), "HOUR".into()),
                Matcher::UrlEncoded("max".into(), "1000".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"prices":[{"snapshotTimeUTC":"2024-01-02T00:00:00","openPrice":{"bid":100.0,"ask":102.0},"highPrice":{"bid":105.0,"ask":107.0},"lowPrice":{"bid":99.0,"ask":101.0},"closePrice":{"bid":104.0,"ask":106.0},"lastTradedVolume":50}]}"#,
            )
            .create();

        // `from` equals the single returned bar's own timestamp, so pagination stops after this
        // one page instead of paging backward looking for more (that path is covered by
        // `fetch_historical_paginates_past_the_per_request_cap` below).
        let bar_ts = days_from_civil(2024, 1, 2) * 86_400;
        let mut adapter = CapitalComAdapter::new(server.url(), "key", "id", "pw");
        let bars = adapter
            .fetch_historical("GERMANY40", Timeframe::Hour(1), bar_ts, 2_000_000_000)
            .unwrap();

        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].timestamp, bar_ts);
        assert_eq!(bars[0].open, 101.0); // mid(100, 102)
        assert_eq!(bars[0].volume, 50.0);
    }

    #[test]
    fn fetch_historical_paginates_past_the_per_request_cap() {
        let mut server = mockito::Server::new();
        let _session = mock_session(&mut server);

        let t0 = days_from_civil(2024, 1, 1) * 86_400;
        let t1 = t0 + 3_600;
        let t2 = t0 + 7_200;
        let t3 = t0 + 10_800;

        fn bar_json(ts: i64) -> String {
            format!(
                r#"{{"snapshotTimeUTC":"{}","openPrice":{{"bid":1.0,"ask":1.0}},"highPrice":{{"bid":1.0,"ask":1.0}},"lowPrice":{{"bid":1.0,"ask":1.0}},"closePrice":{{"bid":1.0,"ask":1.0}},"lastTradedVolume":1}}"#,
                format_datetime(ts)
            )
        }

        // First page (most recent): ends at `to`, contains t2/t3 — still short of `from` = t0.
        let _page1 = server
            .mock("GET", "/api/v1/prices/GERMANY40")
            .match_query(Matcher::AllOf(vec![Matcher::UrlEncoded(
                "to".into(),
                format_datetime(t3),
            )]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"prices":[{},{}]}}"#,
                bar_json(t2),
                bar_json(t3)
            ))
            .create();

        // Second page: cursor is (t2 - 1), contains t0/t1 — earliest (t0) reaches `from`, so
        // pagination stops here.
        let _page2 = server
            .mock("GET", "/api/v1/prices/GERMANY40")
            .match_query(Matcher::AllOf(vec![Matcher::UrlEncoded(
                "to".into(),
                format_datetime(t2 - 1),
            )]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"prices":[{},{}]}}"#,
                bar_json(t0),
                bar_json(t1)
            ))
            .create();

        let mut adapter = CapitalComAdapter::new(server.url(), "key", "id", "pw");
        let bars = adapter
            .fetch_historical("GERMANY40", Timeframe::Hour(1), t0, t3)
            .unwrap();

        assert_eq!(
            bars.iter().map(|b| b.timestamp).collect::<Vec<_>>(),
            vec![t0, t1, t2, t3]
        );
    }

    #[test]
    fn expired_session_triggers_one_relogin_and_retry() {
        let mut server = mockito::Server::new();
        let _session = mock_session(&mut server);

        let _unauthorized = server
            .mock("GET", "/api/v1/prices/GERMANY40")
            .match_query(Matcher::Any)
            .match_header("cst", "cst-token")
            .with_status(401)
            .expect(1)
            .create();

        let _retry = server
            .mock("GET", "/api/v1/prices/GERMANY40")
            .match_query(Matcher::Any)
            .match_header("cst", "cst-token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"prices":[]}"#)
            .create();

        let mut adapter = CapitalComAdapter::new(server.url(), "key", "id", "pw");
        let bars = adapter
            .fetch_historical("GERMANY40", Timeframe::Hour(1), 0, 100)
            .unwrap();
        assert!(bars.is_empty());
    }

    #[test]
    fn poll_live_returns_each_bar_only_once() {
        let mut server = mockito::Server::new();
        let _session = mock_session(&mut server);
        let _prices = server
            .mock("GET", "/api/v1/prices/GERMANY40")
            .match_query(Matcher::UrlEncoded("max".into(), "1".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"prices":[{"snapshotTimeUTC":"2024-01-02T00:00:00","openPrice":{"bid":100.0,"ask":100.0},"highPrice":{"bid":100.0,"ask":100.0},"lowPrice":{"bid":100.0,"ask":100.0},"closePrice":{"bid":100.0,"ask":100.0},"lastTradedVolume":1}]}"#,
            )
            .create();

        let mut adapter = CapitalComAdapter::new(server.url(), "key", "id", "pw");
        adapter
            .subscribe_live("GERMANY40", Timeframe::Hour(1))
            .unwrap();

        let first = adapter.poll_live().unwrap();
        assert_eq!(first.len(), 1);

        let second = adapter.poll_live().unwrap();
        assert!(second.is_empty());
    }
}
