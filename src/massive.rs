//! Massive (<https://massive.com>, vormals Polygon.io) Futures-Adapter.
//!
//! Zweck ist **Börsenvolumen**. Preis und Struktur liefern die
//! Broker-Adapter; was ihnen fehlt, ist eine Volumenreihe, die den Markt
//! misst statt den Ausschnitt eines Anbieters. Ein Rohstoff-CFD trägt
//! Volumen, aber es ist das des Brokers — gemessen schwankt sein Anteil am
//! Börsenvolumen um ein Vielfaches, taugt also für Rangfolgen und nicht für
//! Mengen.
//!
//! # `symbol` ist ein Produktcode, kein Kontrakt
//!
//! Der Aufrufer übergibt `"NG"`, nicht `"NGV26"`. Terminkontrakte laufen
//! aus; ein fest verdrahteter Ticker liefert ab dem Verfall stillschweigend
//! nichts mehr — HTTP 200, leere Liste. Der Adapter löst deshalb bei jedem
//! Abruf den Frontmonat auf und hält ihn kurz vor.
//!
//! **Frontmonat = aktiver Einzelkontrakt mit dem höchsten Volumen**, nicht
//! der mit dem nächsten Verfall. Kurz vor dem Verfall wandert die Liquidität
//! in den Folgemonat, während der ablaufende noch handelbar ist; wer nach
//! Verfallsdatum wählt, folgt einer austrocknenden Reihe. Gemessen am
//! 2026-09-08 führte der Oktoberkontrakt das Drei- bis Sechsfache des
//! Novembers — die Rangfolge nach Volumen ist eindeutig, die nach Verfall
//! wäre es zum Rolltermin nicht.
//!
//! # Eigenheiten der Schnittstelle
//!
//! - `window_start` kommt in **Nanosekunden**. `Bar::timestamp` erwartet
//!   Sekunden.
//! - `order=asc` wird ignoriert, die Antwort ist immer absteigend. Der
//!   Adapter sortiert selbst; darauf zu vertrauen wäre eine stille
//!   Umkehrung der Reihenfolge.
//! - Die Kontraktliste ist alphabetisch und führt Butterflies (`NG:BF …`)
//!   sowie Kalenderspreads (`NGF7-NGF8`) vor den Einzelkontrakten. Beide
//!   sind hier unbrauchbar und werden verworfen.
//! - Ohne `date=` liefert die Kontraktliste historische Zeilen ab 2017.

use std::collections::HashMap;

use kestrel_chartkit::{Bar, DataFeedAdapter, Timeframe};
use serde::Deserialize;

use crate::civil_date::civil_from_days;

const DEFAULT_BASE_URL: &str = "https://api.polygon.io";

/// Wie lange ein aufgelöster Frontmonat gilt, bevor neu gesucht wird.
///
/// Eine Rollung ist ein Tagesereignis, kein Minutenereignis — häufiger zu
/// fragen kostet Aufrufe ohne Erkenntnis. Kurz genug, dass ein Rolltag
/// innerhalb desselben Handelstages nachgezogen wird.
const FRONT_MONTH_TTL_SECS: i64 = 3600;

/// Wie viele Kontrakte die Auflösung höchstens betrachtet. Ein Produkt hat
/// Dutzende Fälligkeiten, aber nur die vordersten tragen nennenswertes
/// Volumen.
const CONTRACT_PAGE_LIMIT: usize = 250;

/// Wie viele Seiten die Auflistung höchstens liest. Bei Erdgas lagen die
/// ersten beiden voller Spreads; drei Seiten sind Reserve, keine Erwartung.
const MAX_CONTRACT_PAGES: usize = 4;

/// Wie viele der nächstfälligen Kontrakte auf Volumen geprüft werden. Der
/// liquideste liegt immer unter den vordersten; alles dahinter kostete nur
/// Abrufe.
const FRONT_MONTH_CANDIDATES: usize = 4;

/// [`DataFeedAdapter`] für Massives Futures-Endpunkte. Braucht einen Schlüssel
/// mit **Futures**-Berechtigung — Massive rechnet je Anlageklasse ab, ein
/// Aktien-Abo schaltet diese Endpunkte nicht frei.
pub struct MassiveAdapter {
    api_key: String,
    base_url: String,
    subscriptions: Vec<(String, Timeframe)>,
    live_cursor: HashMap<String, i64>,
    /// Produktcode -> (Ticker, wann aufgelöst). Siehe `FRONT_MONTH_TTL_SECS`.
    front_month: HashMap<String, (String, i64)>,
}

impl MassiveAdapter {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_base_url(api_key, DEFAULT_BASE_URL)
    }

    fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
            subscriptions: Vec::new(),
            live_cursor: HashMap::new(),
            front_month: HashMap::new(),
        }
    }

    /// Der aktuell liquideste Kontrakt eines Produkts.
    ///
    /// `now` wird übergeben statt gelesen, damit der Test die Uhr stellen
    /// kann — sonst wäre der Zwischenspeicher nicht prüfbar.
    fn front_month(&mut self, product: &str, now: i64) -> Result<String, String> {
        if let Some((ticker, resolved_at)) = self.front_month.get(product) {
            if now - resolved_at < FRONT_MONTH_TTL_SECS {
                return Ok(ticker.clone());
            }
        }

        let day = format_date(now);
        let mut candidates = self.list_outrights(product, &day)?;
        if candidates.is_empty() {
            // Kein leeres Ergebnis zurückgeben: der Aufrufer könnte es als
            // "keine Bars" lesen, dabei ist die Auflösung gescheitert.
            return Err(format!(
                "Massive führt für '{product}' am {day} keinen aktiven Einzelkontrakt \
                 (die Liste enthält nur Spreads, oder der Produktcode stimmt nicht)"
            ));
        }

        // Erst nach Verfall eingrenzen, dann nach Volumen entscheiden.
        //
        // Beides ist nötig. Nur Verfall wäre falsch: zum Rolltermin liegt
        // die Liquidität schon im Folgemonat, während der ablaufende
        // Kontrakt noch handelbar ist. Nur Volumen wäre unbezahlbar: ein
        // Produkt führt über hundert aktive Fälligkeiten, und jede kostete
        // einen eigenen Abruf. Die vordersten `FRONT_MONTH_CANDIDATES`
        // enthalten den liquidesten Kontrakt immer — dahinter liegen
        // Fälligkeiten in Jahren.
        candidates.sort_by(|a, b| a.last_trade_date.cmp(&b.last_trade_date));
        candidates.truncate(FRONT_MONTH_CANDIDATES);

        let mut best: Option<(String, f64)> = None;
        for contract in &candidates {
            let volume = self.recent_volume(&contract.ticker).unwrap_or(0.0);
            if best.as_ref().is_none_or(|(_, seen)| volume > *seen) {
                best = Some((contract.ticker.clone(), volume));
            }
        }
        let (ticker, volume) = best.expect("candidates ist nicht leer");
        if volume <= 0.0 {
            return Err(format!(
                "Massive meldet für keinen aktiven '{product}'-Kontrakt Volumen — \
                 ohne Volumen ist kein Frontmonat bestimmbar"
            ));
        }
        self.front_month
            .insert(product.to_string(), (ticker.clone(), now));
        Ok(ticker)
    }

    /// Alle aktiven Einzelkontrakte eines Produkts an einem Tag.
    ///
    /// Muss blättern. Die Liste ist alphabetisch, und `NG:BF …` sowie
    /// `NGF7-NGF8` sortieren vor `NGV26` — bei Erdgas stand auf den ersten
    /// beiden Seiten zu je 250 Einträgen kein einziger Einzelkontrakt. Wer
    /// nur die erste Seite liest, bekommt eine leere Auswahl und hält sie
    /// für "gibt es nicht".
    fn list_outrights(&self, product: &str, day: &str) -> Result<Vec<ContractRow>, String> {
        let mut out: Vec<ContractRow> = Vec::new();
        let mut next: Option<String> = None;
        for _ in 0..MAX_CONTRACT_PAGES {
            let listing: ContractListing = match &next {
                None => ureq::get(&format!("{}/futures/v1/contracts", self.base_url))
                    .query("product_code", product)
                    .query("date", day)
                    .query("limit", CONTRACT_PAGE_LIMIT.to_string().as_str())
                    .query("apiKey", self.api_key.as_str()),
                Some(url) => ureq::get(url).query("apiKey", self.api_key.as_str()),
            }
            .call()
            .map_err(|e| format!("Massive-Kontraktliste für '{product}' fehlgeschlagen: {e}"))?
            .body_mut()
            .read_json()
            .map_err(|e| format!("Massive-Kontraktliste für '{product}' unlesbar: {e}"))?;

            let empty = listing.results.is_empty();
            out.extend(
                listing
                    .results
                    .into_iter()
                    .filter(|c| c.active && is_outright(&c.ticker)),
            );
            // Weiterblättern, bis Einzelkontrakte da sind — danach nicht mehr:
            // die restlichen Seiten tragen nur fernere Fälligkeiten.
            if !out.is_empty() || empty {
                break;
            }
            match listing.next_url {
                Some(url) => next = Some(url),
                None => break,
            }
        }
        Ok(out)
    }

    /// Volumen der jüngsten Tagesbar — das Maß, an dem der Frontmonat hängt.
    fn recent_volume(&self, ticker: &str) -> Result<f64, String> {
        let bars = self.fetch_aggs(ticker, "1day", None, None, 1)?;
        Ok(bars.first().map(|b| b.volume).unwrap_or(0.0))
    }

    /// Roher Aggregat-Abruf. `from`/`to` in Sekunden, exklusiv oben.
    fn fetch_aggs(
        &self,
        ticker: &str,
        resolution: &str,
        from: Option<i64>,
        to: Option<i64>,
        limit: usize,
    ) -> Result<Vec<Bar>, String> {
        let url = format!("{}/futures/v1/aggs/{ticker}", self.base_url);
        let mut req = ureq::get(&url)
            .query("resolution", resolution)
            .query("limit", limit.to_string().as_str())
            .query("apiKey", self.api_key.as_str());
        if let Some(from) = from {
            req = req.query("window_start.gte", nanos(from).to_string().as_str());
        }
        if let Some(to) = to {
            req = req.query("window_start.lt", nanos(to).to_string().as_str());
        }
        let page: AggPage = req
            .call()
            .map_err(|e| format!("Massive-Aggregate für '{ticker}' fehlgeschlagen: {e}"))?
            .body_mut()
            .read_json()
            .map_err(|e| format!("Massive-Aggregate für '{ticker}' unlesbar: {e}"))?;

        let mut bars: Vec<Bar> = page
            .results
            .into_iter()
            .map(|r| Bar {
                // Nanosekunden -> Sekunden, siehe Modulkopf.
                timestamp: r.window_start / 1_000_000_000,
                open: r.open,
                high: r.high,
                low: r.low,
                close: r.close,
                volume: r.volume,
            })
            .collect();
        // Selbst sortieren: `order=asc` wird ignoriert.
        bars.sort_by_key(|b| b.timestamp);
        Ok(bars)
    }
}

/// Einzelkontrakte tragen weder `:` (Butterfly `NG:BF F7-G7-H7`) noch `-`
/// (Kalenderspread `NGF7-NGF8`).
fn is_outright(ticker: &str) -> bool {
    !ticker.contains(':') && !ticker.contains('-')
}

fn nanos(seconds: i64) -> i128 {
    seconds as i128 * 1_000_000_000
}

fn format_date(seconds: i64) -> String {
    let (y, m, d) = civil_from_days(seconds.div_euclid(86_400));
    format!("{y:04}-{m:02}-{d:02}")
}

/// Massives Auflösungsnamen. Bewusst nur die, die Kestrel fährt, plus Tag —
/// eine erfundene Auflösung würde als HTTP 400 zurückkommen, und das ist ein
/// schlechterer Fehlertext als dieser hier.
fn resolution_of(timeframe: Timeframe) -> Result<&'static str, String> {
    match timeframe {
        Timeframe::Minute(1) => Ok("1minute"),
        Timeframe::Minute(5) => Ok("5minute"),
        Timeframe::Minute(15) => Ok("15minute"),
        Timeframe::Hour(1) => Ok("1hour"),
        Timeframe::Hour(4) => Ok("4hour"),
        Timeframe::Day(1) => Ok("1day"),
        other => Err(format!(
            "Massive-Adapter kennt {other} nicht (unterstützt: Minute(1/5/15), Hour(1/4), Day(1))"
        )),
    }
}

impl DataFeedAdapter for MassiveAdapter {
    type Error = String;

    fn fetch_historical(
        &mut self,
        symbol: &str,
        timeframe: Timeframe,
        from: i64,
        to: i64,
    ) -> Result<Vec<Bar>, Self::Error> {
        let resolution = resolution_of(timeframe)?;
        let ticker = self.front_month(symbol, to)?;
        self.fetch_aggs(&ticker, resolution, Some(from), Some(to), 50_000)
    }

    fn subscribe_live(&mut self, symbol: &str, timeframe: Timeframe) -> Result<(), Self::Error> {
        resolution_of(timeframe)?;
        let entry = (symbol.to_string(), timeframe);
        if !self.subscriptions.contains(&entry) {
            self.subscriptions.push(entry);
        }
        Ok(())
    }

    /// Fragt je Abonnement die jüngsten Bars ab und gibt nur zurück, was seit
    /// dem letzten Aufruf dazugekommen ist.
    ///
    /// Kein Push-Kanal: Massive bietet zwar WebSockets, aber der Adapter
    /// bleibt bei REST, solange die Volumenreihe nur als Referenz unter
    /// einer anderen Preisreihe liegt — sie muss nicht schneller sein als
    /// die Bar, zu der sie gehört. Der Tarif verzögert ohnehin um Minuten.
    fn poll_live(&mut self) -> Result<Vec<Bar>, Self::Error> {
        let subs = self.subscriptions.clone();
        let mut out = Vec::new();
        for (symbol, timeframe) in subs {
            let resolution = resolution_of(timeframe)?;
            // `poll_live` hat keine Uhr im Trait. Der zuletzt gesehene
            // Zeitstempel ist die beste verfügbare Näherung für "jetzt" und
            // reicht für die Frontmonat-Frist; beim ersten Aufruf löst sie
            // ohnehin auf.
            let now = self
                .live_cursor
                .get(&symbol)
                .copied()
                .unwrap_or(i64::MIN)
                .max(0);
            let ticker = self.front_month(&symbol, now)?;
            let bars = self.fetch_aggs(&ticker, resolution, None, None, 10)?;
            let cursor = self.live_cursor.entry(symbol).or_insert(i64::MIN);
            for bar in bars {
                if bar.timestamp > *cursor {
                    *cursor = bar.timestamp;
                    out.push(bar);
                }
            }
        }
        out.sort_by_key(|b| b.timestamp);
        Ok(out)
    }
}

#[derive(Deserialize)]
struct ContractListing {
    #[serde(default)]
    results: Vec<ContractRow>,
    #[serde(default)]
    next_url: Option<String>,
}

#[derive(Deserialize)]
struct ContractRow {
    ticker: String,
    #[serde(default)]
    active: bool,
    /// Letzter Handelstag als `YYYY-MM-DD`. Fehlt er, sortiert der Kontrakt
    /// ans Ende statt nach vorn — ein Kontrakt ohne Verfallsangabe soll
    /// nicht versehentlich als nächstfälliger gelten.
    #[serde(default = "far_future")]
    last_trade_date: String,
}

fn far_future() -> String {
    "9999-12-31".to_string()
}

#[derive(Deserialize)]
struct AggPage {
    #[serde(default)]
    results: Vec<AggRow>,
}

#[derive(Deserialize)]
struct AggRow {
    window_start: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    #[serde(default)]
    volume: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contracts_body() -> &'static str {
        // Wie die echte Liste: Spreads zuerst, Einzelkontrakte danach.
        r#"{"status":"OK","results":[
            {"ticker":"NG:BF F7-G7-H7","active":true},
            {"ticker":"NGF7-NGF8","active":true},
            {"ticker":"NGV26","active":true},
            {"ticker":"NGX26","active":true},
            {"ticker":"NGZ26","active":false}
        ]}"#
    }

    fn day_bar(volume: f64) -> String {
        format!(
            r#"{{"results":[{{"window_start":1788652800000000000,"open":1.0,"high":2.0,"low":0.5,"close":1.5,"volume":{volume}}}]}}"#
        )
    }

    #[test]
    fn unsupported_timeframe_is_rejected() {
        let mut adapter = MassiveAdapter::with_base_url("token", "http://127.0.0.1:0");
        let err = adapter
            .fetch_historical("NG", Timeframe::Minute(3), 0, 100)
            .unwrap_err();
        assert!(err.contains("kennt"), "Fehlertext: {err}");
    }

    /// Der liquideste Kontrakt gewinnt, nicht der mit dem nächsten Verfall.
    /// `NGV26` steht in der Liste vor `NGX26`, hat aber weniger Volumen —
    /// eine Auswahl nach Reihenfolge oder Verfall käme hier falsch heraus.
    #[test]
    fn front_month_is_the_most_liquid_contract() {
        let mut server = mockito::Server::new();
        let _c = server
            .mock("GET", "/futures/v1/contracts")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(contracts_body())
            .create();
        let _v = server
            .mock("GET", "/futures/v1/aggs/NGV26")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(day_bar(100.0))
            .expect_at_least(1)
            .create();
        let _x = server
            .mock("GET", "/futures/v1/aggs/NGX26")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(day_bar(900.0))
            .expect_at_least(1)
            .create();

        let mut adapter = MassiveAdapter::with_base_url("token", server.url());
        assert_eq!(adapter.front_month("NG", 1_788_652_800).unwrap(), "NGX26");
    }

    /// Spreads und Butterflies dürfen nie als Frontmonat herauskommen — sie
    /// stehen in der echten Liste ganz vorn.
    #[test]
    fn spreads_are_never_chosen() {
        assert!(is_outright("NGV26"));
        assert!(!is_outright("NG:BF F7-G7-H7"));
        assert!(!is_outright("NGF7-NGF8"));
    }

    /// Die Antwort kommt absteigend, auch wenn `order=asc` gesetzt ist.
    /// Jeder Konsument erwartet aufsteigend.
    #[test]
    fn bars_come_back_ascending_and_in_seconds() {
        let mut server = mockito::Server::new();
        let _c = server
            .mock("GET", "/futures/v1/contracts")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(contracts_body())
            .create();
        let _v = server
            .mock("GET", "/futures/v1/aggs/NGV26")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(day_bar(900.0))
            .expect_at_least(1)
            .create();
        let _x = server
            .mock("GET", "/futures/v1/aggs/NGX26")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(day_bar(1.0))
            .expect_at_least(1)
            .create();
        let _h = server
            .mock("GET", "/futures/v1/aggs/NGV26")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(
                r#"{"results":[
                    {"window_start":1788656400000000000,"open":3,"high":4,"low":2,"close":3.5,"volume":20},
                    {"window_start":1788652800000000000,"open":1,"high":2,"low":0.5,"close":1.5,"volume":10}
                ]}"#,
            )
            .create();

        let mut adapter = MassiveAdapter::with_base_url("token", server.url());
        let bars = adapter
            .fetch_historical("NG", Timeframe::Hour(1), 1_788_000_000, 1_789_000_000)
            .unwrap();
        assert_eq!(bars.len(), 2);
        assert!(
            bars[0].timestamp < bars[1].timestamp,
            "absteigend durchgereicht: {:?}",
            bars.iter().map(|b| b.timestamp).collect::<Vec<_>>()
        );
        // Sekunden, nicht Nanosekunden: 1788652800 ist 2026, 1.79e18 wäre es nicht.
        assert_eq!(bars[0].timestamp, 1_788_652_800);
        assert_eq!(bars[0].volume, 10.0);
    }

    /// Gegen die echte Schnittstelle. Läuft nur mit `MASSIVE_API_KEY` und
    /// einem Schlüssel mit **Futures**-Berechtigung — ohne die antworten die
    /// Endpunkte mit 403, und ein Aktien-Abo genügt nicht.
    ///
    /// `--ignored`, weil ein Test, der Netz und ein Abo braucht, nicht in
    /// einen normalen Lauf gehört. Er prüft das, was Attrappen nicht können:
    /// dass die echten Feldnamen, Einheiten und Ticker-Formen zu diesem Code
    /// passen.
    #[test]
    #[ignore = "braucht MASSIVE_API_KEY mit Futures-Berechtigung"]
    fn live_smoke_natural_gas() {
        let Ok(key) = std::env::var("MASSIVE_API_KEY") else {
            eprintln!("übersprungen: MASSIVE_API_KEY nicht gesetzt");
            return;
        };
        let mut adapter = MassiveAdapter::new(key);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let ticker = adapter.front_month("NG", now).expect("Frontmonat");
        assert!(
            ticker.starts_with("NG") && is_outright(&ticker),
            "unerwarteter Frontmonat: {ticker}"
        );

        let bars = adapter
            .fetch_historical("NG", Timeframe::Hour(1), now - 7 * 86_400, now)
            .expect("Stundenbars");
        assert!(bars.len() > 24, "nur {} Bars in sieben Tagen", bars.len());
        assert!(
            bars.windows(2).all(|w| w[0].timestamp < w[1].timestamp),
            "nicht aufsteigend"
        );
        // Sekunden, nicht Nanosekunden: alles über 10^12 wäre die falsche Einheit.
        assert!(
            bars.iter().all(|b| b.timestamp > 1_500_000_000 && b.timestamp < 10_000_000_000),
            "Zeitstempel nicht in Sekunden"
        );
        // Der ganze Zweck des Adapters: jede Bar trägt Volumen.
        assert!(
            bars.iter().all(|b| b.volume > 0.0),
            "{} von {} Bars ohne Volumen",
            bars.iter().filter(|b| b.volume <= 0.0).count(),
            bars.len()
        );
        // Und sie ist in sich stimmig.
        assert!(
            bars.iter().all(|b| b.low <= b.close && b.close <= b.high),
            "Schluss außerhalb der Spanne"
        );
    }

    /// Eine Liste ohne Einzelkontrakte muss einen Fehler ergeben, keine
    /// leere Bar-Liste — sonst liest der Aufrufer "keine Daten" statt
    /// "Auflösung gescheitert".
    #[test]
    fn a_listing_without_outrights_is_an_error() {
        let mut server = mockito::Server::new();
        let _c = server
            .mock("GET", "/futures/v1/contracts")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(r#"{"results":[{"ticker":"NG:BF F7-G7-H7","active":true}]}"#)
            .create();
        let mut adapter = MassiveAdapter::with_base_url("token", server.url());
        let err = adapter.front_month("NG", 1_788_652_800).unwrap_err();
        assert!(err.contains("Einzelkontrakt"), "Fehlertext: {err}");
    }
}
