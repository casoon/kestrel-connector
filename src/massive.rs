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

use std::collections::{BTreeMap, HashMap};

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

/// So viele Seiten liest eine Kontraktliste höchstens. Die Liste ist
/// alphabetisch: bei Erdgas stehen die Frontmonate (`NGV26`, `NGX26`) erst
/// auf der dritten Seite, hinter Spreads und den Januar-Kontrakten ferner
/// Jahre (`NGF27`). Wer früher aufhört, wählt unter den Kontrakten, die er
/// zufällig gesehen hat — und fand so bis 2026-10 den falschen.
const LISTING_PAGES: usize = 12;

/// Wie viele frühere Tage eine Kontraktliste nachgeschlagen wird, wenn sie
/// für den gefragten Tag noch leer ist. Kurz nach UTC-Mitternacht ist sie
/// es: der Collector meldete jede Nacht zwischen 00 und 04 Uhr „keinen
/// aktiven Einzelkontrakt".
const LISTING_FALLBACK_DAYS: i64 = 3;

/// Spanne einer Abfrage in Tagen, wenn der Bereich größer ist. Eine Seite
/// fasst 50 000 Bars; ein Kontrakt hat auf 15m knapp 100 am Tag.
const CHUNK_DAYS: i64 = 180;

/// Wie viele nächstfällige Kontrakte je Stichtag als Kandidaten der
/// fortlaufenden Reihe gelten.
const ROLL_CANDIDATES: usize = 6;

/// Abstand der Stichtage, an denen die Kontraktliste für eine fortlaufende
/// Reihe gelesen wird. Ein Kontrakt trägt etwa einen Monat lang Volumen.
const ROLL_SAMPLE_DAYS: i64 = 28;

/// Vorlauf vor dem gefragten Bereich, damit der erste Tag einen Vortag
/// hat, an dessen Volumen die Wahl hängt.
const ROLL_LEAD_DAYS: i64 = 10;

/// Wie viele der nächstfälligen Kontrakte auf Volumen geprüft werden. Der
/// liquideste liegt immer unter den vordersten; alles dahinter kostete nur
/// Abrufe.
const FRONT_MONTH_CANDIDATES: usize = 4;

/// Ein Kontrakt der Terminkurve: Ticker, letzter Handelstag
/// (`YYYY-MM-DD`) und jüngster Tagesschluss.
#[derive(Debug, Clone, PartialEq)]
pub struct TerminKontrakt {
    pub ticker: String,
    pub last_trade_date: String,
    pub close: Option<f64>,
}

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

        // Ein leeres Ergebnis ist ein Fehler, kein „keine Bars": der
        // Aufrufer läse es sonst als Datenlücke, dabei ist die Auflösung
        // gescheitert.
        let day = format_date(now);
        let (_, mut candidates) = self.outrights_near(product, now)?;

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
            let volume = self.volume_before(&contract.ticker, &day).unwrap_or(0.0);
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

    /// Die vordersten `n` aktiven Einzelkontrakte eines Produkts nach
    /// Verfall, je mit jüngstem Tagesschluss — die Terminkurve, aus der ein
    /// Spot-CFD seine tägliche Prämienanpassung ableitet (Kestrel plan/65).
    /// Ein Kontrakt ohne Tagesbar trägt `close: None`, statt die Kurve zu
    /// verwerfen.
    pub fn terminkurve(
        &self,
        product: &str,
        now: i64,
        n: usize,
    ) -> Result<Vec<TerminKontrakt>, String> {
        // Alle Seiten: die Liste ist alphabetisch, `NGF27` steht vor `NGX26`.
        // Nach der ersten Seite mit Einzelkontrakten aufzuhören (wie
        // `list_outrights`) ergäbe hier Januar/Februar statt der vordersten
        // Fälligkeiten.
        let mut vertraege = self.list_outrights(product, &format_date(now))?;
        vertraege.sort_by(|a, b| a.last_trade_date.cmp(&b.last_trade_date));
        vertraege.truncate(n);
        let mut out = Vec::with_capacity(vertraege.len());
        for v in vertraege {
            // Zehn Tage zurück: über ein Wochenende mit Feiertag reicht ein
            // kürzeres Fenster nicht.
            let close = self
                .fetch_aggs(&v.ticker, "1day", Some(now - 10 * 86_400), None, 20)
                .ok()
                .and_then(|bars| bars.last().map(|b| b.close));
            out.push(TerminKontrakt {
                ticker: v.ticker,
                last_trade_date: v.last_trade_date,
                close,
            });
        }
        Ok(out)
    }

    /// Alle aktiven Einzelkontrakte eines Produkts an einem Tag.
    ///
    /// Liest die **ganze** Liste (bis [`LISTING_PAGES`]). Sie ist
    /// alphabetisch, und `NG:BF …` sowie `NGF7-NGF8` sortieren vor `NGV26` —
    /// bei Erdgas stand auf den ersten beiden Seiten zu je 250 Einträgen kein
    /// einziger Einzelkontrakt, auf der dritten zuerst die fernen
    /// Januar-Kontrakte. Nach den ersten Einzelkontrakten aufzuhören ergab
    /// eine leere Auswahl oder den falschen Frontmonat.
    fn list_outrights(&self, product: &str, day: &str) -> Result<Vec<ContractRow>, String> {
        let mut out: Vec<ContractRow> = Vec::new();
        let mut next: Option<String> = None;
        for _ in 0..LISTING_PAGES {
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
            match listing.next_url {
                Some(url) if !empty => next = Some(url),
                _ => break,
            }
        }
        Ok(out)
    }

    /// Die Einzelkontrakte für den Tag von `now`, bei leerer Liste für einen
    /// der letzten [`LISTING_FALLBACK_DAYS`] Tage davor. Gibt den Tag mit
    /// zurück, für den die Liste tatsächlich galt.
    fn outrights_near(
        &self,
        product: &str,
        now: i64,
    ) -> Result<(String, Vec<ContractRow>), String> {
        for back in 0..=LISTING_FALLBACK_DAYS {
            let day = format_date(now - back * 86_400);
            let rows = self.list_outrights(product, &day)?;
            if !rows.is_empty() {
                return Ok((day, rows));
            }
        }
        Err(format!(
            "Massive führt für '{product}' am {} keinen aktiven Einzelkontrakt \
             (die Liste enthält nur Spreads, oder der Produktcode stimmt nicht)",
            format_date(now)
        ))
    }

    /// Volumen der jüngsten **abgeschlossenen** Handelssitzung vor `day` —
    /// das Maß, an dem der Frontmonat hängt. Die laufende Sitzung zählt
    /// nicht: kurz nach Mitternacht ist sie erst wenige Minuten alt, und ein
    /// Kontrakt, der gerade erst eröffnet hat, wirkte dann wie der
    /// schwächste.
    fn volume_before(&self, ticker: &str, day: &str) -> Result<f64, String> {
        let mut rows = self.fetch_rows(ticker, "1day", None, None, 5)?;
        rows.sort_by_key(|r| r.window_start);
        let closed = rows.iter().rev().find(|r| r.day().as_str() < day);
        // Ohne Sitzungsangabe (oder ohne Vortag) bleibt die jüngste Bar.
        Ok(closed.or(rows.last()).map(|r| r.volume).unwrap_or(0.0))
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
        Ok(self
            .fetch_rows(ticker, resolution, from, to, limit)?
            .iter()
            .map(AggRow::bar)
            .collect())
    }

    /// Wie [`Self::fetch_aggs`], aber mit der Sitzung jeder Bar, aufsteigend.
    fn fetch_rows(
        &self,
        ticker: &str,
        resolution: &str,
        from: Option<i64>,
        to: Option<i64>,
        limit: usize,
    ) -> Result<Vec<AggRow>, String> {
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
        let mut page: AggPage = req
            .call()
            .map_err(|e| format!("Massive-Aggregate für '{ticker}' fehlgeschlagen: {e}"))?
            .body_mut()
            .read_json()
            .map_err(|e| format!("Massive-Aggregate für '{ticker}' unlesbar: {e}"))?;
        // Selbst sortieren: `order=asc` wird ignoriert.
        page.results.sort_by_key(|r| r.window_start);
        Ok(page.results)
    }

    /// Alle Bars eines Bereichs in Abschnitten von [`CHUNK_DAYS`] —
    /// eine Seite fasst 50 000, zwei Jahre 15m eines langen Kontrakts auch.
    fn fetch_rows_range(
        &self,
        ticker: &str,
        resolution: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<AggRow>, String> {
        let mut out: Vec<AggRow> = Vec::new();
        let mut start = from;
        while start < to {
            let end = (start + CHUNK_DAYS * 86_400).min(to);
            out.extend(self.fetch_rows(ticker, resolution, Some(start), Some(end), 50_000)?);
            start = end;
        }
        Ok(out)
    }

    /// Die Bars **eines** Einzelkontrakts (`NGX26`, nicht `NG`) für
    /// `[from, to)` in Sekunden, aufsteigend. Rohdaten, ohne Rollregel — die
    /// Grundlage, aus der sich jede fortlaufende Reihe neu bauen lässt.
    pub fn contract_bars(
        &self,
        ticker: &str,
        timeframe: Timeframe,
        from: i64,
        to: i64,
    ) -> Result<Vec<Bar>, String> {
        let resolution = resolution_of(timeframe)?;
        Ok(self
            .fetch_rows_range(ticker, resolution, from, to)?
            .iter()
            .map(AggRow::bar)
            .collect())
    }

    /// Fortlaufende Reihe eines Produkts (`NG`) für `[from, to)`: je
    /// Handelssitzung die Bars des Kontrakts, den [`roll_schedule`] wählt.
    ///
    /// Statt eines einzigen Kontrakts für den ganzen Bereich — das ergab für
    /// die Vergangenheit einen Kontrakt, der damals noch gar nicht liquide
    /// war (Natural Gas: Monatsmittel 4 → 425 Kontrakte je Stunde, der
    /// Lebenslauf eines Kontrakts).
    pub fn continuous(
        &self,
        product: &str,
        timeframe: Timeframe,
        from: i64,
        to: i64,
    ) -> Result<Vec<Bar>, String> {
        let resolution = resolution_of(timeframe)?;

        // Kandidaten: die nächstfälligen Kontrakte an Stichtagen über den
        // Bereich. Eine Liste an einem Tag kennt nur, was dann aktiv ist.
        let mut by_ticker: BTreeMap<String, String> = BTreeMap::new();
        let mut sample = from;
        loop {
            let at = sample.min(to);
            if let Ok((day, mut rows)) = self.outrights_near(product, at) {
                rows.retain(|c| c.last_trade_date.as_str() >= day.as_str());
                rows.sort_by(|a, b| a.last_trade_date.cmp(&b.last_trade_date));
                for c in rows.into_iter().take(ROLL_CANDIDATES) {
                    by_ticker.insert(c.ticker, c.last_trade_date);
                }
            }
            if at >= to {
                break;
            }
            sample += ROLL_SAMPLE_DAYS * 86_400;
        }
        if by_ticker.is_empty() {
            return Err(format!(
                "Massive führt für '{product}' im Bereich {} … {} keinen aktiven \
                 Einzelkontrakt (die Liste enthält nur Spreads, oder der Produktcode \
                 stimmt nicht)",
                format_date(from),
                format_date(to)
            ));
        }
        let mut contracts: Vec<(String, String)> = by_ticker.into_iter().collect();
        contracts.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        // Tagesvolumen je Kontrakt, mit Vorlauf für den Vortag des ersten Tages.
        let lead_from = from - ROLL_LEAD_DAYS * 86_400;
        let mut volumes: Vec<BTreeMap<String, f64>> = Vec::with_capacity(contracts.len());
        for (ticker, _) in &contracts {
            let rows = self.fetch_rows_range(ticker, "1day", lead_from, to)?;
            volumes.push(rows.iter().map(|r| (r.day(), r.volume)).collect());
        }
        let mut sessions: Vec<String> = volumes
            .iter()
            .flat_map(|v| v.keys().cloned())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        sessions.sort();
        let expiries: Vec<String> = contracts.iter().map(|(_, e)| e.clone()).collect();
        let schedule = roll_schedule(&expiries, &volumes, &sessions);

        let mut bars: Vec<Bar> = Vec::new();
        for (idx, (ticker, _)) in contracts.iter().enumerate() {
            let days: std::collections::BTreeSet<&String> = schedule
                .iter()
                .filter(|(_, k)| **k == idx)
                .map(|(d, _)| d)
                .collect();
            if days.is_empty() {
                continue;
            }
            for row in self.fetch_rows_range(ticker, resolution, from, to)? {
                if days.contains(&row.day()) {
                    bars.push(row.bar());
                }
            }
        }
        bars.sort_by_key(|b| b.timestamp);
        bars.dedup_by_key(|b| b.timestamp);
        Ok(bars)
    }
}

/// Welcher Kontrakt an welcher Handelssitzung gilt — die **Rollregel v1**.
///
/// - Maßgeblich ist das Volumen der **vorangegangenen** Sitzung. Das von
///   heute zu nehmen hieße, am Morgen zu wissen, wohin die Liquidität am
///   Abend wandert: im Backtest ein Blick in die Zukunft.
/// - Unter den noch handelbaren Kontrakten (`Verfall >= Tag`) gewinnt das
///   höchste Volumen; bei Gleichstand der nähere Verfall.
/// - Ein Rollen zurück zu einem früheren Verfall gibt es nicht, solange der
///   zuletzt gewählte Kontrakt noch handelbar ist — gegen Hin- und Herspringen
///   um den Rolltermin.
/// - Ohne Vortag (erste Sitzung) gilt der nächstfällige.
///
/// `expiries` ist aufsteigend sortiert (`YYYY-MM-DD`), `volumes[k]` das
/// Tagesvolumen des Kontrakts `k` je Sitzungstag, `sessions` aufsteigend.
pub(crate) fn roll_schedule(
    expiries: &[String],
    volumes: &[BTreeMap<String, f64>],
    sessions: &[String],
) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    let mut last: Option<usize> = None;
    for (i, day) in sessions.iter().enumerate() {
        let alive: Vec<usize> = (0..expiries.len())
            .filter(|&k| expiries[k].as_str() >= day.as_str())
            .collect();
        let Some(&first) = alive.first() else {
            continue;
        };
        let mut best = first;
        if i > 0 {
            let prev = &sessions[i - 1];
            let volume = |k: usize| volumes[k].get(prev).copied().unwrap_or(0.0);
            let mut best_volume = volume(best);
            for &k in &alive[1..] {
                if volume(k) > best_volume {
                    best = k;
                    best_volume = volume(k);
                }
            }
        }
        if let Some(l) = last {
            if l > best && expiries[l].as_str() >= day.as_str() {
                best = l;
            }
        }
        last = Some(best);
        out.insert(day.clone(), best);
    }
    out
}

/// Einzelkontrakte tragen weder `:` (Butterfly `NG:BF F7-G7-H7`) noch `-`
/// (Kalenderspread `NGF7-NGF8`).
fn is_outright(ticker: &str) -> bool {
    !ticker.contains(':') && !ticker.contains('-')
}

fn wall_clock() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
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
        self.continuous(symbol, timeframe, from, to)
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
            // Die Uhr, nicht der zuletzt gesehene Zeitstempel: beim ersten
            // Aufruf war der 0, und die Kontraktliste wurde für den
            // 1970-01-01 gelesen.
            let now = wall_clock();
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
    /// Handelssitzung, zu der die Bar gehört (`YYYY-MM-DD`). Eine CME-
    /// Sitzung beginnt am Vorabend; der UTC-Tag der Bar trennt sie falsch.
    #[serde(default)]
    session_end_date: Option<String>,
}

impl AggRow {
    /// Die Sitzung; ohne Angabe der UTC-Tag der Bar.
    fn day(&self) -> String {
        self.session_end_date
            .clone()
            .unwrap_or_else(|| format_date(self.window_start / 1_000_000_000))
    }

    fn bar(&self) -> Bar {
        Bar {
            // Nanosekunden -> Sekunden, siehe Modulkopf.
            timestamp: self.window_start / 1_000_000_000,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
        }
    }
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

    /// Die Terminkurve liest alle Seiten (die Liste ist alphabetisch, `NGF27`
    /// vor `NGX26`), ordnet nach Verfall und trägt je Kontrakt den jüngsten
    /// Schluss.
    #[test]
    fn terminkurve_nach_verfall_mit_schluss() {
        let mut server = mockito::Server::new();
        let _c = server
            .mock("GET", "/futures/v1/contracts")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(format!(
                r#"{{"results":[
                {{"ticker":"NGF7-NGF8","active":true}},
                {{"ticker":"NGF27","active":true,"last_trade_date":"2026-12-29"}}
            ],"next_url":"{}/seite2"}}"#,
                server.url()
            ))
            .create();
        let _seite2 = server
            .mock("GET", "/seite2")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(
                r#"{"results":[
                {"ticker":"NGZ26","active":true,"last_trade_date":"2026-11-25"},
                {"ticker":"NGX26","active":true,"last_trade_date":"2026-10-28"}
            ]}"#,
            )
            .create();
        let _x = server
            .mock("GET", "/futures/v1/aggs/NGX26")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(day_bar(10.0))
            .create();
        let _z = server
            .mock("GET", "/futures/v1/aggs/NGZ26")
            .match_query(mockito::Matcher::Any)
            .with_status(500)
            .create();
        let adapter = MassiveAdapter::with_base_url("token", server.url());
        let kurve = adapter.terminkurve("NG", 1_791_028_800, 2).unwrap();
        assert_eq!(kurve.len(), 2);
        assert_eq!(kurve[0].ticker, "NGX26");
        assert_eq!(kurve[0].last_trade_date, "2026-10-28");
        assert_eq!(kurve[0].close, Some(1.5));
        assert_eq!(kurve[1].ticker, "NGZ26");
        assert_eq!(
            kurve[1].close, None,
            "ein fehlender Schluss verwirft die Kurve nicht"
        );
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
    /// Jeder Konsument erwartet aufsteigend — und Sekunden statt Nanosekunden.
    #[test]
    fn bars_come_back_ascending_and_in_seconds() {
        let mut server = mockito::Server::new();
        let _c = server
            .mock("GET", "/futures/v1/contracts")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(
                r#"{"results":[{"ticker":"NGV26","active":true,"last_trade_date":"2026-12-29"}]}"#,
            )
            .create();
        let _d = server
            .mock("GET", "/futures/v1/aggs/NGV26")
            .match_query(mockito::Matcher::UrlEncoded(
                "resolution".into(),
                "1day".into(),
            ))
            .with_status(200)
            .with_body(day_bar(900.0))
            .create();
        let _h = server
            .mock("GET", "/futures/v1/aggs/NGV26")
            .match_query(mockito::Matcher::UrlEncoded(
                "resolution".into(),
                "1hour".into(),
            ))
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

    fn sessions(days: &[&str]) -> Vec<String> {
        days.iter().map(|d| d.to_string()).collect()
    }

    fn volumes(rows: &[&[(&str, f64)]]) -> Vec<BTreeMap<String, f64>> {
        rows.iter()
            .map(|r| r.iter().map(|(d, v)| (d.to_string(), *v)).collect())
            .collect()
    }

    /// Das Volumen von **heute** darf die Wahl von heute nicht bestimmen:
    /// `B` überholt `A` am 03., gewählt wird `B` aber erst am 04. — am 03.
    /// hätte der Backtest sonst am Morgen gewusst, was am Abend geschieht.
    #[test]
    fn roll_follows_yesterdays_volume_not_todays() {
        let expiries = sessions(&["2026-10-28", "2026-11-25"]);
        let v = volumes(&[
            &[
                ("2026-10-01", 90.0),
                ("2026-10-02", 80.0),
                ("2026-10-03", 40.0),
            ],
            &[
                ("2026-10-01", 10.0),
                ("2026-10-02", 20.0),
                ("2026-10-03", 90.0),
            ],
        ]);
        let days = sessions(&["2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04"]);
        let plan = roll_schedule(&expiries, &v, &days);
        assert_eq!(plan["2026-10-02"], 0);
        assert_eq!(plan["2026-10-03"], 0, "B ist erst am 03. größer");
        assert_eq!(plan["2026-10-04"], 1);
    }

    /// Nach einem Rollen springt die Reihe nicht zurück, wenn der frühere
    /// Kontrakt kurz wieder mehr Volumen trägt.
    #[test]
    fn roll_never_goes_back_to_an_earlier_expiry() {
        let expiries = sessions(&["2026-10-28", "2026-11-25"]);
        let v = volumes(&[
            &[
                ("2026-10-01", 90.0),
                ("2026-10-02", 10.0),
                ("2026-10-03", 95.0),
            ],
            &[
                ("2026-10-01", 10.0),
                ("2026-10-02", 90.0),
                ("2026-10-03", 20.0),
            ],
        ]);
        let days = sessions(&["2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04"]);
        let plan = roll_schedule(&expiries, &v, &days);
        assert_eq!(plan["2026-10-03"], 1);
        assert_eq!(plan["2026-10-04"], 1, "kein Zurückspringen");
    }

    /// Ein abgelaufener Kontrakt kommt nicht mehr in Frage, auch wenn sein
    /// Volumen am Vortag das größte war.
    #[test]
    fn roll_skips_expired_contracts() {
        let expiries = sessions(&["2026-10-02", "2026-11-25"]);
        let v = volumes(&[&[("2026-10-02", 500.0)], &[("2026-10-02", 10.0)]]);
        let days = sessions(&["2026-10-02", "2026-10-05"]);
        let plan = roll_schedule(&expiries, &v, &days);
        assert_eq!(plan["2026-10-05"], 1);
    }

    /// Der Frontmonat steht auf der **dritten** Seite der alphabetischen
    /// Liste, hinter Spreads und dem Januar-Kontrakt ferner Jahre. Wer nach
    /// der ersten Seite mit Einzelkontrakten aufhört, wählt `NGF27` — das
    /// war der Fehler, der monatelang das Volumen eines Kontrakts mit einem
    /// Zehntel des Umsatzes lieferte.
    #[test]
    fn front_month_is_found_beyond_the_first_page_with_outrights() {
        let mut server = mockito::Server::new();
        let _c = server
            .mock("GET", "/futures/v1/contracts")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(format!(
                r#"{{"results":[
                    {{"ticker":"NGF7-NGF8","active":true}},
                    {{"ticker":"NGF27","active":true,"last_trade_date":"2026-12-29"}}
                ],"next_url":"{}/seite2"}}"#,
                server.url()
            ))
            .create();
        let _s2 = server
            .mock("GET", "/seite2")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(
                r#"{"results":[
                    {"ticker":"NGV26","active":false,"last_trade_date":"2026-09-28"},
                    {"ticker":"NGX26","active":true,"last_trade_date":"2026-10-28"},
                    {"ticker":"NGZ26","active":true,"last_trade_date":"2026-11-25"}
                ]}"#,
            )
            .create();
        for (ticker, volume) in [("NGF27", 5.0), ("NGX26", 900.0), ("NGZ26", 200.0)] {
            server
                .mock("GET", format!("/futures/v1/aggs/{ticker}").as_str())
                .match_query(mockito::Matcher::Any)
                .with_status(200)
                .with_body(day_bar(volume))
                .create();
        }
        let mut adapter = MassiveAdapter::with_base_url("token", server.url());
        assert_eq!(adapter.front_month("NG", 1_788_652_800).unwrap(), "NGX26");
    }

    /// Kurz nach Mitternacht UTC ist die Kontraktliste für den neuen Tag
    /// noch leer; der Vortag trägt sie. Ohne den Rückgriff meldete der
    /// Collector jede Nacht „keinen aktiven Einzelkontrakt".
    #[test]
    fn an_empty_listing_for_today_falls_back_to_yesterday() {
        let mut server = mockito::Server::new();
        // 2026-09-06 (Tag von 1_788_652_800) leer, 2026-09-05 gefüllt.
        let _leer = server
            .mock("GET", "/futures/v1/contracts")
            .match_query(mockito::Matcher::UrlEncoded(
                "date".into(),
                "2026-09-06".into(),
            ))
            .with_status(200)
            .with_body(r#"{"results":[]}"#)
            .create();
        let _voll = server
            .mock("GET", "/futures/v1/contracts")
            .match_query(mockito::Matcher::UrlEncoded(
                "date".into(),
                "2026-09-05".into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"results":[{"ticker":"NGX26","active":true,"last_trade_date":"2026-10-28"}]}"#,
            )
            .create();
        let _a = server
            .mock("GET", "/futures/v1/aggs/NGX26")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(day_bar(100.0))
            .create();
        let mut adapter = MassiveAdapter::with_base_url("token", server.url());
        assert_eq!(adapter.front_month("NG", 1_788_652_800).unwrap(), "NGX26");
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
            bars.iter()
                .all(|b| b.timestamp > 1_500_000_000 && b.timestamp < 10_000_000_000),
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
