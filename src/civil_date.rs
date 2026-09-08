//! Shared Unix-timestamp/civil-date conversion helpers, used by provider adapters that need to
//! format or parse calendar dates for REST query parameters (EODHD's `YYYY-MM-DD`, Capital.com's
//! `YYYY-MM-DDTHH:MM:SS`). Not part of this crate's public API.

// Civil-date conversions by Howard Hinnant, adapted to Unix epoch day zero. `kestrel-chartkit`'s
// `src/timeframe.rs` implements the same algorithm privately; duplicated here since it isn't
// exported from that crate.
pub(crate) fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (
        i32::try_from(year).unwrap_or(1970),
        u32::try_from(month).unwrap_or(1),
        u32::try_from(day).unwrap_or(1),
    )
}

// Nur `capitalcom` und `eodhd` rechnen in diese Richtung; `massive` liefert
// bereits Zeitstempel und braucht nur den Rückweg. Ohne das Attribut warnt
// ein Build mit `--features massive` allein über toten Code.
#[cfg_attr(
    not(any(feature = "capitalcom", feature = "eodhd")),
    allow(dead_code)
)]
pub(crate) fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let adjusted_year = i64::from(year) - i64::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let yoe = adjusted_year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let doy = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
