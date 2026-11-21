//! Roll rule for continuous futures series, without any HTTP: which contract counts for which
//! trading session, decided from daily volumes alone. Pure and `wasm32`-capable, so a caller
//! that fetches the volumes elsewhere (e.g. a Worker) applies exactly the same rule as
//! [`crate::massive`] (feature `massive`).

use std::collections::BTreeMap;

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
pub fn roll_schedule(
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
