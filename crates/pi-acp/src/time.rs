//! Small time helpers.
//!
//! The ACP surface carries ISO 8601 UTC timestamps (`session_info_update.updatedAt`,
//! `session-map.json` `updatedAt`, ...). Rather than pulling in a date crate for
//! a single formatter, this module converts `SystemTime` to the RFC 3339 subset
//! the TS reference emits (`new Date().toISOString()`: `YYYY-MM-DDTHH:MM:SS.sssZ`).

/// Current UTC time as an ISO 8601 / RFC 3339 string with milliseconds
/// (`YYYY-MM-DDTHH:MM:SS.sssZ`, exactly like JS `toISOString()`).
///
/// Falls back to the UNIX epoch instant on system-clock errors (never panics).
pub fn utc_now_iso8601() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format_epoch_millis(now.as_millis())
}

/// Format an epoch-millis count as `YYYY-MM-DDTHH:MM:SS.sssZ`.
pub fn format_epoch_millis(millis: u128) -> String {
    let total_secs = millis / 1000;
    let ms = (millis % 1000) as u32;
    let days = total_secs / 86_400;
    let secs_of_day = total_secs % 86_400;
    let (y, m, d) = civil_from_days(days as i64);
    let hh = secs_of_day / 3600;
    let mm = (secs_of_day % 3600) / 60;
    let ss = secs_of_day % 60;
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{ms:03}Z")
}

/// Days since 1970-01-01 → (year, month, day). Howard Hinnant's civil
/// calendar algorithm (public domain).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m as u32, d as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_known_epoch_millis() {
        // 2026-08-31T08:52:10.000Z
        assert_eq!(
            format_epoch_millis(1_788_166_330_000),
            "2026-08-31T08:52:10.000Z"
        );
        // 2026-01-01T00:00:00.000Z
        assert_eq!(
            format_epoch_millis(1_767_225_600_000),
            "2026-01-01T00:00:00.000Z"
        );
        // 1970-01-01T00:00:00.000Z
        assert_eq!(format_epoch_millis(0), "1970-01-01T00:00:00.000Z");
        // 1999-12-31T23:59:59.999Z
        assert_eq!(
            format_epoch_millis(946_684_799_999),
            "1999-12-31T23:59:59.999Z"
        );
        // 2000-02-29 (leap year)
        assert_eq!(
            format_epoch_millis(951_782_400_000),
            "2000-02-29T00:00:00.000Z"
        );
        // 2024-02-29 (leap year)
        assert_eq!(
            format_epoch_millis(1_709_164_800_000),
            "2024-02-29T00:00:00.000Z"
        );
    }

    #[test]
    fn utc_now_iso8601_is_recent_and_stable_shape() {
        let s = utc_now_iso8601();
        assert!(s.ends_with('Z'), "expected UTC Z suffix, got {s}");
        assert_eq!(s.len(), 24, "YYYY-MM-DDTHH:MM:SS.sssZ is 24 chars, got {s}");
        assert!(s.starts_with("20"), "expected a 2000+ year, got {s}");
    }
}
