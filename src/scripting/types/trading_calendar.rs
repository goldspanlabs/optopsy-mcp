//! US market (NYSE) trading calendar.
//!
//! Provides holiday detection, trading day checks, and Easter computation.
//! Covers the 10 standard NYSE holidays with observed-date rules for weekend
//! shifts. Does NOT cover early-close (half-day) sessions.

use chrono::{Datelike, NaiveDate, Weekday};

/// Returns true if the given date is a US market trading day
/// (not a weekend and not an NYSE-observed holiday).
pub fn is_trading_day(d: NaiveDate) -> bool {
    let wd = d.weekday();
    if wd == Weekday::Sat || wd == Weekday::Sun {
        return false;
    }
    !is_us_market_holiday(d)
}

/// Returns true if the given date is an NYSE-observed holiday.
///
/// Covers the 10 standard NYSE holidays with observed-date rules:
/// - New Year's Day (Jan 1, observed prev Fri if Sat, next Mon if Sun)
/// - MLK Day (3rd Monday of January)
/// - Presidents' Day (3rd Monday of February)
/// - Good Friday (Friday before Easter Sunday)
/// - Memorial Day (last Monday of May)
/// - Juneteenth (Jun 19, observed prev Fri if Sat, next Mon if Sun; since 2022)
/// - Independence Day (Jul 4, observed prev Fri if Sat, next Mon if Sun)
/// - Labor Day (1st Monday of September)
/// - Thanksgiving (4th Thursday of November)
/// - Christmas (Dec 25, observed prev Fri if Sat, next Mon if Sun)
pub fn is_us_market_holiday(d: NaiveDate) -> bool {
    let y = d.year();
    let m = d.month();
    let day = d.day();
    let wd = d.weekday();

    // Fixed holidays with weekend observation rules
    // New Year's Day
    if m == 1 && day == 1 && wd != Weekday::Sat && wd != Weekday::Sun {
        return true;
    }
    // New Year's observed on Friday Dec 31 (when Jan 1 is Saturday)
    if m == 12 && day == 31 && wd == Weekday::Fri {
        return true;
    }
    // New Year's observed on Monday Jan 2 (when Jan 1 is Sunday)
    if m == 1 && day == 2 && wd == Weekday::Mon {
        return true;
    }

    // Independence Day (Jul 4)
    if m == 7 {
        if day == 4 && wd != Weekday::Sat && wd != Weekday::Sun {
            return true;
        }
        if day == 3 && wd == Weekday::Fri {
            return true; // Jul 4 is Saturday → observed Friday Jul 3
        }
        if day == 5 && wd == Weekday::Mon {
            return true; // Jul 4 is Sunday → observed Monday Jul 5
        }
    }

    // Juneteenth (Jun 19, observed since 2022)
    if m == 6 && y >= 2022 {
        if day == 19 && wd != Weekday::Sat && wd != Weekday::Sun {
            return true;
        }
        if day == 18 && wd == Weekday::Fri {
            return true;
        }
        if day == 20 && wd == Weekday::Mon {
            return true;
        }
    }

    // Christmas (Dec 25)
    if m == 12 {
        if day == 25 && wd != Weekday::Sat && wd != Weekday::Sun {
            return true;
        }
        if day == 24 && wd == Weekday::Fri {
            return true; // Dec 25 is Saturday → observed Friday Dec 24
        }
        if day == 26 && wd == Weekday::Mon {
            return true; // Dec 25 is Sunday → observed Monday Dec 26
        }
    }

    // MLK Day — 3rd Monday of January
    if m == 1 && wd == Weekday::Mon && (15..=21).contains(&day) {
        return true;
    }

    // Presidents' Day — 3rd Monday of February
    if m == 2 && wd == Weekday::Mon && (15..=21).contains(&day) {
        return true;
    }

    // Memorial Day — last Monday of May
    if m == 5 && wd == Weekday::Mon && (25..=31).contains(&day) {
        return true;
    }

    // Labor Day — 1st Monday of September
    if m == 9 && wd == Weekday::Mon && (1..=7).contains(&day) {
        return true;
    }

    // Thanksgiving — 4th Thursday of November
    if m == 11 && wd == Weekday::Thu && (22..=28).contains(&day) {
        return true;
    }

    // Good Friday — Friday before Easter Sunday (computus algorithm)
    if wd == Weekday::Fri {
        if let Some(easter) = easter_sunday(y) {
            let good_friday = easter - chrono::Duration::days(2);
            if d == good_friday {
                return true;
            }
        }
    }

    false
}

/// Compute Easter Sunday for a given year using the anonymous Gregorian algorithm.
fn easter_sunday(year: i32) -> Option<NaiveDate> {
    let golden = year % 19;
    let century = year / 100;
    let year_in_century = year % 100;
    let leap_correction = century / 4;
    let leap_remainder = century % 4;
    let moon_correction = (century + 8) / 25;
    let epact_correction = (century - moon_correction + 1) / 3;
    let epact = (19 * golden + century - leap_correction - epact_correction + 15) % 30;
    let quarter = year_in_century / 4;
    let year_remainder = year_in_century % 4;
    let weekday_shift = (32 + 2 * leap_remainder + 2 * quarter - epact - year_remainder) % 7;
    let lunar_correction = (golden + 11 * epact + 22 * weekday_shift) / 451;
    let month = (epact + weekday_shift - 7 * lunar_correction + 114) / 31;
    let day = ((epact + weekday_shift - 7 * lunar_correction + 114) % 31) + 1;
    NaiveDate::from_ymd_opt(year, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_years_day() {
        // 2024: Jan 1 is Monday — holiday
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        ));
        // 2023: Jan 1 is Sunday — observed Monday Jan 2
        assert!(!is_us_market_holiday(
            NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()
        ));
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2023, 1, 2).unwrap()
        ));
        // 2022: Jan 1 is Saturday — observed Friday Dec 31, 2021
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2021, 12, 31).unwrap()
        ));
    }

    #[test]
    fn test_mlk_day() {
        // 2024: 3rd Monday of January = Jan 15
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
        ));
        assert!(!is_us_market_holiday(
            NaiveDate::from_ymd_opt(2024, 1, 8).unwrap()
        ));
        // 2025: Jan 20
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2025, 1, 20).unwrap()
        ));
    }

    #[test]
    fn test_presidents_day() {
        // 2024: 3rd Monday of February = Feb 19
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2024, 2, 19).unwrap()
        ));
        // 2023: Feb 20
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2023, 2, 20).unwrap()
        ));
    }

    #[test]
    fn test_good_friday() {
        // 2024: Easter is March 31 → Good Friday is March 29
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2024, 3, 29).unwrap()
        ));
        // 2023: Easter is April 9 → Good Friday is April 7
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2023, 4, 7).unwrap()
        ));
        // 2025: Easter is April 20 → Good Friday is April 18
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2025, 4, 18).unwrap()
        ));
    }

    #[test]
    fn test_memorial_day() {
        // 2024: last Monday of May = May 27
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2024, 5, 27).unwrap()
        ));
        // 2023: May 29
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2023, 5, 29).unwrap()
        ));
    }

    #[test]
    fn test_juneteenth() {
        // 2024: June 19 is Wednesday — holiday
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2024, 6, 19).unwrap()
        ));
        // 2022: June 19 is Sunday → observed Monday June 20
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2022, 6, 20).unwrap()
        ));
        // 2021: Not observed yet (before 2022)
        assert!(!is_us_market_holiday(
            NaiveDate::from_ymd_opt(2021, 6, 18).unwrap()
        ));
    }

    #[test]
    fn test_independence_day() {
        // 2024: Jul 4 is Thursday — holiday
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2024, 7, 4).unwrap()
        ));
        // 2020: Jul 4 is Saturday → observed Friday Jul 3
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2020, 7, 3).unwrap()
        ));
        // 2021: Jul 4 is Sunday → observed Monday Jul 5
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2021, 7, 5).unwrap()
        ));
    }

    #[test]
    fn test_labor_day() {
        // 2024: 1st Monday of September = Sep 2
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2024, 9, 2).unwrap()
        ));
        // 2023: Sep 4
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2023, 9, 4).unwrap()
        ));
    }

    #[test]
    fn test_thanksgiving() {
        // 2024: 4th Thursday of November = Nov 28
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2024, 11, 28).unwrap()
        ));
        // 2023: Nov 23
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2023, 11, 23).unwrap()
        ));
    }

    #[test]
    fn test_christmas() {
        // 2024: Dec 25 is Wednesday — holiday
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2024, 12, 25).unwrap()
        ));
        // 2021: Dec 25 is Saturday → observed Friday Dec 24
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2021, 12, 24).unwrap()
        ));
        // 2022: Dec 25 is Sunday → observed Monday Dec 26
        assert!(is_us_market_holiday(
            NaiveDate::from_ymd_opt(2022, 12, 26).unwrap()
        ));
    }

    #[test]
    fn test_regular_days_not_holidays() {
        let normal_days = vec![
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 7, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 12, 24).unwrap(),
        ];
        for d in normal_days {
            assert!(!is_us_market_holiday(d), "{d} should not be a holiday");
        }
    }

    #[test]
    fn test_is_trading_day() {
        assert!(is_trading_day(NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()));
        assert!(!is_trading_day(
            NaiveDate::from_ymd_opt(2024, 1, 6).unwrap()
        )); // Saturday
        assert!(!is_trading_day(
            NaiveDate::from_ymd_opt(2024, 1, 7).unwrap()
        )); // Sunday
        assert!(!is_trading_day(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
        )); // MLK Day
    }

    #[test]
    fn test_easter_known_dates() {
        assert_eq!(easter_sunday(2020), NaiveDate::from_ymd_opt(2020, 4, 12));
        assert_eq!(easter_sunday(2021), NaiveDate::from_ymd_opt(2021, 4, 4));
        assert_eq!(easter_sunday(2022), NaiveDate::from_ymd_opt(2022, 4, 17));
        assert_eq!(easter_sunday(2023), NaiveDate::from_ymd_opt(2023, 4, 9));
        assert_eq!(easter_sunday(2024), NaiveDate::from_ymd_opt(2024, 3, 31));
        assert_eq!(easter_sunday(2025), NaiveDate::from_ymd_opt(2025, 4, 20));
        // Edge: earliest possible Easter (March 22)
        assert_eq!(easter_sunday(2285), NaiveDate::from_ymd_opt(2285, 3, 22));
    }
}
