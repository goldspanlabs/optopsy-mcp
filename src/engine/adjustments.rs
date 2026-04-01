//! Compute cumulative adjustment factors from splits and dividends.
//!
//! Adjustment factors convert raw (unadjusted) prices to adjusted prices:
//! `adjusted_price = raw_price * factor`. The factor is 1.0 at present day
//! and decreases going backwards in time as splits/dividends are encountered.
//!
//! Each entry stores `(event_date, factor_for_dates_before_this_event)`.
//! For dates on or after the newest event, the factor is 1.0.

use chrono::NaiveDate;

use crate::data::adjustment_store::{DividendRow, SplitRow};

/// A timeline of cumulative adjustment factors, sorted chronologically.
///
/// `adjusted = raw * factor_at(date)`:
/// - 1.0 for dates on/after the most recent event (current period)
/// - Decreasing for older dates as more splits/dividends are accumulated
#[derive(Debug, Clone)]
pub struct AdjustmentTimeline {
    /// `(event_date, factor_for_dates_strictly_before_this_event)`.
    /// Sorted chronologically (oldest first).
    entries: Vec<(NaiveDate, f64)>,
}

#[derive(Debug)]
enum AdjustmentEvent {
    Split(f64),
    Dividend(f64),
}

impl AdjustmentTimeline {
    /// Build an adjustment timeline from splits, dividends, and a closing price series.
    ///
    /// `closes` is a sorted `Vec<(date, close_price)>` from OHLCV bars — needed for
    /// dividend adjustment where the factor depends on the prior close.
    ///
    /// The algorithm walks **backwards** from the most recent event:
    /// - For splits: cumulative /= ratio (e.g., 4:1 split → pre-split * 0.25)
    /// - For dividends: cumulative *= (close - amount) / close using prior day's close
    pub fn build(
        splits: &[SplitRow],
        dividends: &[DividendRow],
        closes: &[(NaiveDate, f64)],
    ) -> Self {
        let mut events: Vec<(NaiveDate, AdjustmentEvent)> = Vec::new();
        for s in splits {
            events.push((s.date, AdjustmentEvent::Split(s.ratio)));
        }
        for d in dividends {
            events.push((d.date, AdjustmentEvent::Dividend(d.amount)));
        }
        // Sort ascending by date
        events.sort_by_key(|(d, _)| *d);

        if events.is_empty() {
            return Self { entries: Vec::new() };
        }

        let close_map: std::collections::BTreeMap<NaiveDate, f64> =
            closes.iter().copied().collect();

        // Walk backwards from newest event, accumulating factors
        let mut cumulative = 1.0_f64;
        let mut entries: Vec<(NaiveDate, f64)> = Vec::with_capacity(events.len());

        for (date, event) in events.iter().rev() {
            match event {
                AdjustmentEvent::Split(ratio) => {
                    cumulative /= ratio;
                }
                AdjustmentEvent::Dividend(amount) => {
                    // Find the prior trading day's close for the dividend adjustment
                    if let Some((_, &prior_close)) = close_map.range(..*date).next_back() {
                        if prior_close > 0.0 && *amount < prior_close {
                            cumulative *= (prior_close - amount) / prior_close;
                        }
                    }
                    // If no prior close found, skip this dividend's adjustment
                }
            }
            entries.push((*date, cumulative));
        }

        // Reverse to chronological order (oldest first)
        entries.reverse();

        Self { entries }
    }

    /// Get the adjustment factor for a given date.
    ///
    /// - Dates before the oldest event: return the oldest factor (most adjusted)
    /// - Dates on or after the newest event: return 1.0 (current, no adjustment)
    /// - Dates between events: return the factor of the next event after the date
    pub fn factor_at(&self, date: NaiveDate) -> f64 {
        if self.entries.is_empty() {
            return 1.0;
        }

        // partition_point finds the first entry with date > query_date
        let idx = self.entries.partition_point(|(d, _)| *d <= date);

        if idx == 0 {
            // date is before all events → use oldest (most adjusted) factor
            self.entries[0].1
        } else if idx >= self.entries.len() {
            // date is on or after the newest event → no adjustment
            1.0
        } else {
            // between events → use the factor of the next event
            self.entries[idx].1
        }
    }

    /// Compute the split ratio between two dates (for position quantity adjustment).
    ///
    /// Returns the product of all split ratios between `from` (exclusive) and `to` (inclusive).
    /// Only considers splits, not dividends.
    pub fn split_ratio_between(
        splits: &[SplitRow],
        from: NaiveDate,
        to: NaiveDate,
    ) -> f64 {
        let mut ratio = 1.0;
        for s in splits {
            if s.date > from && s.date <= to {
                ratio *= s.ratio;
            }
        }
        ratio
    }

    /// Returns true if there are no adjustment events.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Adjust a raw price by the factor at the given date.
    pub fn adjust_price(&self, raw_price: f64, date: NaiveDate) -> f64 {
        raw_price * self.factor_at(date)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_split(date: &str, ratio: f64) -> SplitRow {
        SplitRow {
            symbol: "TEST".to_string(),
            date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            ratio,
        }
    }

    fn make_div(date: &str, amount: f64) -> DividendRow {
        DividendRow {
            symbol: "TEST".to_string(),
            date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            amount,
        }
    }

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn test_no_events() {
        let tl = AdjustmentTimeline::build(&[], &[], &[]);
        assert!((tl.factor_at(d("2024-01-01")) - 1.0).abs() < f64::EPSILON);
        assert!(tl.is_empty());
    }

    #[test]
    fn test_single_split() {
        let splits = vec![make_split("2020-08-31", 4.0)];
        let tl = AdjustmentTimeline::build(&splits, &[], &[]);

        // Pre-split: factor = 0.25 (raw price * 0.25 = adjusted price)
        assert!((tl.factor_at(d("2020-08-01")) - 0.25).abs() < 1e-10);
        // On or after split date: factor = 1.0 (no adjustment)
        assert!((tl.factor_at(d("2020-08-31")) - 1.0).abs() < 1e-10);
        assert!((tl.factor_at(d("2021-01-01")) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_two_splits() {
        // AAPL: 7:1 on 2014-06-09, 4:1 on 2020-08-31
        let splits = vec![
            make_split("2014-06-09", 7.0),
            make_split("2020-08-31", 4.0),
        ];
        let tl = AdjustmentTimeline::build(&splits, &[], &[]);

        // Before both splits: factor = 1/(7*4) = 1/28
        assert!((tl.factor_at(d("2014-01-01")) - 1.0 / 28.0).abs() < 1e-10);
        // Between splits: factor = 1/4 = 0.25
        assert!((tl.factor_at(d("2015-01-01")) - 0.25).abs() < 1e-10);
        // After both splits: factor = 1.0
        assert!((tl.factor_at(d("2021-01-01")) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_dividend_adjustment() {
        // Dividend of $1.00 on 2024-02-10, prior close = $100
        let divs = vec![make_div("2024-02-10", 1.0)];
        let closes = vec![(d("2024-02-09"), 100.0)];
        let tl = AdjustmentTimeline::build(&[], &divs, &closes);

        // Pre-dividend: factor = (100 - 1) / 100 = 0.99
        assert!((tl.factor_at(d("2024-02-01")) - 0.99).abs() < 1e-10);
        // Post-dividend: factor = 1.0
        assert!((tl.factor_at(d("2024-02-10")) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_split_and_dividend() {
        // Split 2:1 on 2024-06-01, dividend $0.50 on 2024-03-01 (prior close = $50)
        let splits = vec![make_split("2024-06-01", 2.0)];
        let divs = vec![make_div("2024-03-01", 0.50)];
        let closes = vec![(d("2024-02-28"), 50.0)];
        let tl = AdjustmentTimeline::build(&splits, &divs, &closes);

        // Before both: split factor (0.5) * dividend factor (49.5/50 = 0.99) = 0.495
        assert!((tl.factor_at(d("2024-01-01")) - 0.495).abs() < 1e-10);
        // After dividend, before split: factor = 0.5 (only split)
        assert!((tl.factor_at(d("2024-04-01")) - 0.5).abs() < 1e-10);
        // After both: 1.0
        assert!((tl.factor_at(d("2024-07-01")) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_split_ratio_between() {
        let splits = vec![
            make_split("2014-06-09", 7.0),
            make_split("2020-08-31", 4.0),
        ];
        // Between 2014-01-01 and 2021-01-01: both splits apply → 7 * 4 = 28
        let ratio = AdjustmentTimeline::split_ratio_between(&splits, d("2014-01-01"), d("2021-01-01"));
        assert!((ratio - 28.0).abs() < f64::EPSILON);

        // Between 2015-01-01 and 2021-01-01: only the 4:1 split applies
        let ratio = AdjustmentTimeline::split_ratio_between(&splits, d("2015-01-01"), d("2021-01-01"));
        assert!((ratio - 4.0).abs() < f64::EPSILON);

        // Between 2021-01-01 and 2022-01-01: no splits
        let ratio = AdjustmentTimeline::split_ratio_between(&splits, d("2021-01-01"), d("2022-01-01"));
        assert!((ratio - 1.0).abs() < f64::EPSILON);
    }
}
