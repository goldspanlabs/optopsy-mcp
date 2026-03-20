//! Bar interval enum for OHLCV resampling.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Bar interval for OHLCV resampling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Interval {
    #[default]
    Daily,
    Weekly,
    Monthly,
    /// 1-minute bars
    #[serde(rename = "1m")]
    Min1,
    /// 5-minute bars
    #[serde(rename = "5m")]
    Min5,
    /// 10-minute bars
    #[serde(rename = "10m")]
    Min10,
    /// 15-minute bars
    #[serde(rename = "15m")]
    Min15,
    /// 30-minute bars
    #[serde(rename = "30m")]
    Min30,
    /// 1-hour bars
    #[serde(rename = "1h")]
    Hour1,
    /// 4-hour bars
    #[serde(rename = "4h")]
    Hour4,
}

impl Interval {
    /// Approximate number of bars per trading year for annualization.
    ///
    /// Intraday counts assume a 6.5-hour regular session (390 minutes) × 252 trading days.
    /// `Hour1` uses 7 bars/day because hour-truncated bucketing produces bars at hours
    /// 9, 10, 11, 12, 13, 14, 15 for a 09:30–16:00 session.
    /// `Hour4` uses 2 bars/day (approximate for a 6.5h session).
    pub fn bars_per_year(self) -> f64 {
        match self {
            Self::Daily => 252.0,
            Self::Weekly => 52.0,
            Self::Monthly => 12.0,
            Self::Min1 => 252.0 * 390.0,
            Self::Min5 => 252.0 * 78.0,
            Self::Min10 => 252.0 * 39.0,
            Self::Min15 => 252.0 * 26.0,
            Self::Min30 => 252.0 * 13.0,
            Self::Hour1 => 252.0 * 7.0,
            Self::Hour4 => 252.0 * 2.0,
        }
    }

    /// Whether this interval represents intraday data.
    pub fn is_intraday(self) -> bool {
        matches!(
            self,
            Self::Min1
                | Self::Min5
                | Self::Min10
                | Self::Min15
                | Self::Min30
                | Self::Hour1
                | Self::Hour4
        )
    }

    /// Default lookback in calendar days for intraday intervals when `start_date`
    /// is `None`. Returns `None` for daily/weekly/monthly (no limit needed).
    ///
    /// Prevents loading 10+ years of minute/hourly data when callers don't provide
    /// an explicit `start_date`. Callers typically anchor this lookback to
    /// `end_date` when it is set, or to the current date/time otherwise. Shorter
    /// intervals get tighter caps because the bar count grows proportionally.
    pub fn default_intraday_lookback_days(self) -> Option<i64> {
        match self {
            Self::Min1 => Some(180), // ~6 months — ~75K bars
            Self::Min5 => Some(365), // ~1 year — ~20K bars
            Self::Min10 | Self::Min15 | Self::Min30 => Some(730), // ~2 years
            Self::Hour1 | Self::Hour4 => Some(1095), // ~3 years
            Self::Daily | Self::Weekly | Self::Monthly => None,
        }
    }

    /// Fraction of bar range used as synthetic bid-ask spread for slippage.
    ///
    /// Wider bars (daily) have a larger fraction because the range is proportionally
    /// larger relative to the actual spread. Tighter intraday bars use smaller fractions
    /// to avoid overstating transaction costs.
    pub fn spread_fraction(self) -> f64 {
        match self {
            Self::Daily | Self::Weekly | Self::Monthly => 0.10,
            Self::Hour4 => 0.07,
            Self::Hour1 => 0.05,
            Self::Min30 => 0.04,
            Self::Min15 | Self::Min10 => 0.035,
            Self::Min5 => 0.03,
            Self::Min1 => 0.02,
        }
    }
}

impl std::fmt::Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Daily => write!(f, "daily"),
            Self::Weekly => write!(f, "weekly"),
            Self::Monthly => write!(f, "monthly"),
            Self::Min1 => write!(f, "1m"),
            Self::Min5 => write!(f, "5m"),
            Self::Min10 => write!(f, "10m"),
            Self::Min15 => write!(f, "15m"),
            Self::Min30 => write!(f, "30m"),
            Self::Hour1 => write!(f, "1h"),
            Self::Hour4 => write!(f, "4h"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_intraday_lookback_days_mapping() {
        assert_eq!(Interval::Min1.default_intraday_lookback_days(), Some(180));
        assert_eq!(Interval::Min5.default_intraday_lookback_days(), Some(365));
        assert_eq!(Interval::Min10.default_intraday_lookback_days(), Some(730));
        assert_eq!(Interval::Min15.default_intraday_lookback_days(), Some(730));
        assert_eq!(Interval::Min30.default_intraday_lookback_days(), Some(730));
        assert_eq!(Interval::Hour1.default_intraday_lookback_days(), Some(1095));
        assert_eq!(Interval::Hour4.default_intraday_lookback_days(), Some(1095));
    }

    #[test]
    fn default_intraday_lookback_days_none_for_daily_and_above() {
        assert_eq!(Interval::Daily.default_intraday_lookback_days(), None);
        assert_eq!(Interval::Weekly.default_intraday_lookback_days(), None);
        assert_eq!(Interval::Monthly.default_intraday_lookback_days(), None);
    }
}
