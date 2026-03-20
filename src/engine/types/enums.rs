//! Core enum types shared across the backtesting engine.

use chrono::NaiveTime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Days-from-epoch offset: Polars stores `Date` as days since 1970-01-01 (Unix epoch).
/// `chrono::NaiveDate::from_num_days_from_ce` counts from day 1 CE, which is day 719 163
/// relative to the Unix epoch. Add this constant to a Polars date value before passing
/// it to `from_num_days_from_ce_opt`.
pub const EPOCH_DAYS_CE_OFFSET: i32 = 719_163;

/// How to resolve conflicts when both stop-loss and take-profit trigger on the same bar.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ConflictResolution {
    /// Assume stop-loss was hit first (conservative/pessimistic). Default.
    #[default]
    StopLossFirst,
    /// Assume take-profit was hit first (optimistic).
    TakeProfitFirst,
    /// Pick whichever trigger price is closer to the bar's open (more realistic).
    Nearest,
}

/// Trading session time-of-day filter for intraday data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum SessionFilter {
    /// Pre-market session: 04:00 – 09:30 ET
    Premarket,
    /// Regular trading hours: 09:30 – 16:00 ET
    RegularHours,
    /// After-hours session: 16:00 – 20:00 ET
    AfterHours,
    /// Full extended hours: 04:00 – 20:00 ET
    ExtendedHours,
}

impl SessionFilter {
    /// Return the `(start, end)` time range for this session (half-open: `[start, end)`).
    pub fn time_range(self) -> (NaiveTime, NaiveTime) {
        match self {
            Self::Premarket => (
                NaiveTime::from_hms_opt(4, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            ),
            Self::RegularHours => (
                NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
            ),
            Self::AfterHours => (
                NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(20, 0, 0).unwrap(),
            ),
            Self::ExtendedHours => (
                NaiveTime::from_hms_opt(4, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(20, 0, 0).unwrap(),
            ),
        }
    }
}

/// Market direction bias for a strategy (bullish, bearish, neutral, or volatile).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Bullish,
    Bearish,
    Neutral,
    Volatile,
}

/// Position direction: long (+1) or short (-1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum Side {
    Long = 1,
    Short = -1,
}

impl Side {
    /// Return the numeric multiplier: `1.0` for Long, `-1.0` for Short.
    pub fn multiplier(self) -> f64 {
        match self {
            Side::Long => 1.0,
            Side::Short => -1.0,
        }
    }

    /// Return the opposite side (Long becomes Short and vice versa).
    #[must_use]
    pub fn flip(self) -> Self {
        match self {
            Side::Long => Side::Short,
            Side::Short => Side::Long,
        }
    }
}

/// Option contract type: call or put.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub enum OptionType {
    Call,
    Put,
}

impl OptionType {
    /// Return the string representation matching the raw data format (`"c"` or `"p"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            OptionType::Call => "c",
            OptionType::Put => "p",
        }
    }
}

/// Expiration cycle tag for multi-expiration strategies (calendar/diagonal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ExpirationCycle {
    #[default]
    Primary, // Near-term (or same-expiration for non-calendar strategies)
    Secondary, // Far-term (calendar/diagonal only)
}

/// Filter entries by expiration calendar type.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
pub enum ExpirationFilter {
    /// Accept any expiration (default).
    #[default]
    Any,
    /// Accept only expirations that fall on a Friday (weekly options).
    Weekly,
    /// Accept only expirations on the third Friday of the month (standard monthly cycle).
    Monthly,
}

/// Strategy for choosing among multiple entry candidates on the same date.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
pub enum TradeSelector {
    #[default]
    Nearest,
    HighestPremium,
    LowestPremium,
    First,
}

/// Reason a position was closed during simulation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum ExitType {
    Expiration,
    StopLoss,
    TakeProfit,
    MaxHold,
    DteExit,
    Adjustment,
    Signal,
    /// Exit triggered when the absolute net position delta exceeds `exit_net_delta`.
    DeltaExit,
}

/// Label indicating whether a cashflow is a credit (received) or debit (paid).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub enum CashflowLabel {
    CR,
    DR,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_multiplier_long() {
        assert!((Side::Long.multiplier() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn side_multiplier_short() {
        assert!((Side::Short.multiplier() - (-1.0)).abs() < f64::EPSILON);
    }
}
