//! Configuration types, intervals, and value enums for the scripting engine.

use std::collections::HashMap;

use chrono::NaiveDate;

use crate::constants::TRADING_DAYS_PER_YEAR;
use crate::engine::types::{Commission, ExpirationFilter, Slippage, TradeSelector};

// ---------------------------------------------------------------------------
// ScriptConfig — parsed from the Rhai config() callback return value
// ---------------------------------------------------------------------------

/// Configuration extracted from a script's `config()` callback.
#[derive(Debug, Clone)]
pub struct ScriptConfig {
    pub symbol: String,
    pub capital: f64,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub interval: Interval,
    pub multiplier: i32,
    pub timeout_secs: u64,
    pub auto_close_on_end: bool,

    // Data requirements
    pub needs_ohlcv: bool,
    pub needs_options: bool,
    pub cross_symbols: Vec<String>,
    pub declared_indicators: Vec<String>,

    // Engine-enforced settings
    pub slippage: Slippage,
    pub commission: Option<Commission>,
    pub min_days_between_entries: Option<i32>,
    pub expiration_filter: ExpirationFilter,
    pub trade_selector: TradeSelector,

    // Script-readable defaults (NOT engine-enforced)
    pub defaults: HashMap<String, ScriptValue>,
}

/// Interval for bar iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interval {
    Daily,
    Intraday(IntradayInterval),
}

/// Intraday bar sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntradayInterval {
    Min1,
    Min5,
    Min10,
    Min15,
    Min30,
    Hour1,
    Hour2,
    Hour4,
}

impl Interval {
    /// Trading bars per year, used for annualized metrics (Sharpe, CAGR, etc.).
    #[must_use]
    pub fn bars_per_year(self) -> f64 {
        match self {
            Self::Daily => TRADING_DAYS_PER_YEAR,
            Self::Intraday(intra) => match intra {
                IntradayInterval::Min1 => TRADING_DAYS_PER_YEAR * 390.0,
                IntradayInterval::Min5 => TRADING_DAYS_PER_YEAR * 78.0,
                IntradayInterval::Min10 => TRADING_DAYS_PER_YEAR * 39.0,
                IntradayInterval::Min15 => TRADING_DAYS_PER_YEAR * 26.0,
                IntradayInterval::Min30 => TRADING_DAYS_PER_YEAR * 13.0,
                IntradayInterval::Hour1 => TRADING_DAYS_PER_YEAR * 6.5,
                IntradayInterval::Hour2 => TRADING_DAYS_PER_YEAR * 3.25,
                IntradayInterval::Hour4 => TRADING_DAYS_PER_YEAR * 1.625,
            },
        }
    }

    /// Parse from a string like "daily", "1m", "5m", "1h", etc.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "daily" | "1d" => Some(Self::Daily),
            "1m" | "1min" => Some(Self::Intraday(IntradayInterval::Min1)),
            "5m" | "5min" => Some(Self::Intraday(IntradayInterval::Min5)),
            "10m" | "10min" => Some(Self::Intraday(IntradayInterval::Min10)),
            "15m" | "15min" => Some(Self::Intraday(IntradayInterval::Min15)),
            "30m" | "30min" => Some(Self::Intraday(IntradayInterval::Min30)),
            "1h" | "60m" => Some(Self::Intraday(IntradayInterval::Hour1)),
            "2h" => Some(Self::Intraday(IntradayInterval::Hour2)),
            "4h" => Some(Self::Intraday(IntradayInterval::Hour4)),
            _ => None,
        }
    }
}

/// A loosely-typed value for script-readable defaults.
#[derive(Debug, Clone)]
pub enum ScriptValue {
    Float(f64),
    Int(i64),
    String(String),
    Bool(bool),
    None,
}

/// A single OHLCV bar.
#[derive(Debug, Clone)]
pub struct OhlcvBar {
    pub datetime: chrono::NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Cross-symbol bar data (forward-filled to primary timeline).
#[derive(Debug, Clone)]
pub struct CrossSymbolBar {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

// ---------------------------------------------------------------------------
// Action types — returned by script callbacks, processed by the engine
// ---------------------------------------------------------------------------

use crate::engine::types::{OptionType, Side};

/// An action returned by `on_bar` or `on_exit_check` callbacks.
#[derive(Debug, Clone)]
pub enum ScriptAction {
    /// Open an options position with the given legs.
    OpenOptions {
        legs: Vec<LegSpec>,
        qty: Option<i32>,
    },
    /// Open a stock position.
    OpenStock { side: Side, qty: i32 },
    /// Close a specific position.
    Close {
        position_id: Option<usize>,
        reason: String,
    },
    /// Do nothing (from `on_exit_check`).
    Hold,
    /// Stop the backtest loop early.
    Stop { reason: String },
}

/// A leg specification in an `open_options` action.
/// Can be "unresolved" (delta/DTE targets) or "resolved" (specific contract).
#[derive(Debug, Clone)]
pub enum LegSpec {
    /// Engine resolves to a specific contract via `filters.rs`.
    Unresolved {
        side: Side,
        option_type: OptionType,
        delta: f64,
        dte: i32,
    },
    /// Pre-resolved contract from `find_option`.
    Resolved {
        side: Side,
        option_type: OptionType,
        strike: f64,
        expiration: NaiveDate,
        bid: f64,
        ask: f64,
    },
}

// ---------------------------------------------------------------------------
// ScriptSimContext — internal engine state (not exposed to scripts)
// ---------------------------------------------------------------------------

use crate::engine::sim_types::{DateIndex, LastKnown, PriceTable};

/// Internal state maintained by the unified scripting engine.
/// Scripts never see this — it bridges the clean Rhai API to the native engine's
/// `SimContext` / `BacktestParams` dependency chain.
pub struct ScriptSimContext {
    pub price_table: PriceTable,
    pub date_index: DateIndex,
    pub last_known: LastKnown,
    pub slippage: Slippage,
    pub commission: Option<Commission>,
    pub multiplier: i32,
    pub bars_per_year: f64,
    pub min_days_between_entries: Option<i32>,
    pub expiration_filter: ExpirationFilter,
    pub trade_selector: TradeSelector,
    pub next_position_id: usize,
    pub last_entry_date: Option<NaiveDate>,
}
