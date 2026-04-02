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
#[allow(clippy::struct_excessive_bools)]
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

    // Mode flags
    pub procedural: bool,
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

impl std::fmt::Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Daily => write!(f, "daily"),
            Self::Intraday(intra) => write!(f, "{intra}"),
        }
    }
}

impl std::fmt::Display for IntradayInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Min1 => write!(f, "1m"),
            Self::Min5 => write!(f, "5m"),
            Self::Min10 => write!(f, "10m"),
            Self::Min15 => write!(f, "15m"),
            Self::Min30 => write!(f, "30m"),
            Self::Hour1 => write!(f, "1h"),
            Self::Hour2 => write!(f, "2h"),
            Self::Hour4 => write!(f, "4h"),
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
// Order types — next-bar execution model
// ---------------------------------------------------------------------------

/// Order type for the next-bar execution model.
///
/// Orders are submitted on bar N and evaluated for fills on bar N+1 using that
/// bar's full OHLCV range. For non-market orders, the fill price accounts for
/// gap-through behavior:
/// - Limit orders fill at the more favorable of the limit price and the open.
/// - Stop orders fill at the less favorable of the stop price and the open.
#[derive(Debug, Clone)]
pub enum OrderType {
    /// Fills at the next bar's open price, regardless of gaps.
    Market,
    /// Fills if the next bar trades at or through the limit level (buy ≤ limit,
    /// sell ≥ limit). When the market gaps through, fills at the more favorable
    /// of the limit price and the open (e.g., a buy limit below a gap-down open
    /// fills at the open).
    Limit { price: f64 },
    /// Fills if the next bar trades at or through the stop level (buy ≥ stop,
    /// sell ≤ stop). When the market gaps through, fills at the less favorable
    /// of the stop price and the open (e.g., a buy stop above a gap-up open
    /// fills at the open).
    Stop { price: f64 },
    /// Stop triggers first based on the bar's range, then the limit applies.
    /// Fill only if stop is breached AND the limit condition is met within the
    /// same bar. Invalid stop/limit relationships (buy: limit < stop, sell:
    /// limit > stop) are rejected as unfilled.
    StopLimit { stop: f64, limit: f64 },
}

/// Per-order exit modifier, attached to individual orders at submission time.
#[derive(Debug, Clone)]
pub enum ExitModifier {
    Percent(f64),
    Dollar(f64),
}

/// A pending order in the order queue, waiting to be filled on a future bar.
#[derive(Debug, Clone)]
pub struct PendingOrder {
    /// The action to execute when this order is filled.
    pub action: ScriptAction,
    /// The order type determining fill conditions.
    pub order_type: OrderType,
    /// Explicit buy/sell direction for fill logic. `true` = buy (long entry or
    /// short cover), `false` = sell (short entry or long exit). This avoids
    /// inferring direction from the action variant, which is ambiguous for
    /// `Close` actions.
    pub is_buy: bool,
    /// Optional signal name for trade logging.
    pub signal: Option<String>,
    /// The bar index at which this order was submitted.
    pub submitted_bar: usize,
    /// Time-to-live in bars. `None` = Good-Till-Canceled.
    pub ttl: Option<usize>,
    /// Per-order stop loss, applied when the entry order fills.
    pub stop_loss: Option<ExitModifier>,
    /// Per-order profit target, applied when the entry order fills.
    pub profit_target: Option<ExitModifier>,
    /// Per-order trailing stop, stored on position at fill time.
    pub trailing_stop: Option<ExitModifier>,
}

impl PendingOrder {
    /// Check whether this order has expired given the current bar index.
    pub fn is_expired(&self, current_bar: usize) -> bool {
        if let Some(ttl) = self.ttl {
            current_bar.saturating_sub(self.submitted_bar) > ttl
        } else {
            false
        }
    }

    /// Attempt to fill this order given the current bar's OHLCV data.
    /// Returns `Some(fill_price)` if the order should be filled, `None` otherwise.
    pub fn try_fill(&self, open: f64, high: f64, low: f64, _close: f64) -> Option<f64> {
        match &self.order_type {
            OrderType::Market => Some(open),
            OrderType::Limit { price } => {
                if self.is_buy {
                    // Buy limit: fill if low ≤ limit price
                    if low <= *price {
                        Some(price.min(open))
                    } else {
                        None
                    }
                } else {
                    // Sell limit: fill if high ≥ limit price
                    if high >= *price {
                        Some(price.max(open))
                    } else {
                        None
                    }
                }
            }
            OrderType::Stop { price } => {
                if self.is_buy {
                    // Buy stop: fill if high ≥ stop price (breakout)
                    if high >= *price {
                        Some(price.max(open))
                    } else {
                        None
                    }
                } else {
                    // Sell stop: fill if low ≤ stop price (breakdown)
                    if low <= *price {
                        Some(price.min(open))
                    } else {
                        None
                    }
                }
            }
            OrderType::StopLimit { stop, limit } => {
                if self.is_buy {
                    // Buy stop-limit: limit must be >= stop (otherwise inverted)
                    if *limit < *stop {
                        return None;
                    }
                    if high >= *stop && low <= *limit {
                        Some(limit.min(stop.max(open)))
                    } else {
                        None
                    }
                } else {
                    // Sell stop-limit: limit must be <= stop (otherwise inverted)
                    if *limit > *stop {
                        return None;
                    }
                    if low <= *stop && high >= *limit {
                        Some(limit.max(stop.min(open)))
                    } else {
                        None
                    }
                }
            }
        }
    }
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
    /// Cancel all pending orders (optionally filtered by signal name).
    CancelOrders { signal: Option<String> },
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
