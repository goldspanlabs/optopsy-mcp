//! MCP tool parameter structs with validation.
//!
//! Each struct corresponds to a tool's input schema, deriving `JsonSchema` for
//! MCP schema generation and `garde::Validate` for runtime validation. Common
//! base parameters are shared via `BacktestBaseParams` to eliminate field
//! duplication across `run_options_backtest`, `walk_forward`, and `permutation_test`.

use garde::Validate;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::engine::types::{
    default_min_bid_ask, default_multiplier, validate_exit_dte_lt_entry_min, Commission, Direction,
    DteRange, ExpirationFilter, Interval, SimParams, SizingConfig, Slippage, TargetRange,
    TradeSelector,
};
use crate::signals::registry::SignalSpec;

/// Format a garde validation error with the originating tool name for easier debugging.
pub(crate) fn validation_err(tool: &str, e: impl std::fmt::Display) -> String {
    format!("[{tool}] Validation error: {e}")
}

/// Validate that `end_date >= start_date` when both are present.
/// Signature uses `&Option<String>` because garde's `custom()` passes `&self.field`.
#[allow(clippy::ref_option)]
pub(crate) fn validate_end_date_after_start(
    start_date: &Option<String>,
) -> impl FnOnce(&Option<String>, &()) -> garde::Result + '_ {
    move |end_date: &Option<String>, (): &()| {
        if let (Some(start), Some(end)) = (start_date, end_date) {
            if end < start {
                return Err(garde::Error::new(format!(
                    "end_date ({end}) must be >= start_date ({start})"
                )));
            }
        }
        Ok(())
    }
}

/// Returns `true` when the mode string indicates stock mode.
#[allow(clippy::ref_option)]
fn is_stock_mode(mode: &Option<String>) -> bool {
    mode.as_deref() == Some("stock")
}

/// Validate that `mode`, when provided, is one of `"stock"` or `"options"`.
/// Unknown values would silently fall back to options-mode, which is confusing.
#[allow(clippy::ref_option, clippy::trivially_copy_pass_by_ref)]
fn validate_mode(value: &Option<String>, _ctx: &()) -> garde::Result {
    match value.as_deref() {
        None | Some("stock" | "options") => Ok(()),
        Some(other) => Err(garde::Error::new(format!(
            "mode must be \"stock\" or \"options\" (got \"{other}\")"
        ))),
    }
}

/// Validate `strategy` based on `mode`: required (non-empty) in options mode, ignored in stock mode.
#[allow(clippy::ref_option)]
fn validate_strategy_for_mode(
    mode: &Option<String>,
) -> impl FnOnce(&Option<String>, &()) -> garde::Result + '_ {
    move |strategy: &Option<String>, (): &()| {
        if is_stock_mode(mode) {
            return Ok(()); // strategy not needed in stock mode
        }
        match strategy {
            Some(s) if !s.is_empty() => Ok(()),
            _ => Err(garde::Error::new(
                "strategy is required when mode is \"options\" (or omitted)",
            )),
        }
    }
}

/// Resolve `leg_deltas`: use provided deltas or fall back to strategy defaults.
pub(crate) fn resolve_leg_deltas(
    leg_deltas: Option<Vec<TargetRange>>,
    strategy_name: &str,
) -> Result<Vec<TargetRange>, String> {
    if let Some(deltas) = leg_deltas {
        Ok(deltas)
    } else {
        let strategy_def = crate::strategies::find_strategy(strategy_name)
            .ok_or_else(|| format!("Error: Unknown strategy: {strategy_name}"))?;
        Ok(strategy_def.default_deltas())
    }
}

/// Return the default entry DTE range (target: 45, min: 30, max: 60).
pub(crate) fn default_entry_dte() -> DteRange {
    DteRange {
        target: 45,
        min: 30,
        max: 60,
    }
}

/// Return the default exit DTE (0, hold to expiration).
pub(crate) fn default_exit_dte() -> i32 {
    0
}

/// Return the default max concurrent positions (1).
pub(crate) fn default_max_positions() -> i32 {
    1
}

/// Return the default contracts per trade (1).
pub(crate) fn default_quantity() -> i32 {
    1
}

/// Return the default starting capital (10000).
pub(crate) fn default_capital() -> f64 {
    10000.0
}

/// Shared base parameters for all backtest-related tools (`run_options_backtest`, `walk_forward`,
/// `permutation_test`). Extracted to eliminate field duplication across parameter structs.
///
/// When `mode` is `"stock"`, options-specific fields (`strategy`, `leg_deltas`, `entry_dte`,
/// `exit_dte`, `min_bid_ask`, `selector`, etc.) are ignored and `entry_signal` is required.
/// When `mode` is omitted or `"options"` (default), `strategy` is required.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct BacktestBaseParams {
    /// Backtest mode: `"stock"` for stock/equity backtests, `"options"` (default) for options.
    /// Stock mode ignores options-specific fields (strategy, `leg_deltas`, `entry_dte`, `exit_dte`,
    /// `min_bid_ask`, selector, `expiration_filter`, `net_premium`/delta filters).
    #[serde(default)]
    #[garde(custom(validate_mode))]
    pub mode: Option<String>,
    /// The option strategy name (e.g. `short_put`, `iron_condor`, `short_strangle`).
    /// Call `list_strategies` to see all 32 options. Required for options mode, ignored for stock mode.
    #[serde(default)]
    #[garde(custom(validate_strategy_for_mode(&self.mode)))]
    pub strategy: Option<String>,
    /// Per-leg delta targets (optional — uses strategy-specific defaults if omitted)
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub leg_deltas: Option<Vec<TargetRange>>,
    /// Entry DTE range: { target, min, max } (default: { target: 45, min: 30, max: 60 })
    #[serde(default = "default_entry_dte")]
    #[garde(dive)]
    pub entry_dte: DteRange,
    /// DTE at exit (default: 0 — hold to expiration)
    #[serde(default = "default_exit_dte")]
    #[garde(range(min = 0), custom(validate_exit_dte_lt_entry_min(&self.entry_dte)))]
    pub exit_dte: i32,
    /// Slippage model (default: Spread)
    #[serde(default)]
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
    /// Minimum bid/ask threshold — options with bid or ask at or below this value are filtered out (default: 0.05)
    #[serde(default = "default_min_bid_ask")]
    #[garde(range(min = 0.0))]
    pub min_bid_ask: f64,
    /// Stop loss threshold (multiplier of entry cost; values > 1.0 allowed)
    #[garde(inner(range(min = 0.0)))]
    pub stop_loss: Option<f64>,
    /// Take profit threshold (multiplier of entry cost; values > 1.0 allowed)
    #[garde(inner(range(min = 0.0)))]
    pub take_profit: Option<f64>,
    /// Maximum days to hold
    #[garde(inner(range(min = 1)))]
    pub max_hold_days: Option<i32>,
    /// Starting capital (default: 10000)
    #[serde(default = "default_capital")]
    #[garde(range(min = 0.01))]
    pub capital: f64,
    /// Number of contracts per trade (default: 1)
    #[serde(default = "default_quantity")]
    #[garde(range(min = 1))]
    pub quantity: i32,
    /// Dynamic position sizing configuration. When provided, overrides fixed `quantity`
    /// with a per-trade computed size based on equity, risk, or volatility.
    /// Methods: `fixed`, `fixed_fractional`, `risk_per_trade`, `kelly`, `volatility_target`.
    #[serde(default)]
    #[garde(dive)]
    pub sizing: Option<SizingConfig>,
    /// Contract multiplier (default: 100)
    #[serde(default = "default_multiplier")]
    #[garde(range(min = 1))]
    pub multiplier: i32,
    /// Maximum concurrent positions (default: 1)
    #[serde(default = "default_max_positions")]
    #[garde(range(min = 1))]
    pub max_positions: i32,
    /// Trade selection method
    #[garde(skip)]
    pub selector: Option<TradeSelector>,
    /// Entry signal — only open trades on dates where this TA signal fires.
    /// Requires OHLCV data (loaded from cache when needed).
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// Requires OHLCV data (loaded from cache when needed).
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Symbol to backtest (required if multiple symbols are loaded; optional if only one is loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,

    // ── Stock-mode fields (ignored when mode is "options" or omitted) ────────
    /// Position direction: Long or Short (default: Long). Stock mode only.
    #[serde(default)]
    #[garde(skip)]
    pub side: Option<crate::engine::types::Side>,
    /// Bar interval: "daily" (default), "weekly", "monthly", or intraday ("1m", "5m", "30m", "1h"). Stock mode only.
    #[serde(default)]
    #[garde(skip)]
    pub interval: Option<crate::engine::types::Interval>,
    /// Session filter for intraday data. Stock mode only.
    #[serde(default)]
    #[garde(skip)]
    pub session_filter: Option<crate::engine::types::SessionFilter>,
    /// Start date filter (YYYY-MM-DD). Stock mode only.
    #[serde(default)]
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")))]
    pub start_date: Option<String>,
    /// End date filter (YYYY-MM-DD). Stock mode only.
    #[serde(default)]
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")), custom(validate_end_date_after_start(&self.start_date)))]
    pub end_date: Option<String>,

    // ── Entry filters (options mode only) ─────────────────────────────────
    /// Minimum absolute net premium (debit or credit) at entry, in dollars per share.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub min_net_premium: Option<f64>,
    /// Maximum absolute net premium at entry, in dollars per share.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub max_net_premium: Option<f64>,
    /// Minimum signed net position delta at entry.
    #[serde(default)]
    #[garde(skip)]
    pub min_net_delta: Option<f64>,
    /// Maximum signed net position delta at entry.
    #[serde(default)]
    #[garde(skip)]
    pub max_net_delta: Option<f64>,
    /// Minimum calendar days between consecutive position entries (cooldown / stagger).
    #[serde(default)]
    #[garde(inner(range(min = 1)))]
    pub min_days_between_entries: Option<i32>,
    /// Filter expirations by calendar type: `Any` (default), `Weekly` (Fridays only),
    /// or `Monthly` (third Friday of the month only).
    #[serde(default)]
    #[garde(skip)]
    pub expiration_filter: Option<ExpirationFilter>,

    // ── Exit filters ─────────────────────────────────────────────────────────
    /// Exit the position when the absolute net position delta exceeds this value.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub exit_net_delta: Option<f64>,
}

/// Parameters for the `run_options_backtest` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct RunBacktestParams {
    #[serde(flatten)]
    #[garde(dive)]
    pub base: BacktestBaseParams,
}

/// Default shares per trade for stock backtests.
fn default_stock_quantity() -> i32 {
    100
}

/// Parameters for the `run_stock_backtest` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct RunStockBacktestParams {
    /// Ticker symbol (e.g. "SPY", "AAPL")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Position direction: Long or Short (default: Long)
    #[serde(default)]
    #[garde(skip)]
    pub side: Option<crate::engine::types::Side>,
    /// Starting capital in dollars (default: 10000).
    /// Must be enough to cover (quantity × `share_price`). For SPY at ~$600, 100 shares needs ~$60,000.
    #[serde(default = "default_capital")]
    #[garde(range(min = 0.01))]
    pub capital: f64,
    /// Number of shares per trade (default: 100 — i.e. one standard lot).
    /// For covered-call-style strategies, use 100 (= 1 round lot matching 1 option contract).
    /// Do NOT pass large values like 10000 — that means 10,000 shares, not dollars.
    #[serde(default = "default_stock_quantity")]
    #[garde(range(min = 1))]
    pub quantity: i32,
    /// Dynamic position sizing configuration. When provided, overrides fixed `quantity`
    /// with a per-trade computed size based on equity, risk, or volatility.
    #[serde(default)]
    #[garde(dive)]
    pub sizing: Option<SizingConfig>,
    /// Maximum concurrent positions (default: 1)
    #[serde(default = "default_max_positions")]
    #[garde(range(min = 1))]
    pub max_positions: i32,
    /// Slippage model (default: Mid for stocks)
    #[serde(default = "default_stock_slippage")]
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
    /// Stop loss as fraction of entry price (e.g., 0.05 = 5%)
    #[garde(inner(range(min = 0.0)))]
    pub stop_loss: Option<f64>,
    /// Take profit as fraction of entry price (e.g., 0.10 = 10%)
    #[garde(inner(range(min = 0.0)))]
    pub take_profit: Option<f64>,
    /// Maximum days to hold a position
    #[garde(inner(range(min = 1)))]
    pub max_hold_days: Option<i32>,
    /// Minimum calendar days between consecutive position entries (cooldown / stagger).
    #[serde(default)]
    #[garde(inner(range(min = 1)))]
    pub min_days_between_entries: Option<i32>,
    /// Entry signal — REQUIRED. Opens positions when this signal fires.
    /// Use `build_signal(action="search")` to find suitable signals.
    #[garde(skip)]
    pub entry_signal: SignalSpec,
    /// Exit signal — optional. Closes positions when this signal fires.
    #[serde(default)]
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Start date filter (YYYY-MM-DD)
    #[serde(default)]
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")))]
    pub start_date: Option<String>,
    /// End date filter (YYYY-MM-DD)
    #[serde(default)]
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")), custom(validate_end_date_after_start(&self.start_date)))]
    pub end_date: Option<String>,
    /// Bar interval: "daily" (default), "weekly", "monthly", or intraday presets
    /// such as "1m", "5m", "30m", and "1h". Resamples OHLCV data before signal
    /// evaluation and simulation.
    #[serde(default)]
    #[garde(skip)]
    pub interval: Option<Interval>,
    /// Session filter for intraday source data (datasets with a `datetime` column).
    /// Applied before resampling, even when `interval` is "daily", "weekly", or
    /// "monthly". Options: `Premarket` (04:00-09:30), `RegularHours` (09:30-16:00),
    /// `AfterHours` (16:00-20:00), `ExtendedHours` (04:00-20:00).
    #[serde(default)]
    #[garde(skip)]
    pub session_filter: Option<crate::engine::types::SessionFilter>,
}

fn default_stock_slippage() -> Slippage {
    Slippage::Mid
}

fn default_train_days() -> i32 {
    252
}

fn default_test_days() -> i32 {
    63
}

fn default_num_permutations() -> usize {
    100
}

/// Parameters for the `walk_forward` rolling validation tool.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
pub struct WalkForwardParams {
    #[serde(flatten)]
    #[garde(dive)]
    pub base: BacktestBaseParams,

    // ── Walk-forward specific ──────────────────────────────────────────────
    /// Training window in calendar days (default: 252, ~1 year)
    #[serde(default = "default_train_days")]
    #[garde(range(min = 1))]
    pub train_days: i32,
    /// Test window in calendar days (default: 63, ~1 quarter)
    #[serde(default = "default_test_days")]
    #[garde(range(min = 5))]
    pub test_days: i32,
    /// Step size in calendar days (default: `test_days` — non-overlapping windows).
    /// Minimum 5 days to prevent generating an excessive number of windows.
    #[garde(inner(range(min = 5)))]
    pub step_days: Option<i32>,
}

/// Parameters for the `permutation_test` statistical significance tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct PermutationTestParams {
    #[serde(flatten)]
    #[garde(dive)]
    pub base: BacktestBaseParams,

    /// Number of random permutations to run (default: 100, max: 10000)
    #[serde(default = "default_num_permutations")]
    #[garde(range(min = 1, max = 10000))]
    pub num_permutations: usize,
    /// Random seed for reproducibility (optional)
    #[serde(default)]
    #[garde(skip)]
    pub seed: Option<u64>,
}

/// A single strategy entry within a `compare_strategies` request.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
pub struct ServerCompareEntry {
    /// Strategy name (e.g. `short_put`, `iron_condor`)
    #[garde(length(min = 1))]
    pub name: String,
    /// Per-leg delta targets (optional — uses strategy-specific defaults if omitted)
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub leg_deltas: Option<Vec<TargetRange>>,
    /// Entry DTE range: { target, min, max } (default: { target: 45, min: 30, max: 60 })
    #[serde(default = "default_entry_dte")]
    #[garde(dive)]
    pub entry_dte: DteRange,
    /// DTE at exit (default: 0 — hold to expiration)
    #[serde(default = "default_exit_dte")]
    #[garde(range(min = 0), custom(validate_exit_dte_lt_entry_min(&self.entry_dte)))]
    pub exit_dte: i32,
    /// Slippage model (default: Spread)
    #[serde(default)]
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
}

/// A single stock signal configuration for `compare_strategies` in stock mode.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
pub struct StockCompareEntryInput {
    /// Human-readable label for this entry (auto-generated if omitted)
    #[serde(default)]
    #[garde(skip)]
    pub label: Option<String>,
    /// Entry signal — REQUIRED. Opens positions when this signal fires.
    #[garde(skip)]
    pub entry_signal: SignalSpec,
    /// Exit signal — optional. Closes positions when this signal fires.
    #[serde(default)]
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Position direction (default: Long)
    #[serde(default)]
    #[garde(skip)]
    pub side: Option<crate::engine::types::Side>,
    /// Bar interval (default: daily)
    #[serde(default)]
    #[garde(skip)]
    pub interval: Option<crate::engine::types::Interval>,
    /// Session filter for intraday data
    #[serde(default)]
    #[garde(skip)]
    pub session_filter: Option<crate::engine::types::SessionFilter>,
    /// Stop loss as fraction of entry price
    #[garde(inner(range(min = 0.0)))]
    pub stop_loss: Option<f64>,
    /// Take profit as fraction of entry price
    #[garde(inner(range(min = 0.0)))]
    pub take_profit: Option<f64>,
    /// Slippage model (default: Mid for stocks)
    #[serde(default = "default_stock_slippage")]
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
}

/// Parameters for the `compare_strategies` side-by-side comparison tool.
///
/// When `mode` is `"stock"`, use `stock_entries` instead of `strategies`.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct CompareStrategiesParams {
    /// Backtest mode: `"stock"` for stock/equity comparisons, `"options"` (default) for options.
    #[serde(default)]
    #[garde(custom(validate_mode))]
    pub mode: Option<String>,
    /// List of strategies with their parameters. Required for options mode.
    #[serde(default)]
    #[garde(dive)]
    pub strategies: Option<Vec<ServerCompareEntry>>,
    /// List of stock signal configurations to compare. Required for stock mode (min 2 entries).
    #[serde(default)]
    #[garde(dive)]
    pub stock_entries: Option<Vec<StockCompareEntryInput>>,
    /// Shared simulation parameters
    #[garde(dive)]
    pub sim_params: SimParams,
    /// Entry signal — only open trades on dates where this TA signal fires.
    /// Requires OHLCV data (loaded from cache when needed).
    #[serde(default)]
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// Requires OHLCV data (loaded from cache when needed).
    #[serde(default)]
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Symbol to compare on (required if multiple symbols are loaded; optional if only one is loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
}

/// Parameters for the `build_signal` tool, supporting multiple actions (search, create, etc.).
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct BuildSignalParams {
    /// Action to perform: "catalog", "search", "create", "list", "delete", "validate", or "get"
    #[garde(length(min = 1))]
    pub action: String,
    /// Natural language description for action="search" (e.g. "RSI oversold", "MACD bullish")
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 500), pattern(r"[^ \t\n\r]")))]
    pub prompt: Option<String>,
    /// Signal name (required for create, delete, get)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 64), pattern(r"^[A-Za-z0-9_-]+$")))]
    pub name: Option<String>,
    /// Formula expression (required for create, validate).
    /// Uses price columns (close, open, high, low, volume) with operators and functions.
    /// Examples: "close > sma(close, 20)", "volume > sma(volume, 20) * 2.0"
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 2000)))]
    pub formula: Option<String>,
    /// Optional description of what this signal detects
    #[serde(default)]
    #[garde(inner(length(max = 500)))]
    pub description: Option<String>,
    /// Whether to persist the signal to disk (default: true for create)
    #[serde(default = "default_save")]
    #[garde(skip)]
    pub save: bool,
}

fn default_save() -> bool {
    true
}

#[allow(clippy::unnecessary_wraps)]
fn default_price_limit() -> Option<usize> {
    Some(500)
}

/// Parameters for the `get_raw_prices` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct GetRawPricesParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Start date filter (YYYY-MM-DD)
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")))]
    pub start_date: Option<String>,
    /// End date filter (YYYY-MM-DD)
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")), custom(validate_end_date_after_start(&self.start_date)))]
    pub end_date: Option<String>,
    /// Maximum number of price bars to return (default: 500 if omitted).
    /// Data is evenly sampled if the total exceeds this limit.
    /// Pass `null` explicitly to disable the limit and return all bars.
    #[serde(default = "default_price_limit")]
    #[garde(skip)]
    pub limit: Option<usize>,
    /// Bar interval: "daily" (default), "weekly", or "monthly".
    /// Resamples OHLCV data before returning price bars.
    #[serde(default)]
    #[garde(skip)]
    pub interval: Option<Interval>,
    /// When `true` and `limit` is set, return the last N bars (tail) instead of evenly sampling.
    /// Use this for backward pagination: `end_date` + `limit=500` + `tail=true` returns
    /// the 500 most recent bars before `end_date`.
    #[serde(default)]
    #[garde(skip)]
    pub tail: Option<bool>,
}

fn default_sweep_max_positions() -> i32 {
    3
}

/// Return the default out-of-sample percentage (30%).
pub(crate) fn default_oos_pct() -> f64 {
    30.0
}

#[allow(clippy::ref_option, clippy::trivially_copy_pass_by_ref)]
fn validate_leg_delta_targets(value: &Option<Vec<Vec<f64>>>, _ctx: &()) -> garde::Result {
    let Some(targets) = value else {
        return Ok(());
    };
    for (leg_idx, leg_targets) in targets.iter().enumerate() {
        if leg_targets.is_empty() {
            return Err(garde::Error::new(format!(
                "leg {leg_idx} delta targets list must not be empty"
            )));
        }
        if leg_targets.len() > 10 {
            return Err(garde::Error::new(format!(
                "leg {leg_idx} has too many delta targets (max 10, got {})",
                leg_targets.len()
            )));
        }
        for &delta in leg_targets {
            if !delta.is_finite() || !(0.0..=1.0).contains(&delta) {
                return Err(garde::Error::new(format!(
                    "leg {leg_idx} delta target {delta} is invalid (must be a finite value in [0.0, 1.0])"
                )));
            }
        }
    }
    Ok(())
}

/// A strategy entry for the `parameter_sweep` tool, with optional per-leg delta grids.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct SweepStrategyInput {
    /// Strategy name (e.g. `short_put`, `iron_condor`)
    #[garde(length(min = 1))]
    pub name: String,
    /// Per-leg delta targets to sweep. Each inner Vec is one leg's sweep values.
    /// Each delta must be in [0.0, 1.0] with at most 10 values per leg.
    /// Omit to use strategy defaults (no delta sweep).
    #[serde(default)]
    #[garde(custom(validate_leg_delta_targets))]
    pub leg_delta_targets: Option<Vec<Vec<f64>>>,
}

/// Sweep dimensions for `parameter_sweep`: DTE targets, exit DTEs, and slippage models.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct SweepDimensionsInput {
    /// Entry DTE targets to sweep (e.g. [30, 45, 60])
    #[garde(length(min = 1), inner(range(min = 1)))]
    pub entry_dte_targets: Vec<i32>,
    /// Exit DTE values to sweep (e.g. [0, 5, 10])
    #[garde(length(min = 1), inner(range(min = 0)))]
    pub exit_dtes: Vec<i32>,
    /// Slippage models to sweep (default: [Spread])
    #[serde(default = "default_sweep_slippage")]
    #[garde(length(min = 1), dive)]
    pub slippage_models: Vec<Slippage>,
}

fn default_sweep_slippage() -> Vec<Slippage> {
    vec![Slippage::Spread]
}

/// Stock sweep dimensions for `parameter_sweep` in stock mode.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct StockSweepDimensions {
    /// Entry signal variants to sweep (required, min 1)
    #[garde(length(min = 1))]
    pub entry_signals: Vec<SignalSpec>,
    /// Exit signal variants to sweep (optional)
    #[serde(default)]
    #[garde(skip)]
    pub exit_signals: Option<Vec<SignalSpec>>,
    /// Bar intervals to sweep (e.g. `["daily", "1h"]`)
    #[serde(default)]
    #[garde(skip)]
    pub intervals: Option<Vec<Interval>>,
    /// Position sides to sweep (e.g. `["Long", "Short"]`)
    #[serde(default)]
    #[garde(skip)]
    pub sides: Option<Vec<crate::engine::types::Side>>,
    /// Session filters to sweep
    #[serde(default)]
    #[garde(skip)]
    pub session_filters: Option<Vec<crate::engine::types::SessionFilter>>,
    /// Stop loss values to sweep (e.g. [0.03, 0.05, 0.10])
    #[serde(default)]
    #[garde(skip)]
    pub stop_losses: Option<Vec<f64>>,
    /// Take profit values to sweep (e.g. [0.05, 0.10, 0.20])
    #[serde(default)]
    #[garde(skip)]
    pub take_profits: Option<Vec<f64>>,
    /// Slippage models to sweep (default: [Mid])
    #[serde(default = "default_stock_sweep_slippage")]
    #[garde(length(min = 1), dive)]
    pub slippage_models: Vec<Slippage>,
}

fn default_stock_sweep_slippage() -> Vec<Slippage> {
    vec![Slippage::Mid]
}

/// Parameters for the `parameter_sweep` optimization tool.
///
/// When `mode` is `"stock"`, use `stock_sweep` instead of `sweep`/`strategies`/`direction`.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct ParameterSweepParams {
    /// Backtest mode: `"stock"` for stock/equity sweeps, `"options"` (default) for options.
    #[serde(default)]
    #[garde(custom(validate_mode))]
    pub mode: Option<String>,
    /// Strategies to sweep (optional if `direction` is provided). Options mode only.
    #[serde(default)]
    #[garde(dive)]
    pub strategies: Option<Vec<SweepStrategyInput>>,
    /// Sweep dimensions: DTE targets, exit DTEs, slippage models. Required for options mode.
    #[serde(default)]
    #[garde(dive)]
    pub sweep: Option<SweepDimensionsInput>,
    /// Stock sweep dimensions: signals, intervals, sides, session filters. Required for stock mode.
    #[serde(default)]
    #[garde(dive)]
    pub stock_sweep: Option<StockSweepDimensions>,
    /// Shared simulation parameters
    #[garde(dive)]
    pub sim_params: SweepSimParams,
    /// Out-of-sample percentage [0, 100). Set to 0 to disable OOS validation. Default: 30.
    #[serde(default = "default_oos_pct")]
    #[garde(range(min = 0.0, max = 99.99))]
    pub out_of_sample_pct: f64,
    /// Filter strategies by market direction (bullish, bearish, neutral, volatile). Options mode only.
    #[serde(default)]
    #[garde(skip)]
    pub direction: Option<Direction>,
    /// Symbol to sweep (required if multiple symbols loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
    /// Number of permutations to run per combination to compute Sharpe p-values.
    /// When set (e.g. 100), Bonferroni and BH-FDR multiple comparisons corrections are
    /// applied automatically and included in the response. Omit to skip (default).
    /// Note: each permutation adds one extra backtest per combination.
    #[serde(default)]
    #[garde(inner(range(min = 10, max = 1000)))]
    pub num_permutations: Option<usize>,
    /// Optional RNG seed for reproducible permutation tests.
    /// This value is only used when `num_permutations` is provided; otherwise it is ignored.
    #[serde(default)]
    #[garde(skip)]
    pub permutation_seed: Option<u64>,
}

/// `SimParams` variant with sweep-friendly defaults (`max_positions=3`)
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct SweepSimParams {
    /// Starting capital (default: 10000)
    #[serde(default = "default_capital")]
    #[garde(range(min = 0.01))]
    pub capital: f64,
    /// Contracts per trade (default: 1)
    #[serde(default = "default_quantity")]
    #[garde(range(min = 1))]
    pub quantity: i32,
    /// Dynamic position sizing configuration for the sweep.
    #[serde(default)]
    #[garde(dive)]
    pub sizing: Option<SizingConfig>,
    /// Contract multiplier (default: 100)
    #[serde(default = "default_multiplier")]
    #[garde(range(min = 1))]
    pub multiplier: i32,
    /// Max concurrent positions (default: 3)
    #[serde(default = "default_sweep_max_positions")]
    #[garde(range(min = 1))]
    pub max_positions: i32,
    /// Trade selector
    #[serde(default)]
    #[garde(skip)]
    pub selector: TradeSelector,
    /// Stop loss threshold
    #[garde(inner(range(min = 0.0)))]
    pub stop_loss: Option<f64>,
    /// Take profit threshold
    #[garde(inner(range(min = 0.0)))]
    pub take_profit: Option<f64>,
    /// Max hold days
    #[garde(inner(range(min = 1)))]
    pub max_hold_days: Option<i32>,
    /// Entry signal — only open trades on dates where this TA signal fires.
    /// Requires OHLCV data (loaded from cache when needed).
    #[serde(default)]
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// Requires OHLCV data (loaded from cache when needed).
    #[serde(default)]
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Entry signal variants to sweep (cartesian product with other dimensions).
    /// Cannot be used together with `entry_signal` (singular).
    /// Each element is a complete `SignalSpec`. Empty list (default) = no signal sweep.
    #[serde(default)]
    #[garde(skip)]
    pub entry_signals: Vec<SignalSpec>,
    /// Exit signal variants to sweep (cartesian product with other dimensions).
    /// Cannot be used together with `exit_signal` (singular).
    /// Each element is a complete `SignalSpec`. Empty list (default) = no signal sweep.
    #[serde(default)]
    #[garde(skip)]
    pub exit_signals: Vec<SignalSpec>,
    /// Minimum calendar days between consecutive position entries (cooldown / stagger).
    #[serde(default)]
    #[garde(inner(range(min = 1)))]
    pub min_days_between_entries: Option<i32>,
    /// Exit when absolute net position delta exceeds this value.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub exit_net_delta: Option<f64>,

    // ── Stock-mode fields (ignored when mode is "options" or omitted) ────────
    /// Start date filter (YYYY-MM-DD). Stock mode only.
    #[serde(default)]
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")))]
    pub start_date: Option<String>,
    /// End date filter (YYYY-MM-DD). Stock mode only.
    #[serde(default)]
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")), custom(validate_end_date_after_start(&self.start_date)))]
    pub end_date: Option<String>,
}

/// Resolve sweep strategies from input params.
/// If both strategies and direction provided, filter list by direction.
/// If only direction, auto-select matching strategies.
/// If only strategies, use as-is.
/// If neither, error.
pub(crate) fn resolve_sweep_strategies(
    strategies: Option<Vec<SweepStrategyInput>>,
    direction: Option<Direction>,
) -> Result<Vec<crate::engine::sweep::SweepStrategyEntry>, String> {
    match (strategies, direction) {
        (Some(strats), Some(dir)) => {
            // Build a name→direction lookup from the cached registry (one pass, no fresh allocation).
            let dir_map: std::collections::HashMap<&str, Direction> =
                crate::strategies::all_strategies()
                    .iter()
                    .map(|s| (s.name.as_str(), s.direction))
                    .collect();
            let filtered: Vec<_> = strats
                .into_iter()
                .filter(|s| dir_map.get(s.name.as_str()).copied() == Some(dir))
                .collect();
            if filtered.is_empty() {
                return Err(format!(
                    "No provided strategies match direction {dir:?}. Remove the direction filter or add matching strategies.",
                ));
            }
            resolve_strategy_entries(filtered)
        }
        (Some(strats), None) => {
            if strats.is_empty() {
                return Err("`strategies` list must not be empty. Provide at least one strategy or use `direction` to auto-select.".to_string());
            }
            resolve_strategy_entries(strats)
        }
        (None, Some(dir)) => {
            // Auto-select all strategies matching direction.
            // StrategyDef already carries a precomputed `direction` field, so
            // we read it directly instead of calling `strategy_direction` (which
            // would redundantly rebuild all strategies via `find_strategy`).
            let matching: Vec<_> = crate::strategies::all_strategies()
                .iter()
                .filter(|s| s.direction == dir)
                .map(|s| SweepStrategyInput {
                    name: s.name.clone(),
                    leg_delta_targets: None,
                })
                .collect();
            if matching.is_empty() {
                return Err(format!("No strategies match direction {dir:?}.",));
            }
            resolve_strategy_entries(matching)
        }
        (None, None) => Err("Either `strategies` or `direction` must be provided. \
             Use `direction` to auto-select strategies by market outlook, \
             or provide explicit `strategies` list."
            .to_string()),
    }
}

// ── Analysis tool parameter structs ──────────────────────────────────────────

/// Default years of history to fetch.
fn default_years() -> u32 {
    5
}

/// Default number of histogram bins.
fn default_n_bins() -> usize {
    30
}

/// Default rolling window size.
fn default_window() -> usize {
    21
}

/// Default number of regimes.
fn default_n_regimes() -> usize {
    3
}

/// Default lookback window for regime detection.
fn default_lookback_window() -> usize {
    21
}

/// Parameters for the `aggregate_prices` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct AggregatePricesParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Years of history (default: 5)
    #[serde(default = "default_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
    /// Grouping dimension: `"day_of_week"`, `"month"`, `"quarter"`, `"year"`, `"hour_of_day"` (for intraday data)
    #[garde(length(min = 1))]
    pub group_by: String,
    /// Metric to aggregate: "return" (default: close-to-close pct change), "range", "volume", "gap" (open vs prev close pct)
    #[serde(default = "default_agg_metric")]
    #[garde(length(min = 1))]
    pub metric: String,
    /// Bar interval. Defaults to "daily" (auto-selects "1h" when `group_by="hour_of_day"`).
    /// Intraday data must be available in the cache — daily-only data cannot be resampled to intraday.
    #[serde(default)]
    #[garde(skip)]
    pub interval: Option<crate::engine::types::Interval>,
    /// Start date filter (YYYY-MM-DD)
    #[serde(default)]
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")))]
    pub start_date: Option<String>,
    /// End date filter (YYYY-MM-DD)
    #[serde(default)]
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")), custom(validate_end_date_after_start(&self.start_date)))]
    pub end_date: Option<String>,
}

fn default_agg_metric() -> String {
    "return".to_string()
}

pub use crate::tools::response_types::DistributionSource;

/// Parameters for the `distribution` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct DistributionParams {
    /// Data source for the distribution
    #[garde(dive)]
    pub source: DistributionSource,
    /// Number of histogram bins (default: 30)
    #[serde(default = "default_n_bins")]
    #[garde(range(min = 5, max = 200))]
    pub n_bins: usize,
}

pub use crate::tools::response_types::CorrelationSeries;

/// Lag range for cross-correlation analysis.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct LagRange {
    /// Minimum lag (negative = x leads y). Range: -60..0
    #[garde(range(min = -60, max = 0))]
    pub min: i32,
    /// Maximum lag (positive = y leads x). Range: 0..60
    #[garde(range(min = 0, max = 60))]
    pub max: i32,
}

/// Parameters for the `correlate` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct CorrelateParams {
    /// First data series
    #[garde(dive)]
    pub series_a: CorrelationSeries,
    /// Second data series
    #[garde(dive)]
    pub series_b: CorrelationSeries,
    /// Correlation mode: "full" (default), "rolling"
    #[serde(default = "default_corr_mode")]
    #[garde(length(min = 1))]
    pub mode: String,
    /// Rolling window size in bars/observations at the selected interval (for mode="rolling")
    #[serde(default = "default_window")]
    #[garde(range(min = 5, max = 504))]
    pub window: usize,
    /// Years of history (default: 5)
    #[serde(default = "default_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
    /// Optional lag range for cross-correlation and Granger causality analysis.
    /// When provided, computes a correlogram across the lag range and tests for lead/lag relationships.
    #[serde(default)]
    #[garde(dive)]
    pub lag_range: Option<LagRange>,
    /// Bar interval for both series: "daily" (default), "weekly", "monthly", or intraday
    /// ("1m", "5m", "30m", "1h"). Both series are resampled to this interval before
    /// computing correlation and lag analysis.
    #[serde(default)]
    #[garde(skip)]
    pub interval: Option<Interval>,
}

fn default_corr_mode() -> String {
    "full".to_string()
}

/// Parameters for the `rolling_metric` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct RollingMetricParams {
    /// Ticker symbol
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Metric to compute: `"volatility"`, `"sharpe"`, `"mean_return"`, `"max_drawdown"`, `"beta"`, `"correlation"`
    #[garde(length(min = 1))]
    pub metric: String,
    /// Rolling window size in trading days (default: 21)
    #[serde(default = "default_window")]
    #[garde(range(min = 5, max = 504))]
    pub window: usize,
    /// Benchmark symbol (required for "beta" and "correlation" metrics)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub benchmark: Option<String>,
    /// Years of history (default: 5)
    #[serde(default = "default_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
}

/// Parameters for the `regime_detect` tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
#[garde(context(()))]
pub struct RegimeDetectParams {
    /// Ticker symbol
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Detection method: `"volatility_cluster"` (default), `"trend_state"`, or `"hmm"` (Gaussian HMM)
    #[serde(default = "default_regime_method")]
    #[garde(length(min = 1))]
    pub method: String,
    /// Number of regimes to detect (default: 3, range: 2-4)
    #[serde(default = "default_n_regimes")]
    #[garde(range(min = 2, max = 4))]
    pub n_regimes: usize,
    /// Years of history (default: 5)
    #[serde(default = "default_years")]
    #[garde(range(min = 1, max = 50))]
    pub years: u32,
    /// Lookback window for rolling volatility/trend calculation (default: 21 bars at the selected interval)
    #[serde(default = "default_lookback_window")]
    #[garde(range(min = 5, max = 252))]
    pub lookback_window: usize,
    /// Bar interval: "daily" (default), "weekly", "monthly", or intraday ("1m", "5m", "30m", "1h").
    /// OHLCV data is resampled to this interval before applying the detection method.
    #[serde(default)]
    #[garde(skip)]
    pub interval: Option<Interval>,
}

fn default_regime_method() -> String {
    "volatility_cluster".to_string()
}

fn resolve_strategy_entries(
    strats: Vec<SweepStrategyInput>,
) -> Result<Vec<crate::engine::sweep::SweepStrategyEntry>, String> {
    strats
        .into_iter()
        .map(|s| {
            let name = s.name;
            let strategy_def = crate::strategies::find_strategy(&name)
                .ok_or_else(|| format!("Unknown strategy: {name}"))?;

            let leg_delta_targets = if let Some(targets) = s.leg_delta_targets {
                // Validate that the number of legs matches the strategy definition.
                if targets.len() != strategy_def.legs.len() {
                    return Err(format!(
                        "Strategy '{}' expects {} leg(s) but {} leg delta target set(s) were provided",
                        name,
                        strategy_def.legs.len(),
                        targets.len()
                    ));
                }
                // Validate that each leg's sweep list is non-empty.
                for (idx, leg_targets) in targets.iter().enumerate() {
                    if leg_targets.is_empty() {
                        return Err(format!(
                            "Strategy '{name}' leg {idx} has an empty delta target list; each leg must have at least one target",
                        ));
                    }
                }
                targets
            } else {
                // Use strategy defaults — single value per leg
                strategy_def
                    .default_deltas()
                    .iter()
                    .map(|d| vec![d.target])
                    .collect()
            };
            Ok(crate::engine::sweep::SweepStrategyEntry {
                name,
                leg_delta_targets,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── CorrelateParams interval deserialization ────────────────────────────

    #[test]
    fn correlate_params_interval_defaults_to_none() {
        let json = serde_json::json!({
            "series_a": { "symbol": "SPY", "field": "close" },
            "series_b": { "symbol": "QQQ", "field": "close" }
        });
        let p: CorrelateParams = serde_json::from_value(json).unwrap();
        assert!(p.interval.is_none(), "interval should default to None");
        p.validate().unwrap();
    }

    #[test]
    fn correlate_params_all_intervals_parse() {
        let cases = [
            ("1m", Interval::Min1),
            ("5m", Interval::Min5),
            ("30m", Interval::Min30),
            ("1h", Interval::Hour1),
            ("daily", Interval::Daily),
            ("weekly", Interval::Weekly),
            ("monthly", Interval::Monthly),
        ];
        for (s, expected) in cases {
            let json = serde_json::json!({
                "series_a": { "symbol": "SPY", "field": "close" },
                "series_b": { "symbol": "QQQ", "field": "close" },
                "interval": s
            });
            let p: CorrelateParams = serde_json::from_value(json)
                .unwrap_or_else(|e| panic!("interval={s} should parse: {e}"));
            assert_eq!(p.interval, Some(expected), "interval={s}");
        }
    }

    // ─── RegimeDetectParams interval deserialization ─────────────────────────

    #[test]
    fn regime_detect_params_interval_defaults_to_none() {
        let json = serde_json::json!({ "symbol": "SPY" });
        let p: RegimeDetectParams = serde_json::from_value(json).unwrap();
        assert!(p.interval.is_none(), "interval should default to None");
        p.validate().unwrap();
    }

    #[test]
    fn regime_detect_params_all_intervals_parse() {
        let cases = [
            ("1m", Interval::Min1),
            ("5m", Interval::Min5),
            ("30m", Interval::Min30),
            ("1h", Interval::Hour1),
            ("daily", Interval::Daily),
            ("weekly", Interval::Weekly),
            ("monthly", Interval::Monthly),
        ];
        for (s, expected) in cases {
            let json = serde_json::json!({
                "symbol": "SPY",
                "interval": s
            });
            let p: RegimeDetectParams = serde_json::from_value(json)
                .unwrap_or_else(|e| panic!("interval={s} should parse: {e}"));
            assert_eq!(p.interval, Some(expected), "interval={s}");
        }
    }
}
