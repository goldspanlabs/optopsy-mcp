use garde::Validate;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::engine::types::{
    default_min_bid_ask, default_multiplier, validate_exit_dte_lt_entry_min, Commission, Direction,
    DteRange, ExpirationFilter, SimParams, Slippage, TargetRange, TradeSelector,
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

pub(crate) fn default_entry_dte() -> DteRange {
    DteRange {
        target: 45,
        min: 30,
        max: 60,
    }
}

pub(crate) fn default_exit_dte() -> i32 {
    0
}

pub(crate) fn default_max_positions() -> i32 {
    1
}

pub(crate) fn default_quantity() -> i32 {
    1
}

pub(crate) fn default_capital() -> f64 {
    10000.0
}

/// Shared base parameters for all backtest-related tools (`run_backtest`, `walk_forward`,
/// `permutation_test`). Extracted to eliminate field duplication across parameter structs.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
pub struct BacktestBaseParams {
    /// The option strategy name (e.g. `short_put`, `iron_condor`, `short_strangle`).
    /// Call `list_strategies` to see all 32 options.
    #[garde(length(min = 1))]
    pub strategy: String,
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
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Symbol to backtest (required if multiple symbols are loaded; optional if only one is loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,

    // ── Entry filters ────────────────────────────────────────────────────────
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

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct RunBacktestParams {
    #[serde(flatten)]
    #[garde(dive)]
    pub base: BacktestBaseParams,
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

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct CompareStrategiesParams {
    /// List of strategies with their parameters
    #[garde(length(min = 2), dive)]
    pub strategies: Vec<ServerCompareEntry>,
    /// Shared simulation parameters
    #[garde(dive)]
    pub sim_params: SimParams,
    /// Entry signal — only open trades on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[serde(default)]
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[serde(default)]
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Symbol to compare strategies on (required if multiple symbols are loaded; optional if only one is loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
}

pub(crate) fn validate_category_read(category: &str) -> Result<&str, String> {
    match category {
        "options" | "prices" => Ok(category),
        _ => Err(format!(
            "Invalid category: \"{category}\". Must be \"options\" or \"prices\"."
        )),
    }
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct CheckCacheParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Data category: "options" for options chain data, "prices" for OHLCV price data
    #[garde(length(min = 1))]
    pub category: String,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct BuildSignalParams {
    /// Action to perform: "create", "list", "delete", "validate", "get", or "search"
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

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct FetchToParquetParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Time period to fetch (e.g. "6mo", "1y", "5y", "max"). Defaults to "5y".
    #[garde(inner(length(min = 1)))]
    pub period: Option<String>,
}

#[allow(clippy::unnecessary_wraps)]
fn default_price_limit() -> Option<usize> {
    Some(500)
}

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
}

fn default_sweep_max_positions() -> i32 {
    3
}

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

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct ParameterSweepParams {
    /// Strategies to sweep (optional if `direction` is provided)
    #[serde(default)]
    #[garde(dive)]
    pub strategies: Option<Vec<SweepStrategyInput>>,
    /// Sweep dimensions: DTE targets, exit DTEs, slippage models
    #[garde(dive)]
    pub sweep: SweepDimensionsInput,
    /// Shared simulation parameters
    #[garde(dive)]
    pub sim_params: SweepSimParams,
    /// Out-of-sample percentage [0, 100). Set to 0 to disable OOS validation. Default: 30.
    #[serde(default = "default_oos_pct")]
    #[garde(range(min = 0.0, max = 99.99))]
    pub out_of_sample_pct: f64,
    /// Filter strategies by market direction (bullish, bearish, neutral, volatile).
    /// If both `strategies` and `direction` provided, filters the list.
    /// If only `direction`, auto-selects matching strategies.
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
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[serde(default)]
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
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
