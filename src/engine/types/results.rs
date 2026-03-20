//! Result and output types produced by the backtesting engine.

use chrono::NaiveDateTime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::enums::{CashflowLabel, ExitType, OptionType, Side};
use super::pricing::{DteRange, Slippage, TargetRange};
use crate::signals::registry::SignalSpec;

/// Internal quality stats collected during backtest simulation for data coverage reporting.
#[derive(Debug, Clone, Default)]
pub struct BacktestQualityStats {
    pub trading_days_total: usize,
    /// Trading days that had at least one price observation in the price table.
    pub trading_days_with_data: usize,
    pub total_candidates: usize,
    pub positions_opened: usize,
    /// Sampled bid-ask spread percentages, used to compute median spread for quality warnings.
    pub entry_spread_pcts: Vec<f64>,
}

/// Output of a single backtest run: trades, metrics, equity curve, and quality stats.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BacktestResult {
    pub trade_count: usize,
    pub total_pnl: f64,
    pub metrics: PerformanceMetrics,
    pub equity_curve: Vec<EquityPoint>,
    pub trade_log: Vec<TradeRecord>,
    /// Internal quality diagnostics collected during simulation (not serialized to clients).
    #[serde(skip)]
    pub quality: BacktestQualityStats,
    /// Diagnostic warnings surfaced to callers (e.g. entries skipped due to insufficient capital).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Aggregate performance metrics derived from the equity curve and trade log.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PerformanceMetrics {
    pub sharpe: f64,
    pub sortino: f64,
    /// Maximum peak-to-trough drawdown as a fraction of peak equity.
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub calmar: f64,
    /// Value at Risk at the 95% confidence level (5th percentile of daily returns).
    pub var_95: f64,
    /// Conditional Value at Risk (Expected Shortfall) at 95% — mean loss beyond `VaR`.
    /// More conservative than `VaR` for fat-tailed distributions (options strategies).
    #[serde(default)]
    pub cvar_95: f64,
    /// Historical (non-parametric) `VaR` at 95% — uses empirical percentile without
    /// assuming normality.
    #[serde(default)]
    pub historical_var_95: f64,
    pub total_return_pct: f64,
    pub cagr: f64,
    pub avg_trade_pnl: f64,
    pub avg_winner: f64,
    /// Average P&L of losing trades (negative value).
    pub avg_loser: f64,
    pub avg_days_held: f64,
    pub max_consecutive_losses: usize,
    /// Expected value per trade: `win_rate * avg_winner + (1 - win_rate) * avg_loser`.
    pub expectancy: f64,
    /// Ulcer Index — root-mean-square of drawdowns, emphasizing sustained drawdown pain.
    #[serde(default)]
    pub ulcer_index: f64,
    /// Pain Ratio — excess return / Ulcer Index (drawdown-adjusted return).
    #[serde(default)]
    pub pain_ratio: f64,
    /// Average drawdown duration in bars (how long drawdowns last on average).
    #[serde(default)]
    pub avg_drawdown_duration: f64,
    /// Maximum drawdown duration in bars (longest single drawdown episode).
    #[serde(default)]
    pub max_drawdown_duration: usize,
}

/// Single point on the equity curve representing portfolio value at a moment in time.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EquityPoint {
    pub datetime: NaiveDateTime,
    pub equity: f64,
}

/// Snapshot of a single leg within a completed trade record.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LegDetail {
    pub side: Side,
    pub option_type: OptionType,
    pub strike: f64,
    pub expiration: String,
    pub entry_price: f64,
    /// Fill price at exit (`None` if the option expired worthless or was never closed).
    pub exit_price: Option<f64>,
    pub qty: i32,
}

/// Complete record of a single round-trip trade (entry through exit).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TradeRecord {
    pub trade_id: usize,
    pub entry_datetime: NaiveDateTime,
    pub exit_datetime: NaiveDateTime,
    /// Signed entry cost: negative for credits received, positive for debits paid.
    pub entry_cost: f64,
    /// Signed exit proceeds: positive for credits received, negative for debits paid.
    pub exit_proceeds: f64,
    pub entry_amount: f64,
    pub entry_label: CashflowLabel,
    pub exit_amount: f64,
    pub exit_label: CashflowLabel,
    pub pnl: f64,
    pub days_held: i64,
    pub exit_type: ExitType,
    pub legs: Vec<LegDetail>,
    /// Dynamically computed quantity (set when position sizing is active).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub computed_quantity: Option<i32>,
    /// Portfolio equity at the time of entry (set when position sizing is active).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_equity: Option<f64>,
    /// Per-share stock entry price (set when strategy has a stock leg).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stock_entry_price: Option<f64>,
    /// Per-share stock exit price (set when strategy has a stock leg).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stock_exit_price: Option<f64>,
    /// Stock leg P&L (set when strategy has a stock leg).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stock_pnl: Option<f64>,
}

impl TradeRecord {
    /// Construct a new trade record, automatically deriving cashflow labels from cost signs.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        trade_id: usize,
        entry_datetime: NaiveDateTime,
        exit_datetime: NaiveDateTime,
        entry_cost: f64,
        exit_proceeds: f64,
        pnl: f64,
        days_held: i64,
        exit_type: ExitType,
        legs: Vec<LegDetail>,
    ) -> Self {
        Self {
            trade_id,
            entry_datetime,
            exit_datetime,
            entry_amount: entry_cost.abs(),
            entry_label: if entry_cost < 0.0 {
                CashflowLabel::CR
            } else {
                CashflowLabel::DR
            },
            exit_amount: exit_proceeds.abs(),
            exit_label: if exit_proceeds > 0.0 {
                CashflowLabel::CR
            } else {
                CashflowLabel::DR
            },
            entry_cost,
            exit_proceeds,
            pnl,
            days_held,
            exit_type,
            legs,
            computed_quantity: None,
            entry_equity: None,
            stock_entry_price: None,
            stock_exit_price: None,
            stock_pnl: None,
        }
    }
}

/// Summary metrics for one strategy in a multi-strategy comparison.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CompareResult {
    pub strategy: String,
    pub display_name: String,
    pub trades: usize,
    pub pnl: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub max_dd: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub calmar: f64,
    pub total_return_pct: f64,
    /// Full trade log, included for equity curve overlays in the frontend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trade_log: Vec<TradeRecord>,
    /// If the backtest failed, contains the error message instead of metrics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result of a single parameter sweep combination.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SweepResult {
    /// Human-readable label encoding strategy, deltas, DTE, and slippage.
    pub label: String,
    pub strategy: String,
    pub display_name: String,
    pub leg_deltas: Vec<TargetRange>,
    pub entry_dte: DteRange,
    pub exit_dte: i32,
    pub slippage: Slippage,
    pub trades: usize,
    pub pnl: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub max_dd: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub calmar: f64,
    pub total_return_pct: f64,
    /// Count of distinct entry dates, used to assess statistical independence.
    pub independent_entry_periods: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_signal: Option<SignalSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_signal: Option<SignalSpec>,
    /// Dimension key pairs for sensitivity analysis (not serialized to clients).
    #[serde(skip)]
    pub signal_dim_keys: Vec<(String, String)>,
    /// Raw (unadjusted) Sharpe p-value from permutation test, if `num_permutations` was set.
    /// Use the `multiple_comparisons` field in the sweep response for corrected values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p_value: Option<f64>,
    /// Sizing method name for identification in sweep results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sizing: Option<String>,
}
