//! MCP server implementation for optopsy.
//!
//! Holds shared state (loaded `DataFrames`, data cache, tool router) and exposes
//! all MCP tool handlers via `rmcp`'s `#[tool_router]` and `#[tool_handler]` macros.

pub mod handlers;
mod params;
mod sanitize;

pub use params::FactorProxies;

use garde::Validate;

use rmcp::{
    handler::server::router::tool::ToolRouter,
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ServerHandler,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::data::cache::{validate_path_segment, CachedStore};
use crate::tools;
use crate::tools::response_types::{
    AggregatePricesResponse, BenchmarkAnalysisResponse, CointegrationResponse, CorrelateResponse,
    DistributionResponse, DrawdownAnalysisResponse, FactorAttributionResponse, HypothesisParams,
    HypothesisResponse, MonteCarloResponse, PortfolioOptimizeResponse, RegimeDetectResponse,
    RollingMetricResponse,
};
use params::{
    tool_err, validation_err, AggregatePricesParams, BenchmarkAnalysisParams, CointegrationParams,
    CorrelateParams, DistributionParams, DrawdownAnalysisParams, FactorAttributionParams,
    MonteCarloParams, PortfolioOptimizeParams, RegimeDetectParams, RollingMetricParams,
};
use sanitize::SanitizedResult;

/// Loaded data: `HashMap<Symbol, DataFrame>` for multi-symbol support.
type LoadedData = HashMap<String, polars::prelude::DataFrame>;

/// MCP server for options backtesting, holding loaded data and the tool router.
#[derive(Clone)]
pub struct OptopsyServer {
    /// Multi-symbol in-memory data storage, keyed by uppercase ticker.
    pub data: Arc<RwLock<LoadedData>>,
    /// Shared data layer for local Parquet cache.
    pub cache: Arc<CachedStore>,
    tool_router: ToolRouter<Self>,
}

impl OptopsyServer {
    /// Create a new server instance with the given data cache.
    pub fn new(cache: Arc<CachedStore>) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            cache,
            tool_router: Self::tool_router(),
        }
    }

    /// Ensure OHLCV price data exists for a symbol.
    /// Returns the parquet file path.
    ///
    /// Searches `etf/`, `stocks/`, `futures/`, and `indices/` in order.
    fn ensure_ohlcv(&self, symbol: &str) -> Result<String, String> {
        match self.cache.find_ohlcv(symbol) {
            Some(path) => Ok(path.to_string_lossy().to_string()),
            None => Err(format!(
                "No OHLCV data found for {symbol}. Upload parquet to the cache directory."
            )),
        }
    }
}

use rmcp::handler::server::wrapper::Parameters;

#[tool_router]
impl OptopsyServer {
    /// Aggregate OHLCV price statistics by time dimension (day-of-week, month, quarter, year, hour-of-day).
    /// Returns per-bucket descriptive stats with t-test p-values for significance.
    ///
    /// Use this to identify seasonal patterns, day-of-week effects, intraday hour patterns, or time-based anomalies.
    /// The `"gap"` metric measures the relative move between each bar's open and the previous bar's close
    /// for the selected interval. With daily bars this corresponds to overnight opening gaps; with intraday
    /// data (e.g., `group_by="hour_of_day"`) it reflects bar-to-bar gaps between consecutive closes and opens.
    /// `group_by="hour_of_day"` requires intraday data — pass `interval="1h"` (or `"30m"`, `"5m"`, `"1m"`).
    #[tool(name = "aggregate_prices", annotations(read_only_hint = true))]
    async fn aggregate_prices(
        &self,
        Parameters(params): Parameters<AggregatePricesParams>,
    ) -> SanitizedResult<AggregatePricesResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("aggregate_prices", e))?;
                tools::aggregate_prices::execute(
                    &self.cache,
                    &params.symbol,
                    params.years,
                    &params.group_by,
                    &params.metric,
                    params.interval,
                    params.start_date.as_deref(),
                    params.end_date.as_deref(),
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Analyze the statistical distribution of price returns or trade P&L values.
    /// Returns descriptive stats, histogram, normality test, and tail analysis.
    ///
    /// Two modes: `price_returns` (auto-loads OHLCV) or `trade_pnl` (user-provided array).
    ///
    /// **Example (price returns)**:
    /// ```json
    /// {
    ///   "source": {"type": "price_returns", "symbol": "SPY", "years": 5}
    /// }
    /// ```
    ///
    /// **Example (trade P&L from a backtest)**:
    /// ```json
    /// {
    ///   "source": {"type": "trade_pnl", "values": [150.0, -80.0, 200.0, -50.0, 300.0]}
    /// }
    /// ```
    #[tool(name = "distribution", annotations(read_only_hint = true))]
    async fn distribution(
        &self,
        Parameters(params): Parameters<DistributionParams>,
    ) -> SanitizedResult<DistributionResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("distribution", e))?;
                tools::distribution::execute(&self.cache, &params.source, params.n_bins)
                    .await
                    .map_err(tool_err)
            }
            .await,
        )
    }

    /// Compute correlation between two price series (Pearson, Spearman, R²).
    /// Supports full-period and rolling correlation modes with scatter data for visualization.
    /// Optional `lag_range` enables cross-correlogram and Granger causality testing for lead/lag detection.
    #[tool(name = "correlate", annotations(read_only_hint = true))]
    async fn correlate(
        &self,
        Parameters(params): Parameters<CorrelateParams>,
    ) -> SanitizedResult<CorrelateResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("correlate", e))?;
                tools::correlate::execute(
                    &self.cache,
                    &params.series_a,
                    &params.series_b,
                    &params.mode,
                    params.window,
                    params.years,
                    params.lag_range.as_ref().map(|lr| (lr.min, lr.max)),
                    params
                        .interval
                        .unwrap_or(crate::engine::types::Interval::Daily),
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Compute a rolling metric over time (volatility, Sharpe, mean return, max drawdown, beta, correlation).
    /// Returns a time series of the metric plus summary statistics and trend detection.
    ///
    /// Metrics `beta` and `correlation` require a `benchmark` symbol.
    #[tool(name = "rolling_metric", annotations(read_only_hint = true))]
    async fn rolling_metric(
        &self,
        Parameters(params): Parameters<RollingMetricParams>,
    ) -> SanitizedResult<RollingMetricResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("rolling_metric", e))?;
                tools::rolling_metric::execute(
                    &self.cache,
                    &params.symbol,
                    &params.metric,
                    params.window,
                    params.benchmark.as_deref(),
                    params.years,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Detect market regimes using volatility clustering, trend state analysis, or Hidden Markov Models.
    /// Returns per-regime statistics, a transition probability matrix, and a time series of regime labels.
    ///
    /// Methods: `volatility_cluster` (quantile-based vol regimes), `trend_state` (SMA crossover),
    /// or `hmm` (Gaussian HMM with Baum-Welch EM — learns regime parameters from data).
    #[tool(name = "regime_detect", annotations(read_only_hint = true))]
    async fn regime_detect(
        &self,
        Parameters(params): Parameters<RegimeDetectParams>,
    ) -> SanitizedResult<RegimeDetectResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("regime_detect", e))?;
                tools::regime_detect::execute(
                    &self.cache,
                    &params.symbol,
                    &params.method,
                    params.n_regimes,
                    params.years,
                    params.lookback_window,
                    params
                        .interval
                        .unwrap_or(crate::engine::types::Interval::Daily),
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Scan multiple dimensions for statistically significant trading patterns.
    ///
    /// Applies BH-FDR correction to control false discoveries, computes Deflated Sharpe Ratios,
    /// deduplicates overlapping signals, and ranks hypotheses by a composite score
    /// (structural weight × DSR × regime stability).
    ///
    /// **When to use**: To discover potential trading patterns before building backtests.
    /// Results are HYPOTHESES to investigate — not confirmed strategies.
    ///
    /// **Dimensions scanned**: seasonality (day-of-week, month, turn-of-month),
    /// price action (momentum, consecutive moves), mean reversion (Bollinger, z-score),
    /// volume (spikes, low volume), volatility regime, cross-asset lead/lag,
    /// microstructure (gaps, intraday range), autocorrelation.
    ///
    /// **Output**: Ranked patterns with deployable signal specs that can be passed directly
    /// to `run_stock_backtest` or `run_options_backtest` for validation.
    ///
    /// **Time to run**: 5-15 seconds depending on number of symbols and dimensions.
    #[tool(name = "generate_hypotheses", annotations(read_only_hint = true))]
    async fn generate_hypotheses(
        &self,
        Parameters(params): Parameters<HypothesisParams>,
    ) -> SanitizedResult<HypothesisResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("generate_hypotheses", e))?;

                tracing::info!(
                    symbols = ?params.symbols,
                    dimensions = ?params.dimensions,
                    significance = params.significance,
                    "Hypothesis generation request received"
                );

                let cache = self.cache.clone();
                // Validate all symbols have OHLCV data
                for sym in &params.symbols {
                    let upper = sym.to_uppercase();
                    validate_path_segment(&upper)
                        .map_err(|e| format!("Invalid symbol \"{sym}\": {e}"))?;
                    self.ensure_ohlcv(&upper)?;
                }

                tools::hypothesis::execute(&cache, &params)
                    .await
                    .map_err(tool_err)
            }
            .await,
        )
    }
    /// Analyze the full drawdown distribution of a symbol's price history.
    ///
    /// Decomposes the equity curve into individual drawdown episodes and computes
    /// detailed statistics: episode depths, durations, recovery times, Ulcer Index,
    /// and an underwater curve for charting.
    ///
    /// **When to use**: After seeing `max_drawdown` in backtest results, use this to understand
    /// the full drawdown *distribution* — two strategies with identical `max_drawdown` can have
    /// very different drawdown profiles.
    ///
    /// **Output**: Top 20 drawdown episodes by depth, aggregate distribution stats,
    /// and an underwater curve for visualization.
    #[tool(name = "drawdown_analysis", annotations(read_only_hint = true))]
    async fn drawdown_analysis(
        &self,
        Parameters(params): Parameters<DrawdownAnalysisParams>,
    ) -> SanitizedResult<DrawdownAnalysisResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("drawdown_analysis", e))?;
                tools::drawdown_analysis::execute(&self.cache, &params.symbol, params.years)
                    .await
                    .map_err(tool_err)
            }
            .await,
        )
    }

    /// Test for cointegration between two price series using the Engle-Granger method.
    ///
    /// Fits a cointegrating regression (B = alpha + beta * A), computes the spread (residuals),
    /// and tests stationarity via an ADF test. If cointegrated, the spread is mean-reverting
    /// and suitable for pairs/statistical arbitrage strategies.
    ///
    /// **When to use**: Before building pairs trading strategies. Correlation measures
    /// co-movement of *returns*; cointegration measures whether a *spread* between two
    /// prices is mean-reverting — a much stronger condition for stat-arb.
    ///
    /// **Output**: Hedge ratio, ADF test, spread statistics (z-score, half-life), and
    /// a spread time series for charting.
    #[tool(name = "cointegration_test", annotations(read_only_hint = true))]
    async fn cointegration_test(
        &self,
        Parameters(params): Parameters<CointegrationParams>,
    ) -> SanitizedResult<CointegrationResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("cointegration_test", e))?;
                tools::cointegration::execute(
                    &self.cache,
                    &params.symbol_a,
                    &params.symbol_b,
                    params.years,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Run Monte Carlo simulations to estimate forward-looking risk and return distributions.
    ///
    /// Fits a return distribution from historical data, then generates thousands of synthetic
    /// equity paths via bootstrapped block resampling. Produces confidence intervals on
    /// terminal wealth, max drawdown distributions, and ruin probabilities.
    ///
    /// **When to use**: After backtesting, to estimate the range of possible outcomes
    /// going forward. Complements the permutation test (which tests *past* significance)
    /// with *forward-looking* risk quantification.
    ///
    /// **Output**: Percentile paths (5th/25th/50th/75th/95th), ruin probabilities,
    /// drawdown distribution, and terminal wealth histogram.
    #[tool(name = "monte_carlo", annotations(read_only_hint = true))]
    async fn monte_carlo(
        &self,
        Parameters(params): Parameters<MonteCarloParams>,
    ) -> SanitizedResult<MonteCarloResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("monte_carlo", e))?;
                tools::monte_carlo::execute(
                    &self.cache,
                    &params.symbol,
                    params.n_simulations,
                    params.horizon_days,
                    params.initial_capital,
                    params.years,
                    params.seed,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Decompose returns into systematic factor exposures and idiosyncratic alpha.
    ///
    /// Runs a multi-factor regression using ETF proxies for Market, Size (SMB),
    /// Value (HML), and Momentum factors. Answers: "Is my return explained by
    /// known risk factors, or is there genuine alpha?"
    ///
    /// **When to use**: After finding a profitable strategy, to verify the alpha
    /// isn't simply market beta or factor exposure in disguise.
    ///
    /// **Output**: Factor betas with significance tests, alpha estimate,
    /// R² (how much is explained), and return attribution breakdown.
    #[tool(name = "factor_attribution", annotations(read_only_hint = true))]
    async fn factor_attribution(
        &self,
        Parameters(params): Parameters<FactorAttributionParams>,
    ) -> SanitizedResult<FactorAttributionResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("factor_attribution", e))?;
                tools::factor_attribution::execute(
                    &self.cache,
                    &params.symbol,
                    &params.benchmark,
                    params.factor_proxies.as_ref(),
                    params.years,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Optimize portfolio weights using risk parity, minimum variance, and/or maximum Sharpe.
    ///
    /// Takes 2-20 symbols and computes optimal allocations using three methods:
    /// - **`risk_parity`**: Equal risk contribution from each asset
    /// - **`min_variance`**: Minimize total portfolio volatility
    /// - **`max_sharpe`**: Maximize risk-adjusted return (tangency portfolio)
    ///
    /// **When to use**: After identifying a set of assets/strategies, to determine
    /// optimal allocation weights rather than using equal weighting.
    ///
    /// **Output**: Optimal weights per method, expected portfolio metrics,
    /// correlation matrix, and per-asset statistics.
    #[tool(name = "portfolio_optimize", annotations(read_only_hint = true))]
    async fn portfolio_optimize(
        &self,
        Parameters(params): Parameters<PortfolioOptimizeParams>,
    ) -> SanitizedResult<PortfolioOptimizeResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("portfolio_optimize", e))?;
                tools::portfolio_optimize::execute(
                    &self.cache,
                    &params.symbols,
                    params.methods.as_deref(),
                    params.years,
                    params.risk_free_rate,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Compute benchmark-relative performance metrics: Jensen's alpha, beta, Treynor ratio,
    /// Information Ratio, tracking error, and up/down capture ratios.
    ///
    /// **When to use**: To evaluate an active strategy relative to a passive benchmark.
    /// Sharpe measures absolute risk-adjusted return; Information Ratio measures
    /// risk-adjusted *excess* return over the benchmark.
    ///
    /// **Output**: Alpha (with significance test), beta, Treynor, IR, tracking error,
    /// up/down capture ratios, and R².
    #[tool(name = "benchmark_analysis", annotations(read_only_hint = true))]
    async fn benchmark_analysis(
        &self,
        Parameters(params): Parameters<BenchmarkAnalysisParams>,
    ) -> SanitizedResult<BenchmarkAnalysisResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("benchmark_analysis", e))?;
                tools::benchmark_analysis::execute(
                    &self.cache,
                    &params.symbol,
                    &params.benchmark,
                    params.years,
                )
                .await
                .map_err(tool_err)
            }
            .await,
        )
    }

    /// Returns the Rhai scripting API reference documentation.
    ///
    /// **When to use**: Before writing a Rhai backtest script, fetch this reference
    /// to learn the available `ctx` methods, callbacks, helpers, and indicators.
    ///
    /// **No parameters needed** — returns the full scripting reference as text.
    #[tool(name = "get_scripting_reference", annotations(read_only_hint = true))]
    async fn get_scripting_reference(&self) -> Result<String, String> {
        std::fs::read_to_string("scripts/SCRIPTING_REFERENCE.md")
            .map_err(|e| format!("Failed to read scripting reference: {e}"))
    }

    /// Run a Rhai backtest script — unified scripting interface for backtesting.
    ///
    /// **When to use**: Execute custom backtest strategies defined as Rhai scripts.
    /// Scripts define `config()`, `on_bar(ctx)`, and optional callbacks
    /// (`on_exit_check`, `on_position_opened`, `on_position_closed`).
    /// See `scripts/SCRIPTING_REFERENCE.md` for the full API.
    ///
    /// **Primary mode**: Pass `strategy` with a script filename (without `.rhai`).
    /// Scripts are loaded from `scripts/strategies/{name}.rhai`.
    /// Write the `.rhai` file first, then reference it by name.
    ///
    /// **Fallback**: Pass `script` with inline Rhai source for quick one-off tests.
    ///
    /// **Example call**:
    /// ```json
    /// {
    ///   "strategy": "short_put",
    ///   "params": { "SYMBOL": "SPY", "CAPITAL": 50000, "DELTA_TARGET": 0.30, "DTE_TARGET": 45 }
    /// }
    /// ```
    ///
    /// **Output**: Trade log, equity curve, performance metrics (same format as other backtest tools)
    #[tool(name = "run_script", annotations(read_only_hint = true))]
    async fn run_script(
        &self,
        Parameters(params): Parameters<tools::run_script::RunScriptParams>,
    ) -> SanitizedResult<tools::run_script::RunScriptResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("run_script", e))?;
                handlers::run_script::execute(self, params)
                    .await
                    .map_err(tool_err)
            }
            .await,
        )
    }
}

#[tool_handler]
impl ServerHandler for OptopsyServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: rmcp::model::ProtocolVersion::default(),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "optopsy-mcp".into(),
                title: Some("Optopsy Backtesting Engine".into()),
                version: env!("CARGO_PKG_VERSION").into(),
                description: Some("Event-driven backtesting engine for options (31 strategies) and stocks (signal-driven), with realistic position management and AI-compatible analysis tools".into()),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Backtesting engine for options and stocks. Data is auto-loaded when you call any analysis tool — \
                just pass the symbol parameter.\
                \n\n## WORKFLOW\
                \n\
                \n### 1. Run a Backtest\
                \n  - **run_script** — Execute Rhai backtest scripts for options, stock, and wheel strategies.\
                \n    Pass `strategy` (filename from scripts/strategies/) or `script` (inline Rhai source).\
                \n    See scripts/SCRIPTING_REFERENCE.md for the full ctx API.\
                \n  - OHLCV and options data is loaded from cache automatically.\
                \n  - To compare parameters, run run_script multiple times with different params.\
                \n\
                \n### 2. Discover Patterns (optional)\
                \n  - generate_hypotheses({ symbols: [\"SPY\"] }) — scan for statistically significant patterns\
                \n  - Results are HYPOTHESES — validate with a backtest before trusting\
                \n\
                \n### 3. Analyze Results\
                \n  After a backtest, use analytical tools to evaluate:\
                \n  - drawdown_analysis — drawdown distribution and episode tracking\
                \n  - monte_carlo — forward-looking risk simulation\
                \n  - factor_attribution — decompose returns into factor exposures\
                \n  - benchmark_analysis — compare vs. benchmark (alpha, beta, capture ratios)\
                \n  - distribution — P&L or return distribution + normality tests\
                \n\
                \n### 4. Market Analysis Tools\
                \n  - aggregate_prices — seasonal/time-bucket return patterns\
                \n  - correlate — cross-asset correlation + Granger causality\
                \n  - rolling_metric — rolling Sharpe, volatility, beta, etc.\
                \n  - regime_detect — market regime identification (HMM, volatility, trend)\
                \n  - cointegration_test — pairs trading validation\
                \n  - portfolio_optimize — optimal weight allocation (risk parity, min variance, max Sharpe)\
                \n\
                \n## RULES\
                \n- Each tool response includes suggested_next_steps — follow them"
                    .into(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use polars::prelude::*;

    /// Load OHLCV prices from a cached parquet file for chart overlay.
    ///
    /// When `filter_datetimes` is provided, only OHLCV bars whose `datetime` matches
    /// one of the given timestamps are returned. This is used for options backtests
    /// where the options and OHLCV data share aligned timestamps (e.g. 15:59:00).
    ///
    /// When `resample_interval` is provided, the data is resampled to that interval
    /// before building the output (e.g. Daily for stock backtests to avoid returning
    /// millions of intraday bars).
    #[allow(clippy::too_many_lines)]
    fn load_underlying_prices(
        path: &std::path::Path,
        filter_datetimes: Option<&Column>,
        resample_interval: Option<crate::engine::types::Interval>,
        date_range: Option<(Option<chrono::NaiveDate>, Option<chrono::NaiveDate>)>,
    ) -> Vec<tools::response_types::UnderlyingPrice> {
        let args = ScanArgsParquet::default();
        let path_str = path.to_string_lossy();
        let Ok(lf) = LazyFrame::scan_parquet(path_str.as_ref().into(), args) else {
            return vec![];
        };

        let Ok(schema) = lf.clone().collect_schema() else {
            return vec![];
        };
        let has_datetime_col = schema
            .get("datetime")
            .is_some_and(|dt| matches!(dt, polars::prelude::DataType::Datetime(_, _)));
        let date_col_name = if has_datetime_col { "datetime" } else { "date" };

        let mut lazy = lf.select([
            col(date_col_name),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
        ]);

        if let Some(dt_filter) = filter_datetimes {
            if let Ok(unique) = dt_filter.unique() {
                if let Ok(list) = unique.take_materialized_series().implode() {
                    lazy = lazy.filter(col(date_col_name).is_in(lit(list.into_series()), false));
                }
            }
        }

        let Ok(df) = lazy
            .sort([date_col_name], SortMultipleOptions::default())
            .collect()
        else {
            return vec![];
        };

        let df = if let Some(interval) = resample_interval {
            crate::engine::ohlcv::resample_ohlcv(&df, interval).unwrap_or(df)
        } else {
            df
        };

        let df = if let Some(interval) = resample_interval {
            if interval.is_intraday() && df.column("datetime").is_ok() {
                if let Some((start, end)) = &date_range {
                    let mut filtered = df.clone();
                    if let Some(s) = start {
                        let start_dt = s.and_hms_opt(0, 0, 0).unwrap();
                        filtered = filtered
                            .clone()
                            .lazy()
                            .filter(col("datetime").gt_eq(lit(start_dt)))
                            .collect()
                            .unwrap_or(filtered);
                    }
                    if let Some(e) = end {
                        let end_next = e.succ_opt().unwrap_or(*e).and_hms_opt(0, 0, 0).unwrap();
                        filtered = filtered
                            .clone()
                            .lazy()
                            .filter(col("datetime").lt(lit(end_next)))
                            .collect()
                            .unwrap_or(filtered);
                    }
                    filtered
                } else {
                    let cutoff = (chrono::Utc::now() - chrono::Duration::days(7)).naive_utc();
                    df.clone()
                        .lazy()
                        .filter(col("datetime").gt_eq(lit(cutoff)))
                        .collect()
                        .unwrap_or(df)
                }
            } else {
                df
            }
        } else {
            df
        };

        let date_col_name = if df.column("datetime").is_ok() {
            "datetime"
        } else {
            "date"
        };
        let has_datetime = df
            .column(date_col_name)
            .ok()
            .is_some_and(|c| matches!(c.dtype(), polars::prelude::DataType::Datetime(_, _)));

        let Ok(opens) = df.column("open").and_then(|c| Ok(c.f64()?.clone())) else {
            return vec![];
        };
        let Ok(highs) = df.column("high").and_then(|c| Ok(c.f64()?.clone())) else {
            return vec![];
        };
        let Ok(lows) = df.column("low").and_then(|c| Ok(c.f64()?.clone())) else {
            return vec![];
        };
        let Ok(closes) = df.column("close").and_then(|c| Ok(c.f64()?.clone())) else {
            return vec![];
        };
        let volumes = df
            .column("volume")
            .and_then(|c| Ok(c.cast(&polars::prelude::DataType::UInt64)?.u64()?.clone()))
            .ok();

        let mut prices = Vec::with_capacity(df.height());

        if has_datetime {
            let Ok(dt_col_ref) = df.column(date_col_name) else {
                return vec![];
            };
            for i in 0..df.height() {
                let (Some(open), Some(high), Some(low), Some(close)) =
                    (opens.get(i), highs.get(i), lows.get(i), closes.get(i))
                else {
                    continue;
                };
                let Ok(ndt) =
                    crate::engine::price_table::extract_datetime_from_column(dt_col_ref, i)
                else {
                    continue;
                };
                prices.push(tools::response_types::UnderlyingPrice {
                    date: ndt.and_utc().timestamp(),
                    open,
                    high,
                    low,
                    close,
                    volume: volumes.as_ref().and_then(|v| v.get(i)),
                });
            }
            return prices;
        }

        let Ok(dates) = df.column("date").and_then(|c| Ok(c.date()?.clone())) else {
            return vec![];
        };
        for i in 0..df.height() {
            let (Some(days), Some(open), Some(high), Some(low), Some(close)) = (
                dates.phys.get(i),
                opens.get(i),
                highs.get(i),
                lows.get(i),
                closes.get(i),
            ) else {
                continue;
            };
            if let Some(date) = chrono::NaiveDate::from_num_days_from_ce_opt(
                days + crate::engine::types::EPOCH_DAYS_CE_OFFSET,
            ) {
                prices.push(tools::response_types::UnderlyingPrice {
                    date: date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp(),
                    open,
                    high,
                    low,
                    close,
                    volume: volumes.as_ref().and_then(|v| v.get(i)),
                });
            }
        }
        prices
    }

    /// Write a synthetic intraday OHLCV `DataFrame` to a temp parquet file.
    /// Returns the path. 12 bars across 2 dates at various times.
    fn write_intraday_parquet() -> tempfile::NamedTempFile {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
        let datetimes = vec![
            d1.and_hms_opt(9, 30, 0).unwrap(),
            d1.and_hms_opt(10, 0, 0).unwrap(),
            d1.and_hms_opt(15, 59, 0).unwrap(),
            d1.and_hms_opt(16, 0, 0).unwrap(),
            d2.and_hms_opt(9, 30, 0).unwrap(),
            d2.and_hms_opt(10, 0, 0).unwrap(),
            d2.and_hms_opt(15, 59, 0).unwrap(),
            d2.and_hms_opt(16, 0, 0).unwrap(),
        ];
        let n = datetimes.len();
        let df = df! {
            "datetime" => &datetimes,
            "open" => vec![100.0; n],
            "high" => vec![101.0; n],
            "low" => vec![99.0; n],
            "close" => vec![100.5; n],
            "volume" => vec![1000_i64; n],
        }
        .unwrap();

        let tmp = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        polars::prelude::ParquetWriter::new(std::fs::File::create(tmp.path()).unwrap())
            .finish(&mut df.clone())
            .unwrap();
        tmp
    }

    #[test]
    fn underlying_prices_no_filter_returns_all_bars() {
        let tmp = write_intraday_parquet();
        let prices = load_underlying_prices(tmp.path(), None, None, None);
        assert_eq!(prices.len(), 8);
    }

    #[test]
    fn underlying_prices_filter_matches_only_given_timestamps() {
        let tmp = write_intraday_parquet();

        // Build a filter column with only 15:59:00 timestamps (like options data)
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
        let filter_dts = vec![
            d1.and_hms_opt(15, 59, 0).unwrap(),
            d2.and_hms_opt(15, 59, 0).unwrap(),
        ];
        let filter_col: Column = Series::new("datetime".into(), &filter_dts).into();

        let prices = load_underlying_prices(tmp.path(), Some(&filter_col), None, None);
        assert_eq!(prices.len(), 2, "should only return 15:59 bars");
        // Verify epochs correspond to 15:59:00 times
        let dt0 = chrono::DateTime::from_timestamp(prices[0].date, 0)
            .unwrap()
            .naive_utc();
        let dt1 = chrono::DateTime::from_timestamp(prices[1].date, 0)
            .unwrap()
            .naive_utc();
        assert_eq!(dt0.format("%H:%M:%S").to_string(), "15:59:00");
        assert_eq!(dt1.format("%H:%M:%S").to_string(), "15:59:00");
    }

    #[test]
    fn underlying_prices_resample_daily_reduces_to_one_per_date() {
        let tmp = write_intraday_parquet();
        let prices = load_underlying_prices(
            tmp.path(),
            None,
            Some(crate::engine::types::Interval::Daily),
            None,
        );
        // 8 intraday bars across 2 dates → 2 daily bars
        assert_eq!(prices.len(), 2, "should have one bar per date");
        // Daily bars should have date-only format (no time component)
        // Daily bars should have midnight epoch (no time component)
        let dt0 = chrono::DateTime::from_timestamp(prices[0].date, 0)
            .unwrap()
            .naive_utc();
        assert_eq!(
            dt0.format("%H:%M:%S").to_string(),
            "00:00:00",
            "daily should be midnight epoch"
        );
    }

    #[test]
    fn underlying_prices_filter_with_no_matches_returns_empty() {
        let tmp = write_intraday_parquet();

        // Filter with a timestamp that doesn't exist in the data
        let filter_dts = vec![NaiveDate::from_ymd_opt(2099, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()];
        let filter_col: Column = Series::new("datetime".into(), &filter_dts).into();

        let prices = load_underlying_prices(tmp.path(), Some(&filter_col), None, None);
        assert!(prices.is_empty());
    }
}
