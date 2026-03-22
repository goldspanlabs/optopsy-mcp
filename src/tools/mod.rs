//! Tool modules for the MCP server.
//!
//! Each submodule implements a single MCP tool, delegating to the engine layer for
//! computation and returning AI-enriched response types for LLM consumption.

mod macros;

pub mod aggregate_prices;
pub mod ai_format;
pub mod ai_helpers;
pub mod backtest;
pub mod benchmark_analysis;
pub mod build_signal;
pub mod cointegration;
pub mod compare;
pub mod construct_signal;
pub mod correlate;
pub mod distribution;
pub mod drawdown_analysis;
pub mod factor_attribution;
pub mod hypothesis;
pub mod list_symbols;
pub mod monte_carlo;
pub mod permutation_test;
pub mod portfolio;
pub mod portfolio_optimize;
pub mod raw_prices;
pub mod regime_detect;
pub mod response_types;
pub mod rolling_metric;
pub mod signals;
pub mod stock_backtest;
pub mod strategies;
pub mod sweep;
pub mod walk_forward;
pub mod wheel_backtest;
