//! Rhai engine configuration, sandboxing, and type/function registration.

use rhai::{Dynamic, Engine};

use super::helpers;
use super::types::{BarContext, ScriptPosition};

/// Build a sandboxed Rhai engine with all custom types and functions registered.
#[must_use]
pub fn build_engine() -> Engine {
    let mut engine = Engine::new();

    // Safety limits
    engine.set_max_operations(1_000_000); // ~10s of Rhai-level compute
    engine.set_max_expr_depths(64, 64); // expression nesting depth
    engine.set_max_call_levels(32); // recursion depth
    engine.set_max_string_size(10_000); // 10KB strings
    engine.set_max_array_size(10_000); // array elements
    engine.set_max_map_size(500); // map properties

    // Redirect print() to tracing (prevents stdout corruption in stdio MCP transport)
    engine.on_print(|msg| {
        tracing::debug!(script_output = msg);
    });
    engine.on_debug(|msg, source, pos| {
        tracing::debug!(
            script_debug = msg,
            source = source.unwrap_or(""),
            line = pos.line().unwrap_or(0),
        );
    });

    // Register custom types
    register_bar_context(&mut engine);
    register_script_position(&mut engine);

    // Register global helper functions
    register_global_helpers(&mut engine);

    // Register high-level action helpers and strategy constructors
    register_action_helpers(&mut engine);
    register_strategy_helpers(&mut engine);

    engine
}

/// Register `BarContext` as a Rhai custom type with getters and methods.
fn register_bar_context(engine: &mut Engine) {
    // Property getters for current bar data
    engine.register_get("date", BarContext::get_date);
    engine.register_get("datetime", BarContext::get_datetime);
    engine.register_get("open", BarContext::get_open);
    engine.register_get("high", BarContext::get_high);
    engine.register_get("low", BarContext::get_low);
    engine.register_get("close", BarContext::get_close);
    engine.register_get("volume", BarContext::get_volume);
    engine.register_get("bar_idx", BarContext::get_bar_idx);

    // Portfolio getters
    engine.register_get("cash", BarContext::get_cash);
    engine.register_get("equity", BarContext::get_equity);

    // Methods
    engine.register_fn("price", BarContext::price);
    engine.register_fn("positions", BarContext::get_positions);
    engine.register_get("position_count", BarContext::position_count);
    engine.register_fn("has_positions", BarContext::has_positions);

    // Indicators (current bar)
    engine.register_fn("sma", BarContext::sma);
    engine.register_fn("ema", BarContext::ema);
    engine.register_fn("rsi", BarContext::rsi);
    engine.register_fn("atr", BarContext::atr);
    engine.register_fn("macd_line", BarContext::macd_line);
    engine.register_fn("macd_signal", BarContext::macd_signal);
    engine.register_fn("macd_hist", BarContext::macd_hist);
    engine.register_fn("bbands_upper", BarContext::bbands_upper);
    engine.register_fn("bbands_mid", BarContext::bbands_mid);
    engine.register_fn("bbands_lower", BarContext::bbands_lower);
    engine.register_fn("stochastic", BarContext::stochastic);
    engine.register_fn("cci", BarContext::cci);
    engine.register_fn("obv", BarContext::obv);

    // Trend indicators
    engine.register_fn("adx", BarContext::adx);
    engine.register_fn("plus_di", BarContext::plus_di);
    engine.register_fn("minus_di", BarContext::minus_di);
    engine.register_fn("psar", BarContext::psar);
    engine.register_fn("supertrend", BarContext::supertrend);

    // Volatility
    engine.register_fn("keltner_upper", BarContext::keltner_upper);
    engine.register_fn("keltner_lower", BarContext::keltner_lower);
    engine.register_fn("donchian_upper", BarContext::donchian_upper);
    engine.register_fn("donchian_mid", BarContext::donchian_mid);
    engine.register_fn("donchian_lower", BarContext::donchian_lower);
    engine.register_fn("tr", BarContext::tr);

    // Momentum
    engine.register_fn("williams_r", BarContext::williams_r);
    engine.register_fn("mfi", BarContext::mfi);
    engine.register_fn("rank", BarContext::rank);
    engine.register_fn("iv_rank", BarContext::iv_rank);

    // Generic + multi-param
    engine.register_fn("indicator", BarContext::indicator);
    engine.register_fn("indicator_with", BarContext::indicator_with);

    // Date/time
    engine.register_fn("day_of_week", BarContext::day_of_week);
    engine.register_fn("month", BarContext::month);
    engine.register_fn("day_of_month", BarContext::day_of_month);
    engine.register_fn("hour", BarContext::hour);
    engine.register_fn("minute", BarContext::minute);
    engine.register_fn("week_of_year", BarContext::week_of_year);

    // Indicator lookback (for crossover detection)
    engine.register_fn("sma_at", BarContext::sma_at);
    engine.register_fn("ema_at", BarContext::ema_at);
    engine.register_fn("rsi_at", BarContext::rsi_at);
    engine.register_fn("indicator_at", BarContext::indicator_at);
    engine.register_fn("crossed_above", BarContext::crossed_above);
    engine.register_fn("crossed_below", BarContext::crossed_below);

    // Multi-param indicator overloads
    engine.register_fn("macd_line_custom", BarContext::macd_line_custom);
    engine.register_fn("macd_signal_custom", BarContext::macd_signal_custom);
    engine.register_fn("macd_hist_custom", BarContext::macd_hist_custom);
    engine.register_fn("bbands_upper_custom", BarContext::bbands_upper_custom);
    engine.register_fn("bbands_mid_custom", BarContext::bbands_mid_custom);
    engine.register_fn("bbands_lower_custom", BarContext::bbands_lower_custom);
    engine.register_fn("stochastic_custom", BarContext::stochastic_custom);

    // Historical bar lookback (MQL4-inspired)
    engine.register_fn("high", |ctx: &mut BarContext, n: i64| -> Dynamic {
        ctx.high_at(n)
    });
    engine.register_fn("low", |ctx: &mut BarContext, n: i64| -> Dynamic {
        ctx.low_at(n)
    });
    engine.register_fn("open", |ctx: &mut BarContext, n: i64| -> Dynamic {
        ctx.open_at(n)
    });
    engine.register_fn("close", |ctx: &mut BarContext, n: i64| -> Dynamic {
        ctx.close_at(n)
    });
    engine.register_fn("volume", |ctx: &mut BarContext, n: i64| -> Dynamic {
        ctx.volume_at(n)
    });

    // Range queries (MQL4-inspired iHighest/iLowest)
    engine.register_fn("highest_high", BarContext::highest_high);
    engine.register_fn("lowest_low", BarContext::lowest_low);
    engine.register_fn("highest_close", BarContext::highest_close);
    engine.register_fn("lowest_close", BarContext::lowest_close);

    // Portfolio methods
    engine.register_get("unrealized_pnl", BarContext::get_unrealized_pnl);
    engine.register_get("realized_pnl", BarContext::get_realized_pnl);
    engine.register_get("total_exposure", BarContext::get_total_exposure);

    // Options strategy building
    engine.register_fn("build_strategy", BarContext::build_strategy);

    // Cross-symbol data
    engine.register_fn("price_of", BarContext::price_of);
    engine.register_fn("price_of_col", BarContext::price_of_col);

    // Custom series plotting
    engine.register_fn("plot", BarContext::plot);
    engine.register_fn("plot_with", BarContext::plot_with);
}

/// Register `ScriptPosition` as a Rhai custom type with getters.
fn register_script_position(engine: &mut Engine) {
    engine.register_get("id", ScriptPosition::get_id);
    engine.register_get("entry_date", ScriptPosition::get_entry_date);
    engine.register_get("expiration", ScriptPosition::get_expiration);
    engine.register_get("dte", ScriptPosition::get_dte);
    engine.register_get("entry_cost", ScriptPosition::get_entry_cost);
    engine.register_get("unrealized_pnl", ScriptPosition::get_unrealized_pnl);
    engine.register_get("pnl_pct", ScriptPosition::get_pnl_pct);
    engine.register_get("days_held", ScriptPosition::get_days_held);
    engine.register_get("legs", ScriptPosition::get_legs);
    engine.register_get("side", ScriptPosition::get_side);
    engine.register_get("is_options", ScriptPosition::get_is_options);
    engine.register_get("is_stock", ScriptPosition::get_is_stock);
    engine.register_get("source", ScriptPosition::get_source);
}

/// Register global helper functions available in all scripts.
fn register_global_helpers(engine: &mut Engine) {
    engine.register_fn("abs", |x: f64| x.abs());
    engine.register_fn("max", |a: f64, b: f64| a.max(b));
    engine.register_fn("min", |a: f64, b: f64| a.min(b));
    engine.register_fn("round", |x: f64, decimals: i64| {
        let factor = 10_f64.powi(decimals as i32);
        (x * factor).round() / factor
    });
    engine.register_fn("floor", |x: f64| x.floor());
    engine.register_fn("ceil", |x: f64| x.ceil());
}

/// Register global action helper functions (hold_position, close_position, etc.).
fn register_action_helpers(engine: &mut Engine) {
    engine.register_fn("hold_position", helpers::hold_position);
    engine.register_fn("close_position", helpers::close_position);
    engine.register_fn("close_position_id", helpers::close_position_id);
    engine.register_fn("stop_backtest", helpers::stop_backtest);
    engine.register_fn("buy_stock", helpers::buy_stock);
    engine.register_fn("sell_stock", helpers::sell_stock);
}

/// Register strategy helper methods on `BarContext`.
fn register_strategy_helpers(engine: &mut Engine) {
    // Indicator utility
    engine.register_fn("indicators_ready", BarContext::indicators_ready);

    // Position sizing
    engine.register_fn("size_by_equity", BarContext::size_by_equity);
    engine.register_fn("size_by_risk", BarContext::size_by_risk);
    engine.register_fn("size_by_volatility", BarContext::size_by_volatility);
    engine.register_fn("size_by_kelly", BarContext::size_by_kelly);

    // Singles
    engine.register_fn("long_call", BarContext::long_call);
    engine.register_fn("short_call", BarContext::short_call);
    engine.register_fn("long_put", BarContext::long_put);
    engine.register_fn("short_put", BarContext::short_put);
    engine.register_fn("covered_call", BarContext::covered_call);

    // Vertical spreads
    engine.register_fn("bull_call_spread", BarContext::bull_call_spread);
    engine.register_fn("bear_call_spread", BarContext::bear_call_spread);
    engine.register_fn("bull_put_spread", BarContext::bull_put_spread);
    engine.register_fn("bear_put_spread", BarContext::bear_put_spread);

    // Straddles & strangles
    engine.register_fn("long_straddle", BarContext::long_straddle);
    engine.register_fn("short_straddle", BarContext::short_straddle);
    engine.register_fn("long_strangle", BarContext::long_strangle);
    engine.register_fn("short_strangle", BarContext::short_strangle);

    // Butterflies
    engine.register_fn("long_call_butterfly", BarContext::long_call_butterfly);
    engine.register_fn("short_call_butterfly", BarContext::short_call_butterfly);
    engine.register_fn("long_put_butterfly", BarContext::long_put_butterfly);
    engine.register_fn("short_put_butterfly", BarContext::short_put_butterfly);

    // Condors
    engine.register_fn("long_call_condor", BarContext::long_call_condor);
    engine.register_fn("short_call_condor", BarContext::short_call_condor);
    engine.register_fn("long_put_condor", BarContext::long_put_condor);
    engine.register_fn("short_put_condor", BarContext::short_put_condor);

    // Iron strategies
    engine.register_fn("iron_condor", BarContext::iron_condor);
    engine.register_fn("reverse_iron_condor", BarContext::reverse_iron_condor);
    engine.register_fn("iron_butterfly", BarContext::iron_butterfly);
    engine.register_fn("reverse_iron_butterfly", BarContext::reverse_iron_butterfly);

    // Calendar & diagonal
    engine.register_fn("call_calendar", BarContext::call_calendar);
    engine.register_fn("put_calendar", BarContext::put_calendar);
    engine.register_fn("call_diagonal", BarContext::call_diagonal);
    engine.register_fn("put_diagonal", BarContext::put_diagonal);
    engine.register_fn("double_calendar", BarContext::double_calendar);
    engine.register_fn("double_diagonal", BarContext::double_diagonal);
}
