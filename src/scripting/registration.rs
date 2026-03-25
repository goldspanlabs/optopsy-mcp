//! Rhai engine configuration, sandboxing, and type/function registration.

use rhai::Engine;

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
    engine.register_fn("position_count", BarContext::position_count);
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
    // ADX omitted — requires +DI/-DI implementation not yet in IndicatorStore
    engine.register_fn("cci", BarContext::cci);
    engine.register_fn("obv", BarContext::obv);
    engine.register_fn("indicator", BarContext::indicator);
    engine.register_fn("indicator_with", BarContext::indicator_with);

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

    // Options strategy building
    engine.register_fn("build_strategy", BarContext::build_strategy);

    // Cross-symbol data
    engine.register_fn("price_of", BarContext::price_of);
    engine.register_fn("price_of_col", BarContext::price_of_col);

    // Position sizing
    engine.register_fn("compute_quantity", BarContext::compute_quantity);
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
