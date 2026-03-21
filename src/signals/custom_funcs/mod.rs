//! Category-based dispatch for custom formula function calls.
//!
//! Splits the monolithic `build_function_call` match into focused modules.

mod datetime;
pub mod helpers;
mod math;
mod momentum_trend;
mod multi_col;
mod rolling;
mod single_col;
mod stateful;
mod volatility_adv;

pub use helpers::FuncArg;

use polars::prelude::Expr;

/// All known function names handled by `dispatch()`.
/// Used by the cross-symbol extractor to distinguish function names from symbol names.
pub const KNOWN_FUNCTIONS: &[&str] = &[
    // Rolling
    "sma",
    "ema",
    "std",
    "max",
    "min",
    "bbands_mid",
    "bbands_upper",
    "bbands_lower",
    // Math
    "abs",
    "change",
    "pct_change",
    "roc",
    "rel_volume",
    "zscore",
    "range_pct",
    "if",
    "gap",
    "gap_size",
    "gap_filled",
    // Single-column
    "rsi",
    "macd_hist",
    "macd_signal",
    "macd_line",
    "rank",
    "iv_rank",
    "cci",
    "ppo",
    "cmo",
    // Multi-column
    "atr",
    "stochastic",
    "keltner_upper",
    "keltner_lower",
    "obv",
    "mfi",
    "tr",
    "cmf",
    // Momentum/trend
    "williams_r",
    "adx",
    "plus_di",
    "minus_di",
    "psar",
    "tsi",
    "vpt",
    // Volatility/advanced
    "donchian_upper",
    "donchian_mid",
    "donchian_lower",
    "ichimoku_tenkan",
    "ichimoku_kijun",
    "ichimoku_senkou_a",
    "ichimoku_senkou_b",
    "envelope_upper",
    "envelope_lower",
    "supertrend",
    "aroon_up",
    "aroon_down",
    "aroon_osc",
    "ad",
    "pvi",
    "nvi",
    "ulcer",
    // Stateful
    "consecutive_up",
    "consecutive_down",
    // Datetime
    "day_of_week",
    "month",
    "day_of_month",
    "hour",
    "minute",
    "week_of_year",
    // HMM regime (handled by formula rewriting, not dispatch)
    "hmm_regime",
];

/// Dispatch a function call to the appropriate category module.
pub fn dispatch(name: &str, args: &[FuncArg]) -> Result<Expr, String> {
    let n = name.to_lowercase();
    match n.as_str() {
        // Rolling
        "sma" | "ema" | "std" | "max" | "min" | "bbands_mid" | "bbands_upper" | "bbands_lower" => {
            rolling::build(&n, args)
        }
        // Math
        "abs" | "change" | "pct_change" | "roc" | "rel_volume" | "zscore" | "range_pct" | "if"
        | "gap" | "gap_size" | "gap_filled" => math::build(&n, args),
        // Single-column map
        "rsi" | "macd_hist" | "macd_signal" | "macd_line" | "rank" | "iv_rank" | "cci" | "ppo"
        | "cmo" => single_col::build(&n, args),
        // Multi-column
        "atr" | "stochastic" | "keltner_upper" | "keltner_lower" | "obv" | "mfi" | "tr" | "cmf" => {
            multi_col::build(&n, args)
        }
        // Momentum/trend
        "williams_r" | "adx" | "plus_di" | "minus_di" | "psar" | "tsi" | "vpt" => {
            momentum_trend::build(&n, args)
        }
        // Volatility/advanced
        "donchian_upper" | "donchian_mid" | "donchian_lower" | "ichimoku_tenkan"
        | "ichimoku_kijun" | "ichimoku_senkou_a" | "ichimoku_senkou_b" | "envelope_upper"
        | "envelope_lower" | "supertrend" | "aroon_up" | "aroon_down" | "aroon_osc" | "ad"
        | "pvi" | "nvi" | "ulcer" => volatility_adv::build(&n, args),
        // Stateful
        "consecutive_up" | "consecutive_down" => stateful::build(&n, args),
        // Datetime
        "day_of_week" | "month" | "day_of_month" | "hour" | "minute" | "week_of_year" => {
            datetime::build(&n, args)
        }
        other => Err(format!(
            "Unknown function: '{other}'. Available: sma, ema, std, max, min, abs, change, \
             pct_change, rsi, macd_hist, macd_signal, macd_line, roc, bbands_mid, bbands_upper, \
             bbands_lower, atr, stochastic, keltner_upper, keltner_lower, obv, mfi, tr, \
             rel_volume, range_pct, zscore, rank, iv_rank, if, aroon_up, aroon_down, aroon_osc, \
             supertrend, cmf, consecutive_up, consecutive_down, williams_r, cci, ppo, cmo, \
             adx, plus_di, minus_di, psar, tsi, vpt, donchian_upper, donchian_mid, \
             donchian_lower, ichimoku_tenkan, ichimoku_kijun, ichimoku_senkou_a, \
             ichimoku_senkou_b, envelope_upper, envelope_lower, ad, pvi, nvi, ulcer, \
             gap, gap_size, gap_filled, \
             day_of_week, month, day_of_month, hour, minute, week_of_year"
        )),
    }
}
