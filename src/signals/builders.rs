//! Factory module that converts `SignalSpec` enums into concrete `SignalFn` implementations.
//!
//! Dispatches each variant to the appropriate per-category builder function,
//! handling recursion for combinators and saved signal references with depth limiting.

use super::combinators::{AndSignal, OrSignal};
use super::custom::FormulaSignal;
use super::helpers::SignalFn;
use super::momentum::{
    MacdBearish, MacdBullish, MacdCrossover, RsiAbove, RsiBelow, StochasticAbove, StochasticBelow,
};
use super::overlap::{
    EmaCrossover, EmaCrossunder, PriceAboveEma, PriceAboveSma, PriceBelowEma, PriceBelowSma,
    SmaCrossover, SmaCrossunder,
};
use super::price::{ConsecutiveDown, ConsecutiveUp, DrawdownBelow, GapDown, GapUp, RateOfChange};
use super::spec::SignalSpec;
use super::trend::{
    AroonDowntrend, AroonUpAbove, AroonUptrend, SupertrendBearish, SupertrendBullish,
};
use super::volatility::{
    AtrAbove, AtrBelow, BollingerLowerTouch, BollingerUpperTouch, IvPercentileAbove,
    IvPercentileBelow, IvRankAbove, IvRankBelow, KeltnerLowerBreak, KeltnerUpperBreak,
};
use super::volume::{CmfNegative, CmfPositive, MfiAbove, MfiBelow, ObvFalling, ObvRising};

/// Convert a `SignalSpec` into a concrete `Box<dyn SignalFn>`.
pub fn build_signal(spec: &SignalSpec) -> Box<dyn SignalFn> {
    build_signal_depth(spec, 0)
}

const MAX_SIGNAL_DEPTH: usize = 8;

fn build_signal_depth(spec: &SignalSpec, depth: usize) -> Box<dyn SignalFn> {
    if depth >= MAX_SIGNAL_DEPTH {
        tracing::error!(
            max_depth = MAX_SIGNAL_DEPTH,
            "Signal recursion limit exceeded — possible cycle in Saved signal references. \
             Signal will evaluate as always-false. Check for circular Saved signal references."
        );
        return Box::new(FormulaSignal::new("false".to_string()));
    }
    match spec {
        // Per-category builders
        SignalSpec::RsiBelow { .. }
        | SignalSpec::RsiAbove { .. }
        | SignalSpec::MacdBullish { .. }
        | SignalSpec::MacdBearish { .. }
        | SignalSpec::MacdCrossover { .. }
        | SignalSpec::StochasticBelow { .. }
        | SignalSpec::StochasticAbove { .. } => build_momentum(spec),

        SignalSpec::PriceAboveSma { .. }
        | SignalSpec::PriceBelowSma { .. }
        | SignalSpec::PriceAboveEma { .. }
        | SignalSpec::PriceBelowEma { .. }
        | SignalSpec::SmaCrossover { .. }
        | SignalSpec::SmaCrossunder { .. }
        | SignalSpec::EmaCrossover { .. }
        | SignalSpec::EmaCrossunder { .. } => build_overlap(spec),

        SignalSpec::AroonUptrend { .. }
        | SignalSpec::AroonDowntrend { .. }
        | SignalSpec::AroonUpAbove { .. }
        | SignalSpec::SupertrendBullish { .. }
        | SignalSpec::SupertrendBearish { .. } => build_trend(spec),

        SignalSpec::AtrAbove { .. }
        | SignalSpec::AtrBelow { .. }
        | SignalSpec::BollingerLowerTouch { .. }
        | SignalSpec::BollingerUpperTouch { .. }
        | SignalSpec::KeltnerLowerBreak { .. }
        | SignalSpec::KeltnerUpperBreak { .. }
        | SignalSpec::IvRankAbove { .. }
        | SignalSpec::IvRankBelow { .. }
        | SignalSpec::IvPercentileAbove { .. }
        | SignalSpec::IvPercentileBelow { .. } => build_volatility(spec),

        SignalSpec::GapUp { .. }
        | SignalSpec::GapDown { .. }
        | SignalSpec::DrawdownBelow { .. }
        | SignalSpec::ConsecutiveUp { .. }
        | SignalSpec::ConsecutiveDown { .. }
        | SignalSpec::RateOfChange { .. } => build_price(spec),

        SignalSpec::MfiBelow { .. }
        | SignalSpec::MfiAbove { .. }
        | SignalSpec::ObvRising { .. }
        | SignalSpec::ObvFalling { .. }
        | SignalSpec::CmfPositive { .. }
        | SignalSpec::CmfNegative { .. } => build_volume(spec),

        // Special cases: Custom, Saved, CrossSymbol, Combinators
        SignalSpec::Custom {
            name: _,
            formula,
            description: _,
        } => Box::new(FormulaSignal::new(formula.clone())),

        SignalSpec::Saved { name } => match super::storage::load_signal(name) {
            Ok(loaded) => {
                if matches!(loaded, SignalSpec::Saved { .. }) {
                    tracing::error!(
                        "Saved signal '{}' references another Saved signal — cycle rejected",
                        name
                    );
                    return Box::new(FormulaSignal::new("false".to_string()));
                }
                build_signal_depth(&loaded, depth + 1)
            }
            Err(e) => {
                tracing::error!("Failed to load saved signal '{}': {}", name, e);
                Box::new(FormulaSignal::new("false".to_string()))
            }
        },

        SignalSpec::CrossSymbol { signal, .. } => build_signal_depth(signal, depth + 1),

        SignalSpec::And { left, right } => Box::new(AndSignal {
            left: build_signal_depth(left, depth + 1),
            right: build_signal_depth(right, depth + 1),
        }),
        SignalSpec::Or { left, right } => Box::new(OrSignal {
            left: build_signal_depth(left, depth + 1),
            right: build_signal_depth(right, depth + 1),
        }),
    }
}

/// Build momentum signal variants.
fn build_momentum(spec: &SignalSpec) -> Box<dyn SignalFn> {
    match spec {
        SignalSpec::RsiBelow { column, threshold } => Box::new(RsiBelow {
            column: column.clone(),
            threshold: *threshold,
        }),
        SignalSpec::RsiAbove { column, threshold } => Box::new(RsiAbove {
            column: column.clone(),
            threshold: *threshold,
        }),
        SignalSpec::MacdBullish { column } => Box::new(MacdBullish {
            column: column.clone(),
        }),
        SignalSpec::MacdBearish { column } => Box::new(MacdBearish {
            column: column.clone(),
        }),
        SignalSpec::MacdCrossover { column } => Box::new(MacdCrossover {
            column: column.clone(),
        }),
        SignalSpec::StochasticBelow {
            close_col,
            high_col,
            low_col,
            period,
            threshold,
        } => Box::new(StochasticBelow {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::StochasticAbove {
            close_col,
            high_col,
            low_col,
            period,
            threshold,
        } => Box::new(StochasticAbove {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        _ => unreachable!(),
    }
}

/// Build overlap signal variants.
fn build_overlap(spec: &SignalSpec) -> Box<dyn SignalFn> {
    match spec {
        SignalSpec::PriceAboveSma { column, period } => Box::new(PriceAboveSma {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::PriceBelowSma { column, period } => Box::new(PriceBelowSma {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::PriceAboveEma { column, period } => Box::new(PriceAboveEma {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::PriceBelowEma { column, period } => Box::new(PriceBelowEma {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::SmaCrossover {
            column,
            fast_period,
            slow_period,
        } => Box::new(SmaCrossover {
            column: column.clone(),
            fast_period: *fast_period,
            slow_period: *slow_period,
        }),
        SignalSpec::SmaCrossunder {
            column,
            fast_period,
            slow_period,
        } => Box::new(SmaCrossunder {
            column: column.clone(),
            fast_period: *fast_period,
            slow_period: *slow_period,
        }),
        SignalSpec::EmaCrossover {
            column,
            fast_period,
            slow_period,
        } => Box::new(EmaCrossover {
            column: column.clone(),
            fast_period: *fast_period,
            slow_period: *slow_period,
        }),
        SignalSpec::EmaCrossunder {
            column,
            fast_period,
            slow_period,
        } => Box::new(EmaCrossunder {
            column: column.clone(),
            fast_period: *fast_period,
            slow_period: *slow_period,
        }),
        _ => unreachable!(),
    }
}

/// Build trend signal variants.
fn build_trend(spec: &SignalSpec) -> Box<dyn SignalFn> {
    match spec {
        SignalSpec::AroonUptrend {
            high_col,
            low_col,
            period,
        } => Box::new(AroonUptrend {
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
        }),
        SignalSpec::AroonDowntrend {
            high_col,
            low_col,
            period,
        } => Box::new(AroonDowntrend {
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
        }),
        SignalSpec::AroonUpAbove {
            high_col,
            period,
            threshold,
        } => Box::new(AroonUpAbove {
            high_col: high_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::SupertrendBullish {
            close_col,
            high_col,
            low_col,
            period,
            multiplier,
        } => Box::new(SupertrendBullish {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            multiplier: *multiplier,
        }),
        SignalSpec::SupertrendBearish {
            close_col,
            high_col,
            low_col,
            period,
            multiplier,
        } => Box::new(SupertrendBearish {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            multiplier: *multiplier,
        }),
        _ => unreachable!(),
    }
}

/// Build volatility signal variants (ATR, Bollinger, Keltner, IV).
fn build_volatility(spec: &SignalSpec) -> Box<dyn SignalFn> {
    match spec {
        SignalSpec::AtrAbove {
            close_col,
            high_col,
            low_col,
            period,
            threshold,
        } => Box::new(AtrAbove {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::AtrBelow {
            close_col,
            high_col,
            low_col,
            period,
            threshold,
        } => Box::new(AtrBelow {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::BollingerLowerTouch { column, period } => Box::new(BollingerLowerTouch {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::BollingerUpperTouch { column, period } => Box::new(BollingerUpperTouch {
            column: column.clone(),
            period: *period,
        }),
        SignalSpec::KeltnerLowerBreak {
            close_col,
            high_col,
            low_col,
            period,
            multiplier,
        } => Box::new(KeltnerLowerBreak {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            multiplier: *multiplier,
        }),
        SignalSpec::KeltnerUpperBreak {
            close_col,
            high_col,
            low_col,
            period,
            multiplier,
        } => Box::new(KeltnerUpperBreak {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            period: *period,
            multiplier: *multiplier,
        }),
        SignalSpec::IvRankAbove {
            lookback,
            threshold,
        } => Box::new(IvRankAbove {
            lookback: *lookback,
            threshold: *threshold,
        }),
        SignalSpec::IvRankBelow {
            lookback,
            threshold,
        } => Box::new(IvRankBelow {
            lookback: *lookback,
            threshold: *threshold,
        }),
        SignalSpec::IvPercentileAbove {
            lookback,
            threshold,
        } => Box::new(IvPercentileAbove {
            lookback: *lookback,
            threshold: *threshold,
        }),
        SignalSpec::IvPercentileBelow {
            lookback,
            threshold,
        } => Box::new(IvPercentileBelow {
            lookback: *lookback,
            threshold: *threshold,
        }),
        _ => unreachable!(),
    }
}

/// Build price signal variants.
fn build_price(spec: &SignalSpec) -> Box<dyn SignalFn> {
    match spec {
        SignalSpec::GapUp {
            open_col,
            close_col,
            threshold,
        } => Box::new(GapUp {
            open_col: open_col.clone(),
            close_col: close_col.clone(),
            threshold: *threshold,
        }),
        SignalSpec::GapDown {
            open_col,
            close_col,
            threshold,
        } => Box::new(GapDown {
            open_col: open_col.clone(),
            close_col: close_col.clone(),
            threshold: *threshold,
        }),
        SignalSpec::DrawdownBelow {
            column,
            window,
            threshold,
        } => Box::new(DrawdownBelow {
            column: column.clone(),
            window: (*window).max(1),
            threshold: *threshold,
        }),
        SignalSpec::ConsecutiveUp { column, count } => Box::new(ConsecutiveUp {
            column: column.clone(),
            count: (*count).max(1),
        }),
        SignalSpec::ConsecutiveDown { column, count } => Box::new(ConsecutiveDown {
            column: column.clone(),
            count: (*count).max(1),
        }),
        SignalSpec::RateOfChange {
            column,
            period,
            threshold,
        } => Box::new(RateOfChange {
            column: column.clone(),
            period: (*period).max(1),
            threshold: *threshold,
        }),
        _ => unreachable!(),
    }
}

/// Build volume signal variants.
fn build_volume(spec: &SignalSpec) -> Box<dyn SignalFn> {
    match spec {
        SignalSpec::MfiBelow {
            high_col,
            low_col,
            close_col,
            volume_col,
            period,
            threshold,
        } => Box::new(MfiBelow {
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            close_col: close_col.clone(),
            volume_col: volume_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::MfiAbove {
            high_col,
            low_col,
            close_col,
            volume_col,
            period,
            threshold,
        } => Box::new(MfiAbove {
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            close_col: close_col.clone(),
            volume_col: volume_col.clone(),
            period: *period,
            threshold: *threshold,
        }),
        SignalSpec::ObvRising {
            price_col,
            volume_col,
        } => Box::new(ObvRising {
            price_col: price_col.clone(),
            volume_col: volume_col.clone(),
        }),
        SignalSpec::ObvFalling {
            price_col,
            volume_col,
        } => Box::new(ObvFalling {
            price_col: price_col.clone(),
            volume_col: volume_col.clone(),
        }),
        SignalSpec::CmfPositive {
            close_col,
            high_col,
            low_col,
            volume_col,
            period,
        } => Box::new(CmfPositive {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            volume_col: volume_col.clone(),
            period: *period,
        }),
        SignalSpec::CmfNegative {
            close_col,
            high_col,
            low_col,
            volume_col,
            period,
        } => Box::new(CmfNegative {
            close_col: close_col.clone(),
            high_col: high_col.clone(),
            low_col: low_col.clone(),
            volume_col: volume_col.clone(),
            period: *period,
        }),
        _ => unreachable!(),
    }
}
