//! Factory module that converts `SignalSpec` enums into concrete `SignalFn` implementations.
//!
//! All indicator logic is handled by `FormulaSignal` via the Formula variant.
//! This module handles recursion for combinators and saved signal references
//! with depth limiting.

use super::combinators::{AndSignal, OrSignal};
use super::custom::FormulaSignal;
use super::helpers::SignalFn;
use super::spec::SignalSpec;

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
        SignalSpec::Formula { formula } => Box::new(FormulaSignal::new(formula.clone())),

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
