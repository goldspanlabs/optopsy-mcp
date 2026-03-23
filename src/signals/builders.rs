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
            Ok((loaded, _, _)) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_df() -> polars::prelude::DataFrame {
        polars::prelude::df! {
            "close" => &[100.0, 101.0, 102.0, 103.0, 104.0],
        }
        .unwrap()
    }

    #[test]
    fn build_formula_signal() {
        let spec = SignalSpec::Formula {
            formula: "close > 101".into(),
        };
        let signal = build_signal(&spec);
        assert_eq!(signal.name(), "custom_formula");
        let result = signal.evaluate(&dummy_df()).unwrap();
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn build_and_signal() {
        let spec = SignalSpec::And {
            left: Box::new(SignalSpec::Formula {
                formula: "close > 101".into(),
            }),
            right: Box::new(SignalSpec::Formula {
                formula: "close < 104".into(),
            }),
        };
        let signal = build_signal(&spec);
        assert_eq!(signal.name(), "and");
        let result = signal.evaluate(&dummy_df()).unwrap();
        let bools = result.bool().unwrap();
        // 100: F, 101: F, 102: T, 103: T, 104: F (104 not < 104)
        assert!(!bools.get(0).unwrap());
        assert!(bools.get(2).unwrap());
        assert!(bools.get(3).unwrap());
        assert!(!bools.get(4).unwrap());
    }

    #[test]
    fn build_or_signal() {
        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::Formula {
                formula: "close < 101".into(),
            }),
            right: Box::new(SignalSpec::Formula {
                formula: "close > 103".into(),
            }),
        };
        let signal = build_signal(&spec);
        assert_eq!(signal.name(), "or");
        let result = signal.evaluate(&dummy_df()).unwrap();
        let bools = result.bool().unwrap();
        // 100: T (<101), 101: F, 102: F, 103: F, 104: T (>103)
        assert!(bools.get(0).unwrap());
        assert!(!bools.get(1).unwrap());
        assert!(bools.get(4).unwrap());
    }

    #[test]
    fn build_saved_missing_returns_false_signal() {
        let spec = SignalSpec::Saved {
            name: "nonexistent_signal_xyz".into(),
        };
        let signal = build_signal(&spec);
        // Missing saved signal returns always-false formula
        let result = signal.evaluate(&dummy_df()).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn depth_limit_returns_false_signal() {
        // Build a deeply nested And chain that exceeds MAX_SIGNAL_DEPTH (8)
        let mut spec = SignalSpec::Formula {
            formula: "close > 0".into(),
        };
        for _ in 0..10 {
            spec = SignalSpec::And {
                left: Box::new(spec),
                right: Box::new(SignalSpec::Formula {
                    formula: "close > 0".into(),
                }),
            };
        }
        // Should still succeed (depth limit triggers false for deep branches)
        let signal = build_signal(&spec);
        let result = signal.evaluate(&dummy_df()).unwrap();
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn nested_combinators_evaluate_correctly() {
        // (close > 101 AND close < 104) OR close == 100
        let spec = SignalSpec::Or {
            left: Box::new(SignalSpec::And {
                left: Box::new(SignalSpec::Formula {
                    formula: "close > 101".into(),
                }),
                right: Box::new(SignalSpec::Formula {
                    formula: "close < 104".into(),
                }),
            }),
            right: Box::new(SignalSpec::Formula {
                formula: "close == 100".into(),
            }),
        };
        let signal = build_signal(&spec);
        let result = signal.evaluate(&dummy_df()).unwrap();
        let bools = result.bool().unwrap();
        // 100: T (==100), 101: F, 102: T (>101 & <104), 103: T, 104: F
        assert!(bools.get(0).unwrap());
        assert!(!bools.get(1).unwrap());
        assert!(bools.get(2).unwrap());
        assert!(bools.get(3).unwrap());
        assert!(!bools.get(4).unwrap());
    }
}
