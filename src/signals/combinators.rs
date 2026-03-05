use super::helpers::SignalFn;
use polars::prelude::*;

pub struct AndSignal {
    pub left: Box<dyn SignalFn>,
    pub right: Box<dyn SignalFn>,
}

impl SignalFn for AndSignal {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let left = self.left.evaluate(df)?;
        let right = self.right.evaluate(df)?;
        let left_bool = left.bool()?;
        let right_bool = right.bool()?;
        Ok((left_bool & right_bool).into_series())
    }
    fn name(&self) -> &'static str {
        "and"
    }
}

pub struct OrSignal {
    pub left: Box<dyn SignalFn>,
    pub right: Box<dyn SignalFn>,
}

impl SignalFn for OrSignal {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let left = self.left.evaluate(df)?;
        let right = self.right.evaluate(df)?;
        let left_bool = left.bool()?;
        let right_bool = right.bool()?;
        Ok((left_bool | right_bool).into_series())
    }
    fn name(&self) -> &'static str {
        "or"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial signal that always returns the given booleans.
    struct ConstSignal {
        values: Vec<bool>,
    }

    impl SignalFn for ConstSignal {
        fn evaluate(&self, _df: &DataFrame) -> Result<Series, PolarsError> {
            Ok(BooleanChunked::new("const".into(), &self.values).into_series())
        }
        fn name(&self) -> &'static str {
            "const"
        }
    }

    fn dummy_df(n: usize) -> DataFrame {
        df! { "x" => vec![0.0; n] }.unwrap()
    }

    #[test]
    fn and_signal_both_true() {
        let signal = AndSignal {
            left: Box::new(ConstSignal {
                values: vec![true, true, false],
            }),
            right: Box::new(ConstSignal {
                values: vec![true, false, false],
            }),
        };
        let result = signal.evaluate(&dummy_df(3)).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.get(0).unwrap());
        assert!(!bools.get(1).unwrap());
        assert!(!bools.get(2).unwrap());
    }

    #[test]
    fn and_signal_name() {
        let signal = AndSignal {
            left: Box::new(ConstSignal { values: vec![] }),
            right: Box::new(ConstSignal { values: vec![] }),
        };
        assert_eq!(signal.name(), "and");
    }

    #[test]
    fn or_signal_either_true() {
        let signal = OrSignal {
            left: Box::new(ConstSignal {
                values: vec![true, false, false],
            }),
            right: Box::new(ConstSignal {
                values: vec![false, true, false],
            }),
        };
        let result = signal.evaluate(&dummy_df(3)).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.get(0).unwrap());
        assert!(bools.get(1).unwrap());
        assert!(!bools.get(2).unwrap());
    }

    #[test]
    fn or_signal_name() {
        let signal = OrSignal {
            left: Box::new(ConstSignal { values: vec![] }),
            right: Box::new(ConstSignal { values: vec![] }),
        };
        assert_eq!(signal.name(), "or");
    }

    #[test]
    fn and_signal_all_false() {
        let signal = AndSignal {
            left: Box::new(ConstSignal {
                values: vec![false, false],
            }),
            right: Box::new(ConstSignal {
                values: vec![false, false],
            }),
        };
        let result = signal.evaluate(&dummy_df(2)).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn or_signal_all_true() {
        let signal = OrSignal {
            left: Box::new(ConstSignal {
                values: vec![true, true],
            }),
            right: Box::new(ConstSignal {
                values: vec![true, true],
            }),
        };
        let result = signal.evaluate(&dummy_df(2)).unwrap();
        let bools = result.bool().unwrap();
        assert!(bools.into_no_null_iter().all(|b| b));
    }
}
