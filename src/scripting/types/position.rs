//! Script position types exposed to Rhai scripts.

use chrono::NaiveDate;
use rhai::Dynamic;

use crate::engine::types::{OptionType, Side};

// ---------------------------------------------------------------------------
// ScriptPosition — exposed to Rhai scripts as `pos`
// ---------------------------------------------------------------------------

/// Position object exposed to Rhai scripts.
#[derive(Debug, Clone)]
pub struct ScriptPosition {
    pub id: usize,
    pub entry_date: NaiveDate,
    pub inner: ScriptPositionInner,
    pub entry_cost: f64,
    pub unrealized_pnl: f64,
    pub days_held: i64,
    /// Current simulation date — used by `get_dte()` to compute days to expiration.
    pub current_date: NaiveDate,
    /// `"script"` for positions opened by the script, `"assignment"` for
    /// positions auto-created by the engine on ITM put expiration.
    pub source: String,
    /// Whether this is an implicit position (from assignment) that does NOT
    /// count toward `max_positions`.
    pub implicit: bool,
}

/// The inner variant: options (multi-leg) or stock (single holding).
#[derive(Debug, Clone)]
pub enum ScriptPositionInner {
    Options {
        legs: Vec<ScriptPositionLeg>,
        expiration: NaiveDate,
        secondary_expiration: Option<NaiveDate>,
        multiplier: i32,
    },
    Stock {
        side: Side,
        qty: i32,
        entry_price: f64,
    },
}

/// A single leg of an options position, exposed to scripts.
#[derive(Debug, Clone)]
pub struct ScriptPositionLeg {
    pub strike: f64,
    pub option_type: OptionType,
    pub side: Side,
    pub expiration: NaiveDate,
    pub entry_price: f64,
    pub current_price: f64,
    pub delta: f64,
    pub qty: i32,
}

impl ScriptPosition {
    /// Days to expiration for options positions; `None` for stock.
    #[must_use]
    pub fn dte(&self, today: NaiveDate) -> Option<i64> {
        match &self.inner {
            ScriptPositionInner::Options { expiration, .. } => {
                Some((*expiration - today).num_days())
            }
            ScriptPositionInner::Stock { .. } => None,
        }
    }

    /// P&L as a fraction of absolute entry cost.
    #[must_use]
    pub fn pnl_pct(&self) -> f64 {
        let abs_cost = self.entry_cost.abs();
        if abs_cost < f64::EPSILON {
            0.0
        } else {
            self.unrealized_pnl / abs_cost
        }
    }

    #[must_use]
    pub fn is_options(&self) -> bool {
        matches!(self.inner, ScriptPositionInner::Options { .. })
    }

    #[must_use]
    pub fn is_stock(&self) -> bool {
        matches!(self.inner, ScriptPositionInner::Stock { .. })
    }
}

// ---------------------------------------------------------------------------
// ScriptPosition — Rhai getter methods
// ---------------------------------------------------------------------------

impl ScriptPosition {
    pub fn get_id(&mut self) -> i64 {
        self.id as i64
    }
    pub fn get_entry_date(&mut self) -> String {
        self.entry_date.to_string()
    }
    pub fn get_expiration(&mut self) -> Dynamic {
        match &self.inner {
            ScriptPositionInner::Options { expiration, .. } => {
                Dynamic::from(expiration.to_string())
            }
            ScriptPositionInner::Stock { .. } => Dynamic::UNIT,
        }
    }
    pub fn get_dte(&mut self) -> Dynamic {
        match self.dte(self.current_date) {
            Some(days) => Dynamic::from(days),
            None => Dynamic::UNIT,
        }
    }
    pub fn get_entry_cost(&mut self) -> f64 {
        self.entry_cost
    }
    pub fn get_unrealized_pnl(&mut self) -> f64 {
        self.unrealized_pnl
    }
    pub fn get_pnl_pct(&mut self) -> f64 {
        self.pnl_pct()
    }
    pub fn get_days_held(&mut self) -> i64 {
        self.days_held
    }
    pub fn get_legs(&mut self) -> Dynamic {
        match &self.inner {
            ScriptPositionInner::Options { legs, .. } => {
                let arr: rhai::Array = legs
                    .iter()
                    .map(|leg| {
                        let mut map = rhai::Map::new();
                        map.insert("strike".into(), Dynamic::from(leg.strike));
                        map.insert(
                            "option_type".into(),
                            Dynamic::from(format!("{:?}", leg.option_type).to_lowercase()),
                        );
                        map.insert(
                            "side".into(),
                            Dynamic::from(match leg.side {
                                Side::Long => "long",
                                Side::Short => "short",
                            }),
                        );
                        map.insert(
                            "expiration".into(),
                            Dynamic::from(leg.expiration.to_string()),
                        );
                        map.insert("entry_price".into(), Dynamic::from(leg.entry_price));
                        map.insert("current_price".into(), Dynamic::from(leg.current_price));
                        map.insert("delta".into(), Dynamic::from(leg.delta));
                        map.insert("qty".into(), Dynamic::from(leg.qty as i64));
                        Dynamic::from(map)
                    })
                    .collect();
                Dynamic::from(arr)
            }
            ScriptPositionInner::Stock { .. } => Dynamic::UNIT,
        }
    }
    pub fn get_side(&mut self) -> Dynamic {
        match &self.inner {
            ScriptPositionInner::Stock { side, .. } => Dynamic::from(match side {
                Side::Long => "long",
                Side::Short => "short",
            }),
            ScriptPositionInner::Options { .. } => Dynamic::UNIT,
        }
    }
    pub fn get_is_options(&mut self) -> bool {
        self.is_options()
    }
    pub fn get_is_stock(&mut self) -> bool {
        self.is_stock()
    }
    pub fn get_source(&mut self) -> String {
        self.source.clone()
    }
}
