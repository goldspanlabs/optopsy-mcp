# Dynamic Position Sizing — Backend Design Spec

## Problem

The backtest engine currently uses a fixed `quantity` (contracts per trade) for every trade in a run. This means position size never adapts to the portfolio's equity, the specific trade's risk profile, or market conditions. Users who want to model realistic position sizing (e.g., "risk 2% of my capital per trade") cannot do so.

## Current State

`BacktestBaseParams` already exposes:
- `capital: f64` (default 10,000) — starting equity
- `quantity: i32` (default 1) — fixed contracts per trade
- `multiplier: i32` (default 100) — contract multiplier
- `max_positions: i32` (default 1) — concurrent position limit

These are static for the entire simulation.

## Proposed Design

### New `sizing` parameter

Add an optional `sizing` field to `BacktestBaseParams`. When omitted, the engine falls back to the existing fixed `quantity` behavior (fully backward-compatible).

```rust
/// Dynamic position sizing configuration.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
#[serde(tag = "method")]
pub enum SizingConfig {
    /// Fixed number of contracts per trade (current behavior).
    /// Uses the top-level `quantity` field.
    #[serde(rename = "fixed")]
    Fixed,

    /// Risk a fixed percentage of current equity per trade.
    /// qty = floor(equity * risk_pct / max_loss_per_contract)
    #[serde(rename = "fixed_fractional")]
    FixedFractional {
        /// Fraction of equity to risk per trade (e.g., 0.02 = 2%).
        #[garde(range(min = 0.001, max = 1.0))]
        risk_pct: f64,
    },

    /// Kelly criterion sizing (typically used as half-Kelly or quarter-Kelly).
    /// kelly_f = win_rate - (1 - win_rate) / (avg_winner / avg_loser)
    /// qty = floor(equity * kelly_f * fraction / max_loss_per_contract)
    #[serde(rename = "kelly")]
    Kelly {
        /// Kelly fraction multiplier (e.g., 0.5 = half-Kelly, 0.25 = quarter-Kelly).
        #[garde(range(min = 0.01, max = 1.0))]
        fraction: f64,
        /// Lookback window in trades for computing win_rate and avg_winner/avg_loser.
        /// If None, uses all prior trades.
        lookback: Option<usize>,
    },

    /// Size positions to risk a fixed dollar amount per trade.
    /// qty = floor(risk_amount / max_loss_per_contract)
    #[serde(rename = "risk_per_trade")]
    RiskPerTrade {
        /// Maximum dollar risk per trade.
        #[garde(range(min = 1.0))]
        risk_amount: f64,
    },

    /// Target a specific annualized portfolio volatility.
    /// Scales position size based on recent realized vol of the underlying.
    #[serde(rename = "volatility_target")]
    VolatilityTarget {
        /// Target annualized volatility (e.g., 0.15 = 15%).
        #[garde(range(min = 0.01, max = 2.0))]
        target_vol: f64,
        /// Rolling lookback window in calendar days for realized vol calculation.
        #[garde(range(min = 5, max = 252))]
        lookback_days: i32,
    },
}
```

### Max Loss Per Contract

The engine must compute the max loss per contract for each trade at entry time. This varies by strategy:
- **Credit spreads / iron condors**: `(width - credit) * multiplier`
- **Debit spreads**: `debit * multiplier`
- **Naked short puts/calls**: Theoretically unlimited, so use a practical estimate (e.g., `entry_cost * multiplier * stop_loss_factor`)
- **Straddles/strangles**: Use the premium received × multiplier as an approximation if stop-loss is defined

### Shared Constraints

All sizing methods should respect:
```rust
/// Constraints applied after the sizing method computes a raw quantity.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
pub struct SizingConstraints {
    /// Minimum contracts per trade (default: 1).
    #[serde(default = "default_min_qty")]
    #[garde(range(min = 1))]
    pub min_quantity: i32,

    /// Maximum contracts per trade (default: no limit).
    pub max_quantity: Option<i32>,
}
```

### Integration Point

In the simulation loop (`engine/sim.rs` or equivalent), at the point where a new position is opened:

```
Current flow:  → check entry conditions → open position with `params.quantity` contracts
New flow:      → check entry conditions → compute_position_size(sizing, equity, trade_risk) → open position with computed qty
```

The `equity` value at each entry is the running sum: `capital + sum(closed_trade_pnls)`.

### Response Changes

#### TradeLogEntry

Add a `computed_quantity` field when dynamic sizing is active:
```rust
pub struct TradeLogEntry {
    // ... existing fields ...
    /// Contracts used for this trade (may vary per trade when dynamic sizing is active).
    pub computed_quantity: Option<i32>,
    /// Portfolio equity at the time of entry.
    pub entry_equity: Option<f64>,
}
```

#### SizingSummary

Add a top-level summary block to `BacktestResponse`:
```rust
pub struct SizingSummary {
    pub method: String,
    pub avg_quantity: f64,
    pub min_quantity: i32,
    pub max_quantity: i32,
    pub final_equity: f64,
}
```

### Parameter Sweep Integration

`parameter_sweep` should support sweeping over sizing parameters:
- Sweep `risk_pct` from 0.01 to 0.05 in steps
- Sweep `fraction` (Kelly fraction) from 0.25 to 0.75

This can be done by adding `sizing` variants to the existing grid generation logic.

## Implementation Order

1. **`FixedFractional`** — simplest and most commonly used. Requires max-loss-per-contract calculation.
2. **`RiskPerTrade`** — similar to fixed-fractional but static dollar amount instead of equity percentage.
3. **`Kelly`** — requires rolling window statistics tracking.
4. **`VolatilityTarget`** — requires underlying price history in the simulation loop.

## Frontend Impact

When this is implemented, the frontend (`optopsy-ui`) will need:
- Updated `BacktestParameters` type with `sizing` field
- Updated `TradeLogEntry` type with `computed_quantity` and `entry_equity`
- New `SizingSummary` display in the backtest view
- Per-trade quantity column in the trade log table (already prepared with `qty` per leg)
- Optional "Quantity Over Time" chart showing how position size evolved
