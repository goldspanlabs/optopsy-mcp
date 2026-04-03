# DSL Quantifiers & Portfolio Namespace

**Date:** 2026-04-03
**Status:** Approved
**Scope:** Parser, codegen, validation, engine, Rhai registration

## Overview

Two DSL features to improve multi-leg options position management and portfolio-level risk controls:

1. **Quantifiers** — `when any/all` syntax + aggregation methods over position legs
2. **Portfolio namespace** — `portfolio.*` properties for portfolio-level state

## Feature 3: Quantifiers

### Context

Quantifiers are valid wherever `pos` is in scope: inside `on exit check` (where `pos` is implicit) or nested inside a `for each pos in positions` block in `on_bar`. Outside these contexts, quantifiers are a compile error.

### Syntax: Condition-only (any/all)

```
when any leg in pos.legs has delta > 0.50 then
  close position

when all legs in pos.legs have current_price < 0.05 then
  close position with reason "all legs worthless"
```

### Syntax: Condition + binding

```
when any leg in pos.legs has delta > 0.50 as hot_leg then
  close position with reason "leg " + hot_leg.strike + " too hot"
```

The `as <name>` clause captures the first matching leg into a variable usable in the body.

### Syntax: Aggregation (method-style)

```
when pos.legs.sum(delta) > 1.0 then
  close position with reason "net delta too high"

when pos.legs.count(side == "long") > 2 then
  close position

when pos.legs.min(current_price) < 0.05 then
  close position with reason "a leg is nearly worthless"
```

Supported methods: `sum(field)`, `count(condition)`, `min(field)`, `max(field)`, `avg(field)`.

### Parser AST

New `Stmt` variant:

```rust
WhenAnyAll {
    quantifier: Quantifier,     // Any or All
    binding_var: String,        // "leg"
    iterable: String,           // "pos.legs"
    condition: String,          // "delta > 0.50"
    capture_as: Option<String>, // Some("hot_leg") for binding
    body: Vec<Stmt>,
    otherwise: Option<Vec<Stmt>>,
    line: usize,
}
```

New enum:

```rust
enum Quantifier { Any, All }
```

Aggregation (`pos.legs.sum(delta)`) does not need a new AST node — it is an expression recognized and expanded by codegen.

### Codegen

**`when any` → Rhai:**

```rhai
let __any_match = false;
let hot_leg = ();  // only if `as hot_leg` present
for __leg in pos.legs {
    if __leg.delta > 0.50 {
        __any_match = true;
        hot_leg = __leg;  // only if binding
        break;
    }
}
if __any_match {
    // body
}
```

**`when all` → Rhai:**

```rhai
let __all_match = true;
for __leg in pos.legs {
    if !(__leg.current_price < 0.05) {
        __all_match = false;
        break;
    }
}
if __all_match {
    // body
}
```

**`pos.legs.sum(delta)` → Rhai (inline expansion):**

Each aggregation expansion uses a unique counter suffix (`_0`, `_1`, ...) to avoid variable collisions when multiple aggregations appear in the same expression (e.g., `pos.legs.sum(delta) + pos.legs.max(strike) > 100`).

```rhai
{
    let __agg_0 = 0.0;
    for __el_0 in pos.legs { __agg_0 += __el_0.delta; }
    __agg_0
}
```

**`pos.legs.count(side == "long")` → Rhai:**

```rhai
{
    let __agg_1 = 0;
    for __el_1 in pos.legs {
        if __el_1.side == "long" { __agg_1 += 1; }
    }
    __agg_1
}
```

`min`, `max`, `avg` follow the same loop pattern with unique suffixes.

### Validation

- `when any/all ... in pos.legs` outside `on exit check` or `for each pos in positions` → compile error with guidance
- Unknown leg field in `has FIELD ...` → error listing valid fields: `delta`, `strike`, `current_price`, `entry_price`, `option_type`, `side`, `qty`, `expiration`
- `sum/min/max/avg` on non-numeric field (`option_type`, `side`, `expiration`) → compile error
- `as <name>` binding must not shadow reserved words or existing variables

---

## Feature 4: Portfolio Namespace

### PortfolioState struct

Computed once per bar, cached on `BarContext`:

```rust
pub struct PortfolioState {
    // Core (existing data, regrouped)
    pub cash: f64,
    pub equity: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,

    // Exposure
    pub total_exposure: f64,   // sum of abs(entry_cost) across all positions
    pub exposure_pct: f64,     // total_exposure / equity

    // Greeks (aggregate across all open option positions)
    pub net_delta: f64,        // sum of leg.delta * qty * multiplier
    pub long_delta: f64,       // sum of positive delta contributions
    pub short_delta: f64,      // sum of negative delta contributions

    // Position counts
    pub position_count: i64,
    pub long_count: i64,       // positions with net positive delta or long stock
    pub short_count: i64,

    // P&L
    pub max_position_pnl: f64, // best-performing open position unrealized P&L
    pub min_position_pnl: f64, // worst-performing open position unrealized P&L
    pub drawdown: f64,         // (equity - peak_equity) / peak_equity, always ≤ 0.0
    pub peak_equity: f64,      // high-water mark across simulation
}
// Note: `position_count` excludes implicit positions (e.g., auto-hedged stock from assignment),
// so `long_count + short_count` may not equal `position_count`.
```

### DSL Syntax

```
skip when portfolio.exposure_pct > 0.50
skip when portfolio.net_delta > 100
skip when portfolio.drawdown < -0.10
when portfolio.long_count >= 5 then
  skip
```

### Codegen

`portfolio.X` rewrites to `ctx.portfolio.X`. The portfolio is a Rhai object with property getters for each field.

### Engine Changes

- Track `peak_equity` as a running maximum across the simulation loop
- Compute `PortfolioState` at the start of each bar, before `on_bar()` and `on_exit_check()`
- `drawdown = (equity - peak_equity) / peak_equity`

### Backward Compatibility

Existing `ctx.cash`, `ctx.equity`, etc. continue to work unchanged. `portfolio.*` is an additional namespace that groups them together. No breaking changes.

### Validation

- Unknown property on `portfolio.X` → compile error listing valid properties
- Assignment to portfolio (`set portfolio.cash to 1000`) → compile error (read-only)

---

## Testing

### Quantifiers — Codegen Tests

| Test | Input | Verifies |
|------|-------|----------|
| `test_when_any_leg_condition` | `when any leg in pos.legs has delta > 0.50 then close position` | Loop with early break, action inside guard |
| `test_when_all_legs_condition` | `when all legs in pos.legs have current_price < 0.05 then` | All-check loop, break on first false |
| `test_when_any_with_binding` | `when any leg ... has delta > 0.50 as hot_leg then` | `hot_leg` variable captured |
| `test_legs_sum` | `pos.legs.sum(delta) > 1.0` | Accumulator loop with unique suffix |
| `test_legs_count` | `pos.legs.count(side == "long")` | Count loop with condition |
| `test_legs_min_max_avg` | `pos.legs.min(current_price)`, etc. | Each aggregation method |
| `test_multiple_aggregations` | `pos.legs.sum(delta) + pos.legs.max(strike) > 100` | Unique variable suffixes (`__agg_0`, `__agg_1`) |
| `test_aggregation_non_numeric` | `pos.legs.sum(option_type)` | Compile error (non-numeric field) |
| `test_quantifier_outside_pos_scope` | Quantifier in `on_bar` without `for each pos` | Compile error |
| `test_quantifier_inside_for_each_pos` | Quantifier nested in `for each pos in positions` in `on_bar` | Compiles successfully |
| `test_invalid_leg_field` | `has foo > 1` | Error listing valid fields |

### Portfolio — Codegen Tests

| Test | Input | Verifies |
|------|-------|----------|
| `test_portfolio_property_access` | `portfolio.exposure_pct` | Rewrites to `ctx.portfolio.exposure_pct` |
| `test_portfolio_in_skip_when` | `skip when portfolio.drawdown < -0.10` | Correct Rhai guard |
| `test_portfolio_unknown_property` | `portfolio.foo` | Compile error |
| `test_portfolio_assignment_rejected` | `set portfolio.cash to 1000` | Read-only error |

### Integration Tests (Engine-Level)

| Test | Scenario | Verifies |
|------|----------|----------|
| `test_quantifier_closes_on_delta` | Position leg delta exceeds threshold | Position closed by quantifier |
| `test_portfolio_exposure_skip` | Exposure limit prevents new entries | Entries skipped after N positions |

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/scripting/dsl/parser.rs` | Add `WhenAnyAll` variant, `Quantifier` enum, parse `when any/all` syntax |
| `src/scripting/dsl/codegen.rs` | Generate Rhai loops for quantifiers, expand aggregation methods, rewrite `portfolio.X` |
| `src/scripting/dsl/validate.rs` | Context checks (pos-scope: exit_check or for-each-pos), field validation, portfolio property validation |
| `src/scripting/types/bar_context.rs` | Add `PortfolioState` struct, `portfolio()` method on `BarContext` |
| `src/scripting/types/position.rs` | No changes (leg fields already sufficient) |
| `src/scripting/registration.rs` | Register `PortfolioState` getters with Rhai engine |
| `src/scripting/engine.rs` | Track `peak_equity`, compute `PortfolioState` each bar |
| `src/scripting/dsl/tests.rs` | All codegen tests listed above |
| `src/scripting/engine/` (integration tests) | Engine-level integration tests |
