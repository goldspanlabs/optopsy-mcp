# Quantifiers & Portfolio Namespace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `when any/all` quantifier syntax and `portfolio.*` namespace to the Trading DSL for multi-leg options position management and portfolio-level risk controls.

**Architecture:** Two independent features sharing the same DSL pipeline (parser → codegen → validate). Quantifiers add a new AST node (`WhenAnyAll`) parsed from `when any/all ... has ...` syntax and code-generated as Rhai loops. Aggregation methods (`pos.legs.sum(delta)`) are expression-level rewrites in codegen. Portfolio namespace adds a `PortfolioState` struct computed per-bar and exposed via `ctx.portfolio` in Rhai.

**Tech Stack:** Rust, Rhai scripting engine, existing DSL transpiler pipeline

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/scripting/dsl/parser.rs` | Modify | Add `Quantifier` enum, `WhenAnyAll` variant to `Stmt`, parse `when any/all` syntax |
| `src/scripting/dsl/codegen.rs` | Modify | Generate Rhai loops for `WhenAnyAll`, expand `pos.legs.sum/count/min/max/avg()` expressions, rewrite `portfolio.X` → `ctx.portfolio.X` |
| `src/scripting/dsl/validate.rs` | Modify | Validate quantifiers only in `on_exit_check`, validate leg fields, portfolio property names, reject portfolio assignment |
| `src/scripting/types/bar_context.rs` | Modify | Add `PortfolioState` struct, `get_portfolio()` method on `BarContext` |
| `src/scripting/types/position.rs` | No change | Leg fields already sufficient |
| `src/scripting/registration.rs` | Modify | Register `PortfolioState` type with getters in Rhai engine |
| `src/scripting/engine.rs` | Modify | Track `peak_equity`, pass `PortfolioState` data to `BarContext` |
| `src/scripting/dsl/tests.rs` | Modify | All codegen and validation tests |

---

### Task 1: Add `PortfolioState` Struct and Rhai Registration

**Files:**
- Modify: `src/scripting/types/bar_context.rs:46-98` (add field + struct + getters)
- Modify: `src/scripting/registration.rs:52-194` (register PortfolioState getters)
- Test: `src/scripting/dsl/tests.rs`

- [ ] **Step 1: Write failing test for `portfolio.exposure_pct` codegen**

Add to `src/scripting/dsl/tests.rs`:

```rust
#[test]
fn test_portfolio_property_access() {
    let dsl = r#"
strategy "Portfolio Test"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  skip when portfolio.exposure_pct > 0.50
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.portfolio.exposure_pct"),
        "Should rewrite portfolio.exposure_pct to ctx.portfolio.exposure_pct.\nGenerated:\n{rhai}"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_portfolio_property_access --release -- --nocapture`
Expected: FAIL — `portfolio` is currently not recognized, either passes through as-is or errors.

- [ ] **Step 3: Define `PortfolioState` struct in `bar_context.rs`**

Add after the `BarContext` struct definition (after line 98):

```rust
// ---------------------------------------------------------------------------
// PortfolioState — the `portfolio` namespace exposed to Rhai scripts
// ---------------------------------------------------------------------------

/// Portfolio-level state computed once per bar.
/// Exposed to scripts as `ctx.portfolio` with property getters.
#[derive(Clone, Debug)]
pub struct PortfolioState {
    pub cash: f64,
    pub equity: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub total_exposure: f64,
    pub exposure_pct: f64,
    pub net_delta: f64,
    pub long_delta: f64,
    pub short_delta: f64,
    pub position_count: i64,
    pub long_count: i64,
    pub short_count: i64,
    pub max_position_pnl: f64,
    pub min_position_pnl: f64,
    pub drawdown: f64,
    pub peak_equity: f64,
}

impl PortfolioState {
    // --- Rhai getters ---
    pub fn get_cash(&mut self) -> f64 { self.cash }
    pub fn get_equity(&mut self) -> f64 { self.equity }
    pub fn get_unrealized_pnl(&mut self) -> f64 { self.unrealized_pnl }
    pub fn get_realized_pnl(&mut self) -> f64 { self.realized_pnl }
    pub fn get_total_exposure(&mut self) -> f64 { self.total_exposure }
    pub fn get_exposure_pct(&mut self) -> f64 { self.exposure_pct }
    pub fn get_net_delta(&mut self) -> f64 { self.net_delta }
    pub fn get_long_delta(&mut self) -> f64 { self.long_delta }
    pub fn get_short_delta(&mut self) -> f64 { self.short_delta }
    pub fn get_position_count(&mut self) -> i64 { self.position_count }
    pub fn get_long_count(&mut self) -> i64 { self.long_count }
    pub fn get_short_count(&mut self) -> i64 { self.short_count }
    pub fn get_max_position_pnl(&mut self) -> f64 { self.max_position_pnl }
    pub fn get_min_position_pnl(&mut self) -> f64 { self.min_position_pnl }
    pub fn get_drawdown(&mut self) -> f64 { self.drawdown }
    pub fn get_peak_equity(&mut self) -> f64 { self.peak_equity }
}
```

- [ ] **Step 4: Add `portfolio` field to `BarContext`**

Add to the `BarContext` struct fields (after `pending_orders_count`):

```rust
    /// Portfolio-level aggregate state, exposed as `ctx.portfolio`.
    pub portfolio: PortfolioState,
```

Add a getter method in the `impl BarContext` block:

```rust
    pub fn get_portfolio(&mut self) -> PortfolioState {
        self.portfolio.clone()
    }
```

- [ ] **Step 5: Register `PortfolioState` in Rhai engine**

In `src/scripting/registration.rs`, add a new function and call it from `build_engine()`:

```rust
/// Register `PortfolioState` as a Rhai custom type with getters.
fn register_portfolio_state(engine: &mut Engine) {
    engine.register_get("cash", PortfolioState::get_cash);
    engine.register_get("equity", PortfolioState::get_equity);
    engine.register_get("unrealized_pnl", PortfolioState::get_unrealized_pnl);
    engine.register_get("realized_pnl", PortfolioState::get_realized_pnl);
    engine.register_get("total_exposure", PortfolioState::get_total_exposure);
    engine.register_get("exposure_pct", PortfolioState::get_exposure_pct);
    engine.register_get("net_delta", PortfolioState::get_net_delta);
    engine.register_get("long_delta", PortfolioState::get_long_delta);
    engine.register_get("short_delta", PortfolioState::get_short_delta);
    engine.register_get("position_count", PortfolioState::get_position_count);
    engine.register_get("long_count", PortfolioState::get_long_count);
    engine.register_get("short_count", PortfolioState::get_short_count);
    engine.register_get("max_position_pnl", PortfolioState::get_max_position_pnl);
    engine.register_get("min_position_pnl", PortfolioState::get_min_position_pnl);
    engine.register_get("drawdown", PortfolioState::get_drawdown);
    engine.register_get("peak_equity", PortfolioState::get_peak_equity);
}
```

Add the import at the top of registration.rs:

```rust
use super::types::{BarContext, PortfolioState, ScriptPosition};
```

In `build_engine()`, add `register_portfolio_state(&mut engine);` after the existing type registrations. Also register `portfolio` as a getter on `BarContext`:

```rust
    // Portfolio namespace
    engine.register_get("portfolio", BarContext::get_portfolio);
```

- [ ] **Step 6: Add `portfolio` to codegen's `CTX_PROPERTIES` list**

In `src/scripting/dsl/codegen.rs`, add `"portfolio"` to the `CTX_PROPERTIES` array so `portfolio.X` rewrites to `ctx.portfolio.X`:

```rust
const CTX_PROPERTIES: &[&str] = &[
    // ... existing entries ...
    "pending_orders_count",
    // Portfolio namespace
    "portfolio",
];
```

- [ ] **Step 7: Update `BarContextFactory::build()` to compute and pass `PortfolioState`**

In `src/scripting/engine.rs`, in the `BarContextFactory` struct add a `peak_equity: f64` field (or track it externally in the simulation loop). In the `build()` method, compute `PortfolioState` from the positions arc and equity:

Add a new helper method to `BarContextFactory` or compute inline:

```rust
fn compute_portfolio_state(
    positions: &[ScriptPosition],
    equity: f64,
    cash: f64,
    capital: f64,
    peak_equity: f64,
) -> PortfolioState {
    let unrealized_pnl: f64 = positions.iter().map(|p| p.unrealized_pnl).sum();
    let realized_pnl = equity - capital;
    let total_exposure: f64 = positions.iter().map(|p| p.entry_cost.abs()).sum();
    let exposure_pct = if equity.abs() > f64::EPSILON {
        total_exposure / equity
    } else {
        0.0
    };

    let mut net_delta = 0.0;
    let mut long_delta = 0.0;
    let mut short_delta = 0.0;
    let mut long_count: i64 = 0;
    let mut short_count: i64 = 0;

    for pos in positions {
        match &pos.inner {
            ScriptPositionInner::Options { legs, multiplier, .. } => {
                let mut pos_delta = 0.0;
                for leg in legs {
                    let leg_delta = leg.delta * leg.qty as f64 * *multiplier as f64;
                    pos_delta += leg_delta;
                    if leg_delta > 0.0 {
                        long_delta += leg_delta;
                    } else {
                        short_delta += leg_delta;
                    }
                }
                if pos_delta > 0.0 {
                    long_count += 1;
                } else if pos_delta < 0.0 {
                    short_count += 1;
                }
                net_delta += pos_delta;
            }
            ScriptPositionInner::Stock { side, qty, .. } => {
                let d = match side {
                    Side::Long => *qty as f64,
                    Side::Short => -(*qty as f64),
                };
                net_delta += d;
                if d > 0.0 {
                    long_delta += d;
                    long_count += 1;
                } else {
                    short_delta += d;
                    short_count += 1;
                }
            }
        }
    }

    let position_count = positions.iter().filter(|p| !p.implicit).count() as i64;
    let max_position_pnl = positions.iter().map(|p| p.unrealized_pnl).fold(f64::NEG_INFINITY, f64::max);
    let min_position_pnl = positions.iter().map(|p| p.unrealized_pnl).fold(f64::INFINITY, f64::min);

    let drawdown = if peak_equity.abs() > f64::EPSILON {
        (equity - peak_equity) / peak_equity
    } else {
        0.0
    };

    PortfolioState {
        cash,
        equity,
        unrealized_pnl,
        realized_pnl,
        total_exposure,
        exposure_pct,
        net_delta,
        long_delta,
        short_delta,
        position_count,
        long_count,
        short_count,
        max_position_pnl: if positions.is_empty() { 0.0 } else { max_position_pnl },
        min_position_pnl: if positions.is_empty() { 0.0 } else { min_position_pnl },
        drawdown,
        peak_equity,
    }
}
```

In the simulation loop, track `peak_equity` as a running max. Before the `for (bar_idx, bar)` loop, add:

```rust
let mut peak_equity = config.capital;
```

At the end of each bar (after equity is updated), add:

```rust
peak_equity = peak_equity.max(equity);
```

Pass `peak_equity` to `BarContextFactory::build()` and compute `PortfolioState` inside it, assigning to the new `portfolio` field on `BarContext`.

- [ ] **Step 8: Run test to verify it passes**

Run: `cargo test test_portfolio_property_access --release -- --nocapture`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add -A && git commit -m "feat(dsl): add PortfolioState struct and portfolio namespace"
```

---

### Task 2: Portfolio Codegen — `skip when` and `when ... then` with Portfolio Properties

**Files:**
- Modify: `src/scripting/dsl/tests.rs`

- [ ] **Step 1: Write failing tests for portfolio in various positions**

Add to `src/scripting/dsl/tests.rs`:

```rust
#[test]
fn test_portfolio_in_skip_when() {
    let dsl = r#"
strategy "Portfolio Skip"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  skip when portfolio.drawdown < -0.10
  skip when portfolio.net_delta > 100
  buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.portfolio.drawdown < -0.10"),
        "Should rewrite portfolio.drawdown.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("ctx.portfolio.net_delta > 100"),
        "Should rewrite portfolio.net_delta.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_portfolio_in_when_then() {
    let dsl = r#"
strategy "Portfolio Guard"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  when portfolio.long_count >= 5 then
    hold position
  otherwise
    buy 100 shares
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("ctx.portfolio.long_count >= 5"),
        "Should rewrite portfolio.long_count in when condition.\nGenerated:\n{rhai}"
    );
}
```

- [ ] **Step 2: Run tests to verify they pass (should already work from Task 1)**

Run: `cargo test test_portfolio_in_skip_when test_portfolio_in_when_then --release -- --nocapture`
Expected: PASS — `portfolio` is in `CTX_PROPERTIES` so `rewrite_expr` already handles it.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "test(dsl): add portfolio codegen tests for skip when and when then"
```

---

### Task 3: Portfolio Validation — Unknown Properties and Read-Only Enforcement

**Files:**
- Modify: `src/scripting/dsl/validate.rs`
- Test: `src/scripting/dsl/tests.rs`

- [ ] **Step 1: Write failing tests for portfolio validation**

Add to `src/scripting/dsl/tests.rs`:

```rust
#[test]
fn test_portfolio_unknown_property_rejected() {
    let dsl = r#"
strategy "Bad Portfolio"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  skip when portfolio.foo > 1
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("portfolio"),
        "Should mention portfolio in error.\nGot: {}", err.message
    );
}

#[test]
fn test_portfolio_assignment_rejected() {
    let dsl = r#"
strategy "Bad Assignment"
  symbol SPY
  interval daily
  data ohlcv

on each bar
  set portfolio.cash to 1000
  buy 100 shares
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("read-only") || err.message.contains("cannot assign"),
        "Should reject assignment to portfolio.\nGot: {}", err.message
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test test_portfolio_unknown_property_rejected test_portfolio_assignment_rejected --release -- --nocapture`
Expected: FAIL — no validation exists yet.

- [ ] **Step 3: Add portfolio validation to `validate.rs`**

Add known portfolio properties list and validation functions:

```rust
/// Valid portfolio namespace properties.
const PORTFOLIO_PROPERTIES: &[&str] = &[
    "cash", "equity", "unrealized_pnl", "realized_pnl",
    "total_exposure", "exposure_pct",
    "net_delta", "long_delta", "short_delta",
    "position_count", "long_count", "short_count",
    "max_position_pnl", "min_position_pnl",
    "drawdown", "peak_equity",
];

/// Check for invalid `portfolio.X` property accesses and `set portfolio.X` assignments.
pub fn check_portfolio_access(program: &DslProgram) -> Result<(), DslError> {
    let blocks: Vec<&Option<Vec<Stmt>>> = vec![
        &program.on_bar,
        &program.on_exit_check,
        &program.on_position_opened,
        &program.on_position_closed,
        &program.on_end,
    ];

    for block in blocks.into_iter().flatten() {
        check_portfolio_in_stmts(block)?;
    }
    check_portfolio_in_stmts(&program.body)?;

    Ok(())
}

fn check_portfolio_in_stmts(stmts: &[Stmt]) -> Result<(), DslError> {
    for stmt in stmts {
        match stmt {
            Stmt::Set { name, expr, line } => {
                if name.starts_with("portfolio.") {
                    return Err(DslError::new(
                        *line,
                        "cannot assign to `portfolio` — it is read-only",
                    ));
                }
                check_portfolio_expr(expr, *line)?;
            }
            Stmt::SkipWhen { condition, line } => {
                check_portfolio_expr(condition, *line)?;
            }
            Stmt::When { condition, then_body, else_body, line } => {
                check_portfolio_expr(condition, *line)?;
                check_portfolio_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_portfolio_in_stmts(eb)?;
                }
            }
            Stmt::ForEach { iterable, body, line, .. } => {
                check_portfolio_expr(iterable, *line)?;
                check_portfolio_in_stmts(body)?;
            }
            Stmt::TryOpen { body, .. } => {
                check_portfolio_in_stmts(body)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn check_portfolio_expr(expr: &str, line: usize) -> Result<(), DslError> {
    let prefix = "portfolio.";
    let mut search = expr;
    while let Some(pos) = search.find(prefix) {
        let after = &search[pos + prefix.len()..];
        // Extract the property name (alphanumeric + underscore)
        let prop_end = after
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(after.len());
        let prop = &after[..prop_end];

        if !prop.is_empty() && !PORTFOLIO_PROPERTIES.contains(&prop) {
            return Err(DslError::new(
                line,
                format!(
                    "unknown portfolio property `{prop}`. Valid properties: {}",
                    PORTFOLIO_PROPERTIES.join(", ")
                ),
            ));
        }
        search = &after[prop_end..];
    }
    Ok(())
}
```

- [ ] **Step 4: Wire validation into `transpile()` in `mod.rs`**

In `src/scripting/dsl/mod.rs`, add the portfolio check after the interval check:

```rust
pub fn transpile(source: &str) -> Result<String, DslError> {
    let program = parser::parse(source)?;
    validate::check_interval_time_keywords(&program)?;
    validate::check_portfolio_access(&program)?;
    Ok(codegen::generate(&program))
}
```

Make `check_portfolio_access` public in `validate.rs`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test test_portfolio_unknown_property_rejected test_portfolio_assignment_rejected --release -- --nocapture`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(dsl): add portfolio property validation and read-only enforcement"
```

---

### Task 4: Parser — Add `WhenAnyAll` AST Node

**Files:**
- Modify: `src/scripting/dsl/parser.rs:78-207` (add Quantifier enum, WhenAnyAll variant)
- Modify: `src/scripting/dsl/parser.rs:644-837` (parse `when any/all` in `parse_statements`)

- [ ] **Step 1: Write failing test for `when any` parsing**

Add to `src/scripting/dsl/tests.rs`:

```rust
#[test]
fn test_when_any_leg_condition() {
    let dsl = r#"
strategy "Delta Check"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when any leg in pos.legs has delta > 0.50 then
    close position "delta too high"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    // Should generate a loop that checks any leg
    assert!(
        rhai.contains("__any_match"),
        "Should generate __any_match variable.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("pos.legs"),
        "Should iterate over pos.legs.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("delta > 0.50"),
        "Should check delta > 0.50.\nGenerated:\n{rhai}"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_when_any_leg_condition --release -- --nocapture`
Expected: FAIL — `when any` is not parsed yet.

- [ ] **Step 3: Add `Quantifier` enum and `WhenAnyAll` to `Stmt`**

In `src/scripting/dsl/parser.rs`, after the `OrderExitSpec` enum (around line 101), add:

```rust
/// Quantifier type for `when any/all` statements.
#[derive(Debug, Clone, Copy)]
pub enum Quantifier {
    Any,
    All,
}
```

Add the `WhenAnyAll` variant to the `Stmt` enum (after `When`):

```rust
    WhenAnyAll {
        quantifier: Quantifier,
        binding_var: String,
        iterable: String,
        condition: String,
        capture_as: Option<String>,
        then_body: Vec<Stmt>,
        else_body: Option<Vec<Stmt>>,
        line: usize,
    },
```

- [ ] **Step 4: Parse `when any/all` in `parse_statements`**

In `parse_statements()`, modify the `when` branch. Currently (line 672):

```rust
} else if let Some(rest) = content.strip_prefix("when ") {
    let (stmt, next) = parse_when_chain(lines, i, rest)?;
```

Change to check for `when any` / `when all` first:

```rust
} else if let Some(rest) = content.strip_prefix("when any ") {
    let (stmt, next) = parse_when_any_all(lines, i, Quantifier::Any, rest)?;
    stmts.push(stmt);
    i = next;
} else if let Some(rest) = content.strip_prefix("when all ") {
    let (stmt, next) = parse_when_any_all(lines, i, Quantifier::All, rest)?;
    stmts.push(stmt);
    i = next;
} else if let Some(rest) = content.strip_prefix("when ") {
    let (stmt, next) = parse_when_chain(lines, i, rest)?;
```

- [ ] **Step 5: Implement `parse_when_any_all`**

Add the parser function:

```rust
/// Parse `when any/all VAR in ITERABLE has CONDITION [as CAPTURE] then` block.
///
/// Grammar:
///   when any|all VAR in ITERABLE has|have CONDITION [as CAPTURE] then
///     BODY
///   [otherwise
///     ELSE_BODY]
fn parse_when_any_all(
    lines: &[Line],
    start: usize,
    quantifier: Quantifier,
    rest: &str,
) -> Result<(Stmt, usize), DslError> {
    let line_num = lines[start].num;
    let base_indent = lines[start].indent;

    // rest is e.g. "leg in pos.legs has delta > 0.50 then"
    // or "legs in pos.legs have current_price < 0.05 as hot_leg then"

    // Must end with " then"
    let rest = rest
        .strip_suffix(" then")
        .ok_or_else(|| DslError::new(line_num, "when any/all clause must end with 'then'"))?;

    // Split on " in "
    let in_pos = rest.find(" in ").ok_or_else(|| {
        DslError::new(line_num, "expected 'in' after variable name: when any VAR in ITERABLE has ...")
    })?;
    let binding_var = rest[..in_pos].trim().to_string();
    let after_in = rest[in_pos + 4..].trim();

    // Split on " has " or " have "
    let (iterable, after_has) = if let Some(pos) = after_in.find(" has ") {
        (&after_in[..pos], after_in[pos + 5..].trim())
    } else if let Some(pos) = after_in.find(" have ") {
        (&after_in[..pos], after_in[pos + 6..].trim())
    } else {
        return Err(DslError::new(
            line_num,
            "expected 'has' or 'have' after iterable: when any VAR in ITERABLE has CONDITION then",
        ));
    };
    let iterable = iterable.trim().to_string();

    // Check for " as CAPTURE" at the end
    let (condition, capture_as) = if let Some(as_pos) = after_has.rfind(" as ") {
        let capture = after_has[as_pos + 4..].trim().to_string();
        let cond = after_has[..as_pos].trim().to_string();
        (cond, Some(capture))
    } else {
        (after_has.to_string(), None)
    };

    // Collect the then-body (indented deeper)
    let mut then_body_lines = vec![];
    let mut i = start + 1;
    while i < lines.len() && lines[i].indent > base_indent {
        then_body_lines.push(lines[i].clone());
        i += 1;
    }

    if then_body_lines.is_empty() {
        return Err(DslError::new(line_num, "when any/all block has no indented body"));
    }

    let then_body = parse_statements(&then_body_lines)?;

    // Check for otherwise
    let else_body = if i < lines.len()
        && lines[i].indent == base_indent
        && lines[i].content == "otherwise"
    {
        let mut else_body_lines = vec![];
        i += 1;
        while i < lines.len() && lines[i].indent > base_indent {
            else_body_lines.push(lines[i].clone());
            i += 1;
        }
        if else_body_lines.is_empty() {
            return Err(DslError::new(lines[i - 1].num, "otherwise block has no indented body"));
        }
        Some(parse_statements(&else_body_lines)?)
    } else {
        None
    };

    Ok((
        Stmt::WhenAnyAll {
            quantifier,
            binding_var,
            iterable,
            condition,
            capture_as,
            then_body,
            else_body,
            line: line_num,
        },
        i,
    ))
}
```

- [ ] **Step 6: Run test to verify it still fails (parser works, but codegen doesn't handle it yet)**

Run: `cargo test test_when_any_leg_condition --release -- --nocapture`
Expected: FAIL — codegen panics or doesn't match `WhenAnyAll`.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(dsl): add WhenAnyAll AST node and parser for when any/all syntax"
```

---

### Task 5: Codegen — Generate Rhai Loops for `WhenAnyAll`

**Files:**
- Modify: `src/scripting/dsl/codegen.rs:534-866` (add WhenAnyAll to `generate_stmts`)
- Modify: `src/scripting/dsl/codegen.rs:79-143` (add WhenAnyAll to `collect_from_stmts`)

- [ ] **Step 1: Add `WhenAnyAll` to `generate_stmts` in codegen**

In `generate_stmts()`, add a new match arm after the `Stmt::When` arm (around line 616):

```rust
            Stmt::WhenAnyAll {
                quantifier,
                binding_var,
                iterable,
                condition,
                capture_as,
                then_body,
                else_body,
                ..
            } => {
                let iter_rw = rewrite_expr(iterable);
                let cond_rw = rewrite_quantifier_condition(condition, binding_var);

                match quantifier {
                    Quantifier::Any => {
                        out.push_str(&format!("{indent}let __any_match = false;\n"));
                        if let Some(ref cap) = capture_as {
                            out.push_str(&format!("{indent}let {cap} = ();\n"));
                        }
                        out.push_str(&format!("{indent}for {binding_var} in {iter_rw} {{\n"));
                        let inner = "    ".repeat(depth + 1);
                        out.push_str(&format!("{inner}if {cond_rw} {{\n"));
                        let inner2 = "    ".repeat(depth + 2);
                        out.push_str(&format!("{inner2}__any_match = true;\n"));
                        if let Some(ref cap) = capture_as {
                            out.push_str(&format!("{inner2}{cap} = {binding_var};\n"));
                        }
                        out.push_str(&format!("{inner2}break;\n"));
                        out.push_str(&format!("{inner}}}\n"));
                        out.push_str(&format!("{indent}}}\n"));
                        out.push_str(&format!("{indent}if __any_match {{\n"));
                        generate_stmts(out, then_body, depth + 1, kind, scope_vars);
                        out.push_str(&format!("{indent}}}"));
                        if let Some(ref eb) = else_body {
                            out.push_str(" else {\n");
                            generate_stmts(out, eb, depth + 1, kind, scope_vars);
                            out.push_str(&format!("{indent}}}"));
                        }
                        out.push('\n');
                    }
                    Quantifier::All => {
                        out.push_str(&format!("{indent}let __all_match = true;\n"));
                        out.push_str(&format!("{indent}for {binding_var} in {iter_rw} {{\n"));
                        let inner = "    ".repeat(depth + 1);
                        out.push_str(&format!("{inner}if !({cond_rw}) {{\n"));
                        let inner2 = "    ".repeat(depth + 2);
                        out.push_str(&format!("{inner2}__all_match = false;\n"));
                        out.push_str(&format!("{inner2}break;\n"));
                        out.push_str(&format!("{inner}}}\n"));
                        out.push_str(&format!("{indent}}}\n"));
                        out.push_str(&format!("{indent}if __all_match {{\n"));
                        generate_stmts(out, then_body, depth + 1, kind, scope_vars);
                        out.push_str(&format!("{indent}}}"));
                        if let Some(ref eb) = else_body {
                            out.push_str(" else {\n");
                            generate_stmts(out, eb, depth + 1, kind, scope_vars);
                            out.push_str(&format!("{indent}}}"));
                        }
                        out.push('\n');
                    }
                }
            }
```

- [ ] **Step 2: Add `rewrite_quantifier_condition` helper**

This rewrites the condition to qualify field access on the binding var. The condition `delta > 0.50` should NOT have `ctx.` prepended to `delta` since it refers to the leg field, not a ctx property.

```rust
/// Rewrite a quantifier condition expression. Field references on the binding
/// variable should remain as `binding_var.field`, not be rewritten to `ctx.field`.
/// The condition is prefixed with `binding_var.` for bare identifiers that are
/// known leg fields.
fn rewrite_quantifier_condition(condition: &str, binding_var: &str) -> String {
    const LEG_FIELDS: &[&str] = &[
        "delta", "strike", "current_price", "entry_price",
        "option_type", "side", "qty", "expiration",
    ];

    let mut result = String::with_capacity(condition.len() + 32);
    let chars: Vec<char> = condition.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '"' {
            // Copy string literals verbatim
            result.push(chars[i]);
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    result.push(chars[i]);
                    i += 1;
                }
                if i < chars.len() {
                    result.push(chars[i]);
                    i += 1;
                }
            }
            if i < chars.len() {
                result.push(chars[i]);
                i += 1;
            }
            continue;
        }

        if chars[i].is_alphabetic() || chars[i] == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let preceded_by_dot = start > 0 && chars[start - 1] == '.';

            if preceded_by_dot {
                result.push_str(&word);
            } else if LEG_FIELDS.contains(&word.as_str()) {
                result.push_str(&format!("{binding_var}.{word}"));
            } else if word == "and" {
                result.push_str("&&");
            } else if word == "or" {
                result.push_str("||");
            } else if word == "not" {
                result.push('!');
            } else {
                result.push_str(&word);
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }

    result
}
```

- [ ] **Step 3: Add `WhenAnyAll` to `collect_from_stmts` for indicator collection**

In `collect_from_stmts()` (around line 79), add a match arm:

```rust
            Stmt::WhenAnyAll {
                condition,
                then_body,
                else_body,
                ..
            } => {
                scan_expr(condition, specs, seen);
                collect_from_stmts(then_body, specs, seen);
                if let Some(ref eb) = else_body {
                    collect_from_stmts(eb, specs, seen);
                }
            }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_when_any_leg_condition --release -- --nocapture`
Expected: PASS

- [ ] **Step 5: Write and run test for `when all`**

Add to `src/scripting/dsl/tests.rs`:

```rust
#[test]
fn test_when_all_legs_condition() {
    let dsl = r#"
strategy "All Legs Check"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when all legs in pos.legs have current_price < 0.05 then
    close position "all legs worthless"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__all_match"),
        "Should generate __all_match variable.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("legs.current_price < 0.05"),
        "Should check current_price < 0.05 on each leg.\nGenerated:\n{rhai}"
    );
}
```

Run: `cargo test test_when_all_legs_condition --release -- --nocapture`
Expected: PASS

- [ ] **Step 6: Write and run test for binding (`as hot_leg`)**

```rust
#[test]
fn test_when_any_with_binding() {
    let dsl = r#"
strategy "Binding Test"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when any leg in pos.legs has delta > 0.50 as hot_leg then
    close position "hot leg at strike " + hot_leg.strike
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("let hot_leg = ();"),
        "Should declare hot_leg.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("hot_leg = leg;"),
        "Should capture leg into hot_leg.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("hot_leg.strike"),
        "Should reference hot_leg.strike in body.\nGenerated:\n{rhai}"
    );
}
```

Run: `cargo test test_when_any_with_binding --release -- --nocapture`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(dsl): codegen for when any/all quantifiers with binding support"
```

---

### Task 6: Codegen — Aggregation Methods (`pos.legs.sum/count/min/max/avg`)

**Files:**
- Modify: `src/scripting/dsl/codegen.rs` (expression-level rewriting in `rewrite_expr`)
- Test: `src/scripting/dsl/tests.rs`

- [ ] **Step 1: Write failing test for `pos.legs.sum(delta)`**

```rust
#[test]
fn test_legs_sum_aggregation() {
    let dsl = r#"
strategy "Sum Delta"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when pos.legs.sum(delta) > 1.0 then
    close position "net delta too high"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__agg"),
        "Should generate aggregation variable.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains(".delta"),
        "Should access .delta on each leg.\nGenerated:\n{rhai}"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_legs_sum_aggregation --release -- --nocapture`
Expected: FAIL — `pos.legs.sum(delta)` is not recognized.

- [ ] **Step 3: Add aggregation expansion to `rewrite_expr`**

The pattern to detect is `ITERABLE.sum(FIELD)`, `ITERABLE.count(COND)`, `ITERABLE.min(FIELD)`, `ITERABLE.max(FIELD)`, `ITERABLE.avg(FIELD)`.

Add a preprocessing step in `rewrite_expr`, called before the main char-level scanner. Add this function. Note: no regex crate is available, so this uses manual string scanning.

```rust
/// Aggregation methods recognized on iterables (e.g., `pos.legs.sum(delta)`).
const AGGREGATION_METHODS: &[&str] = &["sum", "count", "min", "max", "avg"];

/// Expand aggregation method calls on iterables.
/// `pos.legs.sum(delta)` → `{ let __agg = 0.0; for __el in pos.legs { __agg += __el.delta; } __agg }`
/// Scans for `.METHOD(` where METHOD is in AGGREGATION_METHODS, then walks backward to extract the iterable.
fn preprocess_aggregations(expr: &str) -> String {
    let mut result = expr.to_string();

    for method in AGGREGATION_METHODS {
        let pattern = format!(".{method}(");
        // Process from right to left to avoid offset invalidation
        while let Some(dot_pos) = result.rfind(&pattern) {
            // Extract iterable by walking backward from the dot
            let before = &result[..dot_pos];
            let iterable_start = before
                .rfind(|c: char| !c.is_alphanumeric() && c != '_' && c != '.')
                .map(|p| p + 1)
                .unwrap_or(0);
            let iterable = &result[iterable_start..dot_pos];

            if iterable.is_empty() {
                break;
            }

            // Extract args inside parens
            let args_start = dot_pos + pattern.len();
            let args_end = match result[args_start..].find(')') {
                Some(p) => args_start + p,
                None => break,
            };
            let arg = result[args_start..args_end].trim();

            let replacement = match *method {
                "sum" => format!(
                    "{{ let __agg = 0.0; for __el in {iterable} {{ __agg += __el.{arg}; }} __agg }}"
                ),
                "count" => {
                    let cond = rewrite_quantifier_condition(arg, "__el");
                    format!(
                        "{{ let __agg = 0; for __el in {iterable} {{ if {cond} {{ __agg += 1; }} }} __agg }}"
                    )
                }
                "min" => format!(
                    "{{ let __agg = 1e308; for __el in {iterable} {{ if __el.{arg} < __agg {{ __agg = __el.{arg}; }} }} __agg }}"
                ),
                "max" => format!(
                    "{{ let __agg = -1e308; for __el in {iterable} {{ if __el.{arg} > __agg {{ __agg = __el.{arg}; }} }} __agg }}"
                ),
                "avg" => format!(
                    "{{ let __sum = 0.0; let __cnt = 0; for __el in {iterable} {{ __sum += __el.{arg}; __cnt += 1; }} if __cnt > 0 {{ __sum / __cnt }} else {{ 0.0 }} }}"
                ),
                _ => continue,
            };

            result = format!("{}{}{}", &result[..iterable_start], replacement, &result[args_end + 1..]);
        }
    }

    result
}
```

Call `preprocess_aggregations` at the start of `rewrite_expr`:

```rust
pub fn rewrite_expr(expr: &str) -> String {
    let expr = preprocess_aggregations(expr);
    let expr = preprocess_inline_if(&expr);
    // ... rest unchanged
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_legs_sum_aggregation --release -- --nocapture`
Expected: PASS

- [ ] **Step 5: Write and run tests for count, min, max, avg**

```rust
#[test]
fn test_legs_count_aggregation() {
    let dsl = r#"
strategy "Count Legs"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when pos.legs.count(side == "long") > 2 then
    close position "too many long legs"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__agg"),
        "Should generate count variable.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("__el.side == \"long\""),
        "Should qualify side with __el.\nGenerated:\n{rhai}"
    );
}

#[test]
fn test_legs_min_max_avg_aggregation() {
    let dsl = r#"
strategy "Min Price"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when pos.legs.min(current_price) < 0.05 then
    close position "a leg is nearly worthless"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__el.current_price"),
        "Should access current_price on __el.\nGenerated:\n{rhai}"
    );
    assert!(
        rhai.contains("__agg"),
        "Should generate min aggregation variable.\nGenerated:\n{rhai}"
    );
}
```

Run: `cargo test test_legs_count_aggregation test_legs_min_max_avg_aggregation --release -- --nocapture`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(dsl): codegen for pos.legs aggregation methods (sum, count, min, max, avg)"
```

---

### Task 7: Validation — Quantifiers Context Check and Leg Field Validation

**Files:**
- Modify: `src/scripting/dsl/validate.rs`
- Modify: `src/scripting/dsl/mod.rs` (wire in new validation)
- Test: `src/scripting/dsl/tests.rs`

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn test_quantifier_outside_exit_check_rejected() {
    let dsl = r#"
strategy "Bad Quantifier"
  symbol SPY
  interval daily
  data ohlcv, options

on each bar
  when any leg in pos.legs has delta > 0.50 then
    close position "bad"
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("on exit check") || err.message.contains("exit_check"),
        "Should mention on exit check in error.\nGot: {}", err.message
    );
}

#[test]
fn test_invalid_leg_field_rejected() {
    let dsl = r#"
strategy "Bad Field"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when any leg in pos.legs has foo > 1 then
    close position "bad"
"#;

    let err = transpile(dsl).unwrap_err();
    assert!(
        err.message.contains("foo"),
        "Should mention unknown field.\nGot: {}", err.message
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test test_quantifier_outside_exit_check_rejected test_invalid_leg_field_rejected --release -- --nocapture`
Expected: FAIL

- [ ] **Step 3: Add quantifier validation to `validate.rs`**

```rust
/// Valid leg field names for quantifier conditions.
const LEG_FIELDS: &[&str] = &[
    "delta", "strike", "current_price", "entry_price",
    "option_type", "side", "qty", "expiration",
];

/// Numeric-only leg fields (valid for sum/min/max/avg).
const NUMERIC_LEG_FIELDS: &[&str] = &[
    "delta", "strike", "current_price", "entry_price", "qty",
];

/// Check that quantifier statements are only used inside `on_exit_check` blocks,
/// and that leg field references are valid.
pub fn check_quantifiers(program: &DslProgram) -> Result<(), DslError> {
    // Quantifiers are ONLY allowed in on_exit_check
    let disallowed_blocks: Vec<(&str, &Option<Vec<Stmt>>)> = vec![
        ("on each bar", &program.on_bar),
        ("on position opened", &program.on_position_opened),
        ("on position closed", &program.on_position_closed),
        ("on end", &program.on_end),
    ];

    for (block_name, block) in disallowed_blocks {
        if let Some(ref stmts) = block {
            check_no_quantifiers_in_stmts(stmts, block_name)?;
        }
    }
    check_no_quantifiers_in_stmts(&program.body, "procedural body")?;

    // In on_exit_check, validate the quantifier fields
    if let Some(ref stmts) = program.on_exit_check {
        check_quantifier_fields_in_stmts(stmts)?;
    }

    // Check aggregation methods in all blocks for field validity
    check_aggregation_fields(program)?;

    Ok(())
}

fn check_no_quantifiers_in_stmts(stmts: &[Stmt], block_name: &str) -> Result<(), DslError> {
    for stmt in stmts {
        match stmt {
            Stmt::WhenAnyAll { line, .. } => {
                return Err(DslError::new(
                    *line,
                    format!(
                        "quantifiers (`when any/all`) can only be used inside `on exit check`. \
                         Found in `{block_name}`. Use `for each pos in positions` in `on each bar` instead."
                    ),
                ));
            }
            Stmt::When { then_body, else_body, .. } => {
                check_no_quantifiers_in_stmts(then_body, block_name)?;
                if let Some(ref eb) = else_body {
                    check_no_quantifiers_in_stmts(eb, block_name)?;
                }
            }
            Stmt::ForEach { body, .. } => {
                check_no_quantifiers_in_stmts(body, block_name)?;
            }
            Stmt::TryOpen { body, .. } => {
                check_no_quantifiers_in_stmts(body, block_name)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn check_quantifier_fields_in_stmts(stmts: &[Stmt]) -> Result<(), DslError> {
    for stmt in stmts {
        match stmt {
            Stmt::WhenAnyAll { condition, line, .. } => {
                check_condition_fields(condition, *line)?;
            }
            Stmt::When { then_body, else_body, .. } => {
                check_quantifier_fields_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_quantifier_fields_in_stmts(eb)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Check that the condition in a quantifier only references known leg fields.
fn check_condition_fields(condition: &str, line: usize) -> Result<(), DslError> {
    // Extract bare identifiers from the condition (not preceded by dot, quote, or digit)
    let chars: Vec<char> = condition.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '"' {
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' { i += 1; }
                i += 1;
            }
            if i < chars.len() { i += 1; }
            continue;
        }
        if chars[i].is_alphabetic() || chars[i] == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let preceded_by_dot = start > 0 && chars[start - 1] == '.';
            // Skip logical operators and known non-field tokens
            if !preceded_by_dot
                && !["and", "or", "not", "true", "false"].contains(&word.as_str())
                && !word.parse::<f64>().is_ok()
                && !LEG_FIELDS.contains(&word.as_str())
            {
                return Err(DslError::new(
                    line,
                    format!(
                        "unknown leg field `{word}` in quantifier condition. \
                         Valid fields: {}",
                        LEG_FIELDS.join(", ")
                    ),
                ));
            }
        } else {
            i += 1;
        }
    }
    Ok(())
}

/// Check aggregation expressions like pos.legs.sum(FIELD) for valid fields.
fn check_aggregation_fields(program: &DslProgram) -> Result<(), DslError> {
    let blocks: Vec<&Option<Vec<Stmt>>> = vec![
        &program.on_bar,
        &program.on_exit_check,
        &program.on_position_opened,
        &program.on_position_closed,
        &program.on_end,
    ];
    for block in blocks.into_iter().flatten() {
        check_aggregation_in_stmts(block)?;
    }
    check_aggregation_in_stmts(&program.body)?;
    Ok(())
}

fn check_aggregation_in_stmts(stmts: &[Stmt]) -> Result<(), DslError> {
    for stmt in stmts {
        let exprs_and_lines: Vec<(&str, usize)> = match stmt {
            Stmt::SkipWhen { condition, line } => vec![(condition.as_str(), *line)],
            Stmt::Set { expr, line, .. } => vec![(expr.as_str(), *line)],
            Stmt::When { condition, then_body, else_body, line } => {
                check_aggregation_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_aggregation_in_stmts(eb)?;
                }
                vec![(condition.as_str(), *line)]
            }
            Stmt::WhenAnyAll { then_body, else_body, .. } => {
                check_aggregation_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_aggregation_in_stmts(eb)?;
                }
                vec![]
            }
            Stmt::ForEach { body, .. } => {
                check_aggregation_in_stmts(body)?;
                vec![]
            }
            _ => vec![],
        };

        for (expr, line) in exprs_and_lines {
            // Look for .sum(X), .min(X), .max(X), .avg(X)
            for method in &["sum", "min", "max", "avg"] {
                let pattern = format!(".{method}(");
                if let Some(pos) = expr.find(&pattern) {
                    let after = &expr[pos + pattern.len()..];
                    if let Some(end) = after.find(')') {
                        let field = after[..end].trim();
                        if !NUMERIC_LEG_FIELDS.contains(&field) {
                            return Err(DslError::new(
                                line,
                                format!(
                                    "`{method}()` requires a numeric field, got `{field}`. \
                                     Valid fields: {}",
                                    NUMERIC_LEG_FIELDS.join(", ")
                                ),
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Wire validation into `transpile()` in `mod.rs`**

```rust
pub fn transpile(source: &str) -> Result<String, DslError> {
    let program = parser::parse(source)?;
    validate::check_interval_time_keywords(&program)?;
    validate::check_portfolio_access(&program)?;
    validate::check_quantifiers(&program)?;
    Ok(codegen::generate(&program))
}
```

- [ ] **Step 5: Add `WhenAnyAll` handling to existing validation functions**

In `check_stmts()` (the intraday keyword checker), add a match arm for `WhenAnyAll`:

```rust
            Stmt::WhenAnyAll {
                condition,
                then_body,
                else_body,
                line,
                ..
            } => {
                check_expr(condition, *line, interval, check_intraday)?;
                check_stmts(then_body, interval, check_intraday)?;
                if let Some(ref eb) = else_body {
                    check_stmts(eb, interval, check_intraday)?;
                }
            }
```

In `check_reserved_names_in_stmts()`, add a match arm:

```rust
            Stmt::WhenAnyAll {
                binding_var,
                capture_as,
                then_body,
                else_body,
                line,
                ..
            } => {
                if is_reserved_name(binding_var) {
                    return Err(DslError::new(
                        *line,
                        format!(
                            "variable `{binding_var}` conflicts with reserved day/month name. \
                             Choose a different variable name."
                        ),
                    ));
                }
                if let Some(ref cap) = capture_as {
                    if is_reserved_name(cap) {
                        return Err(DslError::new(
                            *line,
                            format!(
                                "variable `{cap}` conflicts with reserved day/month name. \
                                 Choose a different variable name."
                            ),
                        ));
                    }
                }
                check_reserved_names_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_reserved_names_in_stmts(eb)?;
                }
            }
```

Also in `check_portfolio_in_stmts()`, add:

```rust
            Stmt::WhenAnyAll { condition, then_body, else_body, line, .. } => {
                check_portfolio_expr(condition, *line)?;
                check_portfolio_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_portfolio_in_stmts(eb)?;
                }
            }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test test_quantifier_outside_exit_check_rejected test_invalid_leg_field_rejected --release -- --nocapture`
Expected: PASS

- [ ] **Step 7: Run full test suite**

Run: `cargo test --release`
Expected: All tests pass

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat(dsl): add quantifier and portfolio validation with field checks"
```

---

### Task 8: Full Test Suite and Edge Cases

**Files:**
- Modify: `src/scripting/dsl/tests.rs`

- [ ] **Step 1: Add comprehensive edge case tests**

```rust
#[test]
fn test_close_position_without_reason_in_exit_check() {
    // close position (bare) should work in on_exit_check
    let dsl = r#"
strategy "Simple Exit"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when any leg in pos.legs has delta > 0.50 then
    close position "delta breach"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("return close_position(\"delta breach\")"));
}

#[test]
fn test_multiple_quantifiers_in_exit_check() {
    let dsl = r#"
strategy "Multi Quantifier"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when any leg in pos.legs has delta > 0.50 then
    close position "delta too high"
  when all legs in pos.legs have current_price < 0.10 then
    close position "all legs cheap"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("__any_match"));
    assert!(rhai.contains("__all_match"));
}

#[test]
fn test_portfolio_and_quantifier_together() {
    let dsl = r#"
strategy "Combined"
  symbol SPY
  interval daily
  data ohlcv, options

on each bar
  skip when portfolio.exposure_pct > 0.50
  buy 100 shares

on exit check
  when any leg in pos.legs has delta > 0.50 then
    close position "delta too high"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(rhai.contains("ctx.portfolio.exposure_pct > 0.50"));
    assert!(rhai.contains("__any_match"));
}

#[test]
fn test_aggregation_in_condition_with_comparison() {
    let dsl = r#"
strategy "Avg Delta"
  symbol SPY
  interval daily
  data ohlcv, options

on exit check
  when pos.legs.avg(delta) > 0.30 then
    close position "avg delta too high"
  otherwise
    hold position
"#;

    let rhai = transpile(dsl).unwrap();
    assert!(
        rhai.contains("__sum") && rhai.contains("__cnt"),
        "Should generate avg aggregation.\nGenerated:\n{rhai}"
    );
}
```

- [ ] **Step 2: Run all tests**

Run: `cargo test --release`
Expected: All tests pass

- [ ] **Step 3: Run clippy and fmt**

Run: `cargo clippy --all-targets --release` and `cargo fmt --check`
Expected: No warnings, no formatting issues. Fix any issues.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test(dsl): comprehensive tests for quantifiers and portfolio namespace"
```

---

### Task 9: Final — Build Verification and Squash

**Files:** None (verification only)

- [ ] **Step 1: Full build**

Run: `cargo build --release`
Expected: Clean build, no warnings

- [ ] **Step 2: Full test suite**

Run: `cargo test --release`
Expected: All tests pass

- [ ] **Step 3: Clippy clean**

Run: `cargo clippy --all-targets --release -- -D warnings`
Expected: No warnings

- [ ] **Step 4: Squash commits into one feature commit**

Squash all task commits into a single commit:

```bash
git rebase -i HEAD~N  # where N is the number of task commits
```

Final commit message:

```
feat(dsl): add quantifiers (when any/all) and portfolio namespace (#167)

- Add `when any/all VAR in ITERABLE has CONDITION [as CAPTURE] then` syntax
  for iterating over position legs in on_exit_check
- Add aggregation methods: pos.legs.sum(field), .count(cond), .min(field),
  .max(field), .avg(field)
- Add `portfolio.*` namespace with 16 properties: cash, equity, exposure_pct,
  net_delta, drawdown, position counts, etc.
- PortfolioState computed per-bar with peak_equity tracking for drawdown
- Validation: quantifiers restricted to on_exit_check, field validation,
  portfolio property validation, read-only enforcement
```
