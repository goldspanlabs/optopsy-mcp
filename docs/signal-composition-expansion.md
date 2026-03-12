# Signal Composition Expansion — Design Document

> **Goal:** Let an AI agent design, iterate, and compose signals entirely through formula strings, eliminating the need to construct nested `SignalSpec` JSON trees.

---

## Current State

The custom formula parser (`src/signals/custom.rs`) supports **8 functions**:

| Function | Signature | Polars mapping |
|----------|-----------|----------------|
| `sma` | `sma(col, period)` | `rolling_mean` |
| `ema` | `ema(col, period)` | `ewm_mean` (α = 2/(n+1)) |
| `std` | `std(col, period)` | `rolling_std` |
| `max` | `max(col, period)` | `rolling_max` |
| `min` | `min(col, period)` | `rolling_min` |
| `abs` | `abs(expr)` | `.abs()` |
| `change` | `change(col, period)` | `col - col.shift(period)` |
| `pct_change` | `pct_change(col, period)` | `(col - shifted) / shifted` |

**Valid columns:** `close`, `open`, `high`, `low`, `volume`, `adjclose`

The 40+ built-in indicators (RSI, MACD, Bollinger, Stochastic, ATR, etc.) are **only accessible as `SignalSpec` enum variants**, not as formula functions. Agents must construct nested JSON `And`/`Or` trees to combine them.

### Current agent experience (verbose JSON)

```json
{
  "type": "And",
  "left": { "type": "RsiBelow", "column": "close", "threshold": 30 },
  "right": { "type": "PriceAboveSma", "column": "close", "period": 50 }
}
```

### Target agent experience (single formula string)

```
rsi(close, 14) < 30 and close > sma(close, 50) and atr(close, high, low, 14) > 2.0
```

---

## Layer 1: TA Functions in the Formula Parser

**Priority:** P0
**Files:** `src/signals/custom.rs`
**Effort:** Medium

Add indicator functions to `build_function_call()` in the parser. These compile to Polars expressions or use `map`/`apply` for indicators that need imperative computation.

### New single-column functions

| Function | Signature | Notes |
|----------|-----------|-------|
| `rsi` | `rsi(col, period)` | Wilder's RSI via `rust_ti::bulk::rsi` |
| `macd_hist` | `macd_hist(col)` | MACD histogram (12/26/9 default) |
| `macd_signal` | `macd_signal(col)` | MACD signal line |
| `macd_line` | `macd_line(col)` | MACD line |
| `bbands_upper` | `bbands_upper(col, period)` | Bollinger upper band (2σ) |
| `bbands_lower` | `bbands_lower(col, period)` | Bollinger lower band (2σ) |
| `bbands_mid` | `bbands_mid(col, period)` | Bollinger middle (= SMA) |
| `roc` | `roc(col, period)` | Rate of change (%) |

### New multi-column functions

| Function | Signature | Notes |
|----------|-----------|-------|
| `atr` | `atr(close, high, low, period)` | Average true range |
| `stochastic` | `stochastic(close, high, low, period)` | Stochastic %K |
| `keltner_upper` | `keltner_upper(close, high, low, period, mult)` | Upper Keltner channel |
| `keltner_lower` | `keltner_lower(close, high, low, period, mult)` | Lower Keltner channel |
| `obv` | `obv(close, volume)` | On-balance volume |
| `mfi` | `mfi(close, high, low, volume, period)` | Money flow index |

### Implementation pattern: single-column indicators

For indicators like `rsi` that can't be expressed as pure Polars rolling operations, use `col.map()` with the existing `rust_ti` compute functions:

```rust
// In build_function_call(), add to the match:
"rsi" => {
    let (col_expr, period) = extract_col_period(&args, "rsi")?;
    Ok(col_expr.map(
        move |s| {
            let ca = s.f64()?;
            let vals: Vec<f64> = ca.into_no_null_iter().collect();
            let n = s.len();
            if n <= period {
                return Ok(Some(
                    Series::new("rsi".into(), vec![f64::NAN; n])
                ));
            }
            let rsi_vals = sti::rsi(&vals);
            let padded = pad_series(&rsi_vals, n);
            Ok(Some(Series::new("rsi".into(), padded)))
        },
        GetOutput::from_type(DataType::Float64),
    ))
}
```

### Implementation pattern: multi-column indicators

Multi-column indicators like `atr(close, high, low, 14)` need a new arg-extraction helper and `as_struct` grouping:

```rust
fn extract_three_cols_period(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, Expr, Expr, usize), String> {
    if args.len() != 4 {
        return Err(format!(
            "{func_name}() takes 4 arguments: (col1, col2, col3, period)"
        ));
    }
    let c1 = args[0].clone().into_expr();
    let c2 = args[1].clone().into_expr();
    let c3 = args[2].clone().into_expr();
    let period = args[3].as_usize()?;
    Ok((c1, c2, c3, period))
}
```

For `atr`, since Polars doesn't have a native ATR expression, use `map_multiple` on a struct of the three columns:

```rust
"atr" => {
    let (close_expr, high_expr, low_expr, period) =
        extract_three_cols_period(&args, "atr")?;
    // Build a struct column, then map over it
    Ok(as_struct(vec![close_expr, high_expr, low_expr]).map(
        move |s| {
            let ca = s.struct_()?;
            let close = ca.field_by_name("close")?.f64()?;
            let high = ca.field_by_name("high")?.f64()?;
            let low = ca.field_by_name("low")?.f64()?;
            let c: Vec<f64> = close.into_no_null_iter().collect();
            let h: Vec<f64> = high.into_no_null_iter().collect();
            let l: Vec<f64> = low.into_no_null_iter().collect();
            let atr_vals = compute_atr(&c, &h, &l, period);
            let padded = pad_series(&atr_vals, s.len());
            Ok(Some(Series::new("atr".into(), padded)))
        },
        GetOutput::from_type(DataType::Float64),
    ))
}
```

### Additional changes

- `FuncArg` needs `Clone` derived
- Add `extract_two_cols` helper for `obv(close, volume)`
- Add `extract_three_cols_period_mult` for `keltner_*(close, high, low, period, multiplier)` (5 args)
- Update the error message in the `other =>` match arm to list new functions
- Import `compute_atr`, `compute_bollinger_bands`, `compute_keltner_channel`, `compute_stochastic` from sibling modules

---

## Layer 2: Conditional Expressions (if/then/else)

**Priority:** P1
**Files:** `src/signals/custom.rs`
**Effort:** Small

### Grammar change

Add `if()` as a function in `build_function_call`:

```
if(condition, then_value, else_value)
```

### Implementation

Maps directly to Polars `when(...).then(...).otherwise(...)`:

```rust
"if" => {
    if args.len() != 3 {
        return Err("if() takes exactly 3 arguments: (condition, then_value, else_value)".into());
    }
    let cond = args[0].into_expr();
    let then_val = args[1].into_expr();
    let else_val = args[2].into_expr();
    Ok(when(cond).then(then_val).otherwise(else_val))
}
```

### Agent usage examples

Adaptive RSI threshold based on volatility:
```
if(atr(close, high, low, 14) > 3.0, rsi(close, 14) < 25, rsi(close, 14) < 35)
```

Conditional volume filter:
```
if(pct_change(close, 1) > 0.02, volume > sma(volume, 20) * 3.0, volume > sma(volume, 20) * 1.5)
```

---

## Layer 3: Multi-Timeframe Support

**Priority:** P2
**Files:** `src/signals/custom.rs`, `src/signals/mod.rs`, `src/signals/spec.rs`, `src/engine/core.rs`
**Effort:** Large

### Approach: `tf()` virtual timeframe function

Add a `tf(timeframe, expr)` function to the formula language that:

1. Resamples the OHLCV DataFrame to the specified timeframe
2. Evaluates the inner expression on the resampled frame
3. Forward-fills the result back to daily resolution
4. Makes it available as a virtual column for the outer expression

### Supported timeframes

| Keyword | Resample rule |
|---------|---------------|
| `weekly` | Calendar week (Mon–Fri) |
| `monthly` | Calendar month |

### Formula syntax

```
close > tf(weekly, sma(close, 20))
rsi(close, 14) < 30 and tf(monthly, ema(close, 12)) > tf(monthly, ema(close, 12))[1]
```

### Parser changes

In the tokenizer, add `weekly` and `monthly` as keywords (or treat them as identifiers consumed by the `tf` function).

In `build_function_call`:

```rust
"tf" => {
    if args.len() != 2 {
        return Err("tf() takes 2 arguments: (timeframe, expression)".into());
    }
    let timeframe = match &args[0] {
        FuncArg::Ident(s) => match s.as_str() {
            "weekly" => TimeframeSpec::Weekly,
            "monthly" => TimeframeSpec::Monthly,
            other => return Err(format!("Unknown timeframe: '{other}'. Use weekly or monthly")),
        },
        _ => return Err("First argument to tf() must be a timeframe name".into()),
    };
    let inner_expr = args[1].into_expr();
    // Return a placeholder that the evaluator resolves at runtime
    Ok(Expr::Alias(
        Box::new(inner_expr),
        format!("__tf_{timeframe:?}_{unique_id}").into(),
    ))
}
```

### Spec changes

Extend `SignalSpec::Custom` to carry detected timeframe references:

```rust
// In src/signals/spec.rs
Custom {
    name: String,
    formula: String,
    description: Option<String>,
    timeframes: Vec<TimeframeSpec>,  // NEW — auto-populated by parse
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum TimeframeSpec {
    Weekly,
    Monthly,
}
```

The parser populates `timeframes` by collecting all `tf()` calls encountered during parsing. This tells the engine which resampled DataFrames to prepare.

### Evaluation changes

In `FormulaSignal::evaluate()` (or a new `evaluate_with_timeframes()` method):

1. **Detect** `tf()` references in the parsed `Expr` tree
2. **Resample** the input DataFrame using Polars `group_by_dynamic()`:
   ```rust
   let weekly = df.clone().lazy()
       .group_by_dynamic(
           col("date"),
           [],
           DynamicGroupOptions {
               every: Duration::parse("1w"),
               period: Duration::parse("1w"),
               offset: Duration::parse("0d"),
               ..Default::default()
           },
       )
       .agg([
           col("open").first().alias("open"),
           col("high").max().alias("high"),
           col("low").min().alias("low"),
           col("close").last().alias("close"),
           col("volume").sum().alias("volume"),
       ])
       .collect()?;
   ```
3. **Evaluate** inner expressions on resampled data
4. **Forward-fill** and left-join results back to daily resolution
5. **Substitute** `tf()` placeholders with the joined column references
6. **Evaluate** the outer expression on the enriched DataFrame

### Engine changes (`src/engine/core.rs`)

In `build_signal_filters()`, when a `Custom` signal has non-empty `timeframes`:
- The OHLCV DataFrame is resampled to each needed timeframe
- Resampled frames are passed to the signal evaluator
- No changes to the options/stock event loop (they still receive `HashSet<NaiveDate>`)

---

## Layer 4: Cross-Column Derived Features

**Priority:** P3
**Files:** `src/signals/custom.rs`
**Effort:** Small

Add convenience functions that are composable from existing primitives but common enough to warrant first-class support.

### New functions

| Function | Signature | Expansion |
|----------|-----------|-----------|
| `tr` | `tr(close, high, low)` | `max(high - low, abs(high - close[1]), abs(low - close[1]))` |
| `rel_volume` | `rel_volume(volume, period)` | `volume / sma(volume, period)` |
| `range_pct` | `range_pct(close, high, low)` | `(close - low) / (high - low)` |
| `zscore` | `zscore(col, period)` | `(col - sma(col, period)) / std(col, period)` |
| `rank` | `rank(col, period)` | Percentile rank within rolling window |

### Implementation

Most are thin wrappers that build the equivalent Polars expression:

```rust
"rel_volume" => {
    let (col_expr, period) = extract_col_period(&args, "rel_volume")?;
    let sma_expr = col_expr.clone().rolling_mean(RollingOptionsFixedWindow {
        window_size: period,
        min_periods: period,
        ..Default::default()
    });
    Ok(col_expr / sma_expr)
}

"zscore" => {
    let (col_expr, period) = extract_col_period(&args, "zscore")?;
    let mean = col_expr.clone().rolling_mean(RollingOptionsFixedWindow {
        window_size: period, min_periods: period, ..Default::default()
    });
    let std = col_expr.clone().rolling_std(RollingOptionsFixedWindow {
        window_size: period, min_periods: period, ..Default::default()
    });
    Ok((col_expr - mean) / std)
}

"range_pct" => {
    if args.len() != 3 {
        return Err("range_pct() takes 3 arguments: (close, high, low)".into());
    }
    let close_e = args[0].into_expr();
    let high_e = args[1].into_expr();
    let low_e = args[2].into_expr();
    Ok((close_e - low_e.clone()) / (high_e - low_e))
}
```

---

## Supporting Changes

### Signal catalog (`src/signals/registry.rs`)

Add entries for every new formula function to `SIGNAL_CATALOG` so the `build_signal` search action can discover them:

```rust
SignalMeta {
    name: "rsi (formula)",
    category: "formula_functions",
    description: "RSI as a formula function: rsi(close, 14) < 30",
    params: &["column", "period"],
    example: "rsi(close, 14) < 30",
},
```

### Tool response (`src/tools/construct_signal/`)

Update the `formula_help` field in `BuildSignalResponse` to include:
- New function signatures
- `if()` syntax
- `tf()` syntax with examples
- Multi-column function argument order

### CLAUDE.md

Update the "Available formula functions" list in the `build_signal` tool reference.

### Tests

For each new function, add to the existing test section in `src/signals/custom.rs`:

```rust
#[test]
fn formula_rsi_parses() {
    let expr = parse_formula("rsi(close, 14) < 30").unwrap();
    // Verify it produces a valid Polars expression
}

#[test]
fn formula_atr_multi_col() {
    let expr = parse_formula("atr(close, high, low, 14) > 2.0").unwrap();
}

#[test]
fn formula_if_ternary() {
    let expr = parse_formula("if(close > 100, 1, 0)").unwrap();
}

#[test]
fn formula_nested_composition() {
    let expr = parse_formula(
        "rsi(close, 14) < 30 and close > bbands_lower(close, 20) and rel_volume(volume, 20) > 2.0"
    ).unwrap();
}
```

Integration tests should evaluate formulas against sample DataFrames to verify correctness of indicator values.

---

## Complete Function Reference (Post-Implementation)

### Existing (8)
`sma`, `ema`, `std`, `max`, `min`, `abs`, `change`, `pct_change`

### Layer 1: TA indicators (14)
`rsi`, `macd_hist`, `macd_signal`, `macd_line`, `bbands_upper`, `bbands_lower`, `bbands_mid`, `roc`, `atr`, `stochastic`, `keltner_upper`, `keltner_lower`, `obv`, `mfi`

### Layer 2: Control flow (1)
`if`

### Layer 3: Multi-timeframe (1)
`tf`

### Layer 4: Derived features (5)
`tr`, `rel_volume`, `range_pct`, `zscore`, `rank`

**Total: 29 functions** (up from 8)

---

## Agent Workflow After Implementation

1. **Discover** available functions via `build_signal` action `catalog` (updated with formula functions)
2. **Search** for relevant indicators via `build_signal` action `search` (e.g., "momentum oversold with volatility filter")
3. **Compose** a formula string:
   ```
   rsi(close, 14) < 30 and close > tf(weekly, sma(close, 20)) and rel_volume(volume, 20) > 2.0
   ```
4. **Validate** via `build_signal` action `validate`
5. **Backtest** via `run_stock_backtest` or `run_options_backtest` with `entry_signal: { "type": "Custom", "formula": "..." }`
6. **Iterate** — tweak parameters in the formula string, re-validate, re-backtest
7. **Save** promising signals via `build_signal` action `create` for reuse
8. **Sweep** signal variants via `parameter_sweep` with `entry_signals` array
