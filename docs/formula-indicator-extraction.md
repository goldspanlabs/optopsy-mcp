# Formula Indicator Extraction — Design Doc

## Problem

Before the formula DSL refactor, `compute_indicator_data` matched on 40+ `SignalSpec` variants (e.g. `RsiBelow`, `MacdBullish`) to produce chart overlay data. Now everything is `SignalSpec::Formula { formula: String }`, so the function returns `vec![]` for all signals. Backtest responses include an empty `indicator_data` field — trades and metrics are correct, but chart visualizations have no indicator overlays.

## Goal

Parse a formula string like `rsi(close, 14) < 30 and close > bbands_lower(close, 20)` and extract indicator series (RSI line, Bollinger lower band) for chart rendering, without re-evaluating the formula.

## Approach

### Phase 1: Extract function calls from the formula AST

The formula parser (`src/signals/custom.rs`) already tokenizes and parses formulas into an expression tree. Add a lightweight AST visitor that collects function call nodes.

```rust
struct IndicatorCall {
    name: String,          // "rsi", "bbands_lower", "macd_hist"
    args: Vec<FuncArg>,    // cloned from the parse tree
}

fn extract_indicator_calls(formula: &str) -> Vec<IndicatorCall>
```

This reuses the existing `Parser` and `Expr` types. Walk the parsed tree, collect every `Expr::FunctionCall { name, args }` node, deduplicate by (name, args).

### Phase 2: Map function calls to indicator series

For each `IndicatorCall`, compute the full indicator series against the OHLCV DataFrame. This is the same computation `build_function_call` already does — the difference is we want the raw f64 series, not a boolean comparison result.

```rust
fn compute_indicator_series(
    call: &IndicatorCall,
    df: &DataFrame,
) -> Option<(String, Vec<f64>)>
```

Returns `(label, values)` — e.g. `("RSI(14)", [NaN, NaN, ..., 45.2, 38.1, ...])`.

### Phase 3: Map to IndicatorData

Convert each series into the existing `IndicatorData` struct, which includes:
- `name` — display label
- `display_type` — `Overlay` (same axis as price) or `Subchart` (separate pane)
- `points` — `Vec<IndicatorPoint>` with date + value
- `reference_lines` — threshold lines (e.g. RSI 30/70)

Display type mapping:
| Function | Display | Reference lines |
|----------|---------|-----------------|
| `rsi` | Subchart | 30, 70 |
| `macd_hist`, `macd_signal`, `macd_line` | Subchart | 0 |
| `stochastic` | Subchart | 20, 80 |
| `mfi` | Subchart | 20, 80 |
| `sma`, `ema`, `bbands_*`, `keltner_*`, `supertrend` | Overlay | — |
| `atr`, `tr` | Subchart | — |
| `obv`, `cmf` | Subchart | 0 |
| `aroon_*` | Subchart | — |

### Phase 4: Wire into compute_indicator_data

Replace the `Formula` match arm:

```rust
SignalSpec::Formula { formula } => {
    let calls = extract_indicator_calls(formula);
    let dates = extract_date_strings(ohlcv_df, date_col);
    calls.iter()
        .filter_map(|call| {
            let (label, values, display, refs) = compute_indicator_series(call, ohlcv_df)?;
            Some(IndicatorData {
                name: label,
                display_type: display,
                points: build_points(&values, &dates),
                reference_lines: refs,
                total_points: None,
            })
        })
        .collect()
}
```

## Scope

- Only extract top-level indicator function calls — ignore bare column refs and arithmetic
- `and`/`or` branches: extract from both sides, deduplicate by name
- Comparison operators (`<`, `>`) are stripped — we want the indicator value, not the boolean
- Lookback expressions like `sma(close, 5)[1]` — extract `sma(close, 5)`, ignore the shift
- `if(cond, then, else)` — extract indicators from the condition only

## Files Modified

| File | Changes |
|------|---------|
| `src/signals/custom.rs` | Add `extract_indicator_calls()` — AST visitor |
| `src/signals/indicators.rs` | Replace `Formula` arm with extraction + computation |
| `src/signals/indicators.rs` | Reuse existing helper functions (`compute_rsi_indicator`, etc.) |

## Testing

- Parse `rsi(close, 14) < 30` → extracts `[IndicatorCall("rsi", [close, 14])]`
- Parse `rsi(close, 14) < 30 and close > bbands_lower(close, 20)` → extracts 2 calls
- Parse `close > sma(close, 50)` → extracts `[IndicatorCall("sma", [close, 50])]`
- Parse `close > 100` → extracts nothing (no function calls)
- Evaluate extracted RSI against a sample DataFrame → produces non-empty `IndicatorData`
