# HMM Regime Entry Signal Integration

## Overview

Integrate HMM regime detection into the backtest entry signal system as a new formula function `hmm_regime()`. Users can gate trades by HMM state — e.g., "only trade when SPY is in the bullish regime."

Currently `regime_detect` runs HMM as a standalone analysis tool. This design wires HMM regime classification into the formula engine so it composes with all existing signals.

## Function Signature

```
hmm_regime(n_regimes, fit_years)              # fits on primary backtest symbol
hmm_regime(SPY, n_regimes, fit_years)         # fits on explicit symbol
```

**Arguments (positional, overloaded by count):**

| Args | Signature | Example |
|------|-----------|---------|
| 2 | `(n_regimes: int, fit_years: int)` | `hmm_regime(3, 5)` |
| 3 | `(symbol: ident, n_regimes: int, fit_years: int)` | `hmm_regime(SPY, 3, 5)` |

- `n_regimes`: 2-4 (number of HMM states)
- `fit_years`: 1-50 (years of pre-backtest data for HMM fitting)
- `symbol`: optional ticker as an unquoted identifier; defaults to backtest's primary symbol when omitted

**Usage in formulas:**

```
hmm_regime(3, 5) == bullish
hmm_regime(SPY, 3, 5) != bearish
hmm_regime(3, 5) >= 1
hmm_regime(SPY, 3, 5) == 2 and rsi(close, 14) < 30
```

## Named Regime Aliases

Both numeric indices and named aliases are supported in comparisons. Named aliases are unquoted identifiers (not string literals), consistent with the existing formula syntax. States are sorted by ascending mean return after HMM fitting (state 0 = lowest mean return).

| n_regimes | Index 0 | Index 1 | Index 2 | Index 3 |
|-----------|---------|---------|---------|---------|
| 2 | `bearish` | `bullish` | — | — |
| 3 | `bearish` | `neutral` | `bullish` | — |
| 4 | `strong_bear` | `mild_bear` | `mild_bull` | `strong_bull` |

Alias resolution happens during the formula rewriting pass (see below), not in the parser. The pre-evaluation pass replaces `hmm_regime(SPY, 3, 5) == bullish` with `__hmm_regime_SPY_3_5_65 == 2` before the formula reaches the tokenizer.

## Approach: Formula Rewriting with Pre-computed Column Injection

`hmm_regime` is implemented as a **formula rewriting** pre-evaluation pass. Before the formula string reaches the tokenizer/parser:

1. Scan the formula for `hmm_regime(...)` calls and their comparison expressions
2. Compute the regime column and inject it into the DataFrame
3. Rewrite the formula string, replacing the entire `hmm_regime(...) == alias` expression with `__hmm_regime_SPY_3_5_65 == 2`

The parser never sees `hmm_regime` — it only sees a column reference and integer comparison, both of which it already handles.

### Why Formula Rewriting (Not Parser Changes)

The existing tokenizer does not support string literals (`Token` enum has no `StringLit` variant). The parser is stateless — it builds Polars `Expr` nodes without access to DataFrames, date ranges, or the primary backtest symbol. Adding string literal support would require changes to the tokenizer, parser, `FuncArg` enum, and potentially every dispatch function.

Formula rewriting avoids all of this: the pre-evaluation pass handles HMM-specific syntax (symbol idents, named aliases, arg parsing) and produces a rewritten formula using only existing token types (identifiers and numbers). Zero changes to the tokenizer, parser, `FuncArg`, or dispatch table.

### Rewriting Rules

The pre-evaluation pass scans the formula string and applies these rewrites:

**Function call → column reference:**
```
hmm_regime(3, 5)            →  __hmm_regime_AAPL_3_5_65   (primary symbol, default threshold)
hmm_regime(SPY, 3, 5)       →  __hmm_regime_SPY_3_5_65    (explicit symbol, default threshold)
hmm_regime(SPY, 3, 5, 0.8)  →  __hmm_regime_SPY_3_5_80   (explicit threshold)
```

**Named alias → integer literal:**
```
== bullish    →  == 2      (for n_regimes=3)
!= bearish   →  != 0      (for n_regimes=3)
== neutral    →  == 1      (for n_regimes=3)
```

**Combined:**
```
hmm_regime(SPY, 3, 5) == bullish and rsi(close, 14) < 30
→ __hmm_regime_SPY_3_5_65 == 2 and rsi(close, 14) < 30
```

Unknown aliases produce an error listing valid names for that `n_regimes`.

### Cross-Symbol Interaction

Because `hmm_regime(...)` is rewritten before cross-symbol detection runs, the symbol identifier (e.g., `SPY`) is never seen by `extract_formula_cross_symbols()`. The rewritten column name `__hmm_regime_SPY_3_5_65` starts with `__`, but `extract_formula_cross_symbols()` currently has no `__` prefix check and would flag it as a cross-symbol. A `__` prefix exclusion must be added to `extract_formula_cross_symbols()` so internal computed columns are skipped.

`hmm_regime` must be added to `KNOWN_FUNCTIONS` in `custom_funcs/mod.rs` so the cross-symbol extractor skips it if it encounters it in an unrewritten context (defensive).

### Parser Column Resolution

The parser's `parse_primary()` treats unknown identifiers as cross-symbol references — uppercasing them and appending `_close` (e.g., `SPY` becomes `col("SPY_close")`). This would mishandle `__hmm_regime_SPY_3_5_65`, producing `col("__HMM_REGIME_SPY_3_5_close")` instead of `col("__hmm_regime_SPY_3_5_65")`.

**Fix:** The HMM rewrite pass returns a set of injected column names alongside the rewritten formula. This set is passed through to the parser context. In `parse_primary()`, before the cross-symbol fallback, check if the identifier matches an injected column name — if so, emit `col(name)` directly without uppercasing or appending `_close`.

This requires a small change to the parser: `parse_primary()` (and its parent `Parser`) gains an optional `injected_columns: &HashSet<String>` parameter, defaulting to empty. Only the HMM integration path provides a non-empty set. All existing call sites are unaffected.

Alternatively, the check can use the `__` prefix convention: any identifier starting with `__` is treated as a literal column reference. This is simpler (no parameter threading) and establishes a general convention for future computed columns.

## No Look-ahead: Forward Filtering

### Problem

Viterbi decoding (used by `regime_detect` today) operates on the full observation sequence — future data influences past state assignments. This is look-ahead bias, unacceptable for backtesting.

### Solution: Online Forward Filter

Fit the HMM on pre-backtest data only. Classify backtest bars using forward filtering — each bar's regime is determined using only data up to and including that bar.

**Algorithm:**

1. Fit HMM via Baum-Welch EM on returns in `[start_date - fit_years, start_date)`
2. Initialize forward filter with the model's stationary distribution as prior
3. For each bar `t` in the apply window `[start_date, end_date]`:
   - **Predict:** `prior[j] = sum_i(posterior[t-1][i] * A[i][j])`
   - **Update:** `posterior[t][j] = prior[j] * N(return_t | mu_j, sigma_sq_j)`
   - **Normalize:** `posterior[t][j] /= sum_j(posterior[t][j])`
   - **Classify:** `regime[t] = argmax_j(posterior[t][j])`
4. If observations is empty, return `vec![]`

Complexity: O(K^2 * T) per series, fast for K=2-4.

**Numerical stability:** The predict-update cycle can produce very small posterior values over long sequences. Use log-space computation (log-sum-exp for normalization) to prevent underflow, consistent with the existing Baum-Welch implementation in `hmm.rs` which uses scaling factors.

**New function:** `forward_filter(hmm: &GaussianHmm, observations: &[f64]) -> Vec<usize>` in `engine/hmm.rs`.

### Regime Switching Stability

The raw forward filter uses `argmax(posterior)` to classify each bar. When the posterior is near 50/50 between states, this produces daily regime flips that generate unrealistic entry/exit churn in backtests.

**Mitigation: Posterior probability threshold.** Instead of raw argmax, a regime switch only occurs when the new state's posterior exceeds a confidence threshold. If no state exceeds the threshold, the previous regime carries forward.

- Default threshold: `0.65` (switch only when posterior > 65%)
- This acts as a hysteresis filter — once in a state, you stay until the evidence for another state is strong
- Combined with the HMM's diagonal-biased transition matrix (0.7 stay probability), this produces stable regime labels

The `forward_filter` function signature becomes:

```
forward_filter(hmm: &GaussianHmm, observations: &[f64], threshold: f64) -> Vec<usize>
```

The formula function exposes this as an optional 4th argument with a sensible default:

```
hmm_regime(3, 5)                  # threshold defaults to 0.65
hmm_regime(SPY, 3, 5)             # threshold defaults to 0.65
hmm_regime(3, 5, 0.8)             # explicit threshold (stricter)
hmm_regime(SPY, 3, 5, 0.8)       # explicit threshold
```

Arg parsing by count becomes:
- 2 args: `(n_regimes, fit_years)` — primary symbol, default threshold
- 3 args: `(symbol, n_regimes, fit_years)` or `(n_regimes, fit_years, threshold)` — disambiguated by whether arg 1 is an identifier or number
- 4 args: `(symbol, n_regimes, fit_years, threshold)`

Threshold range: `0.5 < threshold <= 1.0` (must exceed random chance for K states).

## End-to-End Data Flow

For a backtest with entry signal `hmm_regime(SPY, 3, 5) == bullish and rsi(close, 14) < 30`:

```
1. Parse backtest params                              (existing)
2. Load primary OHLCV DataFrame                       (existing)
3. Scan formula for hmm_regime() calls                (NEW)
   → Extract: [("SPY", 3, 5, 0.65)]
   → Rewrite formula: "__hmm_regime_SPY_3_5_65 == 2 and rsi(close, 14) < 30"
4. For each unique (symbol, n_regimes, fit_years, threshold): (NEW)
   a. Load symbol OHLCV from cache
   b. Split at backtest start_date
   c. Fit HMM on pre-backtest returns (min 50 bars)
   d. Forward-filter classify backtest bars (with threshold)
   e. Inject __hmm_regime_SPY_3_5_65 column into primary DF
   f. Check for overlapping emission distributions; emit warning if found
5. Detect cross-symbols on rewritten formula          (existing, sees no hmm_regime)
6. Pre-join any remaining cross-symbol DFs            (existing)
7. build_signal() → parse rewritten formula → Expr    (existing)
   __hmm_regime_SPY_3_5_65 resolves to col(...)
   2 resolves to lit(2)
8. Evaluate Expr on DataFrame → bool Series           (existing)
9. Extract active dates → HashSet                     (existing)
10. Event loop gates entries on HashSet               (existing)
```

## Column Naming Convention

`__hmm_regime_{SYMBOL}_{N_REGIMES}_{FIT_YEARS}_{THRESHOLD}` (e.g., `__hmm_regime_SPY_3_5_65_65`)

- Double underscore prefix signals internal/computed columns
- `fit_years` is included to avoid collisions when the same symbol/n_regimes pair is used with different fit windows
- `threshold` is encoded as an integer (65 for 0.65) to keep column names clean
- If the same full tuple appears in both entry and exit signals, the column is computed once and reused

## Code Changes

| File | Change |
|------|--------|
| `engine/hmm.rs` | Add `forward_filter(hmm, observations, threshold)` function (log-space, returns `Vec<usize>`); add `overlapping_emissions(hmm)` check |
| `signals/hmm_rewrite.rs` | New module: formula scanning, arg extraction, alias resolution, string rewriting, alias leak validation |
| `signals/mod.rs` | Call HMM rewrite pass before cross-symbol detection; inject computed columns into DF |
| `signals/custom.rs` | In `parse_primary()`, treat identifiers starting with `__` as literal column references (skip uppercase + `_close` suffix) |
| `signals/custom_funcs/mod.rs` | Add `hmm_regime` to `KNOWN_FUNCTIONS` (defensive, prevents cross-symbol false positive) |
| `signals/registry.rs` | Add `SignalInfo` entries for `hmm_regime` to `SIGNAL_CATALOG` under a "regime" category; update `catalog_has_all_signals` test count; add `__` prefix exclusion in `extract_formula_cross_symbols()` |

**No changes to:** tokenizer (beyond the `__` prefix check in `parse_primary`), `FuncArg` enum, dispatch table, `SignalSpec`, `SignalFn` trait, `AndSignal`/`OrSignal` combinators, `active_dates()` / `active_dates_multi()`, backtest event loops, `BacktestParams` / `StockBacktestParams` structs, MCP tool definitions, or any existing formula functions.

## Error Handling

### Rewrite-time Errors

- Wrong arg count: `"hmm_regime expects 2 or 3 arguments, got N"`
- `n_regimes` out of range: `"n_regimes must be 2-4, got N"`
- `fit_years` out of range: `"fit_years must be 1-50, got N"`
- `threshold` out of range: `"threshold must be between 0.5 (exclusive) and 1.0 (inclusive), got N"`
- Unknown alias: `"unknown regime name 'foo' for n_regimes=3; expected: bearish, neutral, bullish"`
- Missing comparison: `"hmm_regime() must be used in a comparison (==, !=, >, <, >=, <=)"`
- Alias leak: after rewriting, validate that no regime alias identifiers (`bullish`, `bearish`, `neutral`, `strong_bear`, `mild_bear`, `mild_bull`, `strong_bull`) survive in the formula as bare tokens; if they do, raise an explicit error rather than letting the parser treat them as cross-symbols

### Pre-evaluation Errors

- Symbol not found in cache: `"no OHLCV data found for 'XYZ'; available categories: stocks, etf, indices"`
- Insufficient fit data: `"hmm_regime requires at least 50 bars before backtest start date; only found N bars for SPY with fit_years=5"`

### Runtime Edge Cases

- All bars classified as same regime: signal works, just always true or always false
- HMM convergence issues: existing 100-iteration cap and tolerance check handle this
- NaN returns in fit window: filtered out before fitting (existing behavior)
- Date gaps in apply window: null regime value, formula evaluates to false (matches existing null handling)
- Empty apply window: `forward_filter` returns `vec![]`, column is all-null, no entries fire

## Quantitative Caveats

### Fit Window Sensitivity

HMM regime labels are **not stable across different fit windows**. Fitting on 3 years vs 7 years of SPY data can produce qualitatively different state decompositions — different number of effective states, different emission means, different transition dynamics. Users may naively assume that `hmm_regime(SPY, 3, 5) == bullish` and `hmm_regime(SPY, 3, 7) == bullish` identify the same market condition. They may not.

**Guardrails:**
- The `regime_detect` tool (standalone) should be run first to inspect the fitted states and verify they are interpretable before using `hmm_regime()` in a backtest signal. The `suggested_next_steps` in `regime_detect` responses should recommend this workflow.
- The `build_signal` catalog entry for `hmm_regime` should include a note: "Run `regime_detect` first to inspect state decomposition for your symbol and fit window before using in backtests."
- When the HMM fit produces states with heavily overlapping emission distributions (e.g., means within 1 std dev of each other), emit a warning in the backtest output: `"HMM states have overlapping distributions — regime labels may be unreliable. Consider using fewer states or a longer fit window."`

### State Label Non-stationarity

States are sorted by ascending mean return of the **fit window**. This is a sensible convention, but mean return is a noisy sorting key, especially for short fit windows or n_regimes=4 where states may have overlapping return distributions. The "bullish" state in the fit window (highest mean return) may exhibit low or negative returns in the apply window due to non-stationarity. The named aliases (`bullish`, `bearish`, etc.) describe fit-window characteristics, not guarantees about apply-window behavior.

This is inherent to any regime model applied out of sample and is not a bug. However:
- The `key_findings` in backtest output should report per-regime realized performance in the **apply window** (not just fit-window labels), so users can see whether the regime filter actually separated good from bad trading environments.
- Consider adding a per-regime summary to the backtest response: for each regime, show the number of trades, win rate, and mean P&L. This makes regime effectiveness immediately visible without requiring a separate analysis step.

## Alternatives Considered

**Parser modification (add string literals + dispatch):** Would require `Token::StringLit`, `FuncArg::String`, tokenizer changes, and threading the primary symbol through the dispatch context. Rejected because it touches many files and the existing parser is cleanly stateless.

**Lazy computation inside parser dispatch:** Would require threading DataFrames and date ranges through the parser. Rejected because it breaks the parser's stateless design.

**Regime as a virtual cross-symbol:** Would reuse cross-symbol pre-join infrastructure. Rejected because cross-symbols load from Parquet files and don't have access to the backtest start date for fit/apply splitting.

## Testing Strategy

### Unit Tests (`engine/hmm.rs`)

- `forward_filter` returns same length as input observations
- `forward_filter` on data drawn from known regimes recovers correct states
- `forward_filter` with single bar doesn't panic
- `forward_filter` with empty observations returns empty vec
- `forward_filter` results mostly agree with Viterbi on same-fit data (sanity check)
- `forward_filter` with threshold=0.65 produces fewer regime switches than threshold=0.5
- `forward_filter` carries forward previous regime when no state exceeds threshold

### Unit Tests (`signals/hmm_rewrite.rs`)

- Extracts `hmm_regime(3, 5)` with no symbol → uses provided primary symbol
- Extracts `hmm_regime(SPY, 3, 5)` with explicit symbol
- Rewriting replaces entire `hmm_regime(...) == bullish` with `__hmm_regime_SPY_3_5_65 == 2`
- Rewriting preserves surrounding formula: `hmm_regime(3, 5) == 1 and rsi(close, 14) < 30`
- Wrong arg count returns error
- Alias resolution: `bullish` maps to correct int for n_regimes 2, 3, 4
- Unknown alias returns descriptive error with valid options
- Numeric comparison passes through unchanged: `hmm_regime(3, 5) >= 1`
- Multiple `hmm_regime` calls in one formula are all rewritten
- Same tuple deduplication: two identical calls produce one column computation
- Alias leak validation: bare `bullish` not adjacent to `hmm_regime` produces clear error

### Integration Tests (`signals/mod.rs`)

- HMM pre-evaluation detects `hmm_regime(...)` in formula and injects column
- Injected column has correct name (`__hmm_regime_SPY_3_5_65`)
- Column values are valid regime indices (0 to n_regimes-1)
- No look-ahead: regime at bar `t` doesn't change when future bars are removed
- Formula with `hmm_regime` + other signals (`and rsi(...)`) evaluates correctly
- Same `(symbol, n_regimes, fit_years)` in entry and exit signal computes once
- Cross-symbol extractor does not pick up HMM column names as cross-symbols

### Backtest Integration Tests

- Stock backtest with `hmm_regime` entry signal runs end-to-end
- Options backtest with `hmm_regime` entry signal runs end-to-end
- Parameter sweep with `hmm_regime` in entry signals works
- Backtest with insufficient pre-fit data returns clear error
- Overlapping emission distributions produce warning in backtest output
- Per-regime trade summary (count, win rate, mean P&L) appears in backtest response
