# HMM Regime Entry Signal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `hmm_regime()` formula function so users can gate backtest entries by HMM state (e.g., `hmm_regime(SPY, 3, 5) == bullish`).

**Architecture:** Formula rewriting pre-pass detects `hmm_regime(...)` calls, fits HMM on pre-backtest data, forward-filters the apply window with a posterior threshold, injects a regime column into the DataFrame, and rewrites the formula to reference that column. The existing parser/evaluator handles the rest unchanged.

**Tech Stack:** Rust, Polars, existing `engine/hmm.rs` (Baum-Welch EM), `signals/` module (formula DSL)

**Spec:** `docs/superpowers/specs/2026-03-20-hmm-regime-entry-signal-design.md`

**Worktree:** `.worktrees/feat-hmm-regime-signal` (branch: `feat/hmm-regime-signal`)

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `src/engine/hmm.rs` | Modify | Add `forward_filter()` and `overlapping_emissions()` |
| `src/signals/hmm_rewrite.rs` | Create | Formula scanning, arg extraction, alias resolution, rewriting |
| `src/signals/mod.rs` | Modify | Wire HMM rewrite pass before cross-symbol detection |
| `src/signals/custom.rs` | Modify | `__` prefix convention in `parse_primary()` |
| `src/signals/custom_funcs/mod.rs` | Modify | Add `hmm_regime` to `KNOWN_FUNCTIONS` |
| `src/signals/registry.rs` | Modify | `__` prefix exclusion + `SIGNAL_CATALOG` entries |

---

## Task 1: Add `forward_filter()` to `engine/hmm.rs`

**Files:**
- Modify: `src/engine/hmm.rs`

- [ ] **Step 1: Write failing tests for `forward_filter`**

Add these tests to the existing `mod tests` block at the end of `src/engine/hmm.rs`:

```rust
    #[test]
    fn test_forward_filter_length_matches_observations() {
        let data = two_state_data(400);
        let hmm = fit(&data, 2);
        let result = forward_filter(&hmm, &data[200..], 0.65);
        assert_eq!(result.len(), 200);
    }

    #[test]
    fn test_forward_filter_empty_observations() {
        let hmm = fit(&[1.0, 2.0, 3.0, 4.0], 2);
        let result = forward_filter(&hmm, &[], 0.65);
        assert!(result.is_empty());
    }

    #[test]
    fn test_forward_filter_single_bar() {
        let data = two_state_data(400);
        let hmm = fit(&data, 2);
        let result = forward_filter(&hmm, &[0.01], 0.65);
        assert_eq!(result.len(), 1);
        assert!(result[0] < hmm.n_states);
    }

    #[test]
    fn test_forward_filter_values_in_range() {
        let data = two_state_data(400);
        let hmm = fit(&data, 2);
        let result = forward_filter(&hmm, &data, 0.65);
        for &s in &result {
            assert!(s < 2, "state {s} out of range for 2-state HMM");
        }
    }

    #[test]
    fn test_forward_filter_recovers_known_regimes() {
        let data = two_state_data(400);
        let hmm = fit(&data[..200], 2); // fit on first half only
        let result = forward_filter(&hmm, &data[200..], 0.65); // apply to second half

        // Second half of two_state_data is the high-return regime (state 1)
        let state1_count = result.iter().filter(|&&s| s == 1).count();
        assert!(
            state1_count as f64 / result.len() as f64 > 0.5,
            "expected mostly state 1 in second half, got {state1_count}/{}",
            result.len()
        );
    }

    #[test]
    fn test_forward_filter_threshold_reduces_switches() {
        let data = two_state_data(400);
        let hmm = fit(&data, 2);

        let loose = forward_filter(&hmm, &data, 0.5001);
        let strict = forward_filter(&hmm, &data, 0.8);

        let count_switches = |path: &[usize]| -> usize {
            path.windows(2).filter(|w| w[0] != w[1]).count()
        };

        assert!(
            count_switches(&strict) <= count_switches(&loose),
            "stricter threshold should produce fewer or equal switches: strict={}, loose={}",
            count_switches(&strict),
            count_switches(&loose)
        );
    }

    #[test]
    fn test_forward_filter_carries_forward_when_below_threshold() {
        // With threshold=1.0, no state ever exceeds threshold, so the initial state
        // should carry forward for every bar (zero switches).
        let data = two_state_data(400);
        let hmm = fit(&data, 2);
        let result = forward_filter(&hmm, &data, 1.0);

        let switches: usize = result.windows(2).filter(|w| w[0] != w[1]).count();
        assert_eq!(switches, 0, "threshold=1.0 should produce zero switches");
    }

    #[test]
    fn test_forward_filter_mostly_agrees_with_viterbi() {
        let data = two_state_data(400);
        let hmm = fit(&data, 2);
        let viterbi_path = viterbi(&hmm, &data);
        let filter_path = forward_filter(&hmm, &data, 0.5001);

        let agree = viterbi_path
            .iter()
            .zip(filter_path.iter())
            .filter(|(a, b)| a == b)
            .count();
        let pct = agree as f64 / data.len() as f64;
        assert!(
            pct > 0.7,
            "forward filter and viterbi should mostly agree: {:.1}%",
            pct * 100.0
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib engine::hmm::tests::test_forward_filter -- --nocapture`
Expected: compilation error — `forward_filter` not defined

- [ ] **Step 3: Implement `forward_filter()`**

Add this function after the existing `viterbi()` function and before `#[cfg(test)]`:

```rust
/// Online forward filter: classify each observation using only past data.
///
/// Unlike Viterbi (which uses the full sequence), this processes bars one at a time,
/// avoiding look-ahead bias. A regime switch only happens when the new state's
/// posterior exceeds `threshold`; otherwise the previous regime carries forward.
///
/// `threshold` must be in (0.5, 1.0]. Values near 1.0 produce very stable (sticky)
/// regime labels; values near 0.5 behave like raw argmax.
#[allow(clippy::needless_range_loop)]
pub fn forward_filter(hmm: &GaussianHmm, observations: &[f64], threshold: f64) -> Vec<usize> {
    let t = observations.len();
    let k = hmm.n_states;
    if t == 0 {
        return vec![];
    }

    let mut result = Vec::with_capacity(t);

    // Compute stationary distribution as initial prior:
    // Approximate by using hmm.initial (already a valid distribution).
    let mut posterior = hmm.initial.clone();

    // Classify first bar
    let mut log_post = vec![0.0_f64; k];
    for j in 0..k {
        log_post[j] = posterior[j].max(1e-300).ln()
            + log_gaussian(observations[0], hmm.means[j], hmm.variances[j]);
    }
    // Log-sum-exp normalization
    let max_lp = log_post.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let mut norm_post = vec![0.0_f64; k];
    let sum_exp: f64 = log_post.iter().map(|&lp| (lp - max_lp).exp()).sum();
    let log_norm = max_lp + sum_exp.ln();
    for j in 0..k {
        norm_post[j] = (log_post[j] - log_norm).exp();
    }

    // Apply threshold: initial state is argmax (no previous to carry forward)
    let initial_state = norm_post
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .map_or(0, |(i, _)| i);
    result.push(initial_state);
    posterior.copy_from_slice(&norm_post);

    let mut prev_state = initial_state;

    // Process remaining bars
    for tt in 1..t {
        // Predict: propagate through transition matrix
        let mut predicted = vec![0.0_f64; k];
        for j in 0..k {
            for i in 0..k {
                predicted[j] += posterior[i] * hmm.transition[i][j];
            }
        }

        // Update: weight by emission likelihood (in log space)
        for j in 0..k {
            log_post[j] = predicted[j].max(1e-300).ln()
                + log_gaussian(observations[tt], hmm.means[j], hmm.variances[j]);
        }

        // Log-sum-exp normalization
        let max_lp = log_post.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let sum_exp: f64 = log_post.iter().map(|&lp| (lp - max_lp).exp()).sum();
        let log_norm = max_lp + sum_exp.ln();
        for j in 0..k {
            norm_post[j] = (log_post[j] - log_norm).exp();
        }

        // Classify with threshold hysteresis
        let argmax_state = norm_post
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map_or(0, |(i, _)| i);

        let state = if argmax_state != prev_state && norm_post[argmax_state] > threshold {
            argmax_state
        } else {
            prev_state
        };

        result.push(state);
        posterior.copy_from_slice(&norm_post);
        prev_state = state;
    }

    result
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib engine::hmm::tests -- --nocapture`
Expected: all HMM tests pass (existing + new)

- [ ] **Step 5: Commit**

```bash
git add src/engine/hmm.rs
git commit -m "feat: add forward_filter() for online HMM regime classification

Processes bars one at a time using only past data, avoiding look-ahead
bias. Posterior threshold hysteresis prevents rapid regime switching."
```

---

## Task 2: Add `overlapping_emissions()` to `engine/hmm.rs`

**Files:**
- Modify: `src/engine/hmm.rs`

- [ ] **Step 1: Write failing tests**

Add to the `mod tests` block:

```rust
    #[test]
    fn test_overlapping_emissions_well_separated() {
        let data = two_state_data(400);
        let hmm = fit(&data, 2);
        // two_state_data has means -0.01 and 0.02 with small variance — should not overlap
        assert!(!overlapping_emissions(&hmm));
    }

    #[test]
    fn test_overlapping_emissions_identical_means() {
        let hmm = GaussianHmm {
            n_states: 2,
            initial: vec![0.5, 0.5],
            transition: vec![vec![0.7, 0.3], vec![0.3, 0.7]],
            means: vec![0.01, 0.011], // very close
            variances: vec![0.001, 0.001],
        };
        assert!(overlapping_emissions(&hmm));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib engine::hmm::tests::test_overlapping_emissions -- --nocapture`
Expected: compilation error — `overlapping_emissions` not defined

- [ ] **Step 3: Implement `overlapping_emissions()`**

Add after `forward_filter()`, before `#[cfg(test)]`:

```rust
/// Check if any pair of HMM states has overlapping emission distributions.
///
/// Two states overlap if their means are within 1 standard deviation of each other
/// (using the larger of the two std devs). This indicates the HMM may not have
/// found meaningfully distinct regimes.
pub fn overlapping_emissions(hmm: &GaussianHmm) -> bool {
    for i in 0..hmm.n_states {
        for j in (i + 1)..hmm.n_states {
            let std_max = hmm.variances[i].sqrt().max(hmm.variances[j].sqrt());
            let mean_gap = (hmm.means[i] - hmm.means[j]).abs();
            if mean_gap < std_max {
                return true;
            }
        }
    }
    false
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib engine::hmm::tests -- --nocapture`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add src/engine/hmm.rs
git commit -m "feat: add overlapping_emissions() check for HMM state quality"
```

---

## Task 3: Create `signals/hmm_rewrite.rs` — formula scanning and rewriting

This is the core new module. It scans formula strings for `hmm_regime(...)` calls, extracts arguments, resolves named aliases to integers, and rewrites the formula to reference injected columns.

**Files:**
- Create: `src/signals/hmm_rewrite.rs`
- Modify: `src/signals/mod.rs` (add `mod hmm_rewrite;`)

- [ ] **Step 1: Write failing tests**

Create `src/signals/hmm_rewrite.rs` with the test module first:

```rust
//! Formula rewriting for `hmm_regime()` calls.
//!
//! Scans a formula string for `hmm_regime(...)` patterns, extracts arguments,
//! resolves named regime aliases to integer literals, and rewrites the formula
//! to reference pre-computed `__hmm_regime_*` columns.

/// Extracted HMM regime call parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct HmmCall {
    /// Symbol to fit HMM on (uppercase). `None` = use primary backtest symbol.
    pub symbol: Option<String>,
    /// Number of HMM states (2-4).
    pub n_regimes: usize,
    /// Years of pre-backtest data for fitting.
    pub fit_years: usize,
    /// Posterior probability threshold for regime switching.
    pub threshold: f64,
}

/// Default posterior threshold.
pub const DEFAULT_THRESHOLD: f64 = 0.65;

/// All regime alias names that must not leak past rewriting.
const REGIME_ALIASES: &[&str] = &[
    "bearish",
    "bullish",
    "neutral",
    "strong_bear",
    "mild_bear",
    "mild_bull",
    "strong_bull",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_two_args_no_symbol() {
        let calls = extract_hmm_calls("hmm_regime(3, 5) == 2").unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0],
            HmmCall {
                symbol: None,
                n_regimes: 3,
                fit_years: 5,
                threshold: DEFAULT_THRESHOLD,
            }
        );
    }

    #[test]
    fn test_extract_three_args_with_symbol() {
        let calls = extract_hmm_calls("hmm_regime(SPY, 3, 5) == bullish").unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].symbol, Some("SPY".to_string()));
        assert_eq!(calls[0].n_regimes, 3);
        assert_eq!(calls[0].fit_years, 5);
        assert!((calls[0].threshold - DEFAULT_THRESHOLD).abs() < 1e-10);
    }

    #[test]
    fn test_extract_four_args_with_threshold() {
        let calls = extract_hmm_calls("hmm_regime(SPY, 3, 5, 0.8) == 1").unwrap();
        assert_eq!(calls.len(), 1);
        assert!((calls[0].threshold - 0.8).abs() < 1e-10);
    }

    #[test]
    fn test_extract_three_args_disambiguate_threshold() {
        // 3 args where first is number: (n_regimes, fit_years, threshold)
        let calls = extract_hmm_calls("hmm_regime(3, 5, 0.8) >= 1").unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].symbol, None);
        assert_eq!(calls[0].n_regimes, 3);
        assert_eq!(calls[0].fit_years, 5);
        assert!((calls[0].threshold - 0.8).abs() < 1e-10);
    }

    #[test]
    fn test_extract_wrong_arg_count() {
        assert!(extract_hmm_calls("hmm_regime(1) == 0").is_err());
        assert!(extract_hmm_calls("hmm_regime(A, 3, 5, 0.8, 99) == 0").is_err());
    }

    #[test]
    fn test_extract_n_regimes_out_of_range() {
        assert!(extract_hmm_calls("hmm_regime(1, 5) == 0").is_err());
        assert!(extract_hmm_calls("hmm_regime(5, 5) == 0").is_err());
    }

    #[test]
    fn test_extract_threshold_out_of_range() {
        assert!(extract_hmm_calls("hmm_regime(3, 5, 0.3) == 0").is_err());
    }

    #[test]
    fn test_rewrite_with_named_alias() {
        let result = rewrite_formula("hmm_regime(SPY, 3, 5) == bullish", "AAPL").unwrap();
        assert_eq!(result.formula, "__hmm_regime_SPY_3_5_65 == 2");
    }

    #[test]
    fn test_rewrite_with_numeric() {
        let result = rewrite_formula("hmm_regime(3, 5) >= 1", "SPY").unwrap();
        assert_eq!(result.formula, "__hmm_regime_SPY_3_5_65 >= 1");
    }

    #[test]
    fn test_rewrite_preserves_surrounding() {
        let result =
            rewrite_formula("hmm_regime(3, 5) == 1 and rsi(close, 14) < 30", "SPY").unwrap();
        assert_eq!(
            result.formula,
            "__hmm_regime_SPY_3_5_65 == 1 and rsi(close, 14) < 30"
        );
    }

    #[test]
    fn test_rewrite_no_symbol_uses_primary() {
        let result = rewrite_formula("hmm_regime(2, 5) == bullish", "AAPL").unwrap();
        assert_eq!(result.formula, "__hmm_regime_AAPL_2_5_65 == 1");
    }

    #[test]
    fn test_rewrite_multiple_calls() {
        let result = rewrite_formula(
            "hmm_regime(SPY, 3, 5) == bullish and hmm_regime(QQQ, 2, 3) == bearish",
            "AAPL",
        )
        .unwrap();
        assert!(result.formula.contains("__hmm_regime_SPY_3_5_65 == 2"));
        assert!(result.formula.contains("__hmm_regime_QQQ_2_3_65 == 0"));
        assert_eq!(result.calls.len(), 2);
    }

    #[test]
    fn test_rewrite_deduplicates_same_calls() {
        let result = rewrite_formula(
            "hmm_regime(SPY, 3, 5) == bullish or hmm_regime(SPY, 3, 5) != bearish",
            "AAPL",
        )
        .unwrap();
        // Same (SPY, 3, 5, 0.65) should appear only once in calls
        assert_eq!(result.calls.len(), 1);
    }

    #[test]
    fn test_rewrite_unknown_alias_errors() {
        let err = rewrite_formula("hmm_regime(3, 5) == foobar", "SPY").unwrap_err();
        assert!(err.contains("unknown regime name"));
        assert!(err.contains("bearish, neutral, bullish"));
    }

    #[test]
    fn test_alias_leak_detected() {
        // "bullish" not adjacent to hmm_regime should error
        let err = rewrite_formula("close > 100 and bullish", "SPY").unwrap_err();
        assert!(err.contains("regime alias"));
    }

    #[test]
    fn test_no_hmm_calls_passthrough() {
        let result = rewrite_formula("rsi(close, 14) < 30", "SPY").unwrap();
        assert_eq!(result.formula, "rsi(close, 14) < 30");
        assert!(result.calls.is_empty());
    }

    #[test]
    fn test_column_name_format() {
        assert_eq!(column_name("SPY", 3, 5, 0.65), "__hmm_regime_SPY_3_5_65");
        assert_eq!(column_name("QQQ", 2, 10, 0.8), "__hmm_regime_QQQ_2_10_80");
    }

    #[test]
    fn test_alias_to_index() {
        assert_eq!(alias_to_index("bearish", 2), Some(0));
        assert_eq!(alias_to_index("bullish", 2), Some(1));
        assert_eq!(alias_to_index("bearish", 3), Some(0));
        assert_eq!(alias_to_index("neutral", 3), Some(1));
        assert_eq!(alias_to_index("bullish", 3), Some(2));
        assert_eq!(alias_to_index("strong_bear", 4), Some(0));
        assert_eq!(alias_to_index("mild_bear", 4), Some(1));
        assert_eq!(alias_to_index("mild_bull", 4), Some(2));
        assert_eq!(alias_to_index("strong_bull", 4), Some(3));
        assert_eq!(alias_to_index("bullish", 4), None);
        assert_eq!(alias_to_index("foobar", 3), None);
    }

    #[test]
    fn test_aliases_for_n_regimes() {
        assert_eq!(aliases_for(2), &["bearish", "bullish"]);
        assert_eq!(aliases_for(3), &["bearish", "neutral", "bullish"]);
        assert_eq!(
            aliases_for(4),
            &["strong_bear", "mild_bear", "mild_bull", "strong_bull"]
        );
    }

    #[test]
    fn test_rewrite_with_not_equal_alias() {
        let result = rewrite_formula("hmm_regime(SPY, 3, 5) != bearish", "AAPL").unwrap();
        assert_eq!(result.formula, "__hmm_regime_SPY_3_5_65 != 0");
    }

    #[test]
    fn test_rewrite_explicit_threshold() {
        let result = rewrite_formula("hmm_regime(SPY, 3, 5, 0.8) == bullish", "AAPL").unwrap();
        assert_eq!(result.formula, "__hmm_regime_SPY_3_5_80 == 2");
        assert!((result.calls[0].threshold - 0.8).abs() < 1e-10);
    }
}
```

- [ ] **Step 2: Add `mod hmm_rewrite;` to `src/signals/mod.rs`**

Add near the top of `src/signals/mod.rs`, alongside other module declarations:

```rust
pub mod hmm_rewrite;
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --lib signals::hmm_rewrite::tests -- --nocapture`
Expected: compilation errors — functions not defined

- [ ] **Step 4: Implement the module**

Add the implementation above the `#[cfg(test)]` block in `src/signals/hmm_rewrite.rs`:

```rust
/// Result of rewriting a formula: the rewritten string + extracted HMM calls.
#[derive(Debug, Clone)]
pub struct RewriteResult {
    /// The rewritten formula string (hmm_regime calls replaced with column refs).
    pub formula: String,
    /// Deduplicated HMM calls found in the formula.
    pub calls: Vec<HmmCall>,
    /// Column names injected (one per unique call).
    pub injected_columns: Vec<String>,
}

/// Build the column name for an HMM regime call.
pub fn column_name(symbol: &str, n_regimes: usize, fit_years: usize, threshold: f64) -> String {
    let thresh_int = (threshold * 100.0).round() as u32;
    format!("__hmm_regime_{symbol}_{n_regimes}_{fit_years}_{thresh_int}")
}

/// Return the valid alias names for a given n_regimes.
pub fn aliases_for(n_regimes: usize) -> &'static [&'static str] {
    match n_regimes {
        2 => &["bearish", "bullish"],
        3 => &["bearish", "neutral", "bullish"],
        4 => &["strong_bear", "mild_bear", "mild_bull", "strong_bull"],
        _ => &[],
    }
}

/// Resolve a named alias to its integer index for the given n_regimes.
pub fn alias_to_index(alias: &str, n_regimes: usize) -> Option<usize> {
    aliases_for(n_regimes)
        .iter()
        .position(|&a| a == alias)
}

/// Extract all `hmm_regime(...)` calls from a formula string.
///
/// Does NOT rewrite — just parses the arguments from each call.
pub fn extract_hmm_calls(formula: &str) -> Result<Vec<HmmCall>, String> {
    let mut calls = Vec::new();
    let mut search_from = 0;

    while let Some(start) = formula[search_from..].find("hmm_regime(") {
        let abs_start = search_from + start;
        let args_start = abs_start + "hmm_regime(".len();

        // Find matching closing paren
        let mut depth = 1;
        let mut end = args_start;
        for (i, ch) in formula[args_start..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = args_start + i;
                        break;
                    }
                }
                _ => {}
            }
        }
        if depth != 0 {
            return Err("Unclosed parenthesis in hmm_regime() call".to_string());
        }

        let args_str = &formula[args_start..end];
        let call = parse_hmm_args(args_str)?;
        calls.push(call);
        search_from = end + 1;
    }

    Ok(calls)
}

/// Parse the comma-separated arguments inside `hmm_regime(...)`.
fn parse_hmm_args(args_str: &str) -> Result<HmmCall, String> {
    let parts: Vec<&str> = args_str.split(',').map(|s| s.trim()).collect();

    match parts.len() {
        2 => {
            // (n_regimes, fit_years)
            let n_regimes = parse_int(parts[0], "n_regimes")?;
            let fit_years = parse_int(parts[1], "fit_years")?;
            validate_params(n_regimes, fit_years, DEFAULT_THRESHOLD)?;
            Ok(HmmCall {
                symbol: None,
                n_regimes,
                fit_years,
                threshold: DEFAULT_THRESHOLD,
            })
        }
        3 => {
            // Disambiguate: (symbol, n_regimes, fit_years) vs (n_regimes, fit_years, threshold)
            if parts[0].chars().next().map_or(false, |c| c.is_ascii_alphabetic()) {
                // First arg is identifier → (symbol, n_regimes, fit_years)
                let symbol = parts[0].to_uppercase();
                let n_regimes = parse_int(parts[1], "n_regimes")?;
                let fit_years = parse_int(parts[2], "fit_years")?;
                validate_params(n_regimes, fit_years, DEFAULT_THRESHOLD)?;
                Ok(HmmCall {
                    symbol: Some(symbol),
                    n_regimes,
                    fit_years,
                    threshold: DEFAULT_THRESHOLD,
                })
            } else {
                // First arg is number → (n_regimes, fit_years, threshold)
                let n_regimes = parse_int(parts[0], "n_regimes")?;
                let fit_years = parse_int(parts[1], "fit_years")?;
                let threshold = parse_float(parts[2], "threshold")?;
                validate_params(n_regimes, fit_years, threshold)?;
                Ok(HmmCall {
                    symbol: None,
                    n_regimes,
                    fit_years,
                    threshold,
                })
            }
        }
        4 => {
            // (symbol, n_regimes, fit_years, threshold)
            let symbol = parts[0].to_uppercase();
            if !symbol.chars().next().map_or(false, |c| c.is_ascii_alphabetic()) {
                return Err(format!(
                    "hmm_regime first arg must be a symbol identifier, got '{}'",
                    parts[0]
                ));
            }
            let n_regimes = parse_int(parts[1], "n_regimes")?;
            let fit_years = parse_int(parts[2], "fit_years")?;
            let threshold = parse_float(parts[3], "threshold")?;
            validate_params(n_regimes, fit_years, threshold)?;
            Ok(HmmCall {
                symbol: Some(symbol),
                n_regimes,
                fit_years,
                threshold,
            })
        }
        n => Err(format!("hmm_regime expects 2, 3, or 4 arguments, got {n}")),
    }
}

fn parse_int(s: &str, name: &str) -> Result<usize, String> {
    s.parse::<usize>()
        .map_err(|_| format!("{name} must be an integer, got '{s}'"))
}

fn parse_float(s: &str, name: &str) -> Result<f64, String> {
    s.parse::<f64>()
        .map_err(|_| format!("{name} must be a number, got '{s}'"))
}

fn validate_params(n_regimes: usize, fit_years: usize, threshold: f64) -> Result<(), String> {
    if !(2..=4).contains(&n_regimes) {
        return Err(format!("n_regimes must be 2-4, got {n_regimes}"));
    }
    if !(1..=50).contains(&fit_years) {
        return Err(format!("fit_years must be 1-50, got {fit_years}"));
    }
    if threshold <= 0.5 || threshold > 1.0 {
        return Err(format!(
            "threshold must be between 0.5 (exclusive) and 1.0 (inclusive), got {threshold}"
        ));
    }
    Ok(())
}

/// Rewrite a formula string, replacing `hmm_regime(...)` calls and their comparison
/// operators/aliases with column references and integer literals.
///
/// `primary_symbol` is used when the formula omits the symbol arg.
pub fn rewrite_formula(formula: &str, primary_symbol: &str) -> Result<RewriteResult, String> {
    // If no hmm_regime calls, check for leaked aliases and return as-is
    if !formula.contains("hmm_regime(") {
        check_alias_leak(formula)?;
        return Ok(RewriteResult {
            formula: formula.to_string(),
            calls: vec![],
            injected_columns: vec![],
        });
    }

    let mut result = formula.to_string();
    let mut all_calls = Vec::new();
    let mut seen_columns = std::collections::HashSet::new();

    // Process all hmm_regime(...) calls, replacing them with column references
    // We loop because each replacement changes offsets
    loop {
        let Some(start) = result.find("hmm_regime(") else {
            break;
        };
        let args_start = start + "hmm_regime(".len();

        // Find matching closing paren
        let mut depth = 1;
        let mut paren_end = args_start;
        for (i, ch) in result[args_start..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        paren_end = args_start + i;
                        break;
                    }
                }
                _ => {}
            }
        }
        if depth != 0 {
            return Err("Unclosed parenthesis in hmm_regime() call".to_string());
        }

        let args_str = &result[args_start..paren_end];
        let mut call = parse_hmm_args(args_str)?;

        // Resolve symbol
        let sym = call
            .symbol
            .clone()
            .unwrap_or_else(|| primary_symbol.to_uppercase());
        call.symbol = Some(sym.clone());

        let col_name = column_name(&sym, call.n_regimes, call.fit_years, call.threshold);

        // Look for comparison operator + alias/number after the closing paren
        let after = &result[paren_end + 1..];
        let after_trimmed = after.trim_start();

        // Find the comparison operator
        let (op, op_len) = if after_trimmed.starts_with("==") {
            ("==", 2)
        } else if after_trimmed.starts_with("!=") {
            ("!=", 2)
        } else if after_trimmed.starts_with(">=") {
            (">=", 2)
        } else if after_trimmed.starts_with("<=") {
            ("<=", 2)
        } else if after_trimmed.starts_with('>') {
            (">", 1)
        } else if after_trimmed.starts_with('<') {
            ("<", 1)
        } else {
            return Err(
                "hmm_regime() must be used in a comparison (==, !=, >, <, >=, <=)".to_string(),
            );
        };

        let op_start_in_after = after.len() - after_trimmed.len();
        let rhs_start = op_start_in_after + op_len;
        let rhs_trimmed = after[rhs_start..].trim_start();

        // Parse the RHS: either a number or a named alias
        let (rhs_value, rhs_token_len) = parse_rhs(rhs_trimmed, call.n_regimes)?;

        // Calculate the end of the entire expression in `result`
        let expr_end =
            paren_end + 1 + rhs_start + (after[rhs_start..].len() - rhs_trimmed.len()) + rhs_token_len;

        // Build replacement
        let replacement = format!("{col_name} {op} {rhs_value}");
        result.replace_range(start..expr_end, &replacement);

        // Track unique calls
        if seen_columns.insert(col_name.clone()) {
            all_calls.push(call);
        }
    }

    check_alias_leak(&result)?;

    let injected_columns: Vec<String> = all_calls
        .iter()
        .map(|c| {
            let sym = c.symbol.as_deref().unwrap_or(primary_symbol);
            column_name(sym, c.n_regimes, c.fit_years, c.threshold)
        })
        .collect();

    Ok(RewriteResult {
        formula: result,
        calls: all_calls,
        injected_columns,
    })
}

/// Parse the RHS of a comparison: either a bare integer or a named alias.
/// Returns (integer_string, token_length_in_source).
fn parse_rhs(rhs: &str, n_regimes: usize) -> Result<(String, usize), String> {
    // Try integer first
    let token_end = rhs
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rhs.len());
    if token_end > 0 && rhs[..token_end].parse::<usize>().is_ok() {
        let val = &rhs[..token_end];
        return Ok((val.to_string(), token_end));
    }

    // Try named alias
    let token_end = rhs
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(rhs.len());
    if token_end == 0 {
        return Err("Expected a regime name or integer after comparison operator".to_string());
    }
    let name = &rhs[..token_end];

    if let Some(idx) = alias_to_index(name, n_regimes) {
        Ok((idx.to_string(), token_end))
    } else {
        let valid = aliases_for(n_regimes).join(", ");
        Err(format!(
            "unknown regime name '{name}' for n_regimes={n_regimes}; expected: {valid}"
        ))
    }
}

/// Check that no regime alias identifiers leaked through (exist outside hmm_regime context).
fn check_alias_leak(formula: &str) -> Result<(), String> {
    // Simple word-boundary check: split on non-alphanumeric/underscore and check tokens
    for token in formula.split(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
        if REGIME_ALIASES.contains(&token) {
            return Err(format!(
                "regime alias '{token}' found outside hmm_regime() context. \
                 Named aliases (bearish, bullish, etc.) can only be used in \
                 hmm_regime() comparisons."
            ));
        }
    }
    Ok(())
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib signals::hmm_rewrite::tests -- --nocapture`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add src/signals/hmm_rewrite.rs src/signals/mod.rs
git commit -m "feat: add hmm_rewrite module for formula scanning and rewriting

Extracts hmm_regime() calls, resolves named aliases (bullish, bearish,
etc.) to integers, and rewrites formulas to reference __hmm_regime_*
columns. Validates args, detects alias leaks."
```

---

## Task 4: Parser `__` prefix convention in `custom.rs`

**Files:**
- Modify: `src/signals/custom.rs`

- [ ] **Step 1: Write failing test**

Add to the existing test module in `src/signals/custom.rs` (find the `#[cfg(test)] mod tests` block):

```rust
    #[test]
    fn test_double_underscore_ident_is_literal_column() {
        // __hmm_regime_SPY_3_5_65 should resolve to col("__hmm_regime_SPY_3_5_65")
        // NOT col("__HMM_REGIME_SPY_3_5_65_close")
        let expr = parse_formula("__hmm_regime_SPY_3_5_65 == 2").unwrap();
        let fmt = format!("{expr:?}");
        assert!(
            fmt.contains("__hmm_regime_SPY_3_5_65"),
            "should contain literal column name, got: {fmt}"
        );
        assert!(
            !fmt.contains("_close"),
            "should NOT append _close suffix, got: {fmt}"
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib signals::custom::tests::test_double_underscore_ident_is_literal_column -- --nocapture`
Expected: FAIL — the parser currently uppercases and appends `_close`

- [ ] **Step 3: Implement the `__` prefix check**

In `src/signals/custom.rs`, find the `parse_primary()` method. Locate the else branch where plain identifiers are handled (the block with `VALID_COLUMNS` check and the cross-symbol fallback). Add the `__` prefix check before the cross-symbol fallback:

Find this code:
```rust
                    if VALID_COLUMNS.contains(&name_lower.as_str()) {
                        Ok(col(&*name_lower))
                    } else {
                        // Cross-symbol reference, defaults to .close
                        let sym = name.to_uppercase();
                        Ok(col(format!("{sym}_close")))
                    }
```

Replace with:
```rust
                    if VALID_COLUMNS.contains(&name_lower.as_str()) {
                        Ok(col(&*name_lower))
                    } else if name.starts_with("__") {
                        // Internal computed column (e.g., __hmm_regime_SPY_3_5_65)
                        // — use as literal column reference, no transformation
                        Ok(col(name))
                    } else {
                        // Cross-symbol reference, defaults to .close
                        let sym = name.to_uppercase();
                        Ok(col(format!("{sym}_close")))
                    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib signals::custom::tests -- --nocapture`
Expected: all pass (existing + new)

- [ ] **Step 5: Commit**

```bash
git add src/signals/custom.rs
git commit -m "feat: treat __ prefix identifiers as literal column references

Identifiers starting with __ bypass the cross-symbol transformation
(uppercase + _close suffix). Enables pre-computed columns like
__hmm_regime_SPY_3_5_65 to pass through the parser unchanged."
```

---

## Task 5: Add `hmm_regime` to `KNOWN_FUNCTIONS` and `__` exclusion to cross-symbol extractor

**Files:**
- Modify: `src/signals/custom_funcs/mod.rs`
- Modify: `src/signals/registry.rs`

- [ ] **Step 1: Write failing tests**

Add to the test module in `src/signals/registry.rs`:

```rust
    #[test]
    fn test_cross_symbol_extractor_skips_double_underscore() {
        let syms = extract_formula_cross_symbols("__hmm_regime_SPY_3_5_65 == 2");
        assert!(
            syms.is_empty(),
            "__ prefix identifiers should not be treated as cross-symbols: {syms:?}"
        );
    }

    #[test]
    fn test_cross_symbol_extractor_skips_hmm_regime_function() {
        let syms = extract_formula_cross_symbols("hmm_regime(SPY, 3, 5) == 2 and VIX > 20");
        // hmm_regime is a known function (followed by paren) — should be skipped
        // SPY inside the parens: the extractor only sees Ident tokens, and SPY
        // would be detected. But after rewriting, hmm_regime() won't be in the formula.
        // This test verifies hmm_regime itself is not picked up.
        assert!(
            !syms.contains("HMM_REGIME"),
            "hmm_regime should be in KNOWN_FUNCTIONS: {syms:?}"
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib signals::registry::tests::test_cross_symbol_extractor_skips -- --nocapture`
Expected: FAIL — `__hmm_regime_SPY_3_5_65` gets picked up as a cross-symbol

- [ ] **Step 3: Add `hmm_regime` to `KNOWN_FUNCTIONS`**

In `src/signals/custom_funcs/mod.rs`, add to the `KNOWN_FUNCTIONS` array, at the end before the closing `];`:

```rust
    // HMM regime (handled by formula rewriting, not dispatch)
    "hmm_regime",
```

- [ ] **Step 4: Add `__` prefix exclusion to `extract_formula_cross_symbols()`**

In `src/signals/registry.rs`, in the `extract_formula_cross_symbols()` function, add this check right after the `valid_columns` skip and before the `KNOWN_FUNCTIONS` check:

Find:
```rust
            // Known column → skip
            if valid_columns.contains(&lower.as_str()) {
                i += 1;
                continue;
            }
```

Add after it:
```rust
            // Internal computed column (__ prefix) → skip
            if name.starts_with("__") {
                i += 1;
                continue;
            }
```

- [ ] **Step 5: Update `SIGNAL_CATALOG` and test count**

Add these entries to `SIGNAL_CATALOG` in `src/signals/registry.rs`, at the end before the closing `];`:

```rust
    // ── Regime ────────────────────────────────────────────────────────
    SignalInfo {
        name: "HMM Regime Filter",
        category: "regime",
        description: "Gate entries by HMM regime state. Run regime_detect first to inspect states.",
        params: "symbol (optional), n_regimes (2-4), fit_years, threshold (optional, default 0.65)",
        formula_example: "hmm_regime(SPY, 3, 5) == bullish",
    },
    SignalInfo {
        name: "HMM Regime Exclude",
        category: "regime",
        description: "Exclude a specific HMM regime (e.g., avoid bearish). Run regime_detect first.",
        params: "symbol (optional), n_regimes (2-4), fit_years, threshold (optional, default 0.65)",
        formula_example: "hmm_regime(3, 5) != bearish",
    },
    SignalInfo {
        name: "HMM Regime Numeric",
        category: "regime",
        description: "Gate entries by HMM state index (0=lowest return, N-1=highest). Run regime_detect first.",
        params: "symbol (optional), n_regimes (2-4), fit_years, threshold (optional, default 0.65)",
        formula_example: "hmm_regime(3, 5) >= 1",
    },
```

Update the `catalog_has_all_signals` test assertion count (find the existing `assert_eq!(SIGNAL_CATALOG.len(), ...)` and increment by 3).

Also add `"regime"` to the `valid_categories` list in `catalog_categories_are_valid` test.

- [ ] **Step 6: Run all tests to verify**

Run: `cargo test --lib signals -- --nocapture`
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add src/signals/custom_funcs/mod.rs src/signals/registry.rs
git commit -m "feat: register hmm_regime in KNOWN_FUNCTIONS and SIGNAL_CATALOG

Add __ prefix exclusion to cross-symbol extractor so pre-computed
columns aren't treated as symbol references. Add 3 SIGNAL_CATALOG
entries under new 'regime' category."
```

---

## Task 6: Wire HMM rewrite pass into signal evaluation pipeline

This is where everything connects. The HMM rewrite pass runs before cross-symbol detection, fits the HMM, forward-filters, and injects the regime column.

**Key API facts discovered during review:**
- `CachedStore::find_ohlcv(symbol) -> Option<PathBuf>` — finds OHLCV Parquet path
- No `read_ohlcv` free function — use `LazyFrame::scan_parquet(path.into(), ScanArgsParquet::default())?.collect()?`
- `EPOCH_DAYS_CE_OFFSET` constant at `engine::types::enums::EPOCH_DAYS_CE_OFFSET` (value: 719163)
- `build_signal_filters` in `core.rs` takes `(&BacktestParams, &DataFrame)` — no symbol/start_date/CachedStore
- `build_stock_signal_filters` in `stock_sim.rs` takes `(&StockBacktestParams, &DataFrame)` — has symbol but no CachedStore

**Approach:** Add `cache_dir: Option<&Path>` param to both signal filter builders. Derive symbol from `ohlcv_path` for options. Derive start_date from OHLCV DataFrame min date. Return a new DataFrame (not mutate) to avoid `&mut` signature changes. This is the minimal change that threads the needed data without restructuring callers.

**Files:**
- Modify: `src/signals/mod.rs`

- [ ] **Step 1: Write a unit test for `preprocess_hmm_regime`**

Add to `src/signals/mod.rs` test module (or a new test submodule):

```rust
#[cfg(test)]
mod hmm_integration_tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn test_preprocess_no_hmm_calls_passthrough() {
        let formula = "rsi(close, 14) < 30";
        let df = DataFrame::new(vec![
            Column::new("datetime".into(), &[1i64, 2, 3]),
            Column::new("close".into(), &[100.0, 101.0, 102.0]),
        ])
        .unwrap();

        let (rewritten, new_df) =
            preprocess_hmm_regime(formula, "SPY", &df, None, "datetime").unwrap();
        assert_eq!(rewritten, formula);
        assert_eq!(new_df.width(), df.width()); // no columns added
    }
}
```

- [ ] **Step 2: Implement `preprocess_hmm_regime` and `extract_naive_date`**

Add to `src/signals/mod.rs`:

```rust
use crate::engine::hmm;
use crate::engine::types::EPOCH_DAYS_CE_OFFSET;

/// Pre-process a formula string for HMM regime calls.
///
/// 1. Scans for `hmm_regime(...)` calls and rewrites the formula
/// 2. For each unique call: loads data, fits HMM, forward-filters
/// 3. Injects `__hmm_regime_*` columns into the DataFrame
/// 4. Returns (rewritten_formula, modified_dataframe)
///
/// If no `hmm_regime()` calls found, returns formula and DataFrame unchanged.
///
/// `cache_dir` is needed only when `hmm_regime` references a symbol different
/// from `primary_symbol`. Pass `None` if only the primary symbol is used.
pub fn preprocess_hmm_regime(
    formula: &str,
    primary_symbol: &str,
    primary_df: &DataFrame,
    cache_dir: Option<&std::path::Path>,
    date_col: &str,
) -> Result<(String, DataFrame)> {
    let rewrite = hmm_rewrite::rewrite_formula(formula, primary_symbol)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    if rewrite.calls.is_empty() {
        return Ok((rewrite.formula, primary_df.clone()));
    }

    let mut result_df = primary_df.clone();

    // Derive backtest start date from the primary DataFrame (earliest date)
    let backtest_start = extract_naive_date(primary_df.column(date_col)?, 0)?;

    for call in &rewrite.calls {
        let sym = call.symbol.as_deref().unwrap_or(primary_symbol);
        let col_name =
            hmm_rewrite::column_name(sym, call.n_regimes, call.fit_years, call.threshold);

        // Load OHLCV for the HMM symbol
        let hmm_df = if sym.eq_ignore_ascii_case(primary_symbol) {
            primary_df.clone()
        } else {
            let cache = cache_dir.ok_or_else(|| {
                anyhow::anyhow!(
                    "hmm_regime references symbol '{}' but no cache directory available",
                    sym
                )
            })?;
            load_hmm_symbol_ohlcv(cache, sym)?
        };

        // Detect date column in the HMM symbol's data
        let hmm_date_col = crate::engine::stock_sim::detect_date_col(&hmm_df);

        // Extract dates and closes, compute returns
        let dates_col = hmm_df.column(hmm_date_col)?;
        let closes = hmm_df.column("close")?.f64()?;

        let mut returns = Vec::with_capacity(closes.len());
        let mut return_dates = Vec::new();
        for i in 1..closes.len() {
            if let (Some(prev), Some(curr)) = (closes.get(i - 1), closes.get(i)) {
                if prev.abs() > 1e-15 {
                    returns.push((curr - prev) / prev);
                    return_dates.push(extract_naive_date(dates_col, i)?);
                }
            }
        }

        // Split into fit window and apply window
        let fit_years_days = call.fit_years as i64 * 365;
        let fit_start = backtest_start - chrono::Duration::days(fit_years_days);

        let mut fit_returns = Vec::new();
        let mut apply_returns = Vec::new();
        let mut apply_dates = Vec::new();

        for (ret, date) in returns.iter().zip(return_dates.iter()) {
            if *date < backtest_start && *date >= fit_start {
                fit_returns.push(*ret);
            } else if *date >= backtest_start {
                apply_returns.push(*ret);
                apply_dates.push(*date);
            }
        }

        if fit_returns.len() < 50 {
            anyhow::bail!(
                "hmm_regime requires at least 50 bars before backtest start date; \
                 only found {} bars for {} with fit_years={}",
                fit_returns.len(),
                sym,
                call.fit_years
            );
        }

        // Fit HMM on pre-backtest data
        let fitted = hmm::fit(&fit_returns, call.n_regimes);

        // Check for overlapping emissions
        if hmm::overlapping_emissions(&fitted) {
            tracing::warn!(
                "HMM states for {} have overlapping distributions — regime labels may be \
                 unreliable. Consider using fewer states or a longer fit window.",
                sym
            );
        }

        // Forward-filter the apply window
        let regime_labels = hmm::forward_filter(&fitted, &apply_returns, call.threshold);

        // Build a mapping from date → regime label
        let regime_map: std::collections::HashMap<chrono::NaiveDate, usize> = apply_dates
            .into_iter()
            .zip(regime_labels)
            .collect();

        // Create the regime column aligned to the primary DataFrame
        let primary_dates = result_df.column(date_col)?;
        let mut regime_col = Vec::with_capacity(result_df.height());
        for i in 0..result_df.height() {
            let date = extract_naive_date(primary_dates, i)?;
            regime_col.push(regime_map.get(&date).map(|&x| x as u32));
        }

        // Inject as UInt32 column (nullable for dates outside apply window)
        let series = polars::prelude::Series::new(
            col_name.into(),
            regime_col,
        );
        let _ = result_df.with_column(series)?;
    }

    Ok((rewrite.formula, result_df))
}

/// Load OHLCV data for an HMM symbol from the cache directory.
fn load_hmm_symbol_ohlcv(cache_dir: &std::path::Path, symbol: &str) -> Result<DataFrame> {
    use polars::prelude::*;

    // Search across categories (same logic as CachedStore::find_ohlcv)
    for category in &["etf", "stocks", "futures", "indices"] {
        let path = cache_dir.join(category).join(format!("{symbol}.parquet"));
        if path.exists() {
            let args = ScanArgsParquet::default();
            return Ok(LazyFrame::scan_parquet(path.into(), args)?.collect()?);
        }
    }
    anyhow::bail!(
        "no OHLCV data found for '{}'; available categories: stocks, etf, indices, futures",
        symbol
    )
}

/// Extract a NaiveDate from a date/datetime column at the given index.
fn extract_naive_date(
    col: &polars::prelude::Column,
    idx: usize,
) -> Result<chrono::NaiveDate> {
    use polars::prelude::*;
    match col.dtype() {
        DataType::Date => {
            let days = col.date()?.get(idx)
                .ok_or_else(|| anyhow::anyhow!("null date at index {idx}"))?;
            chrono::NaiveDate::from_num_days_from_ce_opt(days + EPOCH_DAYS_CE_OFFSET)
                .ok_or_else(|| anyhow::anyhow!("invalid date at index {idx}"))
        }
        DataType::Datetime(tu, _) => {
            let val = col.datetime()?.get(idx)
                .ok_or_else(|| anyhow::anyhow!("null datetime at index {idx}"))?;
            let ndt = match tu {
                TimeUnit::Milliseconds => {
                    chrono::DateTime::from_timestamp_millis(val)
                        .ok_or_else(|| anyhow::anyhow!("invalid datetime ms at {idx}"))?
                        .naive_utc()
                }
                TimeUnit::Microseconds => {
                    chrono::DateTime::from_timestamp_micros(val)
                        .ok_or_else(|| anyhow::anyhow!("invalid datetime us at {idx}"))?
                        .naive_utc()
                }
                TimeUnit::Nanoseconds => {
                    let secs = val / 1_000_000_000;
                    let nsecs = (val % 1_000_000_000) as u32;
                    chrono::DateTime::from_timestamp(secs, nsecs)
                        .ok_or_else(|| anyhow::anyhow!("invalid datetime ns at {idx}"))?
                        .naive_utc()
                }
            };
            Ok(ndt.date())
        }
        other => Err(anyhow::anyhow!("expected date/datetime column, got {other}")),
    }
}
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test --lib signals::hmm_integration_tests -- --nocapture`
Expected: pass

- [ ] **Step 4: Commit**

```bash
git add src/signals/mod.rs
git commit -m "feat: add preprocess_hmm_regime for HMM column injection

Fits HMM on pre-backtest data, forward-filters the apply window,
and injects regime columns into the primary DataFrame. Returns a
new DataFrame (no mutation) for clean integration with signal builders."
```

---

## Task 7: Wire `preprocess_hmm_regime` into backtest signal filter builders

Both `build_signal_filters` (core.rs:270) and `build_stock_signal_filters` (stock_sim.rs:1239) need to call `preprocess_hmm_regime` before evaluating signals. Add an optional `cache_dir: Option<&Path>` parameter.

**Files:**
- Modify: `src/engine/core.rs`
- Modify: `src/engine/stock_sim.rs`
- Modify: all callers (sweep.rs, tools/stock_backtest.rs, tools/portfolio.rs, server/handlers/optimization.rs)

- [ ] **Step 1: Update `build_signal_filters` in `core.rs`**

Change the signature from:
```rust
pub fn build_signal_filters(
    params: &BacktestParams,
    options_df: &DataFrame,
) -> Result<(DateFilter, DateFilter)>
```
To:
```rust
pub fn build_signal_filters(
    params: &BacktestParams,
    options_df: &DataFrame,
    cache_dir: Option<&std::path::Path>,
) -> Result<(DateFilter, DateFilter)>
```

Inside the function, after `let (ohlcv_df, date_col) = load_signal_ohlcv(params, options_df)?;`, add the HMM preprocessing. The primary symbol is derived from `ohlcv_path`:

```rust
    let (ohlcv_df, date_col) = load_signal_ohlcv(params, options_df)?;

    // HMM regime preprocessing: rewrite formulas and inject regime columns
    let mut ohlcv_df = ohlcv_df;
    let primary_symbol = params
        .ohlcv_path
        .as_deref()
        .and_then(|p| std::path::Path::new(p).file_stem())
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_uppercase();

    let entry_signal = if let Some(ref spec) = params.entry_signal {
        if let Some(formula) = spec.formula_str() {
            let (rewritten, new_df) =
                signals::preprocess_hmm_regime(&formula, &primary_symbol, &ohlcv_df, cache_dir, date_col)?;
            ohlcv_df = new_df;
            Some(crate::signals::registry::SignalSpec::Formula { formula: rewritten })
        } else {
            params.entry_signal.clone()
        }
    } else {
        None
    };

    let exit_signal = if let Some(ref spec) = params.exit_signal {
        if let Some(formula) = spec.formula_str() {
            let (rewritten, new_df) =
                signals::preprocess_hmm_regime(&formula, &primary_symbol, &ohlcv_df, cache_dir, date_col)?;
            ohlcv_df = new_df;
            Some(crate::signals::registry::SignalSpec::Formula { formula: rewritten })
        } else {
            params.exit_signal.clone()
        }
    } else {
        None
    };
```

Then use `entry_signal` and `exit_signal` (the possibly-rewritten versions) instead of `params.entry_signal` and `params.exit_signal` in the rest of the function.

**Note:** `SignalSpec` likely needs a `formula_str()` helper method to extract the formula string from a `Formula` variant. If this doesn't exist, add it to `src/signals/registry.rs`:

```rust
impl SignalSpec {
    /// If this is a `Formula` variant, return the formula string.
    pub fn formula_str(&self) -> Option<String> {
        match self {
            SignalSpec::Formula { formula } => Some(formula.clone()),
            _ => None,
        }
    }
}
```

- [ ] **Step 2: Update `build_stock_signal_filters` in `stock_sim.rs`**

Same pattern. Change signature to add `cache_dir: Option<&std::path::Path>`:

```rust
pub fn build_stock_signal_filters(
    params: &StockBacktestParams,
    ohlcv_df: &polars::prelude::DataFrame,
    cache_dir: Option<&std::path::Path>,
) -> Result<(DateTimeFilter, DateTimeFilter)>
```

After `let date_col = detect_date_col(ohlcv_df);`, add HMM preprocessing using `params.symbol` as the primary symbol.

- [ ] **Step 3: Update all callers to pass `cache_dir`**

Callers that already have a `CachedStore` or cache path available should pass `Some(&store.cache_dir)`. Others pass `None` (HMM regime won't work for cross-symbols but won't break).

Key callers to update:
- `core.rs:350`: `build_signal_filters(params, df, cache_dir)?;`
- `stock_sim.rs` callers in `sweep.rs` (~lines 713, 810, 963)
- `tools/stock_backtest.rs` (~line 47)
- `tools/portfolio.rs` (~line 107)
- `server/handlers/optimization.rs` (~lines 293, 380)

For each, find where `CachedStore` is available in scope and pass `Some(&store.cache_dir)` or `Some(cache_dir)`. If not available, pass `None`.

- [ ] **Step 4: Run `cargo check` to verify compilation**

Run: `cargo check`
Expected: compiles. Fix any remaining type mismatches.

- [ ] **Step 5: Commit**

```bash
git add src/engine/core.rs src/engine/stock_sim.rs src/engine/sweep.rs \
    src/tools/stock_backtest.rs src/tools/portfolio.rs \
    src/server/handlers/optimization.rs src/signals/registry.rs
git commit -m "feat: wire preprocess_hmm_regime into signal filter builders

Both options (core.rs) and stock (stock_sim.rs) backtests now run the
HMM rewrite pass before cross-symbol detection and signal evaluation.
Added cache_dir parameter to build_signal_filters and
build_stock_signal_filters for cross-symbol HMM data loading."
```

---

## Task 8: Integration tests

**Files:**
- Create: `tests/hmm_signal.rs` (integration test file)

- [ ] **Step 1: Write end-to-end tests**

Create `tests/hmm_signal.rs`:

```rust
//! Integration tests for hmm_regime() formula function in backtests.

use optopsy_mcp::signals::hmm_rewrite;

#[test]
fn test_hmm_rewrite_roundtrip() {
    let result = hmm_rewrite::rewrite_formula(
        "hmm_regime(SPY, 3, 5) == bullish and rsi(close, 14) < 30",
        "AAPL",
    )
    .unwrap();

    assert_eq!(
        result.formula,
        "__hmm_regime_SPY_3_5_65 == 2 and rsi(close, 14) < 30"
    );
    assert_eq!(result.calls.len(), 1);
    assert_eq!(result.calls[0].symbol, Some("SPY".to_string()));
    assert_eq!(result.calls[0].n_regimes, 3);
    assert_eq!(result.injected_columns, vec!["__hmm_regime_SPY_3_5_65"]);
}

#[test]
fn test_hmm_forward_filter_no_lookahead() {
    use optopsy_mcp::engine::hmm;

    let mut data = Vec::with_capacity(400);
    let mut seed: u64 = 42;
    for i in 0..400 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let noise = (seed >> 11) as f64 / (1u64 << 53) as f64 * 0.01 - 0.005;
        let mean = if i < 200 { -0.01 } else { 0.02 };
        data.push(mean + noise);
    }

    let hmm_model = hmm::fit(&data[..200], 2);

    // Full vs truncated: first 100 bars must be identical (no look-ahead)
    let full_result = hmm::forward_filter(&hmm_model, &data[200..], 0.65);
    let partial_result = hmm::forward_filter(&hmm_model, &data[200..300], 0.65);

    assert_eq!(
        &full_result[..100],
        &partial_result[..],
        "Forward filter results should be identical regardless of future data"
    );
}

#[test]
fn test_cross_symbol_extractor_ignores_hmm_columns() {
    use optopsy_mcp::signals::registry::extract_formula_cross_symbols;

    let syms =
        extract_formula_cross_symbols("__hmm_regime_SPY_3_5_65 == 2 and VIX > 20");
    assert!(!syms.contains("__HMM_REGIME_SPY_3_5_65"));
    assert!(syms.contains("VIX"));
}

#[test]
fn test_parser_handles_hmm_column_name() {
    use optopsy_mcp::signals::custom::parse_formula;

    let expr = parse_formula("__hmm_regime_SPY_3_5_65 == 2").unwrap();
    let fmt = format!("{expr:?}");
    assert!(fmt.contains("__hmm_regime_SPY_3_5_65"));
    assert!(!fmt.contains("_close"));
}

#[test]
fn test_preprocess_no_hmm_passthrough() {
    use optopsy_mcp::signals::preprocess_hmm_regime;
    use polars::prelude::*;

    let df = DataFrame::new(vec![
        Column::new("datetime".into(), &[1_000_000i64, 2_000_000, 3_000_000]),
        Column::new("close".into(), &[100.0, 101.0, 102.0]),
    ])
    .unwrap();

    let (rewritten, new_df) =
        preprocess_hmm_regime("rsi(close, 14) < 30", "SPY", &df, None, "datetime")
            .unwrap();
    assert_eq!(rewritten, "rsi(close, 14) < 30");
    assert_eq!(new_df.width(), df.width());
}
```

- [ ] **Step 2: Run integration tests**

Run: `cargo test --test hmm_signal -- --nocapture`
Expected: all pass

- [ ] **Step 3: Commit**

```bash
git add tests/hmm_signal.rs
git commit -m "test: add integration tests for hmm_regime formula function

Tests cover rewrite roundtrip, forward filter no-lookahead property,
cross-symbol extractor __ prefix exclusion, parser column handling,
and preprocess passthrough for non-HMM formulas."
```

---

## Task 9: Run full test suite and fix any issues

- [ ] **Step 1: Run all tests**

Run: `cargo test`
Expected: all 989+ tests pass (existing + new)

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --all-targets`
Expected: no warnings

- [ ] **Step 3: Run formatter**

Run: `cargo fmt --check`
If failures: `cargo fmt` then commit the formatting fix.

- [ ] **Step 4: Final commit if any fixes needed**

```bash
git add -A
git commit -m "fix: address clippy warnings and formatting"
```

---

## Task 10: Verify and clean up

- [ ] **Step 1: Verify all public APIs are accessible**

Run a quick check that the main exports work:

```bash
cargo test --test hmm_signal -- --nocapture
```

- [ ] **Step 2: Review all changes**

```bash
git log --oneline feat/cross-symbol-formula-syntax..HEAD
git diff feat/cross-symbol-formula-syntax..HEAD --stat
```

Verify the diff matches the spec's code changes table:
- `engine/hmm.rs` — `forward_filter()` + `overlapping_emissions()`
- `signals/hmm_rewrite.rs` — new module
- `signals/mod.rs` — `preprocess_hmm_regime()` + `mod hmm_rewrite`
- `signals/custom.rs` — `__` prefix in `parse_primary()`
- `signals/custom_funcs/mod.rs` — `hmm_regime` in `KNOWN_FUNCTIONS`
- `signals/registry.rs` — `__` prefix exclusion + `SIGNAL_CATALOG` entries

- [ ] **Step 3: Commit any remaining changes and summarize**

Done. Use `superpowers:finishing-a-development-branch` to decide on merge/PR/cleanup.
