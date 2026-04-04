# Multi-Symbol Portfolio Backtesting: Implementation Guide

> This document describes the remaining work to complete multi-symbol portfolio
> backtesting support. The infrastructure is built; what remains is wiring it
> into the simulation loop.

## What's Already Built

| Component | File(s) | Status |
|-----------|---------|--------|
| `ScriptConfig.symbols` parsing | `types/config.rs`, `engine.rs` | Done |
| `PerSymbolData` struct | `types/config.rs` | Done |
| `load_multi_symbol_data()` | `engine.rs:~3405` | Done (unused) |
| `SymbolContext` type + Rhai API | `types/symbol_context.rs`, `registration.rs` | Done |
| `ctx.sym("SYMBOL")` method | `types/bar_context.rs` | Done |
| Symbol field on `ScriptAction`, `PendingOrder`, `ScriptPosition`, `TradeRecord` | Various | Done |

## User-Facing API (Already Working)

```rhai
fn config() {
    #{
        symbols: ["SPY", "QQQ", "/ES"],   // all equal peers
        capital: 100000,
        data: #{ indicators: ["sma:200", "rsi:14"] }
    }
}

fn on_bar(ctx) {
    let spy = ctx.sym("SPY");
    let qqq = ctx.sym("QQQ");

    if spy.rsi(14) < 30 {
        return [spy.short_put(0.30, 45)];
    }
    if qqq.close > qqq.sma(200) {
        return [buy_stock_for("QQQ", 100)];
    }
    hold_position()
}
```

## What Remains: Simulation Loop Wiring

All changes are in `src/scripting/engine.rs` inside `run_script_backtest()` unless
noted otherwise.

### 1. Data Loading Branch (~line 493)

**Current:** Loads OHLCV/options for one symbol only.

**Change:** Add a branch for `config.symbols.len() > 1`:

```rust
let (per_symbol_map, price_history, indicator_store, ...) = if config.symbols.len() > 1 {
    let (psd, master_dates) = load_multi_symbol_data(&config, data_loader, &mut early_warnings).await?;
    // Use first symbol's bars as `price_history` for the iteration loop
    let first = config.symbols[0].clone();
    let first_data = psd.get(&first).unwrap();
    (Some(psd), Arc::clone(&first_data.bars), Arc::clone(&first_data.indicator_store), ...)
} else {
    // ... existing single-symbol code unchanged ...
    (None, price_history, indicator_store, ...)
};
```

The simulation loop iterates over `price_history` (first symbol's bars, filtered to
the master timeline intersection). All symbols share the same dates.

### 2. BarContextFactory (~line 729)

**Current:** `per_symbol_data: None`

**Change:**
```rust
per_symbol_data: per_symbol_map.map(Arc::new),
```

### 3. Order Fill — Phase A (~line 790)

**Current:** `order.try_fill(bar.open, bar.high, bar.low, bar.close)` always uses
the primary bar.

**Change:** Resolve the target symbol's bar for each order:
```rust
let target_sym = order.symbol.as_deref().unwrap_or(&config.symbol);
let fill_bar = if let Some(psd) = &per_symbol_data {
    psd.get(target_sym).and_then(|d| d.bars.get(bar_idx))
} else {
    Some(bar)
};
let Some(fill_bar) = fill_bar else { unfilled_orders.push(order); continue; };
if let Some(fill_price) = order.try_fill(fill_bar.open, fill_bar.high, fill_bar.low, fill_bar.close) {
```

### 4. Position Opening — Symbol Assignment (~lines 807, 1104, 1363)

**Current:** `symbol: config.symbol.clone()` hardcoded.

**Change:** Use the order/action's symbol:
```rust
// For stock positions (line ~807):
symbol: order.symbol.as_deref().unwrap_or(&config.symbol).to_string(),

// For options positions (line ~1104):
symbol: order.symbol.as_deref().unwrap_or(&config.symbol).to_string(),

// For assignment-created stock (line ~1363):
symbol: closed_pos.symbol.clone(),
```

### 5. Options Entry — Phase C (~line 1091)

**Current:** `resolve_option_legs(legs, &options_by_date, today, &config)` uses
primary symbol's options chain.

**Change:** Look up the target symbol's `options_by_date`:
```rust
let target_sym = order.symbol.as_deref().unwrap_or(&config.symbol);
let target_obd = if let Some(psd) = &per_symbol_data {
    psd.get(target_sym).and_then(|d| d.options_by_date.as_deref())
} else {
    options_by_date.as_deref()
};
let resolved = resolve_option_legs(legs, &target_obd, today, &config);
```

### 6. Stock MTM — Phase D (~line 1549)

**Current:** `(bar.close - *entry_price) * qty * side.multiplier()`

**Change:** Look up the position's symbol's close:
```rust
let close_price = if let Some(psd) = &per_symbol_data {
    psd.get(&pos.symbol)
       .and_then(|d| d.bars.get(bar_idx).map(|b| b.close))
       .unwrap_or(*entry_price)
} else {
    bar.close
};
let pnl = (close_price - *entry_price) * *qty as f64 * side.multiplier();
```

### 7. Options MTM — Phase D (~line 1559)

**Current:** Uses primary `price_table` and `last_known`.

**Change:** Use per-symbol PriceTable and LastKnown:
```rust
let (sym_pt, sym_lk) = if let Some(psd) = &per_symbol_data {
    let d = psd.get(&pos.symbol);
    (d.and_then(|d| d.price_table.as_deref()),
     d.map(|d| &d.last_known))
} else {
    (price_table.as_deref(), Some(&last_known))
};
// Use sym_pt / sym_lk in lookup_option_price() call
```

**Note:** `last_known` is mutated during MTM. In multi-symbol mode, each symbol's
`PerSymbolData.last_known` should be updated separately. Since `PerSymbolData` is
behind an `Arc`, you'll need to either:
- Use `Arc::get_mut` before the loop starts (single owner at that point), or
- Wrap `last_known` in a `Mutex` or `Cell` for interior mutability

### 8. Expiration ITM Detection (~line 1215)

**Current:** `classify_expiration(legs, bar.close)` uses primary close.

**Change:**
```rust
let exp_close = if let Some(psd) = &per_symbol_data {
    psd.get(&pos.symbol)
       .and_then(|d| d.bars.get(bar_idx).map(|b| b.close))
       .unwrap_or(bar.close)
} else {
    bar.close
};
exit_reason = classify_expiration(legs, exp_close);
```

### 9. Called-Away Matching (~line 1403)

**Current:** Matches assignment stock by `source == "assignment"`.

**Change:** Also match by symbol:
```rust
positions[j].symbol == closed_pos.symbol
```

### 10. Stock Action Helpers

**File:** `src/scripting/helpers.rs` + `src/scripting/registration.rs`

Add symbol-aware variants of stock order helpers:

```rust
// helpers.rs
pub fn buy_stock_for(symbol: String, qty: i64) -> Dynamic {
    let mut map = rhai::Map::new();
    map.insert("action".into(), "open_stock".into());
    map.insert("side".into(), "long".into());
    map.insert("qty".into(), qty.into());
    map.insert("symbol".into(), symbol.into());
    map.into()
}
pub fn sell_stock_for(symbol: String, qty: i64) -> Dynamic { /* same pattern, side: "short" */ }
// Also: buy_limit_for, sell_limit_for, buy_stop_for, sell_stop_for, buy_stop_limit_for, sell_stop_limit_for
```

Register in `registration.rs`:
```rust
engine.register_fn("buy_stock_for", helpers::buy_stock_for);
engine.register_fn("sell_stock_for", helpers::sell_stock_for);
// etc.
```

## Testing Strategy

1. **Backward compatibility:** All existing single-symbol tests must pass unchanged
   (751 tests currently green).

2. **Multi-symbol unit test:** Add a test in `src/scripting/tests.rs` that:
   - Creates a `BarContext` with `per_symbol_data` populated for 2 symbols
   - Calls `ctx.sym("SPY")` and `ctx.sym("QQQ")` and verifies different prices
   - Verifies `ctx.sym("INVALID")` returns `()`

3. **Integration test:** Add `tests/multi_symbol.rs` with:
   - An inline Rhai script using `symbols: ["SPY", "QQQ"]`
   - Verify trade records have correct `symbol` field
   - Verify equity curve reflects positions across both symbols
   - Requires test parquet data for both symbols (or mock DataLoader)

4. **Edge cases:**
   - Script declares `symbols: ["SPY"]` (single-element array) — should work identically
     to `symbol: "SPY"`
   - Symbol with no options data — stock trades should work, options strategy helpers
     should return `()` (no candidates)
   - Overlapping date ranges of different lengths — intersection should be reported as
     a warning

## Design Decisions

- **No primary symbol:** All symbols in `symbols` are equal peers. The first symbol's
  bars drive the iteration loop only because we need _some_ bar sequence — any symbol's
  bars would work since they all share the same dates after intersection.

- **No forward-fill:** Only real bars exist. Missing data = no data. The master timeline
  is the date intersection of all symbols' OHLCV ranges.

- **Options loading is try-based:** Symbols without options data (VIX, futures) are
  OHLCV-only. Strategy helpers return `()` when options are unavailable.

- **Single capital pool:** All symbols share one equity/cash pool. This is correct for
  a real portfolio — capital spent on SPY puts is not available for QQQ calls.

- **Shared `CachingDataLoader`:** All symbol data flows through the same caching layer,
  so repeated backtests (sweeps, walk-forward) don't re-read parquet files.
