# Unified Bar-Level Cancellation

## Problem

Sweeps have cancellation but only between iterations — a long-running single backtest within a sweep can't be interrupted mid-run. Single backtests have no cancellation at all. The MCP tool path hardcodes `no_cancel = || false`. All three code paths (REST backtest, REST sweep, MCP tool) need consistent bar-level cancellation.

## Design

### Shared Cancellation State

Replace `sweep_cancellations` on `AppState` with a unified field:

```rust
// state.rs
pub cancellations: Arc<Mutex<HashSet<String>>>,
```

`OptopsyServer` also holds a reference to the same `Arc<Mutex<HashSet<String>>>` so MCP tool handlers can register/check cancellations without needing `AppState`.

The `__cancel_all__` sentinel cancels every in-flight run regardless of source.

### Engine Signature

Collapse `run_script_backtest` and `run_script_backtest_with_progress` into one function:

```rust
pub async fn run_script_backtest(
    script_source: &str,
    params: &HashMap<String, serde_json::Value>,
    data_loader: &dyn DataLoader,
    progress: Option<ProgressCallback>,
    precomputed_options: Option<&PrecomputedOptionsData>,
    is_cancelled: Option<&(dyn Fn() -> bool + Send + Sync)>,
) -> Result<ScriptBacktestResult>
```

Remove `run_script_backtest_with_progress` entirely — all callers use the unified function.

### Bar-Level Check

In the simulation loop (`engine.rs` ~line 714), check cancellation alongside the existing timeout check every 100 bars:

```rust
if bar_idx % 100 == 0 {
    if let Some(cancel_fn) = is_cancelled {
        if cancel_fn() {
            warnings.push("Backtest cancelled by user".to_string());
            break;
        }
    }
    if loop_start.elapsed() > timeout { ... }
    if let Some(ref cb) = progress { cb(bar_idx, price_history.len()); }
}
```

### Sweep Pass-Through

`run_grid_sweep` and `run_bayesian` keep their existing `is_cancelled` parameter for combo-level cancellation, AND pass the same closure through to `run_script_backtest` for bar-level cancellation. Both levels fire.

### MCP Tool Path

`backtest.rs` (`run_script` MCP tool) constructs an `is_cancelled` closure from `OptopsyServer.cancellations` instead of `|| false`. Progress is passed as `None` since MCP tool calls return a single response — there's no streaming channel to send incremental progress. Cancellation is the key capability here.

### Cleanup

After any backtest completes (success, error, or cancellation), the handler removes its run ID from the `cancellations` set. Same pattern sweeps already use. This prevents stale IDs from accumulating.

### `execute_with_progress` Handler

`run_script.rs` handler gains `is_cancelled` parameter, passes it through to `run_script_backtest`.

## Route Consolidation

### Before

```
POST   /runs                → create_backtest (blocking JSON)
POST   /runs/stream         → create_backtest_stream (SSE)
POST   /runs/sweep          → create_sweep (blocking JSON)
POST   /runs/sweep/stream   → create_sweep_stream (SSE)
POST   /runs/sweep/cancel   → cancel_sweeps
```

### After

```
POST   /runs                → streaming backtest (SSE, progress, cancellable)
POST   /runs/cancel         → cancel any in-flight run
POST   /runs/sweep          → streaming sweep (SSE, progress, cancellable)
GET    /runs                → list runs (unchanged)
GET    /runs/{id}           → get run (unchanged)
DELETE /runs/{id}           → delete run (unchanged)
PATCH  /runs/{id}/analysis  → set analysis (unchanged)
```

Removed routes:
- `POST /runs/stream` — merged into `POST /runs`
- `POST /runs/sweep/stream` — merged into `POST /runs/sweep`
- `POST /runs/sweep/cancel` — replaced by unified `POST /runs/cancel`

### Cancel Endpoint

`POST /runs/cancel` accepts optional JSON body:

```json
{ "id": "run-uuid" }
```

If `id` provided, inserts that ID. If omitted, inserts `__cancel_all__`. Returns `200 OK`.

### Run ID Assignment

The streaming backtest handler generates a UUID upfront and sends it as the first SSE event (`event: run_id`, `data: { "id": "..." }`) so the FE knows what to cancel.

## Frontend Changes

### `backtest-api.ts`

- Remove `runBacktestStream` (private function) and the non-streaming branch in `runBacktest`
- `runBacktest` always hits `POST /runs` (now SSE), always accepts `onProgress` and `signal`
- Add `cancelBacktest(id?: string)` → `POST /runs/cancel`

### `sweep-api.ts`

- `runSweep` URL changes from `/runs/sweep/stream` to `/runs/sweep`
- `cancelSweep()` replaced by generic `cancelRun(id?: string)` hitting `/runs/cancel`
- Move `cancelRun` to a shared location (e.g. `backtest-api.ts` or new `run-api.ts`)

## Files Changed

### Backend (`optopsy-mcp/`)

| File | Change |
|------|--------|
| `src/server/state.rs` | Rename `sweep_cancellations` → `cancellations` |
| `src/server/mod.rs` | Add `cancellations` arc to `OptopsyServer` |
| `src/main.rs` | Update routes: remove `/runs/stream`, `/runs/sweep/stream`, `/runs/sweep/cancel`; add `/runs/cancel`; wire shared cancellations arc |
| `src/scripting/engine.rs` | Merge into one `run_script_backtest` fn, add `is_cancelled` param, check in bar loop |
| `src/server/handlers/backtests.rs` | Remove `create_backtest` (blocking); `create_backtest_stream` becomes `create_backtest`; wire cancellation closure; add `cancel_backtest` handler |
| `src/server/handlers/sweeps.rs` | Remove `create_sweep` (blocking); `create_sweep_stream` becomes `create_sweep`; use `state.cancellations`; pass `is_cancelled` through to `run_script_backtest` |
| `src/server/handlers/run_script.rs` | Add `is_cancelled` param to `execute_with_progress` |
| `src/tools/backtest.rs` | Wire `is_cancelled` + progress from `OptopsyServer.cancellations` |

### Frontend (`optopsy-ui/`)

| File | Change |
|------|--------|
| `lib/backtest-api.ts` | Consolidate to SSE-only, add `cancelRun()` |
| `lib/sweep-api.ts` | Update URLs, replace `cancelSweep` with shared `cancelRun` |
