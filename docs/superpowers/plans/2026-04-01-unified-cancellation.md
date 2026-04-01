# Unified Bar-Level Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify cancellation across backtests, sweeps, and MCP tool calls at bar-level granularity, and consolidate REST routes to streaming-only.

**Architecture:** A single `cancellations: Arc<Mutex<HashSet<String>>>` on both `AppState` and `OptopsyServer`. Every in-flight run registers a UUID. The simulation bar loop checks cancellation every 100 bars. Routes consolidated: blocking endpoints removed, streaming endpoints become the primary path.

**Tech Stack:** Rust (axum, tokio, rmcp), TypeScript (Next.js fetch + SSE)

---

### Task 1: Add `cancellations` to `AppState` and `OptopsyServer`

**Files:**
- Modify: `src/server/state.rs:15-17`
- Modify: `src/server/mod.rs:44-57` (struct definition)
- Modify: `src/server/mod.rs:59-118` (constructors)
- Modify: `src/main.rs:99-111` (AppState init)
- Modify: `src/main.rs:116-127` (MCP service factory)

- [ ] **Step 1: Rename field in `AppState`**

In `src/server/state.rs`, rename `sweep_cancellations` to `cancellations`:

```rust
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::data::traits::{ChatStore, RunStore};
use crate::server::OptopsyServer;

/// Shared application state passed to all axum handlers via `State`.
#[derive(Clone)]
pub struct AppState {
    pub server: OptopsyServer,
    pub run_store: Arc<dyn RunStore>,
    pub chat_store: Arc<dyn ChatStore>,
    /// Set of run IDs (backtests and sweeps) that have been requested to cancel.
    pub cancellations: Arc<Mutex<HashSet<String>>>,
}
```

- [ ] **Step 2: Add `cancellations` field to `OptopsyServer`**

In `src/server/mod.rs`, add the field to the struct:

```rust
pub struct OptopsyServer {
    pub data: Arc<RwLock<LoadedData>>,
    pub cache: Arc<CachedStore>,
    pub strategy_store: Option<Arc<dyn StrategyStore>>,
    pub run_store: Option<Arc<dyn RunStore>>,
    pub adjustment_store: Option<Arc<crate::data::adjustment_store::SqliteAdjustmentStore>>,
    /// Shared cancellation tokens for in-flight runs (backtests + sweeps).
    pub cancellations: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    tool_router: ToolRouter<Self>,
}
```

- [ ] **Step 3: Update all `OptopsyServer` constructors**

Each constructor (`new`, `with_strategy_store`, `with_stores`, `with_all_stores`) must initialize the new field. Add `cancellations: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()))` to each `Self { ... }` block.

- [ ] **Step 4: Wire shared cancellations in `main.rs`**

In `src/main.rs`, create the arc once and share it between `AppState` and `OptopsyServer`. Replace the current `AppState` construction (~line 99-111):

```rust
let cancellations = std::sync::Arc::new(std::sync::Mutex::new(
    std::collections::HashSet::new(),
));

let mut server = server::OptopsyServer::with_all_stores(
    cache.clone(),
    strategy_store.clone(),
    run_store.clone(),
    adjustment_store.clone(),
);
server.cancellations = cancellations.clone();

let app_state = AppState {
    server,
    run_store,
    chat_store,
    cancellations: cancellations.clone(),
};
```

Also update the MCP service factory (~line 116-127) to share the same cancellations arc:

```rust
let cancellations_for_mcp = cancellations.clone();
let service = StreamableHttpService::new(
    move || {
        let mut srv = server::OptopsyServer::with_all_stores(
            cache.clone(),
            strategy_store_for_mcp.clone(),
            run_store_for_mcp.clone(),
            adjustment_store_for_mcp.clone(),
        );
        srv.cancellations = cancellations_for_mcp.clone();
        Ok(srv)
    },
    LocalSessionManager::default().into(),
    StreamableHttpServerConfig::default(),
);
```

- [ ] **Step 5: Build and fix compile errors**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-mcp && cargo build --release 2>&1 | head -60`

Expected: Compile errors in sweeps.rs referencing `sweep_cancellations` — these will be fixed in Task 4.

- [ ] **Step 6: Commit**

```bash
git add src/server/state.rs src/server/mod.rs src/main.rs
git commit -m "refactor: unify cancellation state on AppState and OptopsyServer"
```

---

### Task 2: Unify `run_script_backtest` engine function

**Files:**
- Modify: `src/scripting/engine.rs:30` (type alias)
- Modify: `src/scripting/engine.rs:46-52` (remove wrapper, rename)
- Modify: `src/scripting/engine.rs:419-425` (signature change)
- Modify: `src/scripting/engine.rs:714-731` (bar loop)

- [ ] **Step 1: Add `CancelCallback` type alias**

In `src/scripting/engine.rs` near line 30 (next to `ProgressCallback`):

```rust
pub type ProgressCallback = Box<dyn Fn(usize, usize) + Send + Sync>;
pub type CancelCallback = Box<dyn Fn() -> bool + Send + Sync>;
```

- [ ] **Step 2: Remove the old `run_script_backtest` wrapper and rename `run_script_backtest_with_progress`**

Delete lines 46-52 (the old `run_script_backtest` convenience wrapper).

Rename `run_script_backtest_with_progress` (line 419) to `run_script_backtest` and add `is_cancelled` parameter:

```rust
pub async fn run_script_backtest(
    script_source: &str,
    params: &HashMap<String, serde_json::Value>,
    data_loader: &dyn DataLoader,
    progress: Option<ProgressCallback>,
    precomputed_options: Option<&PrecomputedOptionsData>,
    is_cancelled: Option<&CancelCallback>,
) -> Result<ScriptBacktestResult> {
```

- [ ] **Step 3: Add cancellation check to bar loop**

In the bar loop (~line 714), add the cancellation check inside the existing `bar_idx % 100 == 0` block, before the timeout check:

```rust
if bar_idx % 100 == 0 {
    if let Some(cancel_fn) = is_cancelled {
        if cancel_fn() {
            warnings.push("Backtest cancelled by user".to_string());
            break;
        }
    }
    if loop_start.elapsed() > timeout {
        warnings.push(format!(
            "Backtest exceeded {}s timeout at bar {bar_idx}",
            config.timeout_secs
        ));
        break;
    }
    if let Some(ref cb) = progress {
        cb(bar_idx, price_history.len());
    }
}
```

- [ ] **Step 4: Commit**

```bash
git add src/scripting/engine.rs
git commit -m "refactor: unify run_script_backtest with cancellation + progress params"
```

---

### Task 3: Update all callers of `run_script_backtest`

**Files:**
- Modify: `src/engine/sweep.rs:11,49-55`
- Modify: `src/engine/bayesian.rs:13,440-447`
- Modify: `src/server/handlers/run_script.rs:7,25-66`
- Modify: `src/engine/walk_forward.rs:15,309,333`
- Modify: `src/tools/backtest.rs:236-263`

- [ ] **Step 1: Update sweep.rs**

In `src/engine/sweep.rs`, update the import (line 11):

```rust
use crate::scripting::engine::{
    run_script_backtest, CancelCallback, DataLoader, PrecomputedOptionsData,
};
```

Update the signature of `run_grid_sweep` to accept a `CancelCallback`:

```rust
pub async fn run_grid_sweep(
    config: &GridSweepConfig,
    data_loader: &dyn DataLoader,
    is_cancelled: &CancelCallback,
    on_progress: impl Fn(usize, usize),
) -> Result<SweepResponse> {
```

Update the call to `run_script_backtest` (~line 49):

```rust
        match run_script_backtest(
            &config.script_source,
            &run_params,
            data_loader,
            None,
            precomputed.as_ref(),
            Some(is_cancelled),
        )
        .await
```

- [ ] **Step 2: Update bayesian.rs**

In `src/engine/bayesian.rs`, update the import (line 13):

```rust
use crate::scripting::engine::{
    run_script_backtest, CancelCallback, DataLoader, PrecomputedOptionsData,
};
```

Update `run_bayesian` signature:

```rust
pub async fn run_bayesian(
    config: &BayesianConfig,
    data_loader: &dyn DataLoader,
    is_cancelled: &CancelCallback,
    on_progress: impl Fn(usize, usize),
) -> Result<SweepResponse> {
```

Update the `evaluate` helper (~line 430) to accept and pass through `is_cancelled`:

```rust
async fn evaluate(
    script_source: &str,
    base_params: &HashMap<String, Value>,
    swept_params: &HashMap<String, Value>,
    data_loader: &dyn DataLoader,
    precomputed: Option<&PrecomputedOptionsData>,
    is_cancelled: Option<&CancelCallback>,
) -> Result<(SweepResult, Option<PrecomputedOptionsData>)> {
    let mut run_params = base_params.clone();
    run_params.extend(swept_params.clone());

    let bt = run_script_backtest(
        script_source,
        &run_params,
        data_loader,
        None,
        precomputed,
        is_cancelled,
    )
    .await?;
```

Update all call sites of `evaluate` within `run_bayesian` to pass `Some(is_cancelled)`.

- [ ] **Step 3: Update `run_script.rs` handler**

In `src/server/handlers/run_script.rs`, update the import and signatures:

```rust
use crate::scripting::engine::{CachingDataLoader, CancelCallback, ProgressCallback, ScriptBacktestResult};
```

```rust
pub async fn execute(server: &OptopsyServer, params: RunScriptParams) -> Result<ExecuteResult> {
    execute_with_progress(server, params, None, None).await
}

pub async fn execute_with_progress(
    server: &OptopsyServer,
    params: RunScriptParams,
    progress: Option<ProgressCallback>,
    is_cancelled: Option<&CancelCallback>,
) -> Result<ExecuteResult> {
```

Update the call (~line 59):

```rust
    } = crate::scripting::engine::run_script_backtest(
        &source,
        &effective_params,
        &loader,
        progress,
        None,
        is_cancelled,
    )
    .await?;
```

- [ ] **Step 4: Update `walk_forward.rs`**

In `src/engine/walk_forward.rs`, update the import (line 15):

```rust
use crate::scripting::engine::{run_script_backtest, DataLoader};
```

Update both call sites (lines 309 and 333) to pass `None, None, None` for the new params:

```rust
if let Ok(result) = run_script_backtest(&script_source, &run_params, data_loader, None, None, None).await
```

```rust
let oos_result = run_script_backtest(&script_source, &oos_params, data_loader, None, None, None).await?;
```

- [ ] **Step 5: Update `backtest.rs` MCP tool**

In `src/tools/backtest.rs`, wire up `is_cancelled` from `OptopsyServer.cancellations`. Replace the `no_cancel` closure (~line 236) and pass through to sweep calls:

```rust
    // Build cancellation closure from server state
    let cancellations = Arc::clone(&server.cancellations);
    let is_cancelled: CancelCallback = Box::new(move || {
        cancellations
            .lock()
            .is_ok_and(|set| set.contains("__cancel_all__"))
    });
```

Update the grid and bayesian calls to pass `&is_cancelled`:

```rust
        "grid" => {
            // ...
            run_grid_sweep(&config, &loader, &is_cancelled, |_, _| {}).await?
        }
        "bayesian" => {
            // ...
            run_bayesian(&config, &loader, &is_cancelled, |_, _| {}).await?
        }
```

Add `use std::sync::Arc;` and `use crate::scripting::engine::CancelCallback;` to the imports.

- [ ] **Step 6: Build to verify**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-mcp && cargo build --release 2>&1 | head -60`

Expected: Compile errors only from sweeps.rs handler referencing `sweep_cancellations` (fixed in Task 4).

- [ ] **Step 7: Commit**

```bash
git add src/engine/sweep.rs src/engine/bayesian.rs src/server/handlers/run_script.rs src/engine/walk_forward.rs src/tools/backtest.rs
git commit -m "refactor: update all callers to unified run_script_backtest signature"
```

---

### Task 4: Update sweep REST handlers to use unified cancellations

**Files:**
- Modify: `src/server/handlers/sweeps.rs:280-360` (`create_sweep`)
- Modify: `src/server/handlers/sweeps.rs:380-510` (`create_sweep_stream`)
- Modify: `src/server/handlers/sweeps.rs:552-559` (`cancel_sweeps`)

- [ ] **Step 1: Update `create_sweep` (blocking handler)**

Replace all `state.sweep_cancellations` references with `state.cancellations`. In `create_sweep` (~line 301):

```rust
    let cancellations = Arc::clone(&state.cancellations);
    let cancel_run_id = run_id.clone();
    let is_cancelled: CancelCallback = Box::new(move || {
        cancellations
            .lock()
            .is_ok_and(|set| set.contains(&cancel_run_id) || set.contains("__cancel_all__"))
    });
```

Update cleanup (~line 350):

```rust
    if let Ok(mut set) = state.cancellations.lock() {
        set.remove(&run_id);
        set.remove("__cancel_all__");
    }
```

Add `use crate::scripting::engine::CancelCallback;` to imports.

- [ ] **Step 2: Update `create_sweep_stream` (SSE handler)**

Same changes as Step 1 but in the streaming handler (~lines 441-507). Replace `state.sweep_cancellations` with `state.cancellations`:

```rust
        let cancellations = Arc::clone(&state.cancellations);
        let cancel_run_id = run_id.clone();
        let is_cancelled: CancelCallback = Box::new(move || {
            cancellations
                .lock()
                .is_ok_and(|set| set.contains(&cancel_run_id) || set.contains("__cancel_all__"))
        });
```

Cleanup (~line 504):

```rust
        if let Ok(mut set) = state.cancellations.lock() {
            set.remove(&run_id);
            set.remove("__cancel_all__");
        }
```

- [ ] **Step 3: Update `cancel_sweeps` to use unified field**

```rust
pub async fn cancel_sweeps(State(state): State<AppState>) -> StatusCode {
    if let Ok(mut set) = state.cancellations.lock() {
        set.insert("__cancel_all__".to_string());
    }
    StatusCode::NO_CONTENT
}
```

- [ ] **Step 4: Build to verify**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-mcp && cargo build --release 2>&1 | head -60`

Expected: Clean compile.

- [ ] **Step 5: Run tests**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-mcp && cargo test 2>&1 | tail -30`

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/server/handlers/sweeps.rs
git commit -m "refactor: sweep handlers use unified cancellations field"
```

---

### Task 5: Wire cancellation into backtest REST handler

**Files:**
- Modify: `src/server/handlers/backtests.rs:196-250` (`create_backtest_stream`)

- [ ] **Step 1: Add cancellation closure to `create_backtest_stream`**

In `src/server/handlers/backtests.rs`, inside the `tokio::spawn` block (~line 202), after the progress setup, add:

```rust
        // Cancellation
        let run_id = uuid::Uuid::new_v4().to_string();
        let cancellations = Arc::clone(&state.cancellations);
        let cancel_run_id = run_id.clone();
        let is_cancelled: crate::scripting::engine::CancelCallback = Box::new(move || {
            cancellations
                .lock()
                .is_ok_and(|set| set.contains(&cancel_run_id) || set.contains("__cancel_all__"))
        });

        // Send run_id as first SSE event so the FE can target cancellation
        let _ = tx
            .send(
                Event::default()
                    .event("run_id")
                    .data(format!(r#"{{"id":"{run_id}"}}"#)),
            )
            .await;
```

- [ ] **Step 2: Pass `is_cancelled` to `execute_with_progress`**

Update the call (~line 244):

```rust
        let result =
            super::run_script::execute_with_progress(&state.server, run_params, Some(progress_cb), Some(&is_cancelled))
                .await;
```

- [ ] **Step 3: Add cleanup after backtest completes**

After `ticker.abort()` (~line 249), add:

```rust
        // Clean up cancellation flag
        if let Ok(mut set) = state.cancellations.lock() {
            set.remove(&run_id);
            set.remove("__cancel_all__");
        }
```

- [ ] **Step 4: Build and test**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-mcp && cargo build --release 2>&1 | head -60`

Expected: Clean compile.

- [ ] **Step 5: Commit**

```bash
git add src/server/handlers/backtests.rs
git commit -m "feat: wire cancellation into streaming backtest handler"
```

---

### Task 6: Add unified cancel endpoint and consolidate routes

**Files:**
- Modify: `src/server/handlers/backtests.rs` (add `cancel_run` handler, remove `create_backtest`)
- Modify: `src/main.rs:195-237` (route definitions)

- [ ] **Step 1: Add `cancel_run` handler**

Add to `src/server/handlers/backtests.rs`:

```rust
/// Optional request body for `POST /runs/cancel`.
#[derive(Debug, Deserialize)]
pub struct CancelRunRequest {
    pub id: Option<String>,
}

/// `POST /runs/cancel` — Cancel a specific run or all in-flight runs.
#[allow(clippy::unused_async)]
pub async fn cancel_run(
    State(state): State<AppState>,
    body: Option<Json<CancelRunRequest>>,
) -> StatusCode {
    if let Ok(mut set) = state.cancellations.lock() {
        match body.and_then(|b| b.id.clone()) {
            Some(id) => { set.insert(id); }
            None => { set.insert("__cancel_all__".to_string()); }
        }
    }
    StatusCode::NO_CONTENT
}
```

- [ ] **Step 2: Remove `create_backtest` (blocking handler)**

Delete the `create_backtest` function from `src/server/handlers/backtests.rs` (~lines 137-192). Rename `create_backtest_stream` to `create_backtest`.

- [ ] **Step 3: Consolidate routes in `main.rs`**

Replace the `run_routes` block (~lines 195-237):

```rust
        let run_routes = axum::Router::new()
            .route(
                "/runs",
                axum::routing::get(runs::list_runs).post(backtests::create_backtest),
            )
            .route(
                "/runs/cancel",
                axum::routing::post(backtests::cancel_run),
            )
            .route(
                "/runs/{id}",
                axum::routing::get(runs::get_run).delete(runs::delete_run),
            )
            .route(
                "/runs/{id}/analysis",
                axum::routing::patch(runs::set_run_analysis),
            )
            .route(
                "/runs/sweep",
                axum::routing::post(sweeps::create_sweep),
            )
            .route(
                "/runs/sweep/{sweepId}",
                axum::routing::get(runs::get_sweep_detail).delete(runs::delete_sweep),
            )
            .route(
                "/runs/sweep/{sweepId}/analysis",
                axum::routing::patch(runs::set_sweep_analysis),
            )
            .route(
                "/walk-forward",
                axum::routing::post(optopsy_mcp::server::handlers::walk_forward::run_walk_forward),
            )
            .with_state(app_state);
```

Removed routes:
- `/runs/stream` (merged: `POST /runs` is now the SSE handler)
- `/runs/sweep/stream` (merged: `POST /runs/sweep` is now the SSE handler)
- `/runs/sweep/cancel` (replaced by `/runs/cancel`)

- [ ] **Step 4: Remove `create_sweep` (blocking handler) from sweeps.rs**

Delete the blocking `create_sweep` function (~lines 280-375). Rename `create_sweep_stream` to `create_sweep`.

- [ ] **Step 5: Remove `cancel_sweeps` from sweeps.rs**

Delete the `cancel_sweeps` function (~lines 552-559) — replaced by `backtests::cancel_run`.

- [ ] **Step 6: Build and test**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-mcp && cargo build --release 2>&1 | head -60`

Expected: Clean compile.

Run: `cd /Users/michaelchu/Documents/projects/optopsy-mcp && cargo test 2>&1 | tail -30`

Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/server/handlers/backtests.rs src/server/handlers/sweeps.rs src/main.rs
git commit -m "feat: add unified /runs/cancel endpoint, consolidate to streaming-only routes"
```

---

### Task 7: Update test files

**Files:**
- Modify: `tests/script_bb_mean_reversion.rs`
- Modify: `tests/script_wheel.rs`
- Modify: `tests/script_sma200_threshold.rs`
- Modify: `tests/split_covered_call.rs`

- [ ] **Step 1: Update all test imports and call sites**

In each test file, update the import to use the new unified signature. The old 3-arg `run_script_backtest(source, params, loader)` is now 6-arg. Add `None, None, None` for the three new optional params.

For example in `tests/script_wheel.rs`:

```rust
use optopsy_mcp::scripting::engine::{run_script_backtest, DataLoader, ScriptBacktestResult};
```

Every call site like:
```rust
let result = run_script_backtest(&script_source, &params, &loader).await;
```

Becomes:
```rust
let result = run_script_backtest(&script_source, &params, &loader, None, None, None).await;
```

Apply this to all test files:
- `tests/script_bb_mean_reversion.rs` (2 call sites: lines 233, 323)
- `tests/script_wheel.rs` (5 call sites: lines 254, 344, 416, 447, 532)
- `tests/script_sma200_threshold.rs` (2 call sites: lines 122, 179)
- `tests/split_covered_call.rs` (3 call sites: lines 210, 369, 474)

- [ ] **Step 2: Build and run tests**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-mcp && cargo test 2>&1 | tail -30`

Expected: All tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/
git commit -m "test: update test call sites for unified run_script_backtest signature"
```

---

### Task 8: Update frontend API calls

**Files:**
- Modify: `optopsy-ui/lib/backtest-api.ts`
- Modify: `optopsy-ui/lib/sweep-api.ts`
- Modify: `optopsy-ui/app/(app)/runs/page.tsx:14-15,67-72`

- [ ] **Step 1: Rewrite `backtest-api.ts`**

Replace the entire file. The `runBacktest` function now always uses SSE. Add a shared `cancelRun` function:

```typescript
import type { ScriptMeta, BacktestSummary, BacktestDetail } from "./types";
import { getBackendUrl } from "./api-utils";
import { parseSSEStream } from "./sse-parser";

export async function fetchStrategies(): Promise<ScriptMeta[]> {
  const res = await fetch(`${getBackendUrl()}/strategies`);
  if (!res.ok) throw new Error(`Failed to fetch strategies: ${res.status}`);
  return res.json();
}

const DEFAULT_PARAMS = { SYMBOL: "SPY", CAPITAL: 100000 };

export async function runBacktest(
  strategy: string,
  params: Record<string, unknown> = DEFAULT_PARAMS,
  onProgress?: (pct: number) => void,
  signal?: AbortSignal,
): Promise<BacktestDetail> {
  const res = await fetch(`${getBackendUrl()}/runs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ strategy, params }),
    signal,
  });
  if (!res.ok) throw new Error(`Failed to run backtest: ${res.status}`);

  return parseSSEStream<BacktestDetail>(res, onProgress);
}

/** Cancel a specific run by ID, or all in-flight runs if no ID given. */
export async function cancelRun(id?: string): Promise<void> {
  await fetch(`${getBackendUrl()}/runs/cancel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: id ? JSON.stringify({ id }) : undefined,
  });
}

export async function saveAnalysis(id: string, analysis: string): Promise<void> {
  const res = await fetch(`${getBackendUrl()}/runs/${encodeURIComponent(id)}/analysis`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ analysis }),
  });
  if (!res.ok) throw new Error(`Failed to save analysis: ${res.status}`);
}

export async function fetchBacktestsByTag(tag: string): Promise<BacktestSummary[]> {
  const res = await fetch(`${getBackendUrl()}/runs?tag=${encodeURIComponent(tag)}`);
  if (!res.ok) throw new Error(`Failed to fetch backtests by tag: ${res.status}`);
  return res.json();
}
```

- [ ] **Step 2: Update `sweep-api.ts`**

Remove `cancelSweep`. Update `runSweep` URL from `/runs/sweep/stream` to `/runs/sweep`:

```typescript
import type { SweepSessionDetail, SweepParamDef } from "./types";
import { getBackendUrl } from "./api-utils";
import { parseSSEStream } from "./sse-parser";

export async function runSweep(
  request: {
    strategy: string;
    mode: string;
    objective: string;
    params: Record<string, unknown>;
    sweep_params: SweepParamDef[];
    max_evaluations?: number;
  },
  onProgress?: (pct: number) => void,
  signal?: AbortSignal,
): Promise<SweepSessionDetail> {
  const res = await fetch(`${getBackendUrl()}/runs/sweep`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
    signal,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to run sweep: ${res.status} ${text}`);
  }

  return parseSSEStream<SweepSessionDetail>(res, onProgress);
}

export async function saveSweepAnalysis(id: string, analysis: string): Promise<void> {
  const res = await fetch(`${getBackendUrl()}/runs/sweep/${encodeURIComponent(id)}/analysis`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ analysis }),
  });
  if (!res.ok) throw new Error(`Failed to save analysis: ${res.status}`);
}
```

- [ ] **Step 3: Update `runs/page.tsx` imports and cancel handler**

Update imports (~line 14-15):

```typescript
import { fetchStrategies, runBacktest, cancelRun } from "@/lib/backtest-api";
import { runSweep } from "@/lib/sweep-api";
```

Update `handleStop` (~line 67-72):

```typescript
  const handleStop = useCallback(() => {
    cancelRun().catch(() => {});
    backtestAbortRef.current?.abort();
    sweepAbortRef.current?.abort();
  }, []);
```

- [ ] **Step 4: Check for any other `cancelSweep` references**

Search for remaining references and update them. There should be none beyond what was already updated.

- [ ] **Step 5: Build frontend**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-ui && npm run build 2>&1 | tail -20`

Expected: Clean build.

- [ ] **Step 6: Commit**

```bash
cd /Users/michaelchu/Documents/projects/optopsy-ui
git add lib/backtest-api.ts lib/sweep-api.ts app/\(app\)/runs/page.tsx
git commit -m "feat: consolidate FE to streaming-only routes with unified cancelRun"
```

---

### Task 9: Final integration verification

- [ ] **Step 1: Run full BE test suite**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-mcp && cargo test 2>&1 | tail -30`

Expected: All tests pass.

- [ ] **Step 2: Run BE lints**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-mcp && cargo clippy --all-targets 2>&1 | tail -20`

Expected: No warnings.

- [ ] **Step 3: Check BE formatting**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-mcp && cargo fmt --check`

Expected: No formatting issues. If any, run `cargo fmt`.

- [ ] **Step 4: Run FE build and lint**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-ui && npm run build && npm run lint 2>&1 | tail -20`

Expected: Clean build, no lint errors.

- [ ] **Step 5: Check FE formatting**

Run: `cd /Users/michaelchu/Documents/projects/optopsy-ui && npm run format:check 2>&1 | tail -20`

Expected: No formatting issues. If any, run `npm run format`.
