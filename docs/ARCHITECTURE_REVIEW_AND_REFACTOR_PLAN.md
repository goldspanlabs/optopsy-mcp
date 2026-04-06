# Architecture Review and Refactor Plan

> This document captures an architecture review of the `optopsy-mcp` backend
> and proposes a refactor plan focused on separation of concerns, dependency
> direction, and reducing duplicated workflow logic.

## Executive Summary

The backend has a solid top-level module split:

- `engine` contains core simulation and quant logic
- `data` contains persistence and cache access
- `scripting` contains the Rhai execution runtime
- `server` contains Axum/MCP transport code
- `tools` contains MCP-facing use cases and response shaping
- `stats` contains reusable statistical analysis helpers

That overall shape is good. The main architectural weakness is not the
package layout, but the lack of a dedicated application/service layer between
transport code and domain logic.

Today, backtest and sweep workflows are spread across:

- REST handlers
- MCP tool handlers
- task execution code
- pipeline execution code
- shared helper functions placed inside `server::handlers`

As a result:

- transport code is doing too much orchestration
- MCP tools depend on REST handler code
- the same workflow is implemented multiple times
- runtime dependencies are passed around in optional, partially-valid states

This is workable, but it is the main source of tech debt in the backend.

## What Is Working Well

### 1. The high-level domain split is reasonable

The codebase already distinguishes between:

- domain logic in `engine`
- persistence and local cache logic in `data`
- scripting runtime in `scripting`
- transport concerns in `server`

That is a good foundation and means a refactor does not require a rewrite.

### 2. Storage abstraction exists

The store traits in `src/data/traits.rs` give the codebase a usable boundary
around persistence:

- `RunStore`
- `StrategyStore`
- `ChatStore`

This is one of the cleaner areas of the system and should be preserved.

### 3. The project is test-oriented

There is a broad integration test surface under `tests/`, which reduces the
risk of refactoring the orchestration layer.

## Main Architectural Findings

### 1. Missing application/service layer

This is the most important issue.

There is no single layer responsible for application workflows such as:

- resolve a strategy
- run a backtest
- run a sweep
- run a pipeline
- persist a run
- persist a sweep
- submit a queued task
- stream progress

Instead, those workflows are split across `server::handlers` and `tools`.

Examples:

- `src/tools/backtest.rs` imports and reuses code from
  `crate::server::handlers::sweeps`
- `src/tools/backtest.rs` calls
  `crate::server::handlers::backtests::persist_backtest`
- `src/tools/backtest.rs` calls
  `crate::server::handlers::run_script::execute`
- `src/server/handlers/pipeline.rs` delegates directly to
  `crate::tools::backtest::execute`
- `src/server/handlers/tasks.rs` adds a pipeline task path that also delegates
  directly to `crate::tools::backtest::execute`

This is the wrong dependency direction. Transport adapters should depend on
application services, not on each other.

### 2. Workflow duplication across entrypoints

The same backtest/sweep workflow appears in multiple places:

- synchronous SSE REST endpoints
- queued task endpoints
- MCP tools
- synchronous pipeline REST endpoint
- queued pipeline task endpoint

That duplication includes:

- progress callback setup
- cancellation handling
- strategy resolution
- symbol extraction
- capital extraction
- persistence
- result loading
- multi-stage orchestration for sweep -> walk-forward -> monte carlo

This creates drift risk. Any bug fix or behavior change has to be applied in
multiple places.

### 3. `OptopsyServer` holds too many optional dependencies

`src/server/mod.rs` defines `OptopsyServer` with several `Option<...>` fields:

- `strategy_store`
- `run_store`
- `adjustment_store`
- `forward_test_store`

That makes it easy to construct partially-valid runtime states and pushes
"store not configured" checks down into execution paths.

This is functional, but not especially clean. Runtime dependency validity
should be established at bootstrap time rather than repeatedly at call sites.

### 4. Startup/bootstrap logic is duplicated

`src/main.rs` contains repeated setup for HTTP and stdio modes:

- directory creation
- database open
- strategy seeding
- store creation
- server construction

That is manageable now, but it will continue to drift as the project grows.

### 5. Shared SQLite connection is a scaling bottleneck

`Database` currently exposes stores backed by a single
`Arc<Mutex<rusqlite::Connection>>`.

That is simple and consistent, but it serializes all DB access through one
connection. For local/dev usage this is fine. For a more concurrent server
workload, it becomes a bottleneck and may complicate long-running task traffic.

### 6. Task state is in-memory only

`TaskManager` is a process-local in-memory structure. On restart:

- queued tasks disappear
- running tasks disappear
- progress state disappears

That may be acceptable if tasks are explicitly best-effort and short-lived, but
it is still an operational tradeoff that should be called out.

### 7. Logging is not fully consistent

Some production-path logging still uses `eprintln!` in `tools/raw_prices.rs`
instead of `tracing`.

This is a minor issue, but it is another sign that some operational concerns
are still mixed into feature code ad hoc.

## Separation of Concerns Assessment

### Clean areas

- `engine` is mostly isolated domain logic
- `stats` looks like a utility/domain-support layer
- `data` provides a reasonably clear persistence boundary
- `scripting` encapsulates Rhai runtime concerns

### Mixed areas

- `server::handlers` contains both transport code and application orchestration
- `tools` contains both MCP-facing adapter logic and application orchestration
- persistence formatting/helpers for runs and sweeps live under transport code
- the new pipeline flow extends orchestration further into both `tools` and
  `server::handlers`, rather than centralizing it

### Current dependency problem

Current behavior effectively looks like this:

```text
server handlers -> engine/data/scripting
tools -> engine/data/scripting
tools -> server handlers
server handlers -> shared persistence/orchestration helpers
```

The problematic arrow is:

```text
tools -> server handlers
```

That should be replaced with:

```text
server handlers -> application
tools -> application
application -> engine/data/scripting
```

## Recommended Target Architecture

Add an explicit `application` layer under `src/application/`.

Suggested structure:

```text
src/
  application/
    mod.rs
    context.rs
    models.rs
    backtests.rs
    pipeline.rs
    sweeps.rs
    tasks.rs
    strategies.rs
  data/
  engine/
  scripting/
  server/
  stats/
  tools/
```

### Responsibilities by layer

#### `server`

Own only transport concerns:

- Axum route definitions
- request extraction
- response shaping
- SSE transport details
- HTTP status code mapping

#### `tools`

Own only MCP concerns:

- tool parameter schemas
- MCP response schemas
- validation and tool-level error mapping

#### `application`

Own all workflow orchestration:

- strategy resolution
- script execution orchestration
- progress/cancellation wiring
- pipeline orchestration
- run persistence
- sweep persistence
- task submission lifecycle
- application-level command/result types

#### `engine`

Own domain and simulation logic:

- sweep engines
- backtest execution primitives
- metrics
- position/pricing behavior

#### `data`

Own persistence and cache implementations:

- SQLite stores
- Parquet loading
- cache path resolution

## Concrete Refactor Plan

## Phase 1: Extract shared workflow code

Goal: remove transport-to-transport dependencies without changing behavior.

### Step 1. Create `src/application/backtests.rs`

Move or reimplement shared backtest workflow logic here:

- execute a single backtest
- resolve symbol/capital consistently
- persist a single run

Functions to extract from current code:

- logic in `server/handlers/run_script.rs`
- `persist_backtest` from `server/handlers/backtests.rs`
- single-run execution flow from `tools/backtest.rs`

### Step 2. Create `src/application/sweeps.rs`

Move or reimplement shared sweep workflow logic here:

- build param grid
- resolve strategy source
- run grid/bayesian sweep
- apply permutation gate
- persist sweep and child runs

Functions to extract from current code:

- `build_grid`
- `resolve_strategy_source_from_store`
- `persist_sweep_to_store`
- duplicated sweep execution flow in REST/task/MCP paths

### Step 3. Create `src/application/pipeline.rs`

Move or reimplement the full validation pipeline here:

- sweep result handoff
- significance gating
- walk-forward invocation
- OOS sufficiency gate
- monte carlo invocation
- stage/result assembly

The recent pipeline feature is useful, but it currently increases the same
cross-layer coupling this document calls out. It should become an application
service rather than a combined tool/handler workflow.

### Step 4. Create `src/application/tasks.rs`

Move task submission orchestration here:

- register task
- wait for semaphore permit
- wire cancellation
- execute use case
- mark task completed/failed/cancelled

The `TaskManager` type itself can remain in `server` initially, but the
workflow around it should move into `application`.

## Phase 2: Make handlers and tools thin adapters

Goal: each entrypoint becomes a small adapter around the same application API.

### REST handlers should do only:

- deserialize HTTP request
- call application service
- map result to JSON/SSE/HTTP status

### MCP tools should do only:

- deserialize tool params
- call application service
- map result to tool response type

### Rule

After this phase:

- `tools` must not import `server::handlers`
- `server::handlers` must not contain reusable business logic helpers
- pipeline REST/task paths should call `application::pipeline` rather than
  routing through `tools::backtest`

## Phase 3: Introduce explicit application context

Goal: replace optional runtime dependencies with validated dependency bundles.

Create something like:

```rust
pub struct AppServices {
    pub cache: Arc<CachedStore>,
    pub strategy_store: Arc<dyn StrategyStore>,
    pub run_store: Arc<dyn RunStore>,
    pub chat_store: Arc<dyn ChatStore>,
    pub adjustment_store: Arc<SqliteAdjustmentStore>,
    pub forward_test_store: Arc<SqliteForwardTestStore>,
}
```

Then inject this into application services and transport layers.

This removes repeated runtime checks like:

- "strategy store not configured"
- "run store not configured"

and replaces them with explicit construction-time validity.

## Phase 4: Consolidate bootstrap/startup

Goal: one source of truth for runtime wiring.

Create a bootstrap module responsible for:

- loading env
- opening DB
- running migrations
- creating stores
- seeding strategies
- creating task manager
- constructing application/server state

Then have small entrypoints:

- `run_http(...)`
- `run_stdio(...)`

This reduces duplication in `main.rs`.

## Phase 5: Revisit operational debt

These items are less urgent but worth tracking.

### Task durability

Decide whether tasks are:

- ephemeral and best-effort
- durable and restart-safe

If durable, persist at least:

- task metadata
- status
- timestamps
- result identifiers

### Database concurrency

If throughput grows, consider moving from one shared SQLite connection toward:

- a small connection pool
- or a different persistence approach for concurrent server workloads

### Logging consistency

Replace `eprintln!` with `tracing` on production paths.

## Suggested Migration Order

This sequence minimizes risk and preserves behavior during refactoring.

1. Add `src/application/` and move shared backtest persistence there
2. Move shared sweep helpers there
3. Move shared pipeline orchestration there
4. Move shared run/sweep execution there
5. Update MCP tools to call `application`
6. Update REST handlers to call `application`
7. Remove cross-imports between `tools` and `server::handlers`
8. Add explicit application dependency/context types
9. Consolidate `main.rs` bootstrap logic
10. Revisit task durability and DB concurrency if needed

## What Success Looks Like

After the refactor:

- `server` is transport-only
- `tools` is MCP-only
- `application` owns use-case orchestration
- `engine` remains domain logic
- `data` remains persistence/cache logic
- shared workflows exist in one place
- transport layers do not depend on each other
- runtime dependency validity is established at startup

## Bottom Line

The backend is not structurally broken. The top-level module design is already
good enough to support a clean architecture. The main issue is that the
application workflow layer is currently implicit and scattered.

This should be treated as refactorable tech debt, not as a rewrite problem.

The highest-value improvement is to introduce a dedicated `application` layer
and move all backtest/sweep/task orchestration into it.

## Second-Pass Review (Post-Refactor)

This section captures a follow-up review after the application-layer refactor
was implemented. The main architecture goals in this document are now largely
met. The remaining issues are mostly about Rust ergonomics, type design, and
reducing "pragmatic but heavy" orchestration code.

### What Improved

- shared backtest, sweep, pipeline, and task orchestration now lives under
  `src/application/`
- REST handlers and queued task entrypoints no longer route through
  `server::handlers` helpers for backtest/sweep/pipeline logic
- bootstrap wiring is now centralized in `src/bootstrap.rs`
- production-path raw price logging now uses `tracing`
- `application::pipeline` now owns its own request model instead of depending
  on MCP-facing `BacktestToolParams`
- queued task execution now uses a typed completion struct instead of a loose
  `(Value, String)` tuple
- sweep execution is split into smaller helper stages instead of one large
  orchestration body
- required store access is centralized behind `OptopsyServer` capability
  methods instead of repeated open-coded `Option` checks

### Remaining Rust-Focused Findings

#### 1. `OptopsyServer` still models runtime validity with `Option<...>`

This remains the main "not fully idiomatic" design issue, although the
follow-up pass improved the ergonomics by centralizing required capability
checks behind server methods.

`src/server/mod.rs` still allows partially-configured runtime states:

- `strategy_store`
- `run_store`
- `adjustment_store`
- `forward_test_store`

This is workable, but in Rust a stronger approach is usually preferable:

- separate types for stdio-only vs fully-persistent server modes
- or a dedicated dependency/context type passed only to application services
- or capability-specific wrappers rather than one broad mutable runtime shell

As written, the type system still does not prove that a given execution path
has the dependencies it requires; it now just fails more consistently at the
API boundary.

#### 2. Some application modules are still too large and mixed

The follow-up pass improved this by splitting `execute_sweep` into smaller
helpers, but `src/application/sweeps.rs` still combines several concerns in one
module:

- request DTOs
- strategy resolution
- param-grid construction
- execution wiring
- permutation gating
- persistence shaping

That is acceptable pragmatically, but it is not the cleanest Rust style. A more
idiomatic second pass would split this into smaller helpers or submodules such
as:

- `application::sweeps::inputs`
- `application::sweeps::execute`
- `application::sweeps::persist`
- `application::strategies`

The same applies, to a lesser extent, to `src/application/backtests.rs`.

#### 3. Application layer still depends on transport-shaped input models

This issue is substantially addressed for the pipeline path.

`src/application/pipeline.rs` now owns its own request type and transport
adapters translate into it before calling the application layer.

The broader principle still applies for future work: application code should
prefer owning its command/result types rather than borrowing transport-facing
schema models.

#### 4. Task orchestration is cleaner, but still callback-heavy

`src/application/tasks.rs` is a reasonable extraction, but it still relies on:

- boxed progress callbacks
- boxed cancellation callbacks
- `Arc<TaskInfo>` mutation via atomics and mutex-backed state

That is common in async Rust services, but not especially elegant. The
follow-up pass already improved this by replacing the loose `(Value, String)`
completion tuple with a typed `TaskCompletion`, but if this area grows further,
a more idiomatic direction would be:

- a typed task executor abstraction
- explicit task result enums for task-specific payloads
- narrower progress/result adapters at the handler boundary

#### 5. `Box::pin(...)` is a lint-driven compromise

The pipeline task path uses explicit boxing to satisfy `clippy::large_futures`.

This is defensible, but it is a sign that some task closures are carrying too
much state. The better long-term fix is usually to reduce captured state or
factor large async bodies into named functions, not to keep accumulating boxed
futures in handlers.

#### 6. SQLite concurrency model is still intentionally simple

`Database` still exposes a single shared `Arc<Mutex<rusqlite::Connection>>`.

This is fine for local/server-light usage, but from a Rust service-design
perspective it is still the least idiomatic part of the persistence layer for a
concurrent workload. If throughput expectations rise, consider:

- a small SQLite connection pool
- or moving persistence concerns behind async-friendly boundaries

This remains an operational design choice rather than an immediate code smell.

### Recommended Second-Pass Priorities

If a follow-up cleanup is desired, the most worthwhile sequence is:

1. Replace `OptopsyServer` optional store fields with stronger capability or
   context typing
2. Split `application/sweeps.rs` further into submodules if the workflow keeps
   growing
3. Revisit task executor typing if queued workflows continue to grow
4. Only then revisit DB concurrency if actual workload justifies it

### Updated Verdict

The refactor now follows Rust backend best practices reasonably well at the
architectural level:

- dependencies are clearer
- transport layering is much healthier
- bootstrap wiring is more explicit

The remaining gaps are mostly about pushing the design from "good pragmatic
Rust service code" toward "more strongly typed and more composable Rust service
code".
