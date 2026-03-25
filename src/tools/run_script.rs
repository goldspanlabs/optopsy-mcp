//! MCP tool handler for `run_script` — execute a Rhai backtest script.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Base directory for strategy scripts (relative to project root).
const STRATEGIES_DIR: &str = "scripts/strategies";

/// Parameters for the `run_script` MCP tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct RunScriptParams {
    /// Strategy script filename (without `.rhai` extension).
    /// Resolved from `scripts/strategies/{name}.rhai`.
    /// Examples: `"short_put"`, `"iron_condor"`, `"wheel"`, `"my_custom_strategy"`.
    #[garde(skip)]
    pub strategy: Option<String>,

    /// Inline Rhai script source code. Use for quick one-off tests only.
    /// For iterative development, write a `.rhai` file and use `strategy` instead.
    #[garde(skip)]
    pub script: Option<String>,

    /// Parameters injected as an immutable `params` map in the script scope.
    /// Scripts access values via `params.SYMBOL`, `params.CAPITAL`, etc.
    /// Must include SYMBOL and CAPITAL. Strategy-specific params vary by script.
    #[serde(default)]
    #[garde(skip)]
    pub params: HashMap<String, serde_json::Value>,
}

/// Response from a script backtest.
#[derive(Debug, Serialize, JsonSchema)]
pub struct RunScriptResponse {
    pub trade_count: usize,
    pub total_pnl: f64,
    pub metrics: serde_json::Value,
    pub equity_curve_length: usize,
    pub warnings: Vec<String>,
    pub execution_time_ms: u64,
    pub summary: String,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
}

/// Resolve the script source code.
///
/// Resolution order:
/// 1. `strategy` — load from `scripts/strategies/{name}.rhai` (file on disk)
/// 2. `script` — use inline source directly (fallback for one-off tests)
///
/// Parameter injection happens later in `engine.rs` via scope (not source prepending).
pub fn resolve_script_source(params: &RunScriptParams) -> Result<String> {
    match (&params.strategy, &params.script) {
        (Some(name), _) => load_strategy_file(name),
        (None, Some(script)) => Ok(script.clone()),
        (None, None) => {
            anyhow::bail!(
                "Either 'strategy' (script filename) or 'script' (inline source) is required"
            )
        }
    }
}

/// Load a strategy script from `scripts/strategies/{name}.rhai`.
fn load_strategy_file(name: &str) -> Result<String> {
    // Validate name: must be a simple identifier (no path traversal)
    if name.contains('/') || name.contains('\\') || name.contains("..") || name.is_empty() {
        anyhow::bail!("Invalid strategy name: '{name}'. Must be a simple filename.");
    }

    let path = PathBuf::from(STRATEGIES_DIR).join(format!("{name}.rhai"));
    std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("Strategy '{name}' not found at '{}': {e}", path.display()))
}
