//! MCP tool handler for `run_script` — execute a Rhai backtest script.

use std::collections::HashMap;

use anyhow::Result;
use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Parameters for the `run_script` MCP tool.
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct RunScriptParams {
    /// Rhai script source code (inline). Required unless `strategy` is set.
    #[garde(skip)]
    pub script: Option<String>,

    /// Use a built-in strategy script by name (e.g., "short_put", "iron_condor", "wheel").
    #[garde(skip)]
    pub strategy: Option<String>,

    /// Constants injected as `const` declarations, prepended to both inline and
    /// strategy scripts. For strategies: must include SYMBOL, CAPITAL, and strategy-
    /// specific params. Script's own `const` declarations shadow injected ones.
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

/// Resolve the script source (inline or strategy), inject parameters,
/// and return the final Rhai source code.
pub fn resolve_script_source(params: &RunScriptParams) -> Result<String> {
    use crate::scripting::stdlib;

    let base_source = match (&params.script, &params.strategy) {
        (Some(script), _) => script.clone(),
        (None, Some(name)) => stdlib::load_strategy(name)?.to_string(),
        (None, None) => {
            anyhow::bail!(
                "Either 'script' (inline source) or 'strategy' (built-in name) is required"
            )
        }
    };

    // Inject params as const declarations
    if params.params.is_empty() {
        Ok(base_source)
    } else {
        Ok(stdlib::inject_as_const(&base_source, &params.params))
    }
}
