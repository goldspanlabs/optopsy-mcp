//! Handler for the `run_script` MCP tool.

use std::sync::Arc;

use anyhow::Result;

use crate::scripting::engine::{CachingDataLoader, ProgressCallback, ScriptBacktestResult};
use crate::scripting::stdlib::parse_script_meta;
use crate::server::OptopsyServer;
use crate::tools::run_script::{format_indicator_data, RunScriptParams, RunScriptResponse};

/// Execute a Rhai backtest script.
pub async fn execute(server: &OptopsyServer, params: RunScriptParams) -> Result<RunScriptResponse> {
    execute_with_progress(server, params, None).await
}

/// Execute a Rhai backtest script with an optional progress callback.
pub async fn execute_with_progress(
    server: &OptopsyServer,
    params: RunScriptParams,
    progress: Option<ProgressCallback>,
) -> Result<RunScriptResponse> {
    let start = std::time::Instant::now();

    let source = crate::tools::run_script::resolve_script_source(&params)?;

    // Parse script metadata from //! header
    let script_meta = params
        .strategy
        .as_deref()
        .map(|id| parse_script_meta(id, &source));

    let loader = CachingDataLoader::new(Arc::clone(&server.cache));

    // Merge profile params if a profile was requested
    let effective_params = if let Some(ref profile_name) = params.profile {
        use crate::scripting::stdlib::{load_profiles_registry, merge_profile_params};
        let registry = load_profiles_registry();
        let script_profiles = script_meta.as_ref().and_then(|m| m.profiles.as_ref());
        merge_profile_params(profile_name, &registry, script_profiles, &params.params)
    } else {
        params.params.clone()
    };

    let ScriptBacktestResult {
        result,
        metadata: _,
        indicator_data,
        custom_series,
        ..
    } = crate::scripting::engine::run_script_backtest_with_progress(
        &source,
        &effective_params,
        &loader,
        progress,
    )
    .await?;

    // Convert raw indicator arrays and custom series to compact IndicatorData format
    let formatted_indicators = format_indicator_data(&indicator_data, &custom_series);

    Ok(RunScriptResponse {
        script_meta,
        result,
        indicator_data: formatted_indicators,
        execution_time_ms: start.elapsed().as_millis() as u64,
    })
}
