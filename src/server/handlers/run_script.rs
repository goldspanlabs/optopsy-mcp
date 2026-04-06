//! Handler compatibility wrappers for the shared backtest application service.

use anyhow::Result;

pub use crate::application::backtests::ExecuteResult;
use crate::scripting::engine::{CancelCallback, ProgressCallback};
use crate::server::OptopsyServer;
use crate::tools::run_script::RunScriptParams;

/// Execute a Rhai backtest script.
pub async fn execute(server: &OptopsyServer, params: RunScriptParams) -> Result<ExecuteResult> {
    crate::application::backtests::execute_script(server, params).await
}

/// Execute a Rhai backtest script with an optional progress callback.
pub async fn execute_with_progress(
    server: &OptopsyServer,
    params: RunScriptParams,
    progress: Option<ProgressCallback>,
    is_cancelled: Option<&CancelCallback>,
) -> Result<ExecuteResult> {
    crate::application::backtests::execute_script_with_progress(
        server,
        params,
        progress,
        is_cancelled,
    )
    .await
}
