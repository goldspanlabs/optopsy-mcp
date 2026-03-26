#[allow(unused_macros)]
/// Log elapsed time with optional tracing fields.
///
/// # Examples
/// ```ignore
/// let start = std::time::Instant::now();
/// let output = engine_call(args)?;
/// log_elapsed!(start, "Backtest finished", trades = output.trade_count);
/// log_elapsed!(start, "Simple message");
/// ```
macro_rules! log_elapsed {
    ($start:expr, $msg:expr, $($field:tt = $val:expr),+ $(,)?) => {
        tracing::info!(elapsed_ms = $start.elapsed().as_millis(), $($field = $val,)+ $msg)
    };
    ($start:expr, $msg:expr $(,)?) => {
        tracing::info!(elapsed_ms = $start.elapsed().as_millis(), $msg)
    };
}

#[allow(unused_imports)]
pub(crate) use log_elapsed;
