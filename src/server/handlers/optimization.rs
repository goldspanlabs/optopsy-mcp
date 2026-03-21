//! Handler bodies for optimization tools: `parameter_sweep`, `walk_forward`, `permutation_test`.

use crate::data::cache::validate_path_segment;
use crate::engine::types::SimParams;
use crate::tools;
use crate::tools::response_types::{PermutationTestResponse, SweepResponse, WalkForwardResponse};

use super::super::params::{
    resolve_sweep_strategies, tool_err, BacktestBaseParams, ParameterSweepParams,
    PermutationTestParams, WalkForwardParams,
};
use super::super::OptopsyServer;

// ── parameter_sweep ──────────────────────────────────────────────────────

/// Execute the `parameter_sweep` tool logic (stock mode).
async fn sweep_stock(
    server: &OptopsyServer,
    params: ParameterSweepParams,
) -> Result<SweepResponse, String> {
    let stock_dims = params
        .stock_sweep
        .ok_or_else(|| "stock_sweep is required when mode is \"stock\"".to_string())?;

    let symbol = params
        .symbol
        .as_deref()
        .ok_or("symbol is required for stock mode")?
        .to_uppercase();
    validate_path_segment(&symbol).map_err(|e| format!("Invalid symbol: {e}"))?;
    let ohlcv_path = server.ensure_ohlcv(&symbol)?;

    // Resolve cross-symbol OHLCV paths from all entry/exit signals
    let exit_sigs_ref = stock_dims.exit_signals.as_deref().unwrap_or(&[]);
    let cross_ohlcv_paths =
        server.resolve_cross_ohlcv_paths(None, None, &stock_dims.entry_signals, exit_sigs_ref)?;

    let start_date = params
        .sim_params
        .start_date
        .as_deref()
        .map(|s| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| format!("Invalid start_date \"{s}\": expected YYYY-MM-DD"))
        })
        .transpose()?;
    let end_date = params
        .sim_params
        .end_date
        .as_deref()
        .map(|s| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| format!("Invalid end_date \"{s}\": expected YYYY-MM-DD"))
        })
        .transpose()?;

    let intervals = stock_dims
        .intervals
        .unwrap_or_else(|| vec![crate::engine::types::Interval::default()]);
    let sides = stock_dims
        .sides
        .unwrap_or_else(|| vec![crate::engine::types::Side::Long]);
    let session_filters: Vec<Option<crate::engine::types::SessionFilter>> = stock_dims
        .session_filters
        .map_or_else(|| vec![None], |sfs| sfs.into_iter().map(Some).collect());
    let stop_losses: Vec<Option<f64>> = stock_dims.stop_losses.map_or_else(
        || vec![params.sim_params.stop_loss],
        |sls| sls.into_iter().map(Some).collect(),
    );
    let take_profits: Vec<Option<f64>> = stock_dims.take_profits.map_or_else(
        || vec![params.sim_params.take_profit],
        |tps| tps.into_iter().map(Some).collect(),
    );
    let exit_signals: Vec<crate::signals::registry::SignalSpec> =
        stock_dims.exit_signals.unwrap_or_default();

    let base_params = crate::engine::stock_sim::StockBacktestParams {
        symbol: symbol.clone(),
        side: crate::engine::types::Side::Long, // overridden per combo
        capital: params.sim_params.capital,
        quantity: params.sim_params.quantity,
        sizing: params.sim_params.sizing,
        max_positions: params.sim_params.max_positions,
        slippage: crate::engine::types::Slippage::Mid, // overridden per combo
        commission: None,
        stop_loss: None,   // overridden per combo
        take_profit: None, // overridden per combo
        max_hold_days: params.sim_params.max_hold_days,
        max_hold_bars: params.sim_params.max_hold_bars,
        min_days_between_entries: params.sim_params.min_days_between_entries,
        min_bars_between_entries: params.sim_params.min_bars_between_entries,
        conflict_resolution: params.sim_params.conflict_resolution.unwrap_or_default(),
        entry_signal: None, // overridden per combo
        exit_signal: None,  // overridden per combo
        ohlcv_path: Some(ohlcv_path),
        cross_ohlcv_paths,
        start_date,
        end_date,
        interval: crate::engine::types::Interval::default(), // overridden per combo
        session_filter: None,                                // overridden per combo
    };

    let oos_pct = params.out_of_sample_pct / 100.0;
    let num_perms = params.num_permutations;
    let perm_seed = params.permutation_seed;

    let sweep_params = crate::engine::sweep::StockSweepParams {
        entry_signals: stock_dims.entry_signals,
        exit_signals,
        intervals,
        sides,
        session_filters,
        stop_losses,
        take_profits,
        slippage_models: stock_dims.slippage_models,
        base_params,
        out_of_sample_pct: oos_pct,
        num_permutations: num_perms,
        permutation_seed: perm_seed,
    };

    tokio::task::spawn_blocking(move || tools::sweep::execute_stock(&sweep_params))
        .await
        .map_err(|e| format!("Stock sweep task panicked: {e}"))?
        .map_err(tool_err)
}

/// Execute the `parameter_sweep` tool logic (options mode).
async fn sweep_options(
    server: &OptopsyServer,
    params: ParameterSweepParams,
) -> Result<SweepResponse, String> {
    // Validate: singular and plural signal fields are mutually exclusive
    if params.sim_params.entry_signal.is_some() && !params.sim_params.entry_signals.is_empty() {
        return Err(
            "Cannot use both `entry_signal` (singular) and `entry_signals` (plural). \
             Use `entry_signals` for sweeping multiple signals, or `entry_signal` for a fixed signal."
                .to_string(),
        );
    }
    if params.sim_params.exit_signal.is_some() && !params.sim_params.exit_signals.is_empty() {
        return Err(
            "Cannot use both `exit_signal` (singular) and `exit_signals` (plural). \
             Use `exit_signals` for sweeping multiple signals, or `exit_signal` for a fixed signal."
                .to_string(),
        );
    }

    let (symbol, df) = server.ensure_data_loaded(params.symbol.as_deref()).await?;

    let strategies = resolve_sweep_strategies(params.strategies, params.direction)?;

    // Load OHLCV data from cache if any signals are requested or any strategy has a stock leg
    let any_stock_leg = strategies
        .iter()
        .any(|s| crate::strategies::find_strategy(&s.name).is_some_and(|def| def.has_stock_leg));
    let needs_ohlcv = params.sim_params.entry_signal.is_some()
        || params.sim_params.exit_signal.is_some()
        || !params.sim_params.entry_signals.is_empty()
        || !params.sim_params.exit_signals.is_empty()
        || any_stock_leg;
    let ohlcv_path = if needs_ohlcv {
        Some(server.ensure_ohlcv(&symbol)?)
    } else {
        None
    };

    let cross_ohlcv_paths = server.resolve_cross_ohlcv_paths(
        params.sim_params.entry_signal.as_ref(),
        params.sim_params.exit_signal.as_ref(),
        &params.sim_params.entry_signals,
        &params.sim_params.exit_signals,
    )?;

    // Options mode: sweep dimensions are required (validated upstream)
    let sweep_dims = params.sweep.ok_or_else(|| {
        "sweep dimensions are required when mode is \"options\" (or omitted)".to_string()
    })?;
    let sweep_params = crate::engine::sweep::SweepParams {
        strategies,
        sweep: crate::engine::sweep::SweepDimensions {
            entry_dte_targets: sweep_dims.entry_dte_targets,
            exit_dtes: sweep_dims.exit_dtes,
            slippage_models: sweep_dims.slippage_models,
        },
        sim_params: SimParams {
            capital: params.sim_params.capital,
            quantity: params.sim_params.quantity,
            multiplier: params.sim_params.multiplier,
            max_positions: params.sim_params.max_positions,
            selector: params.sim_params.selector,
            stop_loss: params.sim_params.stop_loss,
            take_profit: params.sim_params.take_profit,
            max_hold_days: params.sim_params.max_hold_days,
            max_hold_bars: params.sim_params.max_hold_bars,
            entry_signal: params.sim_params.entry_signal,
            exit_signal: params.sim_params.exit_signal,
            ohlcv_path,
            cross_ohlcv_paths,
            min_days_between_entries: params.sim_params.min_days_between_entries,
            min_bars_between_entries: params.sim_params.min_bars_between_entries,
            conflict_resolution: params.sim_params.conflict_resolution,
            sizing: params.sim_params.sizing,
            exit_net_delta: params.sim_params.exit_net_delta,
        },
        out_of_sample_pct: params.out_of_sample_pct / 100.0,
        direction: params.direction,
        entry_signals: params.sim_params.entry_signals,
        exit_signals: params.sim_params.exit_signals,
        num_permutations: params.num_permutations,
        permutation_seed: params.permutation_seed,
    };

    tokio::task::spawn_blocking(move || tools::sweep::execute(&df, &sweep_params))
        .await
        .map_err(|e| format!("Sweep task panicked: {e}"))?
        .map_err(tool_err)
}

/// Execute the `parameter_sweep` tool logic, dispatching to stock or options mode.
pub async fn execute_sweep(
    server: &OptopsyServer,
    params: ParameterSweepParams,
) -> Result<SweepResponse, String> {
    let is_stock = params.mode.as_deref() == Some("stock");

    tracing::info!(
        mode = if is_stock { "stock" } else { "options" },
        symbol = params.symbol.as_deref().unwrap_or("auto"),
        "Parameter sweep request received"
    );

    if is_stock {
        sweep_stock(server, params).await
    } else {
        sweep_options(server, params).await
    }
}

// ── walk_forward ─────────────────────────────────────────────────────────

/// Execute the `walk_forward` tool logic.
pub async fn execute_walk_forward(
    server: &OptopsyServer,
    params: WalkForwardParams,
) -> Result<WalkForwardResponse, String> {
    let train_days = params.train_days;
    let test_days = params.test_days;
    let step_days = params.step_days;
    let is_stock = params.base.mode.as_deref() == Some("stock");

    tracing::info!(
        strategy = params.base.strategy.as_deref().unwrap_or(""),
        symbol = params.base.symbol.as_deref().unwrap_or("auto"),
        mode = if is_stock { "stock" } else { "options" },
        train_days,
        test_days,
        step_days = ?step_days,
        "Walk-forward request received"
    );

    if is_stock {
        walk_forward_stock(server, params.base, train_days, test_days, step_days).await
    } else {
        walk_forward_options(server, params.base, train_days, test_days, step_days).await
    }
}

async fn walk_forward_stock(
    server: &OptopsyServer,
    base: BacktestBaseParams,
    train_days: i32,
    test_days: i32,
    step_days: Option<i32>,
) -> Result<WalkForwardResponse, String> {
    let resolved = server.resolve_stock_backtest_params(base)?;
    let label = resolved.symbol.clone();

    tokio::task::spawn_blocking(move || {
        let ohlcv_path = resolved
            .params
            .ohlcv_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("ohlcv_path is required"))?;
        let (bars, ohlcv_df) = crate::engine::stock_sim::prepare_stock_data(
            ohlcv_path,
            resolved.params.interval,
            resolved.params.session_filter.as_ref(),
            resolved.params.start_date,
            resolved.params.end_date,
        )?;
        let (entry_dates, exit_dates) = crate::engine::stock_sim::build_stock_signal_filters(
            &resolved.params,
            &ohlcv_df,
            None,
        )?;
        tools::walk_forward::execute_stock(
            &bars,
            &resolved.params,
            &entry_dates,
            &exit_dates,
            &label,
            train_days,
            test_days,
            step_days,
        )
    })
    .await
    .map_err(|e| format!("Walk-forward task panicked: {e}"))?
    .map_err(tool_err)
}

async fn walk_forward_options(
    server: &OptopsyServer,
    base: BacktestBaseParams,
    train_days: i32,
    test_days: i32,
    step_days: Option<i32>,
) -> Result<WalkForwardResponse, String> {
    let (_symbol, df, backtest_params) = server.resolve_backtest_params(base).await?;

    tokio::task::spawn_blocking(move || {
        tools::walk_forward::execute(&df, &backtest_params, train_days, test_days, step_days)
    })
    .await
    .map_err(|e| format!("Walk-forward task panicked: {e}"))?
    .map_err(tool_err)
}

// ── permutation_test ─────────────────────────────────────────────────────

/// Execute the `permutation_test` tool logic.
pub async fn execute_permutation_test(
    server: &OptopsyServer,
    params: PermutationTestParams,
) -> Result<PermutationTestResponse, String> {
    let is_stock = params.base.mode.as_deref() == Some("stock");
    let num_permutations = params.num_permutations;
    let seed = params.seed;

    tracing::info!(
        strategy = params.base.strategy.as_deref().unwrap_or(""),
        symbol = params.base.symbol.as_deref().unwrap_or("auto"),
        mode = if is_stock { "stock" } else { "options" },
        num_permutations,
        "Permutation test request received"
    );

    let perm_params = crate::engine::permutation::PermutationParams {
        num_permutations,
        seed,
    };

    if is_stock {
        permutation_test_stock(server, params.base, perm_params).await
    } else {
        permutation_test_options(server, params.base, perm_params).await
    }
}

async fn permutation_test_stock(
    server: &OptopsyServer,
    base: BacktestBaseParams,
    perm_params: crate::engine::permutation::PermutationParams,
) -> Result<PermutationTestResponse, String> {
    let resolved = server.resolve_stock_backtest_params(base)?;
    let label = resolved.symbol.clone();

    tokio::task::spawn_blocking(move || {
        let ohlcv_path = resolved
            .params
            .ohlcv_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("ohlcv_path is required"))?;
        let (bars, ohlcv_df) = crate::engine::stock_sim::prepare_stock_data(
            ohlcv_path,
            resolved.params.interval,
            resolved.params.session_filter.as_ref(),
            resolved.params.start_date,
            resolved.params.end_date,
        )?;
        let (entry_dates, exit_dates) = crate::engine::stock_sim::build_stock_signal_filters(
            &resolved.params,
            &ohlcv_df,
            None,
        )?;
        tools::permutation_test::execute_stock(
            &bars,
            &resolved.params,
            &entry_dates,
            &exit_dates,
            &perm_params,
            &label,
        )
    })
    .await
    .map_err(|e| format!("Permutation test task panicked: {e}"))?
    .map_err(tool_err)
}

async fn permutation_test_options(
    server: &OptopsyServer,
    base: BacktestBaseParams,
    perm_params: crate::engine::permutation::PermutationParams,
) -> Result<PermutationTestResponse, String> {
    let (_symbol, df, backtest_params) = server.resolve_backtest_params(base).await?;

    tokio::task::spawn_blocking(move || {
        // Derive cache_dir from ohlcv_path ({cache_dir}/{category}/{SYMBOL}.parquet)
        let cache_dir = backtest_params
            .ohlcv_path
            .as_deref()
            .and_then(|p| std::path::Path::new(p).parent())
            .and_then(|p| p.parent());
        let (entry_dates, exit_dates) =
            crate::engine::core::build_signal_filters(&backtest_params, &df, cache_dir)?;
        tools::permutation_test::execute(
            &df,
            &backtest_params,
            &perm_params,
            &entry_dates,
            exit_dates.as_ref(),
        )
    })
    .await
    .map_err(|e| format!("Permutation test task panicked: {e}"))?
    .map_err(tool_err)
}
