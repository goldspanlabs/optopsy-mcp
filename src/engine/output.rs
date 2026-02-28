use anyhow::Result;
use polars::prelude::*;

use super::types::GroupStats;

/// Bin trades by DTE and delta intervals, compute grouped statistics
pub fn bin_and_aggregate(
    df: &DataFrame,
    dte_interval: i32,
    delta_interval: f64,
) -> Result<Vec<GroupStats>> {
    // Create DTE bins
    let df = df
        .clone()
        .lazy()
        .with_column(
            (col("dte") / lit(dte_interval) * lit(dte_interval)).alias("dte_bin"),
        )
        .with_column(
            (col("abs_delta") / lit(delta_interval))
                .floor()
                .cast(DataType::Float64)
                * lit(delta_interval),
        )
        .with_column(
            ((col("abs_delta") / lit(delta_interval)).floor() * lit(delta_interval)).alias("delta_bin"),
        )
        .collect()?;

    // Group by bins and compute stats
    let grouped = df
        .clone()
        .lazy()
        .group_by([col("dte_bin"), col("delta_bin")])
        .agg([
            col("pnl").count().alias("count"),
            col("pnl").mean().alias("mean"),
            col("pnl").std(1).alias("std"),
            col("pnl").min().alias("min"),
            col("pnl").quantile(lit(0.25), QuantileMethod::Linear).alias("q25"),
            col("pnl").median().alias("median"),
            col("pnl").quantile(lit(0.75), QuantileMethod::Linear).alias("q75"),
            col("pnl").max().alias("max"),
            // Win rate: fraction of positive P&L trades
            col("pnl")
                .gt(lit(0.0))
                .cast(DataType::Float64)
                .mean()
                .alias("win_rate"),
            // Profit factor: sum of wins / abs(sum of losses)
            col("pnl")
                .filter(col("pnl").gt(lit(0.0)))
                .sum()
                .alias("total_wins"),
            col("pnl")
                .filter(col("pnl").lt(lit(0.0)))
                .sum()
                .abs()
                .alias("total_losses"),
        ])
        .sort(
            ["dte_bin", "delta_bin"],
            SortMultipleOptions::default(),
        )
        .collect()?;

    // Convert to GroupStats
    let mut results = Vec::new();

    for row_idx in 0..grouped.height() {
        let dte_bin = grouped
            .column("dte_bin")?
            .i32()?
            .get(row_idx)
            .unwrap_or(0);
        let delta_bin = grouped
            .column("delta_bin")?
            .f64()?
            .get(row_idx)
            .unwrap_or(0.0);
        let count = grouped
            .column("count")?
            .u32()?
            .get(row_idx)
            .unwrap_or(0) as usize;
        let mean = grouped.column("mean")?.f64()?.get(row_idx).unwrap_or(0.0);
        let std = grouped.column("std")?.f64()?.get(row_idx).unwrap_or(0.0);
        let min = grouped.column("min")?.f64()?.get(row_idx).unwrap_or(0.0);
        let q25 = grouped.column("q25")?.f64()?.get(row_idx).unwrap_or(0.0);
        let median = grouped
            .column("median")?
            .f64()?
            .get(row_idx)
            .unwrap_or(0.0);
        let q75 = grouped.column("q75")?.f64()?.get(row_idx).unwrap_or(0.0);
        let max = grouped.column("max")?.f64()?.get(row_idx).unwrap_or(0.0);
        let win_rate = grouped
            .column("win_rate")?
            .f64()?
            .get(row_idx)
            .unwrap_or(0.0);
        let total_wins = grouped
            .column("total_wins")?
            .f64()?
            .get(row_idx)
            .unwrap_or(0.0);
        let total_losses = grouped
            .column("total_losses")?
            .f64()?
            .get(row_idx)
            .unwrap_or(0.0);

        let profit_factor = if total_losses > 0.0 {
            total_wins / total_losses
        } else if total_wins > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        results.push(GroupStats {
            dte_range: format!("({}, {}]", dte_bin, dte_bin + dte_interval),
            delta_range: format!(
                "({:.2}, {:.2}]",
                delta_bin,
                delta_bin + delta_interval
            ),
            count,
            mean,
            std,
            min,
            q25,
            median,
            q75,
            max,
            win_rate,
            profit_factor,
        });
    }

    Ok(results)
}
