use anyhow::Result;
use polars::prelude::*;

use super::types::GroupStats;

/// Bin trades by DTE and delta intervals, compute grouped statistics
#[allow(clippy::too_many_lines)]
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
            ((col("dte") - lit(1)) / lit(dte_interval) * lit(dte_interval)).alias("dte_bin"),
        )
        .with_column(
            (((col("abs_delta") - lit(1e-10)) / lit(delta_interval))
                .floor()
                .cast(DataType::Float64)
                * lit(delta_interval))
            .alias("delta_bin"),
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
            col("pnl")
                .quantile(lit(0.25), QuantileMethod::Linear)
                .alias("q25"),
            col("pnl").median().alias("median"),
            col("pnl")
                .quantile(lit(0.75), QuantileMethod::Linear)
                .alias("q75"),
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
        .sort(["dte_bin", "delta_bin"], SortMultipleOptions::default())
        .collect()?;

    // Convert to GroupStats
    let mut results = Vec::new();

    for row_idx in 0..grouped.height() {
        let dte_bin = grouped.column("dte_bin")?.i32()?.get(row_idx).unwrap_or(0);
        let delta_bin = grouped
            .column("delta_bin")?
            .f64()?
            .get(row_idx)
            .unwrap_or(0.0);
        let count = grouped.column("count")?.u32()?.get(row_idx).unwrap_or(0) as usize;
        let mean = grouped.column("mean")?.f64()?.get(row_idx).unwrap_or(0.0);
        let std = grouped.column("std")?.f64()?.get(row_idx).unwrap_or(0.0);
        let min = grouped.column("min")?.f64()?.get(row_idx).unwrap_or(0.0);
        let q25 = grouped.column("q25")?.f64()?.get(row_idx).unwrap_or(0.0);
        let median = grouped.column("median")?.f64()?.get(row_idx).unwrap_or(0.0);
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
            delta_range: format!("({:.2}, {:.2}]", delta_bin, delta_bin + delta_interval),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::similar_names)]
    fn bucketing_right_closed_boundaries() {
        // DTE=7 with interval=7 should land in (0, 7], not (7, 14]
        // DTE=8 should land in (7, 14]
        // delta=0.30 with interval=0.05 should land in (0.25, 0.30], not (0.30, 0.35]
        // delta=0.301 should land in (0.30, 0.35]
        let df = df! {
            "dte" => &[7i32, 8, 14, 1],
            "abs_delta" => &[0.30f64, 0.301, 0.05, 0.25],
            "pnl" => &[100.0f64, -50.0, 75.0, 200.0],
        }
        .unwrap();

        let results = bin_and_aggregate(&df, 7, 0.05).unwrap();

        // Find the group containing DTE=7
        let dte7_group = results
            .iter()
            .find(|g| g.dte_range == "(0, 7]" && g.delta_range == "(0.25, 0.30]")
            .expect("DTE=7, delta=0.30 should be in (0, 7] and (0.25, 0.30]");
        assert!(dte7_group.count >= 1);

        // DTE=8 should be in (7, 14]
        let dte8_group = results
            .iter()
            .find(|g| g.dte_range == "(7, 14]" && g.delta_range == "(0.30, 0.35]")
            .expect("DTE=8, delta=0.301 should be in (7, 14] and (0.30, 0.35]");
        assert!(dte8_group.count >= 1);

        // DTE=14 should be in (7, 14]
        let dte14_group = results
            .iter()
            .find(|g| g.dte_range == "(7, 14]")
            .expect("DTE=14 should be in (7, 14]");
        assert!(dte14_group.count >= 1);

        // DTE=1 should be in (0, 7]
        let dte1_group = results
            .iter()
            .find(|g| g.dte_range == "(0, 7]" && g.delta_range == "(0.20, 0.25]")
            .expect("DTE=1, delta=0.25 should be in (0, 7] and (0.20, 0.25]");
        assert!(dte1_group.count >= 1);
    }
}
