//! Response types for the backtest pipeline.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::risk::MonteCarloResponse;
use super::sweep::SweepResponse;
use super::walk_forward::WalkForwardResponse;

/// Status of a pipeline execution stage or decision gate, used by the frontend
/// for rendering.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StageStatus {
    /// Stage ran successfully, or a gate condition was evaluated and passed.
    Completed,
    /// Stage was not attempted because an earlier gate condition was not met.
    Skipped,
    /// Stage execution failed, or a gate was evaluated and did not pass.
    Failed,
}

/// A single pipeline stage or gate result.
///
/// The frontend renders each stage as a card/row colored by status:
/// green (completed/passed), yellow (skipped), red (failed/error or gate not passed).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StageInfo {
    /// Stage identifier: `"sweep"`, `"significance_gate"`, `"walk_forward"`,
    /// `"oos_data_gate"`, `"monte_carlo"`.
    pub name: String,
    /// Current status of this stage or gate.
    pub status: StageStatus,
    /// Human-readable explanation when skipped or failed, including execution
    /// errors and gate-not-passed outcomes (null when completed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Wall-clock time in milliseconds (0 when skipped).
    pub duration_ms: u64,
}

/// Full pipeline response. Always contains sweep results; downstream stages
/// are present only if their preceding gates passed.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct PipelineResponse {
    /// Executive summary synthesized from all completed stages.
    pub summary: String,
    /// Ordered list of stages and gates with statuses for frontend rendering.
    pub stages: Vec<StageInfo>,

    // -- Sweep (always present) --
    /// Unique sweep ID for referencing the persisted results.
    pub sweep_id: String,
    /// Run IDs for each individual backtest within the sweep.
    pub run_ids: Vec<String>,
    /// Sweep results (ranked combos, sensitivity, timing).
    pub sweep: SweepResponse,

    // -- Pipeline stages (present only if gates passed) --
    /// Walk-forward validation result. Present when the significance gate passes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub walk_forward: Option<WalkForwardResponse>,
    /// Monte Carlo risk simulation on OOS equity. Present when the OOS data gate passes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monte_carlo: Option<MonteCarloResponse>,

    /// Key findings aggregated across all completed stages.
    pub key_findings: Vec<String>,
    /// Suggested next analysis steps for the agent.
    pub suggested_next_steps: Vec<String>,
    /// Total wall-clock time for the entire pipeline in milliseconds.
    pub total_duration_ms: u64,
}
