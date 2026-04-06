//! Response types for workflow orchestration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::pipeline::PipelineResponse;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowKind {
    BaselineValidation,
    StrategyEvaluation,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WalkForwardRobustnessCheck {
    pub label: String,
    pub mode: String,
    pub n_windows: usize,
    pub train_pct: f64,
    pub efficiency_ratio: f64,
    pub profitable_windows: usize,
    pub total_windows: usize,
    pub param_stability: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct StrategyEvaluationResponse {
    pub summary: String,
    pub pipeline: PipelineResponse,
    pub robustness_checks: Vec<WalkForwardRobustnessCheck>,
    pub final_verdict: String,
    pub key_findings: Vec<String>,
    pub suggested_next_steps: Vec<String>,
    pub total_duration_ms: u64,
}

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(tag = "workflow", content = "result", rename_all = "snake_case")]
pub enum WorkflowResponse {
    BaselineValidation(PipelineResponse),
    StrategyEvaluation(StrategyEvaluationResponse),
}
