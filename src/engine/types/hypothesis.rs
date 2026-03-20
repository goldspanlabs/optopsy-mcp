//! Hypothesis generation dimension types.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Dimension to scan for hypothesis generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HypothesisDimension {
    Seasonality,
    PriceAction,
    MeanReversion,
    Volume,
    VolatilityRegime,
    CrossAsset,
    Microstructure,
    Autocorrelation,
    OptionsStructure,
}

impl HypothesisDimension {
    /// All OHLCV-only dimensions (no external data needed).
    pub fn ohlcv_dimensions() -> &'static [Self] {
        &[
            Self::Seasonality,
            Self::PriceAction,
            Self::MeanReversion,
            Self::Volume,
            Self::VolatilityRegime,
            Self::Microstructure,
            Self::Autocorrelation,
        ]
    }

    /// All available dimensions.
    pub fn all() -> &'static [Self] {
        &[
            Self::Seasonality,
            Self::PriceAction,
            Self::MeanReversion,
            Self::Volume,
            Self::VolatilityRegime,
            Self::CrossAsset,
            Self::Microstructure,
            Self::Autocorrelation,
            Self::OptionsStructure,
        ]
    }
}

impl std::fmt::Display for HypothesisDimension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Seasonality => write!(f, "seasonality"),
            Self::PriceAction => write!(f, "price_action"),
            Self::MeanReversion => write!(f, "mean_reversion"),
            Self::Volume => write!(f, "volume"),
            Self::VolatilityRegime => write!(f, "volatility_regime"),
            Self::CrossAsset => write!(f, "cross_asset"),
            Self::Microstructure => write!(f, "microstructure"),
            Self::Autocorrelation => write!(f, "autocorrelation"),
            Self::OptionsStructure => write!(f, "options_structure"),
        }
    }
}

/// Known structural basis for a discovered pattern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StructuralBasis {
    /// IV systematically overprices RV
    VarianceRiskPremium,
    /// Dealer/institutional hedging flows
    HedgingDemand,
    /// Calendar-driven portfolio rebalancing
    RebalancingFlows,
    /// Compensation for providing liquidity
    LiquidityPremium,
    /// Behavioral bias (herding, anchoring)
    MomentumBehavioral,
    /// Statistical arbitrage / liquidity provision
    MeanReversionStatArb,
    /// Cross-asset risk transmission
    MacroTransmission,
    /// Compensation for holding overnight risk
    OvernightRiskPremium,
    /// Futures/options settlement effects
    SettlementMechanics,
    /// No known structural explanation
    EmpiricalOnly,
}

impl std::fmt::Display for StructuralBasis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VarianceRiskPremium => write!(f, "variance_risk_premium"),
            Self::HedgingDemand => write!(f, "hedging_demand"),
            Self::RebalancingFlows => write!(f, "rebalancing_flows"),
            Self::LiquidityPremium => write!(f, "liquidity_premium"),
            Self::MomentumBehavioral => write!(f, "momentum_behavioral"),
            Self::MeanReversionStatArb => write!(f, "mean_reversion_stat_arb"),
            Self::MacroTransmission => write!(f, "macro_transmission"),
            Self::OvernightRiskPremium => write!(f, "overnight_risk_premium"),
            Self::SettlementMechanics => write!(f, "settlement_mechanics"),
            Self::EmpiricalOnly => write!(f, "empirical_only"),
        }
    }
}

impl StructuralBasis {
    /// Weight multiplier for final ranking: known mechanisms get 1.0, empirical-only gets 0.6.
    pub fn weight(&self) -> f64 {
        match self {
            Self::EmpiricalOnly => 0.6,
            _ => 1.0,
        }
    }

    /// Human-readable explanation of why this pattern might exist.
    pub fn explanation(&self) -> &'static str {
        match self {
            Self::VarianceRiskPremium => "Implied volatility systematically overprices realized volatility, creating a persistent premium for volatility sellers",
            Self::HedgingDemand => "Institutional and dealer hedging flows create predictable supply/demand imbalances",
            Self::RebalancingFlows => "Calendar-driven portfolio rebalancing (month-end, quarter-end) creates transient price pressure",
            Self::LiquidityPremium => "Compensation for providing liquidity during stressed or illiquid periods",
            Self::MomentumBehavioral => "Behavioral biases (herding, anchoring, disposition effect) create trending behavior",
            Self::MeanReversionStatArb => "Prices revert to fundamental value after liquidity-driven dislocations",
            Self::MacroTransmission => "Risk transmission across asset classes via shared macro factors (rates, growth, risk appetite)",
            Self::OvernightRiskPremium => "Compensation for holding positions through the overnight session when markets are closed",
            Self::SettlementMechanics => "Futures/options expiration and settlement mechanics create predictable price effects",
            Self::EmpiricalOnly => "No known structural explanation — may be data-mined or reflect an unknown mechanism",
        }
    }
}
