use serde::Serialize;

use super::helpers::DisplayType;

/// The type of a parameter value.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ParamType {
    Number,
    Select,
}

/// A parameter default value (serialized without type tag).
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum ParamDefault {
    Int(i64),
    Float(f64),
    Str(String),
}

/// Definition of a single parameter in the indicator catalog.
#[derive(Debug, Clone, Serialize)]
pub struct ParamDef {
    pub key: String,
    pub label: String,
    #[serde(rename = "type")]
    pub param_type: ParamType,
    pub default: ParamDefault,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
}

/// A catalog entry describing a built-in indicator.
#[derive(Debug, Clone, Serialize)]
pub struct CatalogEntry {
    pub id: String,
    pub label: String,
    pub category: String,
    pub display_type: DisplayType,
    pub params: Vec<ParamDef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub thresholds: Vec<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub intervals: Vec<String>,
    pub source: String,
}

/// Returns a `ParamDef` for a column selector (close/open/high/low).
fn column_param() -> ParamDef {
    ParamDef {
        key: "column".to_string(),
        label: "Column".to_string(),
        param_type: ParamType::Select,
        default: ParamDefault::Str("close".to_string()),
        min: None,
        max: None,
        options: vec![
            "close".to_string(),
            "open".to_string(),
            "high".to_string(),
            "low".to_string(),
        ],
    }
}

/// Returns a `ParamDef` for a period number with the given default (min 2, max 500).
fn period_param(default: i64) -> ParamDef {
    ParamDef {
        key: "period".to_string(),
        label: "Period".to_string(),
        param_type: ParamType::Number,
        default: ParamDefault::Int(default),
        min: Some(2.0),
        max: Some(500.0),
        options: vec![],
    }
}

/// Returns the built-in indicator catalog with 10 entries.
#[allow(clippy::too_many_lines)]
pub fn builtin_catalog() -> Vec<CatalogEntry> {
    vec![
        CatalogEntry {
            id: "sma".to_string(),
            label: "SMA".to_string(),
            category: "trend".to_string(),
            display_type: DisplayType::Overlay,
            params: vec![period_param(20), column_param()],
            thresholds: vec![],
            intervals: vec![],
            source: "builtin".to_string(),
        },
        CatalogEntry {
            id: "ema".to_string(),
            label: "EMA".to_string(),
            category: "trend".to_string(),
            display_type: DisplayType::Overlay,
            params: vec![period_param(20), column_param()],
            thresholds: vec![],
            intervals: vec![],
            source: "builtin".to_string(),
        },
        CatalogEntry {
            id: "rsi".to_string(),
            label: "RSI".to_string(),
            category: "momentum".to_string(),
            display_type: DisplayType::Subchart,
            params: vec![period_param(14), column_param()],
            thresholds: vec![30.0, 70.0],
            intervals: vec![],
            source: "builtin".to_string(),
        },
        CatalogEntry {
            id: "macd".to_string(),
            label: "MACD".to_string(),
            category: "momentum".to_string(),
            display_type: DisplayType::Subchart,
            params: vec![
                ParamDef {
                    key: "fast".to_string(),
                    label: "Fast".to_string(),
                    param_type: ParamType::Number,
                    default: ParamDefault::Int(12),
                    min: Some(2.0),
                    max: Some(100.0),
                    options: vec![],
                },
                ParamDef {
                    key: "slow".to_string(),
                    label: "Slow".to_string(),
                    param_type: ParamType::Number,
                    default: ParamDefault::Int(26),
                    min: Some(2.0),
                    max: Some(200.0),
                    options: vec![],
                },
                ParamDef {
                    key: "signal".to_string(),
                    label: "Signal".to_string(),
                    param_type: ParamType::Number,
                    default: ParamDefault::Int(9),
                    min: Some(2.0),
                    max: Some(100.0),
                    options: vec![],
                },
                column_param(),
            ],
            thresholds: vec![0.0],
            intervals: vec![],
            source: "builtin".to_string(),
        },
        CatalogEntry {
            id: "bbands".to_string(),
            label: "Bollinger Bands".to_string(),
            category: "volatility".to_string(),
            display_type: DisplayType::Overlay,
            params: vec![
                period_param(20),
                ParamDef {
                    key: "mult".to_string(),
                    label: "Multiplier".to_string(),
                    param_type: ParamType::Number,
                    default: ParamDefault::Float(2.0),
                    min: Some(0.5),
                    max: Some(5.0),
                    options: vec![],
                },
                column_param(),
            ],
            thresholds: vec![],
            intervals: vec![],
            source: "builtin".to_string(),
        },
        CatalogEntry {
            id: "stochastic".to_string(),
            label: "Stochastic".to_string(),
            category: "momentum".to_string(),
            display_type: DisplayType::Subchart,
            params: vec![period_param(14)],
            thresholds: vec![20.0, 80.0],
            intervals: vec![],
            source: "builtin".to_string(),
        },
        CatalogEntry {
            id: "atr".to_string(),
            label: "ATR".to_string(),
            category: "volatility".to_string(),
            display_type: DisplayType::Subchart,
            params: vec![period_param(14)],
            thresholds: vec![],
            intervals: vec![],
            source: "builtin".to_string(),
        },
        CatalogEntry {
            id: "adx".to_string(),
            label: "ADX".to_string(),
            category: "trend".to_string(),
            display_type: DisplayType::Subchart,
            params: vec![period_param(14)],
            thresholds: vec![25.0],
            intervals: vec![],
            source: "builtin".to_string(),
        },
        CatalogEntry {
            id: "cci".to_string(),
            label: "CCI".to_string(),
            category: "momentum".to_string(),
            display_type: DisplayType::Subchart,
            params: vec![period_param(20), column_param()],
            thresholds: vec![100.0, -100.0],
            intervals: vec![],
            source: "builtin".to_string(),
        },
        CatalogEntry {
            id: "obv".to_string(),
            label: "OBV".to_string(),
            category: "volume".to_string(),
            display_type: DisplayType::Subchart,
            params: vec![],
            thresholds: vec![],
            intervals: vec![],
            source: "builtin".to_string(),
        },
    ]
}
