use garde::Validate;
use polars::prelude::*;
use rmcp::{
    handler::server::router::tool::ToolRouter,
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ServerHandler,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::data::cache::{validate_path_segment, CachedStore};
use crate::data::DataStore;
use crate::engine::types::{
    default_min_bid_ask, default_multiplier, validate_exit_dte_lt_entry_min, BacktestParams,
    Commission, CompareEntry, CompareParams, Direction, DteRange, ExpirationFilter, SimParams,
    Slippage, TargetRange, TradeSelector, EPOCH_DAYS_CE_OFFSET,
};
use crate::signals::registry::{collect_cross_symbols, SignalSpec};
use crate::tools;
use crate::tools::response_types::{
    BacktestResponse, BuildSignalResponse, CheckCacheResponse, CompareResponse,
    ConstructSignalResponse, FetchResponse, PermutationTestResponse, RawPricesResponse,
    StatusResponse, StrategiesResponse, SuggestResponse, SweepResponse, WalkForwardResponse,
};
use crate::tools::signals::SignalsResponse;

// ---------------------------------------------------------------------------
// SanitizedJson — drop-in replacement for rmcp::Json that replaces NaN/Infinity
// with 0.0 so that `serde_json::to_value` never fails on non-finite floats.
//
// `serde_json::to_value` rejects NaN/Infinity *during* serialization, so we
// cannot sanitize after the fact. Instead we wrap the inner `Serialize` impl
// with `FiniteF64` which intercepts `serialize_f64` and maps non-finite values
// to `0.0` before they reach serde_json.
// ---------------------------------------------------------------------------

/// Wrapper whose `Serialize` impl delegates to `T` but replaces any
/// non-finite `f64` values (NaN, ±Infinity) with `0.0` during serialization.
struct FiniteF64Wrap<'a, T: serde::Serialize + ?Sized>(&'a T);

impl<T: serde::Serialize + ?Sized> serde::Serialize for FiniteF64Wrap<'_, T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(FiniteF64Serializer(serializer))
    }
}

/// Create a `FiniteF64Wrap` from a reference.
fn finite_f64<T: serde::Serialize + ?Sized>(value: &T) -> FiniteF64Wrap<'_, T> {
    FiniteF64Wrap(value)
}

/// A `Serializer` wrapper that intercepts `serialize_f64` calls and clamps
/// non-finite values to `0.0`. All other methods delegate unchanged.
struct FiniteF64Serializer<S>(S);

macro_rules! delegate_serialize {
    ($method:ident, $ty:ty) => {
        fn $method(self, v: $ty) -> Result<Self::Ok, Self::Error> {
            self.0.$method(v)
        }
    };
}

impl<S: serde::Serializer> serde::Serializer for FiniteF64Serializer<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = FiniteF64Compound<S::SerializeSeq>;
    type SerializeTuple = FiniteF64Compound<S::SerializeTuple>;
    type SerializeTupleStruct = FiniteF64Compound<S::SerializeTupleStruct>;
    type SerializeTupleVariant = FiniteF64Compound<S::SerializeTupleVariant>;
    type SerializeMap = FiniteF64Compound<S::SerializeMap>;
    type SerializeStruct = FiniteF64Compound<S::SerializeStruct>;
    type SerializeStructVariant = FiniteF64Compound<S::SerializeStructVariant>;

    delegate_serialize!(serialize_bool, bool);
    delegate_serialize!(serialize_i8, i8);
    delegate_serialize!(serialize_i16, i16);
    delegate_serialize!(serialize_i32, i32);
    delegate_serialize!(serialize_i64, i64);
    delegate_serialize!(serialize_i128, i128);
    delegate_serialize!(serialize_u8, u8);
    delegate_serialize!(serialize_u16, u16);
    delegate_serialize!(serialize_u32, u32);
    delegate_serialize!(serialize_u64, u64);
    delegate_serialize!(serialize_u128, u128);
    delegate_serialize!(serialize_f32, f32);
    delegate_serialize!(serialize_char, char);
    delegate_serialize!(serialize_str, &str);
    delegate_serialize!(serialize_bytes, &[u8]);

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_f64(if v.is_finite() { v } else { 0.0 })
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_none()
    }

    fn serialize_some<T: serde::Serialize + ?Sized>(
        self,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_some(&finite_f64(value))
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_struct(name)
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_variant(name, variant_index, variant)
    }

    fn serialize_newtype_struct<T: serde::Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_newtype_struct(name, &finite_f64(value))
    }

    fn serialize_newtype_variant<T: serde::Serialize + ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0
            .serialize_newtype_variant(name, variant_index, variant, &finite_f64(value))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.0.serialize_seq(len).map(FiniteF64Compound)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.0.serialize_tuple(len).map(FiniteF64Compound)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.0
            .serialize_tuple_struct(name, len)
            .map(FiniteF64Compound)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.0
            .serialize_tuple_variant(name, variant_index, variant, len)
            .map(FiniteF64Compound)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.0.serialize_map(len).map(FiniteF64Compound)
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.0.serialize_struct(name, len).map(FiniteF64Compound)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.0
            .serialize_struct_variant(name, variant_index, variant, len)
            .map(FiniteF64Compound)
    }
}

/// Compound-type wrapper that wraps each element/field through `FiniteF64`.
struct FiniteF64Compound<C>(C);

impl<C: serde::ser::SerializeSeq> serde::ser::SerializeSeq for FiniteF64Compound<C> {
    type Ok = C::Ok;
    type Error = C::Error;
    fn serialize_element<T: serde::Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.0.serialize_element(&finite_f64(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: serde::ser::SerializeTuple> serde::ser::SerializeTuple for FiniteF64Compound<C> {
    type Ok = C::Ok;
    type Error = C::Error;
    fn serialize_element<T: serde::Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.0.serialize_element(&finite_f64(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: serde::ser::SerializeTupleStruct> serde::ser::SerializeTupleStruct
    for FiniteF64Compound<C>
{
    type Ok = C::Ok;
    type Error = C::Error;
    fn serialize_field<T: serde::Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.0.serialize_field(&finite_f64(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: serde::ser::SerializeTupleVariant> serde::ser::SerializeTupleVariant
    for FiniteF64Compound<C>
{
    type Ok = C::Ok;
    type Error = C::Error;
    fn serialize_field<T: serde::Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.0.serialize_field(&finite_f64(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: serde::ser::SerializeMap> serde::ser::SerializeMap for FiniteF64Compound<C> {
    type Ok = C::Ok;
    type Error = C::Error;
    fn serialize_key<T: serde::Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.0.serialize_key(&finite_f64(key))
    }
    fn serialize_value<T: serde::Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.0.serialize_value(&finite_f64(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: serde::ser::SerializeStruct> serde::ser::SerializeStruct for FiniteF64Compound<C> {
    type Ok = C::Ok;
    type Error = C::Error;
    fn serialize_field<T: serde::Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.0.serialize_field(key, &finite_f64(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: serde::ser::SerializeStructVariant> serde::ser::SerializeStructVariant
    for FiniteF64Compound<C>
{
    type Ok = C::Ok;
    type Error = C::Error;
    fn serialize_field<T: serde::Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.0.serialize_field(key, &finite_f64(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

/// Serialize `T` into a `serde_json::Value`, replacing any non-finite f64 with `0.0`.
fn serialize_finite<T: serde::Serialize>(
    value: &T,
) -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(finite_f64(value))
}

/// Like `rmcp::handler::server::wrapper::Json`, but sanitises non-finite f64 values
/// during serialisation so that `serde_json::to_value` never fails on NaN/±Infinity.
pub struct SanitizedJson<T>(pub T);

impl<T: schemars::JsonSchema> schemars::JsonSchema for SanitizedJson<T> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        T::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        T::json_schema(generator)
    }
}

impl<T: serde::Serialize + schemars::JsonSchema + 'static>
    rmcp::handler::server::tool::IntoCallToolResult for SanitizedJson<T>
{
    fn into_call_tool_result(self) -> Result<rmcp::model::CallToolResult, rmcp::ErrorData> {
        let value = serialize_finite(&self.0).map_err(|e| {
            rmcp::ErrorData::internal_error(
                format!("Failed to serialize structured content: {e}"),
                None,
            )
        })?;
        Ok(rmcp::model::CallToolResult::structured(value))
    }
}

/// Newtype wrapper around `Result` to work around orphan rule for `IntoCallToolResult`.
pub struct SanitizedResult<T, E>(pub Result<T, E>);

impl<T: serde::Serialize + schemars::JsonSchema + 'static, E: rmcp::model::IntoContents>
    rmcp::handler::server::tool::IntoCallToolResult for SanitizedResult<T, E>
{
    fn into_call_tool_result(self) -> Result<rmcp::model::CallToolResult, rmcp::ErrorData> {
        match self.0 {
            Ok(value) => SanitizedJson(value).into_call_tool_result(),
            Err(error) => Ok(rmcp::model::CallToolResult::error(error.into_contents())),
        }
    }
}

/// Loaded data: `HashMap<Symbol, DataFrame>` for multi-symbol support.
type LoadedData = HashMap<String, DataFrame>;

/// Format a garde validation error with the originating tool name for easier debugging.
fn validation_err(tool: &str, e: impl std::fmt::Display) -> String {
    format!("[{tool}] Validation error: {e}")
}

#[derive(Clone)]
pub struct OptopsyServer {
    pub data: Arc<RwLock<LoadedData>>,
    pub cache: Arc<CachedStore>,
    tool_router: ToolRouter<Self>,
}

impl OptopsyServer {
    pub fn new(cache: Arc<CachedStore>) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            cache,
            tool_router: Self::tool_router(),
        }
    }

    /// Ensure options data is loaded for a symbol, auto-loading from cache if needed.
    /// Returns `(symbol, DataFrame)`.
    async fn ensure_data_loaded(
        &self,
        symbol: Option<&str>,
    ) -> Result<(String, DataFrame), String> {
        // Fast path: try a read lock first to avoid serializing all requests when data
        // is already loaded. This covers the common case of concurrent reads.
        {
            let data = self.data.read().await;
            if !data.is_empty() {
                return match Self::resolve_symbol(&data, symbol) {
                    Ok((sym, df)) => Ok((sym.clone(), df.clone())),
                    Err(e) => Err(format!("Error: {e}")),
                };
            }
        }

        // Auto-load requires a symbol
        let sym = symbol.ok_or_else(|| {
            "No data loaded and no symbol provided. Pass a symbol (e.g. \"SPY\").".to_string()
        })?;

        // Validate the symbol to prevent path traversal attacks before passing to the data layer.
        let sym_upper = sym.to_uppercase();
        validate_path_segment(&sym_upper).map_err(|e| format!("Invalid symbol: {e}"))?;

        tracing::info!(symbol = %sym, "Auto-loading options data from cache");

        // Load data WITHOUT holding any lock so concurrent requests aren't blocked
        // during I/O. Two concurrent auto-loads for the same symbol may both fetch,
        // but the insert is idempotent.
        let df = self
            .cache
            .load_options(&sym_upper, None, None)
            .await
            .map_err(|e| format!("Failed to auto-load data for {sym}: {e}"))?;

        // Brief write lock just for insertion
        let mut data = self.data.write().await;

        // Another request may have loaded data while we were fetching — check and
        // use existing data if present for this symbol.
        if let Some(existing) = data.get(&sym_upper) {
            return Ok((sym_upper, existing.clone()));
        }

        data.insert(sym_upper.clone(), df.clone());
        Ok((sym_upper, df))
    }

    /// Ensure OHLCV price data exists for a symbol, auto-fetching from Yahoo Finance if needed.
    /// Returns the parquet file path.
    async fn ensure_ohlcv(&self, symbol: &str) -> Result<String, String> {
        // Try local cache, then S3 fallback
        if let Ok(path) = self.cache.ensure_local_for(symbol, "prices").await {
            return Ok(path.to_string_lossy().to_string());
        }

        tracing::info!(symbol = %symbol, "Auto-fetching OHLCV data from Yahoo Finance");

        tools::fetch::execute(&self.cache, symbol, "5y")
            .await
            .map_err(|e| format!("Failed to auto-fetch OHLCV data for {symbol}: {e}"))?;

        let path = self
            .cache
            .cache_path(symbol, "prices")
            .map_err(|e| format!("Error resolving OHLCV path: {e}"))?;
        Ok(path.to_string_lossy().to_string())
    }

    /// Collect all cross-symbol references from entry/exit signals and resolve their OHLCV paths.
    ///
    /// Inspects both the singular `entry_signal`/`exit_signal` and the plural
    /// `entry_signals`/`exit_signals` lists (used by parameter sweep).
    async fn resolve_cross_ohlcv_paths(
        &self,
        entry_signal: Option<&SignalSpec>,
        exit_signal: Option<&SignalSpec>,
        entry_signals: &[SignalSpec],
        exit_signals: &[SignalSpec],
    ) -> Result<HashMap<String, String>, String> {
        let mut all_symbols = std::collections::HashSet::new();
        if let Some(sig) = entry_signal {
            all_symbols.extend(collect_cross_symbols(sig));
        }
        if let Some(sig) = exit_signal {
            all_symbols.extend(collect_cross_symbols(sig));
        }
        for sig in entry_signals {
            all_symbols.extend(collect_cross_symbols(sig));
        }
        for sig in exit_signals {
            all_symbols.extend(collect_cross_symbols(sig));
        }

        let mut paths = HashMap::new();
        for sym in all_symbols {
            let path = self.ensure_ohlcv(&sym).await?;
            paths.insert(sym, path);
        }
        Ok(paths)
    }

    /// Resolve shared backtest base parameters into engine `BacktestParams`, auto-loading
    /// data and OHLCV as needed. Returns `(symbol, DataFrame, BacktestParams)`.
    async fn resolve_backtest_params(
        &self,
        base: BacktestBaseParams,
    ) -> Result<(String, DataFrame, BacktestParams), String> {
        let BacktestBaseParams {
            strategy,
            leg_deltas,
            entry_dte,
            exit_dte,
            slippage,
            commission,
            min_bid_ask,
            stop_loss,
            take_profit,
            max_hold_days,
            capital,
            quantity,
            multiplier,
            max_positions,
            selector,
            entry_signal,
            exit_signal,
            symbol: symbol_param,
            min_net_premium,
            max_net_premium,
            min_net_delta,
            max_net_delta,
            min_days_between_entries,
            expiration_filter,
            exit_net_delta,
        } = base;

        let (symbol, df) = self.ensure_data_loaded(symbol_param.as_deref()).await?;

        let ohlcv_path = if entry_signal.is_some() || exit_signal.is_some() {
            Some(self.ensure_ohlcv(&symbol).await?)
        } else {
            None
        };

        let cross_ohlcv_paths = self
            .resolve_cross_ohlcv_paths(entry_signal.as_ref(), exit_signal.as_ref(), &[], &[])
            .await?;

        let leg_deltas = resolve_leg_deltas(leg_deltas, &strategy)?;

        let backtest_params = BacktestParams {
            strategy,
            leg_deltas,
            entry_dte,
            exit_dte,
            slippage,
            commission,
            min_bid_ask,
            stop_loss,
            take_profit,
            max_hold_days,
            capital,
            quantity,
            multiplier,
            max_positions,
            selector: selector.unwrap_or_default(),
            adjustment_rules: vec![],
            entry_signal,
            exit_signal,
            ohlcv_path,
            cross_ohlcv_paths,
            min_net_premium,
            max_net_premium,
            min_net_delta,
            max_net_delta,
            min_days_between_entries,
            expiration_filter: expiration_filter.unwrap_or_default(),
            exit_net_delta,
        };
        backtest_params
            .validate()
            .map_err(|e| format!("Validation error: {e}"))?;

        Ok((symbol, df, backtest_params))
    }

    /// Resolve a symbol from the loaded data.
    /// If `symbol` is provided, look it up explicitly.
    /// If `symbol` is None:
    ///   - If no data is loaded, return error
    ///   - If exactly one symbol is loaded, use it
    ///   - If multiple symbols are loaded, return error asking for explicit symbol
    fn resolve_symbol<'a>(
        data: &'a HashMap<String, DataFrame>,
        symbol: Option<&str>,
    ) -> Result<(&'a String, &'a DataFrame), String> {
        // Check if no data is loaded first, regardless of whether symbol was provided
        if data.is_empty() {
            return Err("No data loaded. Pass a symbol parameter (e.g. \"SPY\").".to_string());
        }

        match symbol {
            Some(sym) => {
                let sym_upper = sym.to_uppercase();
                data.get_key_value(sym_upper.as_str()).ok_or_else(|| {
                    let mut loaded: Vec<&String> = data.keys().collect();
                    loaded.sort();
                    let loaded_list = loaded
                        .iter()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("Symbol '{sym_upper}' not loaded. Loaded: {loaded_list}.")
                })
            }
            None => {
                if data.len() == 1 {
                    Ok(data
                        .iter()
                        .next()
                        .expect("data.len() == 1 but iter is empty"))
                } else {
                    let mut keys: Vec<&String> = data.keys().collect();
                    keys.sort();
                    let symbols = keys
                        .iter()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    Err(format!(
                        "Multiple symbols loaded: {symbols}. Specify the `symbol` parameter."
                    ))
                }
            }
        }
    }
}

/// Load close prices from a cached OHLCV parquet file for chart overlay.
fn load_underlying_closes(path: &std::path::Path) -> Vec<tools::response_types::UnderlyingPrice> {
    let args = ScanArgsParquet::default();
    let path_str = path.to_string_lossy();
    let Ok(lf) = LazyFrame::scan_parquet(path_str.as_ref().into(), args) else {
        return vec![];
    };
    let Ok(df) = lf
        .select([col("date"), col("close")])
        .sort(["date"], SortMultipleOptions::default())
        .collect()
    else {
        return vec![];
    };

    let Ok(dates) = df.column("date").and_then(|c| Ok(c.date()?.clone())) else {
        return vec![];
    };
    let Ok(closes) = df.column("close").and_then(|c| Ok(c.f64()?.clone())) else {
        return vec![];
    };

    let mut prices = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        if let (Some(days), Some(close)) = (dates.phys.get(i), closes.get(i)) {
            if let Some(date) =
                chrono::NaiveDate::from_num_days_from_ce_opt(days + EPOCH_DAYS_CE_OFFSET)
            {
                prices.push(tools::response_types::UnderlyingPrice {
                    date: date.format("%Y-%m-%d").to_string(),
                    close,
                });
            }
        }
    }
    prices
}

/// Validate that `end_date >= start_date` when both are present.
/// Signature uses `&Option<String>` because garde's `custom()` passes `&self.field`.
#[allow(clippy::ref_option)]
fn validate_end_date_after_start(
    start_date: &Option<String>,
) -> impl FnOnce(&Option<String>, &()) -> garde::Result + '_ {
    move |end_date: &Option<String>, (): &()| {
        if let (Some(start), Some(end)) = (start_date, end_date) {
            if end < start {
                return Err(garde::Error::new(format!(
                    "end_date ({end}) must be >= start_date ({start})"
                )));
            }
        }
        Ok(())
    }
}

/// Resolve `leg_deltas`: use provided deltas or fall back to strategy defaults.
fn resolve_leg_deltas(
    leg_deltas: Option<Vec<TargetRange>>,
    strategy_name: &str,
) -> Result<Vec<TargetRange>, String> {
    if let Some(deltas) = leg_deltas {
        Ok(deltas)
    } else {
        let strategy_def = crate::strategies::find_strategy(strategy_name)
            .ok_or_else(|| format!("Error: Unknown strategy: {strategy_name}"))?;
        Ok(strategy_def.default_deltas())
    }
}

fn default_entry_dte() -> DteRange {
    DteRange {
        target: 45,
        min: 30,
        max: 60,
    }
}

fn default_exit_dte() -> i32 {
    0
}

fn default_max_positions() -> i32 {
    1
}

fn default_quantity() -> i32 {
    1
}

fn default_capital() -> f64 {
    10000.0
}

/// Shared base parameters for all backtest-related tools (`run_backtest`, `walk_forward`,
/// `permutation_test`). Extracted to eliminate field duplication across parameter structs.
#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
pub struct BacktestBaseParams {
    /// The option strategy name (e.g. `short_put`, `iron_condor`, `short_strangle`).
    /// Call `list_strategies` to see all 32 options.
    #[garde(length(min = 1))]
    pub strategy: String,
    /// Per-leg delta targets (optional — uses strategy-specific defaults if omitted)
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub leg_deltas: Option<Vec<TargetRange>>,
    /// Entry DTE range: { target, min, max } (default: { target: 45, min: 30, max: 60 })
    #[serde(default = "default_entry_dte")]
    #[garde(dive)]
    pub entry_dte: DteRange,
    /// DTE at exit (default: 0 — hold to expiration)
    #[serde(default = "default_exit_dte")]
    #[garde(range(min = 0), custom(validate_exit_dte_lt_entry_min(&self.entry_dte)))]
    pub exit_dte: i32,
    /// Slippage model (default: Spread)
    #[serde(default)]
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
    /// Minimum bid/ask threshold — options with bid or ask at or below this value are filtered out (default: 0.05)
    #[serde(default = "default_min_bid_ask")]
    #[garde(range(min = 0.0))]
    pub min_bid_ask: f64,
    /// Stop loss threshold (multiplier of entry cost; values > 1.0 allowed)
    #[garde(inner(range(min = 0.0)))]
    pub stop_loss: Option<f64>,
    /// Take profit threshold (multiplier of entry cost; values > 1.0 allowed)
    #[garde(inner(range(min = 0.0)))]
    pub take_profit: Option<f64>,
    /// Maximum days to hold
    #[garde(inner(range(min = 1)))]
    pub max_hold_days: Option<i32>,
    /// Starting capital (default: 10000)
    #[serde(default = "default_capital")]
    #[garde(range(min = 0.01))]
    pub capital: f64,
    /// Number of contracts per trade (default: 1)
    #[serde(default = "default_quantity")]
    #[garde(range(min = 1))]
    pub quantity: i32,
    /// Contract multiplier (default: 100)
    #[serde(default = "default_multiplier")]
    #[garde(range(min = 1))]
    pub multiplier: i32,
    /// Maximum concurrent positions (default: 1)
    #[serde(default = "default_max_positions")]
    #[garde(range(min = 1))]
    pub max_positions: i32,
    /// Trade selection method
    #[garde(skip)]
    pub selector: Option<TradeSelector>,
    /// Entry signal — only open trades on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Symbol to backtest (required if multiple symbols are loaded; optional if only one is loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,

    // ── Entry filters ────────────────────────────────────────────────────────
    /// Minimum absolute net premium (debit or credit) at entry, in dollars per share.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub min_net_premium: Option<f64>,
    /// Maximum absolute net premium at entry, in dollars per share.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub max_net_premium: Option<f64>,
    /// Minimum signed net position delta at entry.
    #[serde(default)]
    #[garde(skip)]
    pub min_net_delta: Option<f64>,
    /// Maximum signed net position delta at entry.
    #[serde(default)]
    #[garde(skip)]
    pub max_net_delta: Option<f64>,
    /// Minimum calendar days between consecutive position entries (cooldown / stagger).
    #[serde(default)]
    #[garde(inner(range(min = 1)))]
    pub min_days_between_entries: Option<i32>,
    /// Filter expirations by calendar type: `Any` (default), `Weekly` (Fridays only),
    /// or `Monthly` (third Friday of the month only).
    #[serde(default)]
    #[garde(skip)]
    pub expiration_filter: Option<ExpirationFilter>,

    // ── Exit filters ─────────────────────────────────────────────────────────
    /// Exit the position when the absolute net position delta exceeds this value.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub exit_net_delta: Option<f64>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct RunBacktestParams {
    #[serde(flatten)]
    #[garde(dive)]
    pub base: BacktestBaseParams,
}

fn default_train_days() -> i32 {
    252
}

fn default_test_days() -> i32 {
    63
}

fn default_num_permutations() -> usize {
    100
}

#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
pub struct WalkForwardParams {
    #[serde(flatten)]
    #[garde(dive)]
    pub base: BacktestBaseParams,

    // ── Walk-forward specific ──────────────────────────────────────────────
    /// Training window in calendar days (default: 252, ~1 year)
    #[serde(default = "default_train_days")]
    #[garde(range(min = 1))]
    pub train_days: i32,
    /// Test window in calendar days (default: 63, ~1 quarter)
    #[serde(default = "default_test_days")]
    #[garde(range(min = 5))]
    pub test_days: i32,
    /// Step size in calendar days (default: `test_days` — non-overlapping windows).
    /// Minimum 5 days to prevent generating an excessive number of windows.
    #[garde(inner(range(min = 5)))]
    pub step_days: Option<i32>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct PermutationTestParams {
    #[serde(flatten)]
    #[garde(dive)]
    pub base: BacktestBaseParams,

    /// Number of random permutations to run (default: 100, max: 10000)
    #[serde(default = "default_num_permutations")]
    #[garde(range(min = 1, max = 10000))]
    pub num_permutations: usize,
    /// Random seed for reproducibility (optional)
    #[serde(default)]
    #[garde(skip)]
    pub seed: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema, Validate)]
pub struct ServerCompareEntry {
    /// Strategy name (e.g. `short_put`, `iron_condor`)
    #[garde(length(min = 1))]
    pub name: String,
    /// Per-leg delta targets (optional — uses strategy-specific defaults if omitted)
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub leg_deltas: Option<Vec<TargetRange>>,
    /// Entry DTE range: { target, min, max } (default: { target: 45, min: 30, max: 60 })
    #[serde(default = "default_entry_dte")]
    #[garde(dive)]
    pub entry_dte: DteRange,
    /// DTE at exit (default: 0 — hold to expiration)
    #[serde(default = "default_exit_dte")]
    #[garde(range(min = 0), custom(validate_exit_dte_lt_entry_min(&self.entry_dte)))]
    pub exit_dte: i32,
    /// Slippage model (default: Spread)
    #[serde(default)]
    #[garde(dive)]
    pub slippage: Slippage,
    /// Commission structure
    #[serde(default)]
    #[garde(dive)]
    pub commission: Option<Commission>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct CompareStrategiesParams {
    /// List of strategies with their parameters
    #[garde(length(min = 2), dive)]
    pub strategies: Vec<ServerCompareEntry>,
    /// Shared simulation parameters
    #[garde(dive)]
    pub sim_params: SimParams,
    /// Entry signal — only open trades on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[serde(default)]
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[serde(default)]
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Symbol to compare strategies on (required if multiple symbols are loaded; optional if only one is loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
}

fn validate_category_read(category: &str) -> Result<&str, String> {
    match category {
        "options" | "prices" => Ok(category),
        _ => Err(format!(
            "Invalid category: \"{category}\". Must be \"options\" or \"prices\"."
        )),
    }
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct CheckCacheParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Data category: "options" for options chain data, "prices" for OHLCV price data
    #[garde(length(min = 1))]
    pub category: String,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct ConstructSignalParams {
    /// Natural language description e.g. "RSI oversold" or "MACD bullish and above 50-day SMA"
    /// Must contain at least one non-whitespace character.
    #[garde(length(min = 1, max = 500), pattern(r"[^ \t\n\r]"))]
    pub prompt: String,
    /// Optional symbol to check if OHLCV data is cached (e.g. "SPY")
    /// If provided, response will indicate whether data is ready for signal usage
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct BuildSignalParams {
    /// Action to perform: "create", "list", "delete", "validate", or "get"
    #[garde(length(min = 1))]
    pub action: String,
    /// Signal name (required for create, delete, get)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 64), pattern(r"^[A-Za-z0-9_-]+$")))]
    pub name: Option<String>,
    /// Formula expression (required for create, validate).
    /// Uses price columns (close, open, high, low, volume) with operators and functions.
    /// Examples: "close > sma(close, 20)", "volume > sma(volume, 20) * 2.0"
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 2000)))]
    pub formula: Option<String>,
    /// Optional description of what this signal detects
    #[serde(default)]
    #[garde(inner(length(max = 500)))]
    pub description: Option<String>,
    /// Whether to persist the signal to disk (default: true for create)
    #[serde(default = "default_save")]
    #[garde(skip)]
    pub save: bool,
}

fn default_save() -> bool {
    true
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct FetchToParquetParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Time period to fetch (e.g. "6mo", "1y", "5y", "max"). Defaults to "5y".
    #[garde(inner(length(min = 1)))]
    pub period: Option<String>,
}

#[allow(clippy::unnecessary_wraps)]
fn default_price_limit() -> Option<usize> {
    Some(500)
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct GetRawPricesParams {
    /// Ticker symbol (e.g. "SPY")
    #[garde(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$"))]
    pub symbol: String,
    /// Start date filter (YYYY-MM-DD)
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")))]
    pub start_date: Option<String>,
    /// End date filter (YYYY-MM-DD)
    #[garde(inner(pattern(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")), custom(validate_end_date_after_start(&self.start_date)))]
    pub end_date: Option<String>,
    /// Maximum number of price bars to return (default: 500 if omitted).
    /// Data is evenly sampled if the total exceeds this limit.
    /// Pass `null` explicitly to disable the limit and return all bars.
    #[serde(default = "default_price_limit")]
    #[garde(skip)]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct SuggestParametersParams {
    /// Strategy name (e.g. `short_put`, `iron_condor`). Call `list_strategies` to see options.
    #[garde(length(min = 1))]
    pub strategy: String,
    /// Risk preference: "conservative" (tight filters), "moderate" (balanced), or "aggressive" (loose filters)
    #[garde(length(min = 1))]
    pub risk_preference: String,
    /// Target win rate (0.0-1.0), informational only
    #[garde(inner(range(min = 0.0, max = 1.0)))]
    pub target_win_rate: Option<f64>,
    /// Target Sharpe ratio, informational only
    #[garde(skip)]
    pub target_sharpe: Option<f64>,
    /// Symbol to analyze (required if multiple symbols are loaded; optional if only one is loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
}

fn default_sweep_max_positions() -> i32 {
    3
}

fn default_oos_pct() -> f64 {
    30.0
}

#[allow(clippy::ref_option, clippy::trivially_copy_pass_by_ref)]
fn validate_leg_delta_targets(value: &Option<Vec<Vec<f64>>>, _ctx: &()) -> garde::Result {
    let Some(targets) = value else {
        return Ok(());
    };
    for (leg_idx, leg_targets) in targets.iter().enumerate() {
        if leg_targets.is_empty() {
            return Err(garde::Error::new(format!(
                "leg {leg_idx} delta targets list must not be empty"
            )));
        }
        if leg_targets.len() > 10 {
            return Err(garde::Error::new(format!(
                "leg {leg_idx} has too many delta targets (max 10, got {})",
                leg_targets.len()
            )));
        }
        for &delta in leg_targets {
            if !delta.is_finite() || !(0.0..=1.0).contains(&delta) {
                return Err(garde::Error::new(format!(
                    "leg {leg_idx} delta target {delta} is invalid (must be a finite value in [0.0, 1.0])"
                )));
            }
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct SweepStrategyInput {
    /// Strategy name (e.g. `short_put`, `iron_condor`)
    #[garde(length(min = 1))]
    pub name: String,
    /// Per-leg delta targets to sweep. Each inner Vec is one leg's sweep values.
    /// Each delta must be in [0.0, 1.0] with at most 10 values per leg.
    /// Omit to use strategy defaults (no delta sweep).
    #[serde(default)]
    #[garde(custom(validate_leg_delta_targets))]
    pub leg_delta_targets: Option<Vec<Vec<f64>>>,
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct SweepDimensionsInput {
    /// Entry DTE targets to sweep (e.g. [30, 45, 60])
    #[garde(length(min = 1), inner(range(min = 1)))]
    pub entry_dte_targets: Vec<i32>,
    /// Exit DTE values to sweep (e.g. [0, 5, 10])
    #[garde(length(min = 1), inner(range(min = 0)))]
    pub exit_dtes: Vec<i32>,
    /// Slippage models to sweep (default: [Spread])
    #[serde(default = "default_sweep_slippage")]
    #[garde(length(min = 1), dive)]
    pub slippage_models: Vec<Slippage>,
}

fn default_sweep_slippage() -> Vec<Slippage> {
    vec![Slippage::Spread]
}

#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct ParameterSweepParams {
    /// Strategies to sweep (optional if `direction` is provided)
    #[serde(default)]
    #[garde(dive)]
    pub strategies: Option<Vec<SweepStrategyInput>>,
    /// Sweep dimensions: DTE targets, exit DTEs, slippage models
    #[garde(dive)]
    pub sweep: SweepDimensionsInput,
    /// Shared simulation parameters
    #[garde(dive)]
    pub sim_params: SweepSimParams,
    /// Out-of-sample percentage [0, 100). Set to 0 to disable OOS validation. Default: 30.
    #[serde(default = "default_oos_pct")]
    #[garde(range(min = 0.0, max = 99.99))]
    pub out_of_sample_pct: f64,
    /// Filter strategies by market direction (bullish, bearish, neutral, volatile).
    /// If both `strategies` and `direction` provided, filters the list.
    /// If only `direction`, auto-selects matching strategies.
    #[serde(default)]
    #[garde(skip)]
    pub direction: Option<Direction>,
    /// Symbol to sweep (required if multiple symbols loaded)
    #[serde(default)]
    #[garde(inner(length(min = 1, max = 10), pattern(r"^[A-Za-z0-9._-]+$")))]
    pub symbol: Option<String>,
    /// Number of permutations to run per combination to compute Sharpe p-values.
    /// When set (e.g. 100), Bonferroni and BH-FDR multiple comparisons corrections are
    /// applied automatically and included in the response. Omit to skip (default).
    /// Note: each permutation adds one extra backtest per combination.
    #[serde(default)]
    #[garde(inner(range(min = 10, max = 1000)))]
    pub num_permutations: Option<usize>,
    /// Optional RNG seed for reproducible permutation tests.
    /// This value is only used when `num_permutations` is provided; otherwise it is ignored.
    #[serde(default)]
    #[garde(skip)]
    pub permutation_seed: Option<u64>,
}

/// `SimParams` variant with sweep-friendly defaults (`max_positions=3`)
#[derive(Debug, Deserialize, JsonSchema, Validate)]
pub struct SweepSimParams {
    /// Starting capital (default: 10000)
    #[serde(default = "default_capital")]
    #[garde(range(min = 0.01))]
    pub capital: f64,
    /// Contracts per trade (default: 1)
    #[serde(default = "default_quantity")]
    #[garde(range(min = 1))]
    pub quantity: i32,
    /// Contract multiplier (default: 100)
    #[serde(default = "default_multiplier")]
    #[garde(range(min = 1))]
    pub multiplier: i32,
    /// Max concurrent positions (default: 3)
    #[serde(default = "default_sweep_max_positions")]
    #[garde(range(min = 1))]
    pub max_positions: i32,
    /// Trade selector
    #[serde(default)]
    #[garde(skip)]
    pub selector: TradeSelector,
    /// Stop loss threshold
    #[garde(inner(range(min = 0.0)))]
    pub stop_loss: Option<f64>,
    /// Take profit threshold
    #[garde(inner(range(min = 0.0)))]
    pub take_profit: Option<f64>,
    /// Max hold days
    #[garde(inner(range(min = 1)))]
    pub max_hold_days: Option<i32>,
    /// Entry signal — only open trades on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[serde(default)]
    #[garde(skip)]
    pub entry_signal: Option<SignalSpec>,
    /// Exit signal — close open positions on dates where this TA signal fires.
    /// Requires OHLCV data (call `fetch_to_parquet` first).
    #[serde(default)]
    #[garde(skip)]
    pub exit_signal: Option<SignalSpec>,
    /// Entry signal variants to sweep (cartesian product with other dimensions).
    /// Cannot be used together with `entry_signal` (singular).
    /// Each element is a complete `SignalSpec`. Empty list (default) = no signal sweep.
    #[serde(default)]
    #[garde(skip)]
    pub entry_signals: Vec<SignalSpec>,
    /// Exit signal variants to sweep (cartesian product with other dimensions).
    /// Cannot be used together with `exit_signal` (singular).
    /// Each element is a complete `SignalSpec`. Empty list (default) = no signal sweep.
    #[serde(default)]
    #[garde(skip)]
    pub exit_signals: Vec<SignalSpec>,
    /// Minimum calendar days between consecutive position entries (cooldown / stagger).
    #[serde(default)]
    #[garde(inner(range(min = 1)))]
    pub min_days_between_entries: Option<i32>,
    /// Exit when absolute net position delta exceeds this value.
    #[serde(default)]
    #[garde(inner(range(min = 0.0)))]
    pub exit_net_delta: Option<f64>,
}

/// Resolve sweep strategies from input params.
/// If both strategies and direction provided, filter list by direction.
/// If only direction, auto-select matching strategies.
/// If only strategies, use as-is.
/// If neither, error.
fn resolve_sweep_strategies(
    strategies: Option<Vec<SweepStrategyInput>>,
    direction: Option<Direction>,
) -> Result<Vec<crate::engine::sweep::SweepStrategyEntry>, String> {
    match (strategies, direction) {
        (Some(strats), Some(dir)) => {
            // Build a name→direction lookup from the cached registry (one pass, no fresh allocation).
            let dir_map: std::collections::HashMap<&str, Direction> =
                crate::strategies::all_strategies()
                    .iter()
                    .map(|s| (s.name.as_str(), s.direction))
                    .collect();
            let filtered: Vec<_> = strats
                .into_iter()
                .filter(|s| dir_map.get(s.name.as_str()).copied() == Some(dir))
                .collect();
            if filtered.is_empty() {
                return Err(format!(
                    "No provided strategies match direction {dir:?}. Remove the direction filter or add matching strategies.",
                ));
            }
            resolve_strategy_entries(filtered)
        }
        (Some(strats), None) => {
            if strats.is_empty() {
                return Err("`strategies` list must not be empty. Provide at least one strategy or use `direction` to auto-select.".to_string());
            }
            resolve_strategy_entries(strats)
        }
        (None, Some(dir)) => {
            // Auto-select all strategies matching direction.
            // StrategyDef already carries a precomputed `direction` field, so
            // we read it directly instead of calling `strategy_direction` (which
            // would redundantly rebuild all strategies via `find_strategy`).
            let matching: Vec<_> = crate::strategies::all_strategies()
                .iter()
                .filter(|s| s.direction == dir)
                .map(|s| SweepStrategyInput {
                    name: s.name.clone(),
                    leg_delta_targets: None,
                })
                .collect();
            if matching.is_empty() {
                return Err(format!("No strategies match direction {dir:?}.",));
            }
            resolve_strategy_entries(matching)
        }
        (None, None) => Err("Either `strategies` or `direction` must be provided. \
             Use `direction` to auto-select strategies by market outlook, \
             or provide explicit `strategies` list."
            .to_string()),
    }
}

fn resolve_strategy_entries(
    strats: Vec<SweepStrategyInput>,
) -> Result<Vec<crate::engine::sweep::SweepStrategyEntry>, String> {
    strats
        .into_iter()
        .map(|s| {
            let name = s.name;
            let strategy_def = crate::strategies::find_strategy(&name)
                .ok_or_else(|| format!("Unknown strategy: {name}"))?;

            let leg_delta_targets = if let Some(targets) = s.leg_delta_targets {
                // Validate that the number of legs matches the strategy definition.
                if targets.len() != strategy_def.legs.len() {
                    return Err(format!(
                        "Strategy '{}' expects {} leg(s) but {} leg delta target set(s) were provided",
                        name,
                        strategy_def.legs.len(),
                        targets.len()
                    ));
                }
                // Validate that each leg's sweep list is non-empty.
                for (idx, leg_targets) in targets.iter().enumerate() {
                    if leg_targets.is_empty() {
                        return Err(format!(
                            "Strategy '{name}' leg {idx} has an empty delta target list; each leg must have at least one target",
                        ));
                    }
                }
                targets
            } else {
                // Use strategy defaults — single value per leg
                strategy_def
                    .default_deltas()
                    .iter()
                    .map(|d| vec![d.target])
                    .collect()
            };
            Ok(crate::engine::sweep::SweepStrategyEntry {
                name,
                leg_delta_targets,
            })
        })
        .collect()
}

use rmcp::handler::server::wrapper::Parameters;

#[tool_router]
impl OptopsyServer {
    /// Browse all 32 built-in options strategies grouped by category.
    ///
    /// **When to use**: To choose a strategy for analysis
    /// **Prerequisites**: None (informational, no data required)
    /// **Categories**: singles, spreads, straddles, strangles, butterflies, condors, iron, calendars, diagonals
    /// **Next tools**: `suggest_parameters()` or `run_backtest()` (once you pick a strategy)
    #[tool(name = "list_strategies", annotations(read_only_hint = true))]
    async fn list_strategies(&self) -> SanitizedJson<StrategiesResponse> {
        SanitizedJson(tools::strategies::execute())
    }

    /// Browse all 40+ available technical analysis (TA) signals for entry/exit filtering.
    ///
    /// **When to use**: To understand available signal options for entry/exit filtering
    /// **Prerequisites**: None (informational, no data required)
    /// **Categories**: momentum (RSI, MACD, Stoch), trend (SMA, EMA, ADX),
    ///   volatility (`BBands`, `ATR`), overlap, price, volume
    /// **Next tool**: `construct_signal()` (if you want to use signals in backtest)
    /// **Note**: Signals are optional — only needed if you want signal-filtered entry/exit
    #[tool(name = "list_signals", annotations(read_only_hint = true))]
    async fn list_signals(&self) -> SanitizedJson<SignalsResponse> {
        SanitizedJson(tools::signals::execute())
    }

    /// Get status of currently loaded data.
    ///
    /// **When to use**: Check what symbol is currently loaded, row count, available columns
    /// **Prerequisites**: None (works with or without loaded data)
    /// **How it works**: Returns details about the in-memory `DataFrame` (symbol, rows, columns)
    /// **Next tool**: Proceed with `suggest_parameters()` or `run_backtest()`
    /// **Example usage**: After loading SPY, call this to confirm it's loaded and see column names
    #[tool(name = "get_loaded_symbol", annotations(read_only_hint = true))]
    async fn get_loaded_symbol(&self) -> SanitizedJson<StatusResponse> {
        SanitizedJson(tools::status::execute(&self.data).await)
    }

    /// Construct a signal specification from natural language.
    ///
    /// **When to use**: If you want to filter backtests by TA signals (e.g., "RSI oversold")
    /// **Prerequisites**: None (OHLCV data is auto-fetched when signals are used in `run_backtest`)
    /// **How it works**:
    ///   - Fuzzy-searches signal catalog for matches
    ///   - Returns candidate signals with sensible defaults
    ///   - Generates live JSON schema for all signal variants
    /// **Next tool**: `run_backtest()` with `entry_signal`/`exit_signal` parameters set to
    ///   the JSON spec from this tool's response
    /// **Example usage**: "RSI oversold" → returns RSI signal spec with threshold=30
    /// **Note**: Signals are optional; `run_backtest` works without them
    #[tool(name = "construct_signal", annotations(read_only_hint = true))]
    async fn construct_signal(
        &self,
        Parameters(params): Parameters<ConstructSignalParams>,
    ) -> SanitizedResult<ConstructSignalResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("construct_signal", e))?;
                Ok(tools::construct_signal::execute(&params.prompt))
            }
            .await,
        )
    }

    /// Build, validate, save, list, and manage custom formula-based signals.
    ///
    /// **When to use**: When built-in signals don't cover your needs and you want to
    ///   define custom entry/exit conditions using price column formulas
    /// **Prerequisites**: None (formulas are validated at parse time, data needed only at backtest)
    ///
    /// **Actions**:
    ///   - `create` — Build a signal from a formula, optionally save for later use
    ///   - `validate` — Check formula syntax without saving
    ///   - `list` — Show all saved custom signals
    ///   - `get` — Load a saved signal's spec
    ///   - `delete` — Remove a saved signal
    ///
    /// **Formula syntax**:
    ///   - Columns: `close`, `open`, `high`, `low`, `volume`, `adjclose`
    ///   - Lookback: `close[1]` (previous bar), `close[5]` (5 bars ago)
    ///   - Functions: `sma(col, N)`, `ema(col, N)`, `std(col, N)`, `max(col, N)`,
    ///     `min(col, N)`, `abs(expr)`, `change(col, N)`, `pct_change(col, N)`
    ///   - Operators: `+`, `-`, `*`, `/`, `>`, `<`, `>=`, `<=`, `==`, `!=`
    ///   - Logical: `and`, `or`, `not`
    ///
    /// **Examples**: `"close > sma(close, 20)"`, `"volume > sma(volume, 20) * 2.0"`,
    ///   `"close > close[1] * 1.02"`, `"pct_change(close, 1) > 0.03"`
    ///
    /// **Next tool**: `run_backtest()` with `entry_signal`/`exit_signal` set to the returned spec,
    ///   or use `{ "type": "Saved", "name": "signal_name" }` to reference saved signals
    #[tool(
        name = "build_signal",
        annotations(
            destructive_hint = true,
            idempotent_hint = false,
            read_only_hint = false
        )
    )]
    async fn build_signal(
        &self,
        Parameters(params): Parameters<BuildSignalParams>,
    ) -> SanitizedResult<BuildSignalResponse, String> {
        SanitizedResult(async {
            params
                .validate()
                .map_err(|e| validation_err("build_signal", e))?;

            let action = match params.action.as_str() {
                "create" => {
                    let name = params
                        .name
                        .ok_or("'name' is required for action='create'")?;
                    let formula = params
                        .formula
                        .ok_or("'formula' is required for action='create'")?;
                    tools::build_signal::Action::Create {
                        name,
                        formula,
                        description: params.description,
                        save: params.save,
                    }
                }
                "list" => tools::build_signal::Action::List,
                "delete" => {
                    let name = params
                        .name
                        .ok_or("'name' is required for action='delete'")?;
                    tools::build_signal::Action::Delete { name }
                }
                "validate" => {
                    let formula = params
                        .formula
                        .ok_or("'formula' is required for action='validate'")?;
                    tools::build_signal::Action::Validate { formula }
                }
                "get" => {
                    let name = params.name.ok_or("'name' is required for action='get'")?;
                    tools::build_signal::Action::Get { name }
                }
                other => {
                    return Err(format!(
                        "Invalid action: \"{other}\". Must be \"create\", \"list\", \"delete\", \"validate\", or \"get\"."
                    ));
                }
            };

            Ok(tools::build_signal::execute(action))
        }.await)
    }

    /// Full event-driven day-by-day simulation with position management and metrics.
    ///
    /// **When to use**: Run a full capital-constrained backtest simulation
    /// **Prerequisites**: Data is auto-loaded from cache when you pass a symbol.
    ///   OHLCV data is auto-fetched when signals are used.
    /// **Next tools**: `compare_strategies()` (to test variations) or iterate on parameters
    ///
    /// **IMPORTANT**: `strategy` is REQUIRED — it defines WHAT option legs to trade.
    /// Signals only FILTER WHEN to enter/exit — they are optional add-ons.
    ///
    /// **What it simulates**:
    ///   - Day-by-day position opens (respecting `max_positions` constraint)
    ///   - Position management (stop loss, take profit, max hold days, DTE exit)
    ///   - Optional signal-based filtering (if `entry_signal`/`exit_signal` provided)
    ///   - Realistic P&L with bid/ask slippage and commissions
    /// **Output**:
    ///   - Trade log (every open/close with P&L and exit reason)
    ///   - Equity curve (daily capital evolution)
    ///   - Performance metrics (Sharpe, Sortino, Calmar, `VaR`, max drawdown, win rate, etc.)
    ///   - AI-enriched assessment and suggested next steps
    /// **Time to run**: 5-30 seconds depending on data size
    #[tool(name = "run_backtest", annotations(read_only_hint = true))]
    async fn run_backtest(
        &self,
        Parameters(params): Parameters<RunBacktestParams>,
    ) -> SanitizedResult<BacktestResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("run_backtest", e))?;

                tracing::info!(
                    strategy = params.base.strategy.as_str(),
                    symbol = params.base.symbol.as_deref().unwrap_or("auto"),
                    entry_dte_target = params.base.entry_dte.target,
                    entry_dte_min = params.base.entry_dte.min,
                    entry_dte_max = params.base.entry_dte.max,
                    exit_dte = params.base.exit_dte,
                    max_positions = params.base.max_positions,
                    capital = params.base.capital,
                    "Backtest request received"
                );

                let (symbol, df, backtest_params) =
                    self.resolve_backtest_params(params.base).await?;

                // Try to load underlying OHLCV close prices from cache for chart overlay
                let underlying_prices = match self.cache.ensure_local_for(&symbol, "prices").await {
                    Ok(path) => {
                        // Read on blocking thread since it's Polars I/O
                        let prices = tokio::task::spawn_blocking(
                            move || -> Vec<tools::response_types::UnderlyingPrice> {
                                load_underlying_closes(&path)
                            },
                        )
                        .await
                        .unwrap_or_default();
                        prices
                    }
                    Err(_) => vec![],
                };

                // Run backtest on a blocking thread — the engine performs synchronous
                // Polars I/O (scan_parquet) which conflicts with the tokio runtime.
                tokio::task::spawn_blocking(move || {
                    tools::backtest::execute(&df, &backtest_params, underlying_prices)
                })
                .await
                .map_err(|e| format!("Backtest task panicked: {e}"))?
                .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Permutation test for statistical significance of backtest results.
    ///
    /// Shuffles entry candidates across dates N times, re-runs the backtest, and compares
    /// real results against the random distribution. Produces p-values for key metrics
    /// (Sharpe, `PnL`, win rate, profit factor, CAGR).
    ///
    /// **Null hypothesis**: "the specific timing of entries doesn't matter."
    /// If p < 0.05, the strategy has a statistically significant edge.
    ///
    /// **Time to run**: scales linearly with `num_permutations` × single backtest time
    #[tool(name = "permutation_test", annotations(read_only_hint = true))]
    async fn permutation_test(
        &self,
        Parameters(params): Parameters<PermutationTestParams>,
    ) -> SanitizedResult<PermutationTestResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("permutation_test", e))?;

                tracing::info!(
                    strategy = params.base.strategy.as_str(),
                    symbol = params.base.symbol.as_deref().unwrap_or("auto"),
                    num_permutations = params.num_permutations,
                    "Permutation test request received"
                );

                let (_symbol, df, backtest_params) =
                    self.resolve_backtest_params(params.base).await?;

                let perm_params = crate::engine::permutation::PermutationParams {
                    num_permutations: params.num_permutations,
                    seed: params.seed,
                };

                tokio::task::spawn_blocking(move || {
                    let (entry_dates, exit_dates) =
                        crate::engine::core::build_signal_filters(&backtest_params, &df)?;
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
                .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Sweep parameter combinations across strategies, DTE, exit DTE, and slippage.
    ///
    /// **When to use**: To find optimal parameter combinations without manually building
    ///   `compare_strategies` entries. Generates cartesian product internally and ranks by Sharpe.
    /// **Prerequisites**: None — data is auto-loaded from cache when you pass a symbol.
    ///
    /// **How it works**:
    ///   1. Generates cartesian product of delta targets × DTE targets × exit DTEs × slippage models × signal variants
    ///   2. Filters invalid combos (`exit_dte` >= entry DTE min, inverted delta orderings)
    ///   3. Deduplicates identical combinations
    ///   4. Runs backtest on each combo (hard cap: 100 combinations)
    ///   5. Ranks by Sharpe ratio, computes dimension sensitivity
    ///   6. Optionally validates top 3 on out-of-sample data (default: 30% holdout)
    ///
    /// **Modes**:
    ///   - Provide `strategies` list: sweep specific strategies with custom delta grids
    ///   - Provide `direction` only: auto-select all matching strategies (bullish/bearish/neutral/volatile)
    ///   - Both: filter provided list by direction
    ///
    /// **Output**: Ranked results, dimension sensitivity analysis, OOS validation
    #[tool(name = "parameter_sweep", annotations(read_only_hint = true))]
    async fn parameter_sweep(
        &self,
        Parameters(params): Parameters<ParameterSweepParams>,
    ) -> SanitizedResult<SweepResponse, String> {
        SanitizedResult(async {
            params
                .validate()
                .map_err(|e| validation_err("parameter_sweep", e))?;

            // Validate: singular and plural signal fields are mutually exclusive
            if params.sim_params.entry_signal.is_some()
                && !params.sim_params.entry_signals.is_empty()
            {
                return Err(
                    "Cannot use both `entry_signal` (singular) and `entry_signals` (plural). \
                     Use `entry_signals` for sweeping multiple signals, or `entry_signal` for a fixed signal."
                        .to_string(),
                );
            }
            if params.sim_params.exit_signal.is_some()
                && !params.sim_params.exit_signals.is_empty()
            {
                return Err(
                    "Cannot use both `exit_signal` (singular) and `exit_signals` (plural). \
                     Use `exit_signals` for sweeping multiple signals, or `exit_signal` for a fixed signal."
                        .to_string(),
                );
            }

            let (symbol, df) = self.ensure_data_loaded(params.symbol.as_deref()).await?;

            // Auto-fetch OHLCV data if any signals are requested
            let needs_ohlcv = params.sim_params.entry_signal.is_some()
                || params.sim_params.exit_signal.is_some()
                || !params.sim_params.entry_signals.is_empty()
                || !params.sim_params.exit_signals.is_empty();
            let ohlcv_path = if needs_ohlcv {
                Some(self.ensure_ohlcv(&symbol).await?)
            } else {
                None
            };

            let cross_ohlcv_paths = self
                .resolve_cross_ohlcv_paths(
                    params.sim_params.entry_signal.as_ref(),
                    params.sim_params.exit_signal.as_ref(),
                    &params.sim_params.entry_signals,
                    &params.sim_params.exit_signals,
                )
                .await?;

            let strategies = resolve_sweep_strategies(params.strategies, params.direction)?;

            let sweep_params = crate::engine::sweep::SweepParams {
                strategies,
                sweep: crate::engine::sweep::SweepDimensions {
                    entry_dte_targets: params.sweep.entry_dte_targets,
                    exit_dtes: params.sweep.exit_dtes,
                    slippage_models: params.sweep.slippage_models,
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
                    entry_signal: params.sim_params.entry_signal,
                    exit_signal: params.sim_params.exit_signal,
                    ohlcv_path,
                    cross_ohlcv_paths,
                    min_days_between_entries: params.sim_params.min_days_between_entries,
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
                .map_err(|e| format!("Error: {e}"))
        }.await)
    }

    /// Rolling walk-forward validation: train on window 1, test on window 2, slide forward, repeat.
    ///
    /// **When to use**: After finding promising parameters via `run_backtest` or `parameter_sweep`,
    ///   validate that the strategy performs consistently across multiple time periods
    /// **Prerequisites**: None — data is auto-loaded from cache when you pass a symbol
    ///
    /// **How it works**:
    ///   1. Slides rolling train/test windows across the full date range
    ///   2. For each window: runs backtest on train slice, then on test slice
    ///   3. Collects per-window train/test metrics (Sharpe, P&L, trades, win rate)
    ///   4. Computes aggregate statistics: avg test Sharpe, % profitable windows, Sharpe decay
    ///
    /// **Key metrics**:
    ///   - `avg_train_test_sharpe_decay`: high values (>0.5) indicate overfitting
    ///   - `pct_profitable_windows`: % of test windows with positive P&L
    ///   - `std_test_sharpe`: lower = more consistent performance
    ///
    /// **Time to run**: Proportional to number of windows × backtest time per window
    #[tool(name = "walk_forward", annotations(read_only_hint = true))]
    async fn walk_forward(
        &self,
        Parameters(params): Parameters<WalkForwardParams>,
    ) -> SanitizedResult<WalkForwardResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("walk_forward", e))?;

                tracing::info!(
                    strategy = params.base.strategy.as_str(),
                    symbol = params.base.symbol.as_deref().unwrap_or("auto"),
                    train_days = params.train_days,
                    test_days = params.test_days,
                    step_days = ?params.step_days,
                    "Walk-forward request received"
                );

                let (_symbol, df, backtest_params) =
                    self.resolve_backtest_params(params.base).await?;

                let train_days = params.train_days;
                let test_days = params.test_days;
                let step_days = params.step_days;

                tokio::task::spawn_blocking(move || {
                    tools::walk_forward::execute(
                        &df,
                        &backtest_params,
                        train_days,
                        test_days,
                        step_days,
                    )
                })
                .await
                .map_err(|e| format!("Walk-forward task panicked: {e}"))?
                .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Run multiple strategies in parallel and rank by performance metrics.
    ///
    /// **When to use**: After validating one strategy via `run_backtest()`, to test
    ///   parameter variations and find the best-performing approach
    /// **Prerequisites**: None — data is auto-loaded from cache when you pass a symbol
    /// **Why use this**: Compare different delta targets, DTE parameters, or strategies
    ///   side-by-side in a single call (faster than running multiple backtests)
    /// **Next tools**: pick best performer and iterate further, or conclude analysis
    ///
    /// **Modes**:
    ///   - Compare DTE/delta variations of same strategy
    ///   - Compare different strategies with same parameters
    ///   - Compare hybrid parameter sets
    /// **Rankings**: By Sharpe ratio (primary) and total `PnL` (secondary)
    /// **Output**: Metrics for each strategy + recommended best performer
    #[tool(name = "compare_strategies", annotations(read_only_hint = true))]
    async fn compare_strategies(
        &self,
        Parameters(params): Parameters<CompareStrategiesParams>,
    ) -> SanitizedResult<CompareResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("compare_strategies", e))?;

                let (symbol, df) = self.ensure_data_loaded(params.symbol.as_deref()).await?;

                // Auto-fetch OHLCV data if signals are requested
                let ohlcv_path = if params.entry_signal.is_some() || params.exit_signal.is_some() {
                    Some(self.ensure_ohlcv(&symbol).await?)
                } else {
                    None
                };

                let cross_ohlcv_paths = self
                    .resolve_cross_ohlcv_paths(
                        params.entry_signal.as_ref(),
                        params.exit_signal.as_ref(),
                        &[],
                        &[],
                    )
                    .await?;

                let mut sim_params = params.sim_params;
                sim_params.entry_signal = params.entry_signal;
                sim_params.exit_signal = params.exit_signal;
                sim_params.ohlcv_path = ohlcv_path;
                sim_params.cross_ohlcv_paths = cross_ohlcv_paths;

                let compare_params = CompareParams {
                    strategies: params
                        .strategies
                        .into_iter()
                        .map(|s| {
                            let leg_deltas = resolve_leg_deltas(s.leg_deltas, &s.name)?;
                            Ok(CompareEntry {
                                name: s.name,
                                leg_deltas,
                                entry_dte: s.entry_dte,
                                exit_dte: s.exit_dte,
                                slippage: s.slippage,
                                commission: s.commission,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                    sim_params,
                };
                compare_params
                    .validate()
                    .map_err(|e| validation_err("compare_strategies", e))?;

                tokio::task::spawn_blocking(move || tools::compare::execute(&df, &compare_params))
                    .await
                    .map_err(|e| format!("Compare task panicked: {e}"))?
                    .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Check if cached Parquet data exists and when it was last updated.
    ///
    /// **When to use**: To avoid redundant downloads or to verify data staleness
    /// **Prerequisites**: None
    ///
    /// **Returns**:
    ///   - Cache exists (boolean)
    ///   - File path (if exists)
    ///   - File size and row count
    ///   - Last update timestamp
    #[tool(name = "check_cache_status", annotations(read_only_hint = true))]
    async fn check_cache_status(
        &self,
        Parameters(params): Parameters<CheckCacheParams>,
    ) -> SanitizedResult<CheckCacheResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("check_cache_status", e))?;
                let category = validate_category_read(&params.category)?;
                tools::cache_status::execute(&self.cache, &params.symbol, category)
                    .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Download OHLCV price data from Yahoo Finance and cache locally as Parquet.
    ///
    /// **When to use**: To pre-download OHLCV data, or when you need price data
    ///   independently of backtesting (e.g., for charting)
    /// **Prerequisites**: None
    /// **Note**: OHLCV data is auto-fetched when signals are used in `run_backtest`,
    ///   so this tool is only needed for explicit pre-caching or standalone use.
    /// **Periods**: "5y" (default), "6mo", "1y", "max"
    #[tool(
        name = "fetch_to_parquet",
        annotations(
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn fetch_to_parquet(
        &self,
        Parameters(params): Parameters<FetchToParquetParams>,
    ) -> SanitizedResult<FetchResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("fetch_to_parquet", e))?;
                let period = params.period.as_deref().unwrap_or("5y");
                tools::fetch::execute(&self.cache, &params.symbol, period)
                    .await
                    .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Return raw OHLCV price data for a symbol, ready for chart generation.
    ///
    /// **When to use**: When an LLM or user needs raw price data to generate charts
    ///   (candlestick, line, area) or perform custom analysis
    /// **Prerequisites**: `fetch_to_parquet()` must have been called first to cache OHLCV data
    ///
    /// **Returns**: Array of `{ date, open, high, low, close, adjclose, volume }` bars.
    /// Data is evenly sampled down to `limit` points (default 500 if omitted) to avoid
    /// overwhelming LLM context windows. Pass `limit: null` explicitly for the full dataset.
    ///
    /// **Use cases**:
    ///   - Generate candlestick or OHLC charts
    ///   - Plot price action with close/adjclose line charts
    ///   - Overlay backtest equity curves on underlying price data
    ///   - Feed into code interpreters for custom analysis
    #[tool(name = "get_raw_prices", annotations(read_only_hint = true))]
    async fn get_raw_prices(
        &self,
        Parameters(params): Parameters<GetRawPricesParams>,
    ) -> SanitizedResult<RawPricesResponse, String> {
        SanitizedResult(
            async {
                params
                    .validate()
                    .map_err(|e| validation_err("get_raw_prices", e))?;
                tools::raw_prices::load_and_execute(
                    &self.cache,
                    &params.symbol,
                    params.start_date.as_deref(),
                    params.end_date.as_deref(),
                    params.limit,
                )
                .await
                .map_err(|e| format!("Error: {e}"))
            }
            .await,
        )
    }

    /// Analyze the loaded options chain and suggest data-driven parameters.
    ///
    /// **When to use**: To get intelligent parameter suggestions
    ///   based on actual market data (DTE coverage, spread quality, delta distribution)
    /// **Prerequisites**: None — data is auto-loaded from cache when you pass a symbol
    /// **Next tools**: `run_backtest()` with suggested parameters
    ///
    /// **What it analyzes**:
    ///   - DTE distribution and contiguous coverage zones
    ///   - Bid/ask spread quality per DTE bucket
    ///   - Delta distribution per leg (quartile-based targeting)
    ///   - Suggested `exit_dte` based on data coverage
    /// **Risk preferences**: Conservative (tight filters), Moderate (balanced), Aggressive (loose)
    /// **Output**:
    ///   - `leg_deltas` array (optimized delta targets/ranges per leg)
    ///   - `entry_dte` (target/min/max entry DTE range from data)
    ///   - `exit_dte` (recommended exit DTE)
    ///   - slippage model recommendation (Mid/Spread/Liquidity)
    ///   - Confidence score (combines data coverage and calendar quality)
    /// **Saves time**: No need to guess parameters; use market-driven recommendations
    #[tool(name = "suggest_parameters", annotations(read_only_hint = true))]
    async fn suggest_parameters(
        &self,
        Parameters(params): Parameters<SuggestParametersParams>,
    ) -> SanitizedResult<SuggestResponse, String> {
        SanitizedResult(async {
            params
                .validate()
                .map_err(|e| validation_err("suggest_parameters", e))?;

            let strategy = params.strategy;

            let risk_pref = match params.risk_preference.as_str() {
                "conservative" => crate::engine::suggest::RiskPreference::Conservative,
                "moderate" => crate::engine::suggest::RiskPreference::Moderate,
                "aggressive" => crate::engine::suggest::RiskPreference::Aggressive,
                other => {
                    return Err(format!(
                        "Invalid risk_preference: \"{other}\". Must be \"conservative\", \"moderate\", or \"aggressive\"."
                    ));
                }
            };

            let suggest_params = crate::engine::suggest::SuggestParams {
                strategy: strategy.clone(),
                risk_preference: risk_pref,
                target_win_rate: params.target_win_rate,
                target_sharpe: params.target_sharpe,
            };

            let (_, df) = self.ensure_data_loaded(params.symbol.as_deref()).await?;

            tokio::task::spawn_blocking(move || tools::suggest::execute(&df, &suggest_params))
                .await
                .map_err(|e| format!("Suggest task panicked: {e}"))?
                .map_err(|e| format!("Error: {e}"))
        }.await)
    }
}

#[tool_handler]
impl ServerHandler for OptopsyServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: rmcp::model::ProtocolVersion::default(),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "optopsy-mcp".into(),
                title: Some("Optopsy Options Backtesting Engine".into()),
                version: env!("CARGO_PKG_VERSION").into(),
                description: Some("Event-driven options backtesting engine with 32 strategies, realistic position management, and AI-compatible analysis tools".into()),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Options backtesting engine. Data is auto-loaded when you call any analysis tool — \
                just pass the symbol parameter.\
                \n\n## WORKFLOW\
                \n\
                \n### 1. Explore Strategies\
                \n  - list_strategies() — browse all 32 strategies by category\
                \n  - list_signals() / construct_signal() / build_signal() — optional: TA signal filters\
                \n\
                \n### 2. Get Parameters (recommended)\
                \n  - suggest_parameters({ strategy, symbol }) — data-driven recommendations\
                \n\
                \n### 3. Full Simulation\
                \n  - run_backtest({ strategy, symbol, ... }) — event-driven backtest with trade log and metrics\
                \n  - OHLCV data is auto-fetched when signals are used\
                \n\
                \n### 4. Compare & Optimize (optional)\
                \n  - parameter_sweep — PREFERRED for optimization. Generates cartesian product of delta/DTE/slippage combos automatically.\
                \n    Use `direction` to auto-select strategies by market outlook (bullish/bearish/neutral/volatile),\
                \n    or provide explicit `strategies` list with `leg_delta_targets` grids.\
                \n    Includes out-of-sample validation (default 30%) and dimension sensitivity analysis.\
                \n  - compare_strategies — use for manual side-by-side comparison of 2-3 specific configurations\
                \n    you've already chosen. NOT for grid search (use parameter_sweep instead).\
                \n\
                \n## RULES\
                \n- strategy is ALWAYS REQUIRED for backtest/suggest — signals do NOT replace strategies\
                \n- Signals only filter WHEN to trade; the strategy defines WHAT option legs to trade\
                \n- NEVER pass strategy: null — pick one like short_put, iron_condor, etc.\
                \n- For optimization, prefer parameter_sweep over manually enumerating compare_strategies entries\
                \n- Each tool response includes suggested_next_steps — follow them"
                    .into(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Serialize)]
    struct TestStruct {
        normal: f64,
        nan: f64,
        inf: f64,
        neg_inf: f64,
        nested: Vec<f64>,
    }

    #[test]
    fn serialize_finite_replaces_nan_with_zero() {
        let val = TestStruct {
            normal: 1.5,
            nan: f64::NAN,
            inf: f64::INFINITY,
            neg_inf: f64::NEG_INFINITY,
            nested: vec![1.0, f64::NAN, f64::INFINITY],
        };
        let result = serialize_finite(&val).expect("should not fail on NaN/Inf");
        assert_eq!(result["normal"], 1.5);
        assert_eq!(result["nan"], 0.0);
        assert_eq!(result["inf"], 0.0);
        assert_eq!(result["neg_inf"], 0.0);
        assert_eq!(result["nested"][0], 1.0);
        assert_eq!(result["nested"][1], 0.0);
        assert_eq!(result["nested"][2], 0.0);
    }

    #[test]
    fn serialize_finite_preserves_normal_values() {
        let val = TestStruct {
            normal: 42.5,
            nan: 0.0,
            inf: -100.0,
            neg_inf: 99.9,
            nested: vec![1.0, 2.0, 3.0],
        };
        let result = serialize_finite(&val).expect("should succeed");
        assert_eq!(result["normal"], 42.5);
        assert_eq!(result["nan"], 0.0);
        assert_eq!(result["inf"], -100.0);
        assert_eq!(result["neg_inf"], 99.9);
    }

    #[test]
    fn serialize_finite_handles_option_f64() {
        #[derive(serde::Serialize)]
        struct WithOption {
            value: Option<f64>,
            none_value: Option<f64>,
        }
        let val = WithOption {
            value: Some(f64::NAN),
            none_value: None,
        };
        let result = serialize_finite(&val).expect("should not fail");
        assert_eq!(result["value"], 0.0);
        assert!(result["none_value"].is_null());
    }
}
