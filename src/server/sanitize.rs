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
