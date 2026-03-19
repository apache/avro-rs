// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Logic handling the intermediate representation of Avro values.
use crate::schema::{InnerDecimalSchema, UuidSchema, ResolvedNode,
    ResolvedRecord, ResolvedMap, ResolvedArray, ResolvedUnion};
use crate::{
    AvroResult, Error,
    bigdecimal::{deserialize_big_decimal, serialize_big_decimal},
    decimal::Decimal,
    duration::Duration,
    error::Details,
    schema::{
        DecimalSchema, EnumSchema, FixedSchema, Precision, RecordSchema,
        ResolvedSchema, Scale, Schema, SchemaKind,
    },
};
use bigdecimal::BigDecimal;
use log::{error};
use serde_json::{Number, Value as JsonValue};
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    hash::BuildHasher,
    str::FromStr,
};
use uuid::Uuid;

/// Compute the maximum decimal value precision of a byte array of length `len` could hold.
fn max_prec_for_len(len: usize) -> Result<usize, Error> {
    let len = i32::try_from(len).map_err(|e| Details::ConvertLengthToI32(e, len))?;
    Ok((2.0_f64.powi(8 * len - 1) - 1.0).log10().floor() as usize)
}

/// A valid Avro value.
///
/// More information about Avro values can be found in the [Avro
/// Specification](https://avro.apache.org/docs/++version++/specification/#schema-declaration)
#[derive(Clone, Debug, PartialEq, strum::EnumDiscriminants)]
#[strum_discriminants(name(ValueKind))]
pub enum Value {
    /// A `null` Avro value.
    Null,
    /// A `boolean` Avro value.
    Boolean(bool),
    /// A `int` Avro value.
    Int(i32),
    /// A `long` Avro value.
    Long(i64),
    /// A `float` Avro value.
    Float(f32),
    /// A `double` Avro value.
    Double(f64),
    /// A `bytes` Avro value.
    Bytes(Vec<u8>),
    /// A `string` Avro value.
    String(String),
    /// A `fixed` Avro value.
    /// The size of the fixed value is represented as a `usize`.
    Fixed(usize, Vec<u8>),
    /// An `enum` Avro value.
    ///
    /// An Enum is represented by a symbol and its position in the symbols list
    /// of its corresponding schema.
    /// This allows schema-less encoding, as well as schema resolution while
    /// reading values.
    Enum(u32, String),
    /// An `union` Avro value.
    ///
    /// A Union is represented by the value it holds and its position in the type list
    /// of its corresponding schema
    /// This allows schema-less encoding, as well as schema resolution while
    /// reading values.
    Union(u32, Box<Value>),
    /// An `array` Avro value.
    Array(Vec<Value>),
    /// A `map` Avro value.
    Map(HashMap<String, Value>),
    /// A `record` Avro value.
    ///
    /// A Record is represented by a vector of (`<record name>`, `value`).
    /// This allows schema-less encoding.
    ///
    /// See [Record](types.Record) for a more user-friendly support.
    Record(Vec<(String, Value)>),
    /// A date value.
    ///
    /// Serialized and deserialized as `i32` directly. Can only be deserialized properly with a
    /// schema.
    Date(i32),
    /// An Avro Decimal value. Bytes are in big-endian order, per the Avro spec.
    Decimal(Decimal),
    /// An Avro Decimal value.
    BigDecimal(BigDecimal),
    /// Time in milliseconds.
    TimeMillis(i32),
    /// Time in microseconds.
    TimeMicros(i64),
    /// Timestamp in milliseconds.
    TimestampMillis(i64),
    /// Timestamp in microseconds.
    TimestampMicros(i64),
    /// Timestamp in nanoseconds.
    TimestampNanos(i64),
    /// Local timestamp in milliseconds.
    LocalTimestampMillis(i64),
    /// Local timestamp in microseconds.
    LocalTimestampMicros(i64),
    /// Local timestamp in nanoseconds.
    LocalTimestampNanos(i64),
    /// Avro Duration. An amount of time defined by months, days and milliseconds.
    Duration(Duration),
    /// Universally unique identifier.
    Uuid(Uuid),
}

macro_rules! to_value(
    ($type:ty, $variant_constructor:expr) => (
        impl From<$type> for Value {
            fn from(value: $type) -> Self {
                $variant_constructor(value)
            }
        }
    );
);

to_value!(bool, Value::Boolean);
to_value!(i32, Value::Int);
to_value!(i64, Value::Long);
to_value!(f32, Value::Float);
to_value!(f64, Value::Double);
to_value!(String, Value::String);
to_value!(Vec<u8>, Value::Bytes);
to_value!(Uuid, Value::Uuid);
to_value!(Decimal, Value::Decimal);
to_value!(BigDecimal, Value::BigDecimal);
to_value!(Duration, Value::Duration);

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Self::Null
    }
}

impl TryFrom<usize> for Value {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(i64::try_from(value)
            .map_err(|e| Details::ConvertUsizeToI64(e, value))?
            .into())
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.to_owned())
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Self>,
{
    fn from(value: Option<T>) -> Self {
        // FIXME: this is incorrect in case first type in union is not "none"
        Self::Union(
            value.is_some() as u32,
            Box::new(value.map_or_else(|| Self::Null, Into::into)),
        )
    }
}

impl<K, V, S> From<HashMap<K, V, S>> for Value
where
    K: Into<String>,
    V: Into<Self>,
    S: BuildHasher,
{
    fn from(value: HashMap<K, V, S>) -> Self {
        Self::Map(
            value
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        )
    }
}

/// Utility interface to build `Value::Record` objects.
#[derive(Debug, Clone)]
pub struct Record<'a> {
    /// List of fields contained in the record.
    /// Ordered according to the fields in the schema given to create this
    /// `Record` object. Any unset field defaults to `Value::Null`.
    pub fields: Vec<(String, Value)>,
    schema_lookup: &'a BTreeMap<String, usize>,
}

impl Record<'_> {
    /// Create a `Record` given a `Schema`.
    ///
    /// If the `Schema` is not a `Schema::Record` variant, `None` will be returned.
    pub fn new(schema: &Schema) -> Option<Record<'_>> {
        match *schema {
            Schema::Record(RecordSchema {
                fields: ref schema_fields,
                lookup: ref schema_lookup,
                ..
            }) => {
                let mut fields = Vec::with_capacity(schema_fields.len());
                for schema_field in schema_fields.iter() {
                    fields.push((schema_field.name.clone(), Value::Null));
                }

                Some(Record {
                    fields,
                    schema_lookup,
                })
            }
            _ => None,
        }
    }

    /// Add a field to the `Record`.
    ///
    // TODO: This should return an error at least panic
    /// **NOTE**: If the field name does not exist in the schema, the value is silently dropped.
    pub fn put<V>(&mut self, field: &str, value: V)
    where
        V: Into<Value>,
    {
        if let Some(&position) = self.schema_lookup.get(field) {
            self.fields[position].1 = value.into()
        }
    }

    /// Get the value for a given field name.
    ///
    /// Returns `None` if the field is not present in the schema
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.schema_lookup
            .get(field)
            .map(|&position| &self.fields[position].1)
    }
}

impl<'a> From<Record<'a>> for Value {
    fn from(value: Record<'a>) -> Self {
        Self::Record(value.fields)
    }
}

impl TryFrom<JsonValue> for Value {
    type Error = Error;

    fn try_from(value: JsonValue) -> Result<Self, Self::Error> {
        match value {
            JsonValue::Null => Ok(Self::Null),
            JsonValue::Bool(b) => Ok(b.into()),
            JsonValue::Number(ref n) if n.is_i64() => {
                let n = n.as_i64().unwrap();
                if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                    Ok(Value::Int(n as i32))
                } else {
                    Ok(Value::Long(n))
                }
            }
            JsonValue::Number(ref n) if n.is_f64() => Ok(Value::Double(n.as_f64().unwrap())),
            JsonValue::Number(n) => Err(Details::JsonNumberTooLarge(n).into()),
            JsonValue::String(s) => Ok(s.into()),
            JsonValue::Array(items) => {
                let items = items
                    .into_iter()
                    .map(Value::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Value::Array(items))
            }
            JsonValue::Object(items) => {
                let items = items
                    .into_iter()
                    .map(|(key, value)| Value::try_from(value).map(|v| (key, v)))
                    .collect::<Result<HashMap<_, _>, _>>()?;
                Ok(Value::Map(items))
            }
        }
    }
}

/// Convert Avro values to Json values
impl TryFrom<Value> for JsonValue {
    type Error = crate::error::Error;
    fn try_from(value: Value) -> AvroResult<Self> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Boolean(b) => Ok(Self::Bool(b)),
            Value::Int(i) => Ok(Self::Number(i.into())),
            Value::Long(l) => Ok(Self::Number(l.into())),
            Value::Float(f) => Number::from_f64(f.into())
                .map(Self::Number)
                .ok_or_else(|| Details::ConvertF64ToJson(f.into()).into()),
            Value::Double(d) => Number::from_f64(d)
                .map(Self::Number)
                .ok_or_else(|| Details::ConvertF64ToJson(d).into()),
            Value::Bytes(bytes) => Ok(Self::Array(bytes.into_iter().map(|b| b.into()).collect())),
            Value::String(s) => Ok(Self::String(s)),
            Value::Fixed(_size, items) => {
                Ok(Self::Array(items.into_iter().map(|v| v.into()).collect()))
            }
            Value::Enum(_i, s) => Ok(Self::String(s)),
            Value::Union(_i, b) => Self::try_from(*b),
            Value::Array(items) => items
                .into_iter()
                .map(Self::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Array),
            Value::Map(items) => items
                .into_iter()
                .map(|(key, value)| Self::try_from(value).map(|v| (key, v)))
                .collect::<Result<Vec<_>, _>>()
                .map(|v| Self::Object(v.into_iter().collect())),
            Value::Record(items) => items
                .into_iter()
                .map(|(key, value)| Self::try_from(value).map(|v| (key, v)))
                .collect::<Result<Vec<_>, _>>()
                .map(|v| Self::Object(v.into_iter().collect())),
            Value::Date(d) => Ok(Self::Number(d.into())),
            Value::Decimal(ref d) => <Vec<u8>>::try_from(d)
                .map(|vec| Self::Array(vec.into_iter().map(|v| v.into()).collect())),
            Value::BigDecimal(ref bg) => {
                let vec1: Vec<u8> = serialize_big_decimal(bg)?;
                Ok(Self::Array(vec1.into_iter().map(|b| b.into()).collect()))
            }
            Value::TimeMillis(t) => Ok(Self::Number(t.into())),
            Value::TimeMicros(t) => Ok(Self::Number(t.into())),
            Value::TimestampMillis(t) => Ok(Self::Number(t.into())),
            Value::TimestampMicros(t) => Ok(Self::Number(t.into())),
            Value::TimestampNanos(t) => Ok(Self::Number(t.into())),
            Value::LocalTimestampMillis(t) => Ok(Self::Number(t.into())),
            Value::LocalTimestampMicros(t) => Ok(Self::Number(t.into())),
            Value::LocalTimestampNanos(t) => Ok(Self::Number(t.into())),
            Value::Duration(d) => Ok(Self::Array(
                <[u8; 12]>::from(d).iter().map(|&v| v.into()).collect(),
            )),
            Value::Uuid(uuid) => Ok(Self::String(uuid.as_hyphenated().to_string())),
        }
    }
}

impl Value {
    /// Validate the value against the given [Schema](../schema/enum.Schema.html).
    /// This is a convenience method to keep backwards compatibility intact and is slow.
    /// This method copies the supplied schema and attempts to convert it into ResolvedSchema.
    /// Prefer using validate_complete if able.
    ///
    /// See the [Avro specification](https://avro.apache.org/docs/++version++/specification)
    /// for the full set of rules of schema validation.
    pub fn validate(&self, schema: &Schema) -> bool {
        let resolved_schema : Result<ResolvedSchema,_> = schema.clone().try_into();
        match resolved_schema{
           Ok(resolved) => {
               self.validate_against_resolved(&resolved)
           },
           Err(error) => {
               error!("{error:?}");
               false
           }
        }
    }

    /// Validates this value against the given ResolvedSchema.
    pub fn validate_against_resolved(&self, schema: &ResolvedSchema) -> bool {
        match self.validate_internal(ResolvedNode::new(schema)){
            Some(reason) => {
                let log_message =
                    format!("Invalid value: {self:?} for schema: {schema:?}. Reason: {reason}");
                error!("{log_message}");
                false
            }
            None => true
        }
    }

    fn accumulate(accumulator: Option<String>, other: Option<String>) -> Option<String> {
        match (accumulator, other) {
            (None, None) => None,
            (None, s @ Some(_)) => s,
            (s @ Some(_), None) => s,
            (Some(reason1), Some(reason2)) => Some(format!("{reason1}\n{reason2}")),
        }
    }

    /// Validates the value against the provided schema.
    pub(crate) fn validate_internal(
        &self,
        node: ResolvedNode
    ) -> Option<String> {
        match (self, node) {
            (&Value::Null, ResolvedNode::Null) => None,
            (&Value::Boolean(_), ResolvedNode::Boolean) => None,
            (&Value::Int(_), ResolvedNode::Int) => None,
            (&Value::Int(_), ResolvedNode::Date) => None,
            (&Value::Int(_), ResolvedNode::TimeMillis) => None,
            (&Value::Int(_), ResolvedNode::Long) => None,
            (&Value::Long(_), ResolvedNode::Long) => None,
            (&Value::Long(_), ResolvedNode::TimeMicros) => None,
            (&Value::Long(_), ResolvedNode::TimestampMillis) => None,
            (&Value::Long(_), ResolvedNode::TimestampMicros) => None,
            (&Value::Long(_), ResolvedNode::LocalTimestampMillis) => None,
            (&Value::Long(_), ResolvedNode::LocalTimestampMicros) => None,
            (&Value::TimestampMicros(_), ResolvedNode::TimestampMicros) => None,
            (&Value::TimestampMillis(_), ResolvedNode::TimestampMillis) => None,
            (&Value::TimestampNanos(_), ResolvedNode::TimestampNanos) => None,
            (&Value::LocalTimestampMicros(_), ResolvedNode::LocalTimestampMicros) => None,
            (&Value::LocalTimestampMillis(_), ResolvedNode::LocalTimestampMillis) => None,
            (&Value::LocalTimestampNanos(_), ResolvedNode::LocalTimestampNanos) => None,
            (&Value::TimeMicros(_), ResolvedNode::TimeMicros) => None,
            (&Value::TimeMillis(_), ResolvedNode::TimeMillis) => None,
            (&Value::Date(_), ResolvedNode::Date) => None,
            (&Value::Decimal(_), ResolvedNode::Decimal { .. }) => None,
            (&Value::BigDecimal(_), ResolvedNode::BigDecimal) => None,
            (&Value::Duration(_), ResolvedNode::Duration(..)) => None,
            (&Value::Uuid(_), ResolvedNode::Uuid(..)) => None,
            (&Value::Float(_), ResolvedNode::Float) => None,
            (&Value::Float(_), ResolvedNode::Double) => None,
            (&Value::Double(_), ResolvedNode::Double) => None,
            (&Value::Bytes(_), ResolvedNode::Bytes) => None,
            (&Value::Bytes(_), ResolvedNode::Decimal { .. }) => None,
            (Value::Bytes(bytes), ResolvedNode::Uuid(UuidSchema::Bytes)) => {
                if bytes.len() != 16 {
                    Some(format!(
                        "The value's size ({}) is not the right length for a bytes UUID (16)",
                        bytes.len()
                    ))
                } else {
                    None
                }
            }
            (&Value::String(_), ResolvedNode::String) => None,
            (Value::String(string), ResolvedNode::Uuid(UuidSchema::String)) => {
                // Non-hyphenated is 32 characters, hyphenated is longer
                if string.len() < 32 {
                    Some(format!(
                        "The value's size ({}) is not the right length for a string UUID (>=32)",
                        string.len()
                    ))
                } else {
                    None
                }
            }
            (&Value::Fixed(n, _), ResolvedNode::Fixed(FixedSchema { size, .. })) => {
                if n != *size {
                    Some(format!(
                        "The value's size ({n}) is different than the schema's size ({size})"
                    ))
                } else {
                    None
                }
            }
            (Value::Bytes(b), ResolvedNode::Fixed(FixedSchema { size, .. })) => {
                if b.len() != *size {
                    Some(format!(
                        "The bytes' length ({}) is different than the schema's size ({})",
                        b.len(),
                        size
                    ))
                } else {
                    None
                }
            }
            (&Value::Fixed(n, _), ResolvedNode::Duration(..)) => {
                if n != 12 {
                    Some(format!(
                        "The value's size ('{n}') must be exactly 12 to be a Duration"
                    ))
                } else {
                    None
                }
            }
            (&Value::Fixed(n, _), ResolvedNode::Uuid(UuidSchema::Fixed(size, ..))) => {
                if size.size != 16 {
                    Some(format!(
                        "The schema's size ('{}') must be exactly 16 to be a Uuid",
                        size.size
                    ))
                } else if n != 16 {
                    Some(format!(
                        "The value's size ('{n}') must be exactly 16 to be a Uuid"
                    ))
                } else {
                    None
                }
            }
            // TODO: check precision against n
            (&Value::Fixed(_n, _), ResolvedNode::Decimal { .. }) => None,
            (Value::String(s), ResolvedNode::Enum(EnumSchema { symbols, .. })) => {
                if !symbols.contains(s) {
                    Some(format!("'{s}' is not a member of the possible symbols"))
                } else {
                    None
                }
            }
            (
                &Value::Enum(i, ref s),
                ResolvedNode::Enum(EnumSchema {
                    symbols, default, ..
                }),
            ) => symbols
                .get(i as usize)
                .map(|ref symbol| {
                    if symbol != &s {
                        Some(format!("Symbol '{s}' is not at position '{i}'"))
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| match default {
                    Some(_) => None,
                    None => Some(format!("No symbol at position '{i}'")),
                }),
            // (&Value::Union(None), &Schema::Union(_)) => None,
            (&Value::Union(i, ref value), ResolvedNode::Union(inner)) => inner
                .resolve_schemas()
                .get(i as usize)
                .map(|node| value.validate_internal(node.clone()))
                .unwrap_or_else(|| Some(format!("No schema in the union at position '{i}'"))),
            (v, ResolvedNode::Union(inner)) => {
                match inner.structural_match_on_schema(v) {
                    Some(_) => None,
                    None => Some("Could not find matching type in union".to_string()),
                }
            }
            (Value::Array(items), ResolvedNode::Array(inner)) => items.iter().fold(None, |acc, item| {
                Value::accumulate(
                    acc,
                    item.validate_internal(inner.resolve_items()),
                )
            }),
            (Value::Map(items), ResolvedNode::Map(inner)) => {
                items.iter().fold(None, |acc, (_, value)| {
                    Value::accumulate(
                        acc,
                        value.validate_internal(inner.resolve_types()),
                    )
                })
            }
            (
                Value::Record(record_fields),
                ResolvedNode::Record(ResolvedRecord{fields,lookup, name: _, ..})
            ) => {
                let non_nullable_fields_count =
                    fields.iter().filter(|&rf| !rf.is_nullable()).count();

                // If the record contains fewer fields as required fields by the schema, it is invalid.
                if record_fields.len() < non_nullable_fields_count {
                    return Some(format!(
                        "The value's records length ({}) doesn't match the schema ({} non-nullable fields)",
                        record_fields.len(),
                        non_nullable_fields_count
                    ));
                } else if record_fields.len() > fields.len() {
                    return Some(format!(
                        "The value's records length ({}) is greater than the schema's ({} fields)",
                        record_fields.len(),
                        fields.len(),
                    ));
                }

                record_fields
                    .iter()
                    .fold(None, |acc, (field_name, record_field)| {
                        match lookup.get(field_name) {
                            Some(idx) => {
                                let resolved_field = fields.get(*idx).unwrap();
                                Value::accumulate(
                                    acc,
                                    record_field.validate_internal(
                                        resolved_field.resolve_field()
                                    ),
                                )
                            }
                            None => Value::accumulate(
                                acc,
                                Some(format!("There is no schema field for field '{field_name}'")),
                            ),
                        }
                    })
            }
            (Value::Map(items), ResolvedNode::Record(resolved_record)) => {
                resolved_record.fields.iter().fold(None, |acc, field| {
                    if let Some(item) = items.get(field.name) {
                        let res = item.validate_internal(field.resolve_field());
                        Value::accumulate(acc, res)
                    } else if !field.is_nullable() {
                        Value::accumulate(
                            acc,
                            Some(format!(
                                "Field with name '{:?}' is not a member of the map items",
                                field.name
                            )),
                        )
                    } else {
                        acc
                    }
                })
            }
            (v, s) => Some(format!(
                "Unsupported value-schema combination! Value: {v:?}, schema: {s:?}"
            )),
        }
    }

    /// Resolve this value (self) with provided resolved schema.
    /// This resolution techinically follows a superset of the the schema resolution
    /// rules defined by the specification. TODO: documentation
    pub fn resolve_against_resolved(self, resolved: ResolvedSchema) -> AvroResult<Self> {
        self.resolve_internal(ResolvedNode::new(&resolved))
    }

    /// Resolves value against the provided schema.
    /// This resolution follows a superset of the schema resolution rules defined by the
    /// specification. TODO: link to docs
    /// Provided Schema is first cloned then transformed into a ResolvedSchema, which is
    /// slow and may fail. If succesfull, this function simply calls resolve_complete
    /// with this new ResolvedSchema.
    /// Prefer using resolve_complete on a given ResolvedSchema,
    /// this function exists for convenience and to maintain backwards compatibility.
    pub fn resolve(self, schema: &Schema)->AvroResult<Self>{
       self.resolve_against_resolved(schema.clone().try_into()?)
    }

    pub(crate) fn resolve_internal(
        mut self,
        node: ResolvedNode
    ) -> AvroResult<Self> {
        // Check if this schema is a union, and if the reader schema is not.
        if SchemaKind::from(&self) == SchemaKind::Union
            && SchemaKind::from(&node) != SchemaKind::Union
        {
            // Pull out the Union, and attempt to resolve against it.
            let v = match self {
                Value::Union(_i, b) => *b,
                _ => unreachable!(),
            };
            self = v;
        }
        match node {
            ResolvedNode::Null => self.resolve_null(),
            ResolvedNode::Boolean => self.resolve_boolean(),
            ResolvedNode::Int => self.resolve_int(),
            ResolvedNode::Long => self.resolve_long(),
            ResolvedNode::Float => self.resolve_float(),
            ResolvedNode::Double => self.resolve_double(),
            ResolvedNode::Bytes => self.resolve_bytes(),
            ResolvedNode::String => self.resolve_string(),
            ResolvedNode::Fixed(FixedSchema { size, .. }) => self.resolve_fixed(*size),
            ResolvedNode::Enum(EnumSchema {
                symbols,
                default,
                ..
            }) => self.resolve_enum(symbols, default),
            ResolvedNode::BigDecimal => self.resolve_bigdecimal(),
            ResolvedNode::Date => self.resolve_date(),
            ResolvedNode::TimeMillis => self.resolve_time_millis(),
            ResolvedNode::TimeMicros => self.resolve_time_micros(),
            ResolvedNode::TimestampMillis => self.resolve_timestamp_millis(),
            ResolvedNode::TimestampMicros => self.resolve_timestamp_micros(),
            ResolvedNode::TimestampNanos => self.resolve_timestamp_nanos(),
            ResolvedNode::LocalTimestampMillis => self.resolve_local_timestamp_millis(),
            ResolvedNode::LocalTimestampMicros => self.resolve_local_timestamp_micros(),
            ResolvedNode::LocalTimestampNanos => self.resolve_local_timestamp_nanos(),
            ResolvedNode::Duration(_) => self.resolve_duration(),
            ResolvedNode::Uuid(uuid_schema) => self.resolve_uuid(uuid_schema),
            ResolvedNode::Decimal(DecimalSchema {
                scale,
                precision,
                inner,
            }) => self.resolve_decimal(*precision, *scale, inner),
            ResolvedNode::Union(resolved_union) => {
                self.resolve_union(resolved_union)
            }
            ResolvedNode::Array(resolved_array) => {
                self.resolve_array(resolved_array)
            }
            ResolvedNode::Map(resolved_map) => self.resolve_map(resolved_map),
            ResolvedNode::Record(resolved_record) => {
                self.resolve_record(resolved_record)
            }
        }
    }

    fn resolve_uuid(self, inner: &UuidSchema) -> Result<Self, Error> {
        let value = match (self, inner) {
            (uuid @ Value::Uuid(_), _) => uuid,
            (Value::String(ref string), UuidSchema::String) => {
                Value::Uuid(Uuid::from_str(string).map_err(Details::ConvertStrToUuid)?)
            }
            (Value::Bytes(ref bytes), UuidSchema::Bytes) => {
                Value::Uuid(Uuid::from_slice(bytes).map_err(Details::ConvertSliceToUuid)?)
            }
            (Value::Fixed(n, ref bytes), UuidSchema::Fixed(_)) => {
                if n != 16 {
                    return Err(Details::ConvertFixedToUuid(n).into());
                }
                Value::Uuid(Uuid::from_slice(bytes).map_err(Details::ConvertSliceToUuid)?)
            }
            (Value::String(ref string), UuidSchema::Fixed(_)) => {
                let bytes = string.as_bytes();
                if bytes.len() != 16 {
                    return Err(Details::ConvertFixedToUuid(bytes.len()).into());
                }
                Value::Uuid(Uuid::from_slice(bytes).map_err(Details::ConvertSliceToUuid)?)
            }
            (other, _) => return Err(Details::GetUuid(other).into()),
        };
        Ok(value)
    }

    fn resolve_bigdecimal(self) -> Result<Self, Error> {
        Ok(match self {
            bg @ Value::BigDecimal(_) => bg,
            Value::Bytes(b) => Value::BigDecimal(deserialize_big_decimal(&b)?),
            other => return Err(Details::GetBigDecimal(other).into()),
        })
    }

    fn resolve_duration(self) -> Result<Self, Error> {
        Ok(match self {
            duration @ Value::Duration { .. } => duration,
            Value::Fixed(size, bytes) => {
                if size != 12 {
                    return Err(Details::GetDurationFixedBytes(size).into());
                }
                Value::Duration(Duration::from([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                    bytes[8], bytes[9], bytes[10], bytes[11],
                ]))
            }
            other => return Err(Details::ResolveDuration(other).into()),
        })
    }

    fn resolve_decimal(
        self,
        precision: Precision,
        scale: Scale,
        inner: &InnerDecimalSchema,
    ) -> Result<Self, Error> {
        if scale > precision {
            return Err(Details::GetScaleAndPrecision { scale, precision }.into());
        }
        match inner {
            &InnerDecimalSchema::Fixed(FixedSchema { size, .. }) => {
                if max_prec_for_len(size)? < precision {
                    return Err(Details::GetScaleWithFixedSize { size, precision }.into());
                }
            }
            InnerDecimalSchema::Bytes => (),
        };
        match self {
            Value::Decimal(num) => {
                let num_bytes = num.len();
                if max_prec_for_len(num_bytes)? < precision {
                    Err(Details::ComparePrecisionAndSize {
                        precision,
                        num_bytes,
                    }
                    .into())
                } else {
                    Ok(Value::Decimal(num))
                }
                // check num.bits() here
            }
            Value::Fixed(_, bytes) | Value::Bytes(bytes) => {
                if max_prec_for_len(bytes.len())? < precision {
                    Err(Details::ComparePrecisionAndSize {
                        precision,
                        num_bytes: bytes.len(),
                    }
                    .into())
                } else {
                    // precision and scale match, can we assume the underlying type can hold the data?
                    Ok(Value::Decimal(Decimal::from(bytes)))
                }
            }
            other => Err(Details::ResolveDecimal(other).into()),
        }
    }

    fn resolve_date(self) -> Result<Self, Error> {
        match self {
            Value::Date(d) | Value::Int(d) => Ok(Value::Date(d)),
            other => Err(Details::GetDate(other).into()),
        }
    }

    fn resolve_time_millis(self) -> Result<Self, Error> {
        match self {
            Value::TimeMillis(t) | Value::Int(t) => Ok(Value::TimeMillis(t)),
            other => Err(Details::GetTimeMillis(other).into()),
        }
    }

    fn resolve_time_micros(self) -> Result<Self, Error> {
        match self {
            Value::TimeMicros(t) | Value::Long(t) => Ok(Value::TimeMicros(t)),
            Value::Int(t) => Ok(Value::TimeMicros(i64::from(t))),
            other => Err(Details::GetTimeMicros(other).into()),
        }
    }

    fn resolve_timestamp_millis(self) -> Result<Self, Error> {
        match self {
            Value::TimestampMillis(ts) | Value::Long(ts) => Ok(Value::TimestampMillis(ts)),
            Value::Int(ts) => Ok(Value::TimestampMillis(i64::from(ts))),
            other => Err(Details::GetTimestampMillis(other).into()),
        }
    }

    fn resolve_timestamp_micros(self) -> Result<Self, Error> {
        match self {
            Value::TimestampMicros(ts) | Value::Long(ts) => Ok(Value::TimestampMicros(ts)),
            Value::Int(ts) => Ok(Value::TimestampMicros(i64::from(ts))),
            other => Err(Details::GetTimestampMicros(other).into()),
        }
    }

    fn resolve_timestamp_nanos(self) -> Result<Self, Error> {
        match self {
            Value::TimestampNanos(ts) | Value::Long(ts) => Ok(Value::TimestampNanos(ts)),
            Value::Int(ts) => Ok(Value::TimestampNanos(i64::from(ts))),
            other => Err(Details::GetTimestampNanos(other).into()),
        }
    }

    fn resolve_local_timestamp_millis(self) -> Result<Self, Error> {
        match self {
            Value::LocalTimestampMillis(ts) | Value::Long(ts) => {
                Ok(Value::LocalTimestampMillis(ts))
            }
            Value::Int(ts) => Ok(Value::LocalTimestampMillis(i64::from(ts))),
            other => Err(Details::GetLocalTimestampMillis(other).into()),
        }
    }

    fn resolve_local_timestamp_micros(self) -> Result<Self, Error> {
        match self {
            Value::LocalTimestampMicros(ts) | Value::Long(ts) => {
                Ok(Value::LocalTimestampMicros(ts))
            }
            Value::Int(ts) => Ok(Value::LocalTimestampMicros(i64::from(ts))),
            other => Err(Details::GetLocalTimestampMicros(other).into()),
        }
    }

    fn resolve_local_timestamp_nanos(self) -> Result<Self, Error> {
        match self {
            Value::LocalTimestampNanos(ts) | Value::Long(ts) => Ok(Value::LocalTimestampNanos(ts)),
            Value::Int(ts) => Ok(Value::LocalTimestampNanos(i64::from(ts))),
            other => Err(Details::GetLocalTimestampNanos(other).into()),
        }
    }

    fn resolve_null(self) -> Result<Self, Error> {
        match self {
            Value::Null => Ok(Value::Null),
            other => Err(Details::GetNull(other).into()),
        }
    }

    fn resolve_boolean(self) -> Result<Self, Error> {
        match self {
            Value::Boolean(b) => Ok(Value::Boolean(b)),
            other => Err(Details::GetBoolean(other).into()),
        }
    }

    fn resolve_int(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Int(n)),
            Value::Long(n) => {
                let n = i32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
                Ok(Value::Int(n))
            }
            other => Err(Details::GetInt(other).into()),
        }
    }

    fn resolve_long(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Long(i64::from(n))),
            Value::Long(n) => Ok(Value::Long(n)),
            other => Err(Details::GetLong(other).into()),
        }
    }

    fn resolve_float(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Float(n as f32)),
            Value::Long(n) => Ok(Value::Float(n as f32)),
            Value::Float(x) => Ok(Value::Float(x)),
            Value::Double(x) => Ok(Value::Float(x as f32)),
            Value::String(ref x) => match Self::parse_special_float(x) {
                Some(f) => Ok(Value::Float(f)),
                None => Err(Details::GetFloat(self).into()),
            },
            other => Err(Details::GetFloat(other).into()),
        }
    }

    fn resolve_double(self) -> Result<Self, Error> {
        match self {
            Value::Int(n) => Ok(Value::Double(f64::from(n))),
            Value::Long(n) => Ok(Value::Double(n as f64)),
            Value::Float(x) => Ok(Value::Double(f64::from(x))),
            Value::Double(x) => Ok(Value::Double(x)),
            Value::String(ref x) => match Self::parse_special_float(x) {
                Some(f) => Ok(Value::Double(f64::from(f))),
                None => Err(Details::GetDouble(self).into()),
            },
            other => Err(Details::GetDouble(other).into()),
        }
    }

    /// IEEE 754 NaN and infinities are not valid JSON numbers.
    /// So they are represented in JSON as strings.
    fn parse_special_float(value: &str) -> Option<f32> {
        match value {
            "NaN" => Some(f32::NAN),
            "INF" | "Infinity" => Some(f32::INFINITY),
            "-INF" | "-Infinity" => Some(f32::NEG_INFINITY),
            _ => None,
        }
    }

    fn resolve_bytes(self) -> Result<Self, Error> {
        match self {
            Value::Bytes(bytes) => Ok(Value::Bytes(bytes)),
            Value::String(s) => Ok(Value::Bytes(s.into_bytes())),
            Value::Array(items) => Ok(Value::Bytes(
                items
                    .into_iter()
                    .map(Value::try_u8)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            other => Err(Details::GetBytes(other).into()),
        }
    }

    fn resolve_string(self) -> Result<Self, Error> {
        match self {
            Value::String(s) => Ok(Value::String(s)),
            Value::Bytes(bytes) | Value::Fixed(_, bytes) => Ok(Value::String(
                String::from_utf8(bytes).map_err(Details::ConvertToUtf8)?,
            )),
            other => Err(Details::GetString(other).into()),
        }
    }

    fn resolve_fixed(self, size: usize) -> Result<Self, Error> {
        match self {
            Value::Fixed(n, bytes) => {
                if n == size {
                    Ok(Value::Fixed(n, bytes))
                } else {
                    Err(Details::CompareFixedSizes { size, n }.into())
                }
            }
            Value::String(s) => Ok(Value::Fixed(s.len(), s.into_bytes())),
            Value::Bytes(s) => {
                if s.len() == size {
                    Ok(Value::Fixed(size, s))
                } else {
                    Err(Details::CompareFixedSizes { size, n: s.len() }.into())
                }
            }
            other => Err(Details::GetStringForFixed(other).into()),
        }
    }

    pub(crate) fn resolve_enum(
        self,
        symbols: &[String],
        enum_default: &Option<String>
    ) -> Result<Self, Error> {
        let validate_symbol = |symbol: String, symbols: &[String]| {
            if let Some(index) = symbols.iter().position(|item| item == &symbol) {
                Ok(Value::Enum(index as u32, symbol))
            } else {
                match enum_default {
                    Some(default) => {
                        if let Some(index) = symbols.iter().position(|item| item == default) {
                            Ok(Value::Enum(index as u32, default.clone()))
                        } else {
                            Err(Details::GetEnumDefault {
                                symbol,
                                symbols: symbols.into(),
                            }
                            .into())
                        }
                    }
                    _ => Err(Details::GetEnumDefault {
                        symbol,
                        symbols: symbols.into(),
                    }
                    .into()),
                }
            }
        };

        match self {
            Value::Enum(_raw_index, s) => validate_symbol(s, symbols),
            Value::String(s) => validate_symbol(s, symbols),
            other => Err(Details::GetEnum(other).into()),
        }
    }

    fn resolve_union(
        self,
        resolved_union: ResolvedUnion
    ) -> Result<Self, Error> {
        let v = match self {
            // Both are unions case.
            Value::Union(_i, v) => *v,
            // Reader is a union, but writer is not.
            v => v,
        };

        let (i, inner) = resolved_union
            .structural_match_on_schema(&v)
            .ok_or_else(|| Details::FindUnionVariant {
                schema: resolved_union.get_union_schema().clone(),
                value: v.clone(),
            })?;

        Ok( Value::Union(
            i as u32,
            Box::new(v.resolve_internal(inner)?))
        )
    }

    fn resolve_array(
        self,
        resolved_array: ResolvedArray,
    ) -> Result<Self, Error> {
        let items_resolved = resolved_array.resolve_items();
        match self {
            Value::Array(items) => Ok(Value::Array(
                items
                    .into_iter()
                    .map(|item| item.resolve_internal(items_resolved.clone()))
                    .collect::<Result<_, _>>()?,
            )),
            other => Err(Details::GetArray {
                expected: SchemaKind::Array,
                other,
            }
            .into()),
        }
    }

    fn resolve_map(
        self,
        resolved_map: ResolvedMap,
    ) -> Result<Self, Error> {
        let resolved_types = resolved_map.resolve_types();
        match self {
            Value::Map(items) => Ok(Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| {
                        value
                            .resolve_internal(resolved_types.clone())
                            .map(|value| (key, value))
                    })
                    .collect::<Result<_, _>>()?,
            )),
            other => Err(Details::GetMap {
                expected: SchemaKind::Map,
                other,
            }
            .into()),
        }
    }

    fn resolve_record(
        self,
        resolved_record: ResolvedRecord
    ) -> Result<Self, Error> {
        let mut items = match self {
            Value::Map(items) => Ok(items),
            Value::Record(fields) => Ok(fields.into_iter().collect::<HashMap<_, _>>()),
            other => Err(Error::new(Details::GetRecord {
                expected: resolved_record.fields
                    .iter()
                    .map(|field| (field.name.clone(),
                                SchemaKind::from(&field.resolve_field()))
                            )
                    .collect(),
                other,
            })),
        }?;

        let new_fields = resolved_record.fields
            .iter()
            .map(|field| {
                let value = match items.remove(field.name) {
                    Some(value) => value,
                    None => match &field.default {
                        Some(value) => value.clone(), // we have already checked that the defualt
                                                      // agrees with the schema
                        None => {
                            return Err(Details::GetField(field.name.clone()).into());
                        }
                    },
                };
                value
                    .resolve_internal(field.resolve_field())
                    .map(|value| (field.name.clone(), value))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Value::Record(new_fields))
    }

    fn try_u8(self) -> AvroResult<u8> {
        let int = self.resolve(&Schema::Int)?;
        if let Value::Int(n) = int
            && n >= 0
            && n <= i32::from(u8::MAX)
        {
            return Ok(n as u8);
        }

        Err(Details::GetU8(int).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        duration::{Days, Millis, Months}, error::Details, schema::{Name, UnionSchema, RecordField}, to_value
    };
    use apache_avro_test_helper::{
        TestResult,
        logger::{assert_logged, assert_not_logged},
    };
    use num_bigint::BigInt;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn avro_3809_validate_nested_records_with_implicit_namespace() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "name": "record_name",
            "namespace": "space",
            "type": "record",
            "fields": [
              {
                "name": "outer_field_1",
                "type": {
                  "type": "record",
                  "name": "middle_record_name",
                  "namespace": "middle_namespace",
                  "fields": [
                    {
                      "name": "middle_field_1",
                      "type": {
                        "type": "record",
                        "name": "inner_record_name",
                        "fields": [
                          { "name": "inner_field_1", "type": "double" }
                        ]
                      }
                    },
                    { "name": "middle_field_2", "type": "inner_record_name" }
                  ]
                }
              }
            ]
          }"#,
        )?;
        let value = Value::Record(vec![(
            "outer_field_1".into(),
            Value::Record(vec![
                (
                    "middle_field_1".into(),
                    Value::Record(vec![("inner_field_1".into(), Value::Double(1.2f64))]),
                ),
                (
                    "middle_field_2".into(),
                    Value::Record(vec![("inner_field_1".into(), Value::Double(1.6f64))]),
                ),
            ]),
        )]);

        assert!(value.validate(&schema));
        Ok(())
    }

    #[test]
    fn validate() -> TestResult {
        let value_schema_valid = vec![
            (Value::Int(42), Schema::Int, true, ""),
            (Value::Int(43), Schema::Long, true, ""),
            (Value::Float(43.2), Schema::Float, true, ""),
            (Value::Float(45.9), Schema::Double, true, ""),
            (
                Value::Int(42),
                Schema::Boolean,
                false,
                "Invalid value: Int(42) for schema: Boolean. Reason: Unsupported value-schema combination! Value: Int(42), schema: Boolean",
            ),
            (
                Value::Union(0, Box::new(Value::Null)),
                Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int])?),
                true,
                "",
            ),
            (
                Value::Union(1, Box::new(Value::Int(42))),
                Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int])?),
                true,
                "",
            ),
            (
                Value::Union(0, Box::new(Value::Null)),
                Schema::Union(UnionSchema::new(vec![Schema::Double, Schema::Int])?),
                false,
                "Invalid value: Union(0, Null) for schema: Union(UnionSchema { schemas: [Double, Int] }). Reason: Unsupported value-schema combination! Value: Null, schema: Double",
            ),
            (
                Value::Union(3, Box::new(Value::Int(42))),
                Schema::Union(UnionSchema::new(vec![
                    Schema::Null,
                    Schema::Double,
                    Schema::String,
                    Schema::Int,
                ])?),
                true,
                "",
            ),
            (
                Value::Union(1, Box::new(Value::Long(42i64))),
                Schema::Union(UnionSchema::new(vec![
                    Schema::Null,
                    Schema::TimestampMillis,
                ])?),
                true,
                "",
            ),
            (
                Value::Union(2, Box::new(Value::Long(1_i64))),
                Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int])?),
                false,
                "Invalid value: Union(2, Long(1)) for schema: Union(UnionSchema { schemas: [Null, Int] }). Reason: No schema in the union at position '2'",
            ),
            (
                Value::Array(vec![Value::Long(42i64)]),
                Schema::array(Schema::Long).build(),
                true,
                "",
            ),
            (
                Value::Array(vec![Value::Boolean(true)]),
                Schema::array(Schema::Long).build(),
                false,
                "Invalid value: Array([Boolean(true)]) for schema: Array(ArraySchema { items: Long, .. }). Reason: Unsupported value-schema combination! Value: Boolean(true), schema: Long",
            ),
            (
                Value::Record(vec![]),
                Schema::Null,
                false,
                "Invalid value: Record([]) for schema: Null. Reason: Unsupported value-schema combination! Value: Record([]), schema: Null",
            ),
            (
                Value::Fixed(12, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
                Schema::Duration(FixedSchema {
                    name: Name::try_from("TestName")?.into(),
                    aliases: None,
                    doc: None,
                    size: 12,
                    attributes: BTreeMap::new(),
                }),
                true,
                "",
            ),
            (
                Value::Fixed(11, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                Schema::Duration(FixedSchema {
                    name: Name::try_from("TestName")?.into(),
                    aliases: None,
                    doc: None,
                    size: 12,
                    attributes: BTreeMap::new(),
                }),
                false,
                r#"Invalid value: Fixed(11, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) for schema: Duration(FixedSchema { name: Name { name: "TestName", .. }, size: 12, .. }). Reason: The value's size ('11') must be exactly 12 to be a Duration"#,
            ),
            (
                Value::Record(vec![("unknown_field_name".to_string(), Value::Null)]),
                Schema::Record(RecordSchema {
                    name: Name::new("record_name")?.into(),
                    aliases: None,
                    doc: None,
                    fields: vec![
                        RecordField::builder()
                            .name("field_name".to_string())
                            .schema(Schema::Int)
                            .build(),
                    ],
                    lookup: Default::default(),
                    attributes: Default::default(),
                }),
                false,
                r#"Invalid value: Record([("unknown_field_name", Null)]) for schema: Record(RecordSchema { name: Name { name: "record_name", .. }, fields: [RecordField { name: "field_name", schema: Int, .. }], .. }). Reason: There is no schema field for field 'unknown_field_name'"#,
            ),
        ];

        for (value, schema, valid, expected_err_message) in value_schema_valid.into_iter() {
            let err_message = value.validate_internal(ResolvedNode::new(&ResolvedSchema::try_from(schema.clone())?));
            assert_eq!(valid, err_message.is_none());
            if !valid {
                let full_err_message = format!(
                    "Invalid value: {:?} for schema: {:?}. Reason: {}",
                    value,
                    schema,
                    err_message.unwrap()
                );
                assert_eq!(expected_err_message, full_err_message);
            }
        }

        Ok(())
    }

    #[test]
    fn validate_fixed() -> TestResult {
        let schema = ResolvedSchema::try_from(Schema::Fixed(FixedSchema {
            size: 4,
            name: Name::new("some_fixed")?.into(),
            aliases: None,
            doc: None,
            attributes: Default::default(),
        }))?;

        assert!(Value::Fixed(4, vec![0, 0, 0, 0]).validate_against_resolved(&schema));
        let value = Value::Fixed(5, vec![0, 0, 0, 0, 0]);
        assert!(!value.validate_against_resolved(&schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema, "The value's size (5) is different than the schema's size (4)"
            )
            .as_str(),
        );

        assert!(Value::Bytes(vec![0, 0, 0, 0]).validate_against_resolved(&schema));
        let value = Value::Bytes(vec![0, 0, 0, 0, 0]);
        assert!(!value.validate_against_resolved(&schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema, "The bytes' length (5) is different than the schema's size (4)"
            )
            .as_str(),
        );

        Ok(())
    }

    #[test]
    fn validate_enum() -> TestResult {
        let schema = Schema::Enum(EnumSchema {
            name: Name::new("some_enum")?.into(),
            aliases: None,
            doc: None,
            symbols: vec![
                "spades".to_string(),
                "hearts".to_string(),
                "diamonds".to_string(),
                "clubs".to_string(),
            ],
            default: None,
            attributes: Default::default(),
        });
        let [schema] = ResolvedSchema::resolve().build_array([&schema])?;

        assert!(Value::Enum(0, "spades".to_string()).validate_against_resolved(&schema));
        assert!(Value::String("spades".to_string()).validate_against_resolved(&schema));

        let value = Value::Enum(1, "spades".to_string());
        assert!(!value.validate_against_resolved(&schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema, "Symbol 'spades' is not at position '1'"
            )
            .as_str(),
        );

        let value = Value::Enum(1000, "spades".to_string());
        assert!(!value.validate_against_resolved(&schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema, "No symbol at position '1000'"
            )
            .as_str(),
        );

        let value = Value::String("lorem".to_string());
        assert!(!value.validate_against_resolved(&schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema, "'lorem' is not a member of the possible symbols"
            )
            .as_str(),
        );

        let other_schema = Schema::Enum(EnumSchema {
            name: Name::new("some_other_enum")?.into(),
            aliases: None,
            doc: None,
            symbols: vec![
                "hearts".to_string(),
                "diamonds".to_string(),
                "clubs".to_string(),
                "spades".to_string(),
            ],
            default: None,
            attributes: Default::default(),
        });
        let [other_schema] = ResolvedSchema::resolve().build_array([&other_schema])?;

        let value = Value::Enum(0, "spades".to_string());
        assert!(!value.validate_against_resolved(&other_schema));
        assert_logged(
            format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, other_schema, "Symbol 'spades' is not at position '0'"
            )
            .as_str(),
        );

        Ok(())
    }

    #[test]
    fn validate_record() -> TestResult {
        // {
        //    "type": "record",
        //    "fields": [
        //      {"type": "long", "name": "a"},
        //      {"type": "string", "name": "b"},
        //      {
        //          "type": ["null", "int"]
        //          "name": "c",
        //          "default": null
        //      }
        //    ]
        // }
        let raw_schema = Schema::Record(RecordSchema {
            name: Name::new("some_record")?.into(),
            aliases: None,
            doc: None,
            fields: vec![
                RecordField::builder()
                    .name("a".to_string())
                    .schema(Schema::Long)
                    .build(),
                RecordField::builder()
                    .name("b".to_string())
                    .schema(Schema::String)
                    .build(),
                RecordField::builder()
                    .name("c".to_string())
                    .default(JsonValue::Null)
                    .schema(Schema::Union(UnionSchema::new(vec![
                        Schema::Null,
                        Schema::Int,
                    ])?))
                    .build(),
            ],
            lookup: [
                ("a".to_string(), 0),
                ("b".to_string(), 1),
                ("c".to_string(), 2),
            ]
            .iter()
            .cloned()
            .collect(),
            attributes: Default::default(),
        });

        let schema = ResolvedSchema::try_from(&raw_schema)?;

        assert!(
            Value::Record(vec![
                ("a".to_string(), Value::Long(42i64)),
                ("b".to_string(), Value::String("foo".to_string())),
            ])
            .validate_against_resolved(&schema)
        );

        let value = Value::Record(vec![
            ("b".to_string(), Value::String("foo".to_string())),
            ("a".to_string(), Value::Long(42i64)),
        ]);
        assert!(value.validate_against_resolved(&schema));

        let value = Value::Record(vec![
            ("a".to_string(), Value::Boolean(false)),
            ("b".to_string(), Value::String("foo".to_string())),
        ]);
        assert!(!value.validate_against_resolved(&schema));
        assert_logged(
            &format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema,
                "Unsupported value-schema combination! Value: Boolean(false), schema: Long"
            ).to_string(),
        );

        let value = Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("c".to_string(), Value::String("foo".to_string())),
        ]);
        assert!(!value.validate_against_resolved(&schema));
        assert_logged(
            &format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema,
                "Could not find matching type in union"
            ).to_string(),
        );
        assert_not_logged(
            r#"Invalid value: String("foo") for schema: Int. Reason: Unsupported value-schema combination"#,
        );

        let value = Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("d".to_string(), Value::String("foo".to_string())),
        ]);
        assert!(!value.validate_against_resolved(&schema));
        assert_logged(
            &format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema,
                "There is no schema field for field 'd'"
            ).to_string(),
        );

        let value = Value::Record(vec![
            ("a".to_string(), Value::Long(42i64)),
            ("b".to_string(), Value::String("foo".to_string())),
            ("c".to_string(), Value::Null),
            ("d".to_string(), Value::Null),
        ]);
        assert!(!value.validate_against_resolved(&schema));
        assert_logged(
            &format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema,
                "The value's records length (4) is greater than the schema's (3 fields)"
            ).to_string(),
        );

        assert!(
            Value::Map(
                vec![
                    ("a".to_string(), Value::Long(42i64)),
                    ("b".to_string(), Value::String("foo".to_string())),
                ]
                .into_iter()
                .collect()
            )
            .validate_against_resolved(&schema)
        );

        let value = Value::Map(
                vec![("d".to_string(), Value::Long(123_i64)),]
                    .into_iter()
                    .collect()
            );

        assert!(
            !value.validate_against_resolved(&schema)
        );

        assert_logged(
            &format!(
                "Invalid value: {:?} for schema: {:?}. Reason: {}",
                value, schema,
                "Field with name '\"a\"' is not a member of the map items\nField with name '\"b\"' is not a member of the map items"
            ).to_string(),
        );

        let union_schema = ResolvedSchema::try_from(Schema::Union(UnionSchema::new(vec![Schema::Null, raw_schema])?))?;

        assert!(
            Value::Union(
                1,
                Box::new(Value::Record(vec![
                    ("a".to_string(), Value::Long(42i64)),
                    ("b".to_string(), Value::String("foo".to_string())),
                ]))
            )
            .validate_against_resolved(&union_schema)
        );

        assert!(
            Value::Union(
                1,
                Box::new(Value::Map(
                    vec![
                        ("a".to_string(), Value::Long(42i64)),
                        ("b".to_string(), Value::String("foo".to_string())),
                    ]
                    .into_iter()
                    .collect()
                ))
            )
            .validate_against_resolved(&union_schema)
        );

        Ok(())
    }

    #[test]
    fn resolve_bytes_ok() -> TestResult {
        let value = Value::Array(vec![Value::Int(0), Value::Int(42)]);
        assert_eq!(
            value.resolve(&Schema::Bytes)?,
            Value::Bytes(vec![0u8, 42u8])
        );

        Ok(())
    }

    #[test]
    fn resolve_string_from_bytes() -> TestResult {
        let value = Value::Bytes(vec![97, 98, 99]);
        assert_eq!(
            value.resolve(&Schema::String)?,
            Value::String("abc".to_string())
        );

        Ok(())
    }

    #[test]
    fn resolve_string_from_fixed() -> TestResult {
        let value = Value::Fixed(3, vec![97, 98, 99]);
        assert_eq!(
            value.resolve(&Schema::String)?,
            Value::String("abc".to_string())
        );

        Ok(())
    }

    #[test]
    fn resolve_bytes_failure() {
        let value = Value::Array(vec![Value::Int(2000), Value::Int(-42)]);
        assert!(value.resolve(&Schema::Bytes).is_err());
    }

    #[test]
    fn resolve_decimal_bytes() -> TestResult {
        let value = Value::Decimal(Decimal::from(vec![1, 2, 3, 4, 5]));
        value.clone().resolve(&Schema::Decimal(DecimalSchema {
            precision: 10,
            scale: 4,
            inner: InnerDecimalSchema::Bytes,
        }))?;
        assert!(value.resolve(&Schema::String).is_err());

        Ok(())
    }

    #[test]
    fn resolve_decimal_invalid_scale() {
        let value = Value::Decimal(Decimal::from(vec![1, 2]));
        assert!(
            value
                .resolve(&Schema::Decimal(DecimalSchema {
                    precision: 2,
                    scale: 3,
                    inner: InnerDecimalSchema::Bytes,
                }))
                .is_err()
        );
    }

    #[test]
    fn resolve_decimal_invalid_precision_for_length() {
        let value = Value::Decimal(Decimal::from((1u8..=8u8).rev().collect::<Vec<_>>()));
        assert!(
            value
                .resolve(&Schema::Decimal(DecimalSchema {
                    precision: 1,
                    scale: 0,
                    inner: InnerDecimalSchema::Bytes,
                }))
                .is_ok()
        );
    }

    #[test]
    fn resolve_decimal_fixed() {
        let value = Value::Decimal(Decimal::from(vec![1, 2, 3, 4, 5]));
        assert!(
            value
                .clone()
                .resolve(&Schema::Decimal(DecimalSchema {
                    precision: 10,
                    scale: 1,
                    inner: InnerDecimalSchema::Fixed(FixedSchema {
                        name: Name::new("decimal").unwrap().into(),
                        aliases: None,
                        size: 20,
                        doc: None,
                        attributes: Default::default(),
                    })
                }))
                .is_ok()
        );
        assert!(value.resolve(&Schema::String).is_err());
    }

    #[test]
    fn resolve_date() {
        let value = Value::Date(2345);
        assert!(value.clone().resolve(&Schema::Date).is_ok());
        assert!(value.resolve(&Schema::String).is_err());
    }

    #[test]
    fn resolve_time_millis() {
        let value = Value::TimeMillis(10);
        assert!(value.clone().resolve(&Schema::TimeMillis).is_ok());
        assert!(value.resolve(&Schema::TimeMicros).is_err());
    }

    #[test]
    fn resolve_time_micros() {
        let value = Value::TimeMicros(10);
        assert!(value.clone().resolve(&Schema::TimeMicros).is_ok());
        assert!(value.resolve(&Schema::TimeMillis).is_err());
    }

    #[test]
    fn resolve_timestamp_millis() {
        let value = Value::TimestampMillis(10);
        assert!(value.clone().resolve(&Schema::TimestampMillis).is_ok());
        assert!(value.resolve(&Schema::Float).is_err());

        let value = Value::Float(10.0f32);
        assert!(value.resolve(&Schema::TimestampMillis).is_err());
    }

    #[test]
    fn resolve_timestamp_micros() {
        let value = Value::TimestampMicros(10);
        assert!(value.clone().resolve(&Schema::TimestampMicros).is_ok());
        assert!(value.resolve(&Schema::Int).is_err());

        let value = Value::Double(10.0);
        assert!(value.resolve(&Schema::TimestampMicros).is_err());
    }

    #[test]
    fn test_avro_3914_resolve_timestamp_nanos() {
        let value = Value::TimestampNanos(10);
        assert!(value.clone().resolve(&Schema::TimestampNanos).is_ok());
        assert!(value.resolve(&Schema::Int).is_err());

        let value = Value::Double(10.0);
        assert!(value.resolve(&Schema::TimestampNanos).is_err());
    }

    #[test]
    fn test_avro_3853_resolve_timestamp_millis() {
        let value = Value::LocalTimestampMillis(10);
        assert!(value.clone().resolve(&Schema::LocalTimestampMillis).is_ok());
        assert!(value.resolve(&Schema::Float).is_err());

        let value = Value::Float(10.0f32);
        assert!(value.resolve(&Schema::LocalTimestampMillis).is_err());
    }

    #[test]
    fn test_avro_3853_resolve_timestamp_micros() {
        let value = Value::LocalTimestampMicros(10);
        assert!(value.clone().resolve(&Schema::LocalTimestampMicros).is_ok());
        assert!(value.resolve(&Schema::Int).is_err());

        let value = Value::Double(10.0);
        assert!(value.resolve(&Schema::LocalTimestampMicros).is_err());
    }

    #[test]
    fn test_avro_3916_resolve_timestamp_nanos() {
        let value = Value::LocalTimestampNanos(10);
        assert!(value.clone().resolve(&Schema::LocalTimestampNanos).is_ok());
        assert!(value.resolve(&Schema::Int).is_err());

        let value = Value::Double(10.0);
        assert!(value.resolve(&Schema::LocalTimestampNanos).is_err());
    }

    #[test]
    fn resolve_duration() {
        let value = Value::Duration(Duration::new(
            Months::new(10),
            Days::new(5),
            Millis::new(3000),
        ));
        assert!(
            value
                .clone()
                .resolve(&Schema::Duration(FixedSchema {
                    name: Name::try_from("TestName").expect("Name is valid").into(),
                    aliases: None,
                    doc: None,
                    size: 12,
                    attributes: BTreeMap::new()
                }))
                .is_ok()
        );
        assert!(value.resolve(&Schema::TimestampMicros).is_err());
        assert!(
            Value::Long(1i64)
                .resolve(&Schema::Duration(FixedSchema {
                    name: Name::try_from("TestName").expect("Name is valid").into(),
                    aliases: None,
                    doc: None,
                    size: 12,
                    attributes: BTreeMap::new()
                }))
                .is_err()
        );
    }

    #[test]
    fn resolve_uuid() -> TestResult {
        let value = Value::Uuid(Uuid::parse_str("1481531d-ccc9-46d9-a56f-5b67459c0537")?);
        assert!(
            value
                .clone()
                .resolve(&Schema::Uuid(UuidSchema::String))
                .is_ok()
        );
        assert!(
            value
                .clone()
                .resolve(&Schema::Uuid(UuidSchema::Bytes))
                .is_ok()
        );
        assert!(
            value
                .clone()
                .resolve(&Schema::Uuid(UuidSchema::Fixed(FixedSchema {
                    name: Name::new("some_name")?.into(),
                    aliases: None,
                    doc: None,
                    size: 16,
                    attributes: Default::default(),
                })))
                .is_ok()
        );
        assert!(value.resolve(&Schema::TimestampMicros).is_err());

        Ok(())
    }

    #[test]
    fn avro_3678_resolve_float_to_double() {
        let value = Value::Float(2345.1);
        assert!(value.resolve(&Schema::Double).is_ok());
    }

    #[test]
    fn test_avro_3621_resolve_to_nullable_union() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "root",
            "fields": [
                {
                    "name": "event",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "event",
                            "fields": [
                                {
                                    "name": "amount",
                                    "type": "int"
                                },
                                {
                                    "name": "size",
                                    "type": [
                                        "null",
                                        "int"
                                    ],
                                    "default": null
                                }
                            ]
                        }
                    ],
                    "default": null
                }
            ]
        }"#,
        )?;

        let value = Value::Record(vec![(
            "event".to_string(),
            Value::Record(vec![("amount".to_string(), Value::Int(200))]),
        )]);
        assert!(value.resolve(&schema).is_ok());

        let value = Value::Record(vec![(
            "event".to_string(),
            Value::Record(vec![("size".to_string(), Value::Int(1))]),
        )]);
        assert!(value.resolve(&schema).is_err());

        Ok(())
    }

    #[test]
    fn json_from_avro() -> TestResult {
        assert_eq!(JsonValue::try_from(Value::Null)?, JsonValue::Null);
        assert_eq!(
            JsonValue::try_from(Value::Boolean(true))?,
            JsonValue::Bool(true)
        );
        assert_eq!(
            JsonValue::try_from(Value::Int(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Long(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Float(1.0))?,
            JsonValue::Number(Number::from_f64(1.0).unwrap())
        );
        assert_eq!(
            JsonValue::try_from(Value::Double(1.0))?,
            JsonValue::Number(Number::from_f64(1.0).unwrap())
        );
        assert_eq!(
            JsonValue::try_from(Value::Bytes(vec![1, 2, 3]))?,
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into())
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::String("test".into()))?,
            JsonValue::String("test".into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Fixed(3, vec![1, 2, 3]))?,
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into())
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::Enum(1, "test_enum".into()))?,
            JsonValue::String("test_enum".into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Union(1, Box::new(Value::String("test_enum".into()))))?,
            JsonValue::String("test_enum".into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Array(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]))?,
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into())
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::Map(
                vec![
                    ("v1".to_string(), Value::Int(1)),
                    ("v2".to_string(), Value::Int(2)),
                    ("v3".to_string(), Value::Int(3))
                ]
                .into_iter()
                .collect()
            ))?,
            JsonValue::Object(
                vec![
                    ("v1".to_string(), JsonValue::Number(1.into())),
                    ("v2".to_string(), JsonValue::Number(2.into())),
                    ("v3".to_string(), JsonValue::Number(3.into()))
                ]
                .into_iter()
                .collect()
            )
        );
        assert_eq!(
            JsonValue::try_from(Value::Record(vec![
                ("v1".to_string(), Value::Int(1)),
                ("v2".to_string(), Value::Int(2)),
                ("v3".to_string(), Value::Int(3))
            ]))?,
            JsonValue::Object(
                vec![
                    ("v1".to_string(), JsonValue::Number(1.into())),
                    ("v2".to_string(), JsonValue::Number(2.into())),
                    ("v3".to_string(), JsonValue::Number(3.into()))
                ]
                .into_iter()
                .collect()
            )
        );
        assert_eq!(
            JsonValue::try_from(Value::Date(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Decimal(vec![1, 2, 3].into()))?,
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into())
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::TimeMillis(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::TimeMicros(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::TimestampMillis(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::TimestampMicros(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::TimestampNanos(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::LocalTimestampMillis(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::LocalTimestampMicros(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::LocalTimestampNanos(1))?,
            JsonValue::Number(1.into())
        );
        assert_eq!(
            JsonValue::try_from(Value::Duration(
                [
                    1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8, 9u8, 10u8, 11u8, 12u8
                ]
                .into()
            ))?,
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into()),
                JsonValue::Number(4.into()),
                JsonValue::Number(5.into()),
                JsonValue::Number(6.into()),
                JsonValue::Number(7.into()),
                JsonValue::Number(8.into()),
                JsonValue::Number(9.into()),
                JsonValue::Number(10.into()),
                JsonValue::Number(11.into()),
                JsonValue::Number(12.into()),
            ])
        );
        assert_eq!(
            JsonValue::try_from(Value::Uuid(Uuid::parse_str(
                "936DA01F-9ABD-4D9D-80C7-02AF85C822A8"
            )?))?,
            JsonValue::String("936da01f-9abd-4d9d-80c7-02af85c822a8".into())
        );

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type":"Inner"
                }
            ]
        }"#,
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer = Value::Record(vec![("a".into(), inner_value1), ("b".into(), inner_value2)]);
        outer
            .resolve(&schema)
            .expect("Record definition defined in one field must be available in other field");

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_array() -> TestResult {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"array",
                        "items": {
                            "type":"record",
                            "name": "Inner",
                            "fields": [ {
                                "name":"z",
                                "type":"int"
                            }]
                        }
                    }
                },
                {
                    "name":"b",
                    "type": {
                        "type":"map",
                        "values":"Inner"
                    }
                }
            ]
        }"#,
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            ("a".into(), Value::Array(vec![inner_value1])),
            (
                "b".into(),
                Value::Map(vec![("akey".into(), inner_value2)].into_iter().collect()),
            ),
        ]);
        outer_value
            .resolve(&schema)
            .expect("Record defined in array definition must be resolvable from map");

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_map() -> TestResult {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type": {
                        "type":"map",
                        "values":"Inner"
                    }
                }
            ]
        }"#,
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            ("a".into(), inner_value1),
            (
                "b".into(),
                Value::Map(vec![("akey".into(), inner_value2)].into_iter().collect()),
            ),
        ]);
        outer_value
            .resolve(&schema)
            .expect("Record defined in record field must be resolvable from map field");

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_record_wrapper() -> TestResult {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type": {
                        "type":"record",
                        "name": "InnerWrapper",
                        "fields": [ {
                            "name":"j",
                            "type":"Inner"
                        }]
                    }
                }
            ]
        }"#,
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![(
            "j".into(),
            Value::Record(vec![("z".into(), Value::Int(6))]),
        )]);
        let outer_value =
            Value::Record(vec![("a".into(), inner_value1), ("b".into(), inner_value2)]);
        outer_value.resolve(&schema).expect("Record schema defined in field must be resolvable in Record schema defined in other field");

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_map_and_array() -> TestResult {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"map",
                        "values": {
                            "type":"record",
                            "name": "Inner",
                            "fields": [ {
                                "name":"z",
                                "type":"int"
                            }]
                        }
                    }
                },
                {
                    "name":"b",
                    "type": {
                        "type":"array",
                        "items":"Inner"
                    }
                }
            ]
        }"#,
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            (
                "a".into(),
                Value::Map(vec![("akey".into(), inner_value2)].into_iter().collect()),
            ),
            ("b".into(), Value::Array(vec![inner_value1])),
        ]);
        outer_value
            .resolve(&schema)
            .expect("Record defined in map definition must be resolvable from array");

        Ok(())
    }

    #[test]
    fn test_avro_3433_recursive_resolves_union() -> TestResult {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":["null", {
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }]
                },
                {
                    "name":"b",
                    "type":"Inner"
                }
            ]
        }"#,
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer1 = Value::Record(vec![
            ("a".into(), inner_value1),
            ("b".into(), inner_value2.clone()),
        ]);
        outer1
            .resolve(&schema)
            .expect("Record definition defined in union must be resolved in other field");
        let outer2 = Value::Record(vec![("a".into(), Value::Null), ("b".into(), inner_value2)]);
        outer2
            .resolve(&schema)
            .expect("Record definition defined in union must be resolved in other field");

        Ok(())
    }

    #[test]
    fn test_avro_3461_test_multi_level_resolve_outer_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "middle_record_name",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let inner_record = Value::Record(vec![("inner_field_1".into(), Value::Double(5.4))]);
        let middle_record_variation_1 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]);
        let middle_record_variation_2 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(1, Box::new(inner_record.clone())),
        )]);
        let outer_record_variation_1 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_2 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_1)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_3 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_2)),
            ),
            ("outer_field_2".into(), inner_record),
        ]);

        outer_record_variation_1
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_2
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_3
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");

        Ok(())
    }

    #[test]
    fn test_avro_3461_test_multi_level_resolve_middle_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "middle_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let inner_record = Value::Record(vec![("inner_field_1".into(), Value::Double(5.4))]);
        let middle_record_variation_1 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]);
        let middle_record_variation_2 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(1, Box::new(inner_record.clone())),
        )]);
        let outer_record_variation_1 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_2 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_1)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_3 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_2)),
            ),
            ("outer_field_2".into(), inner_record),
        ]);

        outer_record_variation_1
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_2
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_3
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");

        Ok(())
    }

    #[test]
    fn test_avro_3461_test_multi_level_resolve_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "namespace":"inner_namespace",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;

        let inner_record = Value::Record(vec![("inner_field_1".into(), Value::Double(5.4))]);
        let middle_record_variation_1 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]);
        let middle_record_variation_2 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(1, Box::new(inner_record.clone())),
        )]);
        let outer_record_variation_1 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_2 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_1)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_3 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_2)),
            ),
            ("outer_field_2".into(), inner_record),
        ]);

        outer_record_variation_1
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_2
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");
        outer_record_variation_3
            .resolve(&schema)
            .expect("Should be able to resolve value to the schema that is it's definition");

        Ok(())
    }

    #[test]
    fn test_avro_3460_validation_with_refs() -> TestResult {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type":"Inner"
                }
            ]
        }"#,
        )?;

        let inner_value_right = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value_wrong1 = Value::Record(vec![("z".into(), Value::Null)]);
        let inner_value_wrong2 = Value::Record(vec![("a".into(), Value::String("testing".into()))]);
        let outer1 = Value::Record(vec![
            ("a".into(), inner_value_right.clone()),
            ("b".into(), inner_value_wrong1),
        ]);

        let outer2 = Value::Record(vec![
            ("a".into(), inner_value_right),
            ("b".into(), inner_value_wrong2),
        ]);

        assert!(
            !outer1.validate(&schema),
            "field b record is invalid against the schema"
        ); // this should pass, but doesn't
        assert!(
            !outer2.validate(&schema),
            "field b record is invalid against the schema"
        ); // this should pass, but doesn't

        Ok(())
    }

    #[test]
    fn test_avro_3460_validation_with_refs_real_struct() -> TestResult {
        use serde::Serialize;

        #[derive(Serialize, Clone)]
        struct TestInner {
            z: i32,
        }

        #[derive(Serialize)]
        struct TestRefSchemaStruct1 {
            a: TestInner,
            b: String, // could be literally anything
        }

        #[derive(Serialize)]
        struct TestRefSchemaStruct2 {
            a: TestInner,
            b: i32, // could be literally anything
        }

        #[derive(Serialize)]
        struct TestRefSchemaStruct3 {
            a: TestInner,
            b: Option<TestInner>, // could be literally anything
        }

        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }
                },
                {
                    "name":"b",
                    "type":"Inner"
                }
            ]
        }"#,
        )?;

        let test_inner = TestInner { z: 3 };
        let test_outer1 = TestRefSchemaStruct1 {
            a: test_inner.clone(),
            b: "testing".into(),
        };
        let test_outer2 = TestRefSchemaStruct2 {
            a: test_inner.clone(),
            b: 24,
        };
        let test_outer3 = TestRefSchemaStruct3 {
            a: test_inner,
            b: None,
        };

        let test_outer1: Value = to_value(test_outer1)?;
        let test_outer2: Value = to_value(test_outer2)?;
        let test_outer3: Value = to_value(test_outer3)?;

        assert!(
            !test_outer1.validate(&schema),
            "field b record is invalid against the schema"
        );
        assert!(
            !test_outer2.validate(&schema),
            "field b record is invalid against the schema"
        );
        assert!(
            !test_outer3.validate(&schema),
            "field b record is invalid against the schema"
        );

        Ok(())
    }

    fn avro_3674_with_or_without_namespace(with_namespace: bool) -> TestResult {
        use serde::Serialize;

        let schema_str = r#"
        {
            "type": "record",
            "name": "NamespacedMessage",
            [NAMESPACE]
            "fields": [
                {
                    "name": "field_a",
                    "type": {
                        "type": "record",
                        "name": "NestedMessage",
                        "fields": [
                            {
                                "name": "enum_a",
                                "type": {
                                "type": "enum",
                                "name": "EnumType",
                                "symbols": ["SYMBOL_1", "SYMBOL_2"],
                                "default": "SYMBOL_1"
                                }
                            },
                            {
                                "name": "enum_b",
                                "type": "EnumType"
                            }
                        ]
                    }
                }
            ]
        }
        "#;
        let schema_str = schema_str.replace(
            "[NAMESPACE]",
            if with_namespace {
                r#""namespace": "com.domain","#
            } else {
                ""
            },
        );

        let schema = Schema::parse_str(&schema_str)?;

        #[derive(Serialize)]
        enum EnumType {
            #[serde(rename = "SYMBOL_1")]
            Symbol1,
            #[serde(rename = "SYMBOL_2")]
            Symbol2,
        }

        #[derive(Serialize)]
        struct FieldA {
            enum_a: EnumType,
            enum_b: EnumType,
        }

        #[derive(Serialize)]
        struct NamespacedMessage {
            field_a: FieldA,
        }

        let msg = NamespacedMessage {
            field_a: FieldA {
                enum_a: EnumType::Symbol2,
                enum_b: EnumType::Symbol1,
            },
        };

        let test_value: Value = to_value(msg)?;
        assert!(test_value.validate(&schema), "test_value should validate");
        assert!(
            test_value.resolve(&schema).is_ok(),
            "test_value should resolve"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3674_validate_no_namespace_resolution() -> TestResult {
        avro_3674_with_or_without_namespace(false)
    }

    #[test]
    fn test_avro_3674_validate_with_namespace_resolution() -> TestResult {
        avro_3674_with_or_without_namespace(true)
    }

    fn avro_3688_schema_resolution_panic(set_field_b: bool) -> TestResult {
        use serde::{Deserialize, Serialize};

        let schema_str = r#"{
            "type": "record",
            "name": "Message",
            "fields": [
                {
                    "name": "field_a",
                    "type": [
                        "null",
                        {
                            "name": "Inner",
                            "type": "record",
                            "fields": [
                                {
                                    "name": "inner_a",
                                    "type": "string"
                                }
                            ]
                        }
                    ],
                    "default": null
                },
                {
                    "name": "field_b",
                    "type": [
                        "null",
                        "Inner"
                    ],
                    "default": null
                }
            ]
        }"#;

        #[derive(Serialize, Deserialize)]
        struct Inner {
            inner_a: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Message {
            field_a: Option<Inner>,
            field_b: Option<Inner>,
        }

        let schema = Schema::parse_str(schema_str)?;

        let msg = Message {
            field_a: Some(Inner {
                inner_a: "foo".to_string(),
            }),
            field_b: if set_field_b {
                Some(Inner {
                    inner_a: "bar".to_string(),
                })
            } else {
                None
            },
        };

        let test_value: Value = to_value(msg)?;
        assert!(test_value.validate(&schema), "test_value should validate");
        assert!(
            test_value.resolve(&schema).is_ok(),
            "test_value should resolve"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3688_field_b_not_set() -> TestResult {
        avro_3688_schema_resolution_panic(false)
    }

    #[test]
    fn test_avro_3688_field_b_set() -> TestResult {
        avro_3688_schema_resolution_panic(true)
    }

    #[test]
    fn test_avro_3764_use_resolve_schemata() -> TestResult {
        let referenced_schema =
            r#"{"name": "enumForReference", "type": "enum", "symbols": ["A", "B"]}"#;
        let main_schema = r#"{"name": "recordWithReference", "type": "record", "fields": [{"name": "reference", "type": "enumForReference"}]}"#;

        let value: serde_json::Value = serde_json::from_str(
            r#"
            {
                "reference": "A"
            }
        "#,
        )?;

        let avro_value = Value::try_from(value)?;

        let [rs] = ResolvedSchema::resolve().additional(vec![referenced_schema])?.build_array([main_schema])?;

        let resolve_result = avro_value.clone().resolve_against_resolved(rs);

        assert!(
            resolve_result.is_ok(),
            "result of resolving with schemata should be ok, got: {resolve_result:?}"
        );

        let resolve_result = avro_value.resolve(&Schema::parse_str(&main_schema)?);
        assert!(
            resolve_result.is_err(),
            "result of resolving without schemata should be err, got: {resolve_result:?}"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3767_union_resolve_complex_refs() -> TestResult {
        let referenced_enum =
            r#"{"name": "enumForReference", "type": "enum", "symbols": ["A", "B"]}"#;
        let referenced_record = r#"{"name": "recordForReference", "type": "record", "fields": [{"name": "refInRecord", "type": "enumForReference"}]}"#;
        let main_schema = r#"{"name": "recordWithReference", "type": "record", "fields": [{"name": "reference", "type": ["null", "recordForReference"]}]}"#;

        let value: serde_json::Value = serde_json::from_str(
            r#"
            {
                "reference": {
                    "refInRecord": "A"
                }
            }
        "#,
        )?;

        let avro_value = Value::try_from(value)?;

        let [rs] = ResolvedSchema::resolve().additional(vec![referenced_enum, referenced_record])?.build_array([main_schema])?;

        let resolve_result = avro_value.resolve_against_resolved(rs.clone())?;

        assert!(
            resolve_result.validate_against_resolved(&rs),
            "result of validation with schemata should be true"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3782_incorrect_decimal_resolving() -> TestResult {
        let schema = r#"{"name": "decimalSchema", "logicalType": "decimal", "type": "fixed", "precision": 8, "scale": 0, "size": 8}"#;

        let avro_value = Value::Decimal(Decimal::from(
            BigInt::from(12345678u32).to_signed_bytes_be(),
        ));
        let schema = Schema::parse_str(schema)?;
        let resolve_result = avro_value.resolve(&schema);
        assert!(
            resolve_result.is_ok(),
            "resolve result must be ok, got: {resolve_result:?}"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3779_bigdecimal_resolving() -> TestResult {
        let schema =
            r#"{"name": "bigDecimalSchema", "logicalType": "big-decimal", "type": "bytes" }"#;

        let avro_value = Value::BigDecimal(BigDecimal::from(12345678u32));
        let schema = Schema::parse_str(schema)?;
        let resolve_result: AvroResult<Value> = avro_value.resolve(&schema);
        assert!(
            resolve_result.is_ok(),
            "resolve result must be ok, got: {resolve_result:?}"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3892_resolve_fixed_from_bytes() -> TestResult {
        let value = Value::Bytes(vec![97, 98, 99]);
        assert_eq!(
            value.resolve(&Schema::Fixed(FixedSchema {
                name: Arc::new("test".try_into()?),
                aliases: None,
                doc: None,
                size: 3,
                attributes: Default::default()
            }))?,
            Value::Fixed(3, vec![97, 98, 99])
        );

        let value = Value::Bytes(vec![97, 99]);
        assert!(
            value
                .resolve(&Schema::Fixed(FixedSchema {
                    name: Arc::new("test".try_into()?),
                    aliases: None,
                    doc: None,
                    size: 3,
                    attributes: Default::default()
                }))
                .is_err(),
        );

        let value = Value::Bytes(vec![97, 98, 99, 100]);
        assert!(
            value
                .resolve(&Schema::Fixed(FixedSchema {
                    name: Arc::new("test".try_into()?),
                    aliases: None,
                    doc: None,
                    size: 3,
                    attributes: Default::default()
                }))
                .is_err(),
        );

        Ok(())
    }

    #[test]
    fn avro_3928_from_serde_value_to_types_value() -> TestResult {
        assert_eq!(Value::try_from(serde_json::Value::Null)?, Value::Null);
        assert_eq!(Value::try_from(json!(true))?, Value::Boolean(true));
        assert_eq!(Value::try_from(json!(false))?, Value::Boolean(false));
        assert_eq!(Value::try_from(json!(0))?, Value::Int(0));
        assert_eq!(Value::try_from(json!(i32::MIN))?, Value::Int(i32::MIN));
        assert_eq!(Value::try_from(json!(i32::MAX))?, Value::Int(i32::MAX));
        assert_eq!(
            Value::try_from(json!(i32::MIN as i64 - 1))?,
            Value::Long(i32::MIN as i64 - 1)
        );
        assert_eq!(
            Value::try_from(json!(i32::MAX as i64 + 1))?,
            Value::Long(i32::MAX as i64 + 1)
        );
        assert_eq!(Value::try_from(json!(1.23))?, Value::Double(1.23));
        assert_eq!(Value::try_from(json!(-1.23))?, Value::Double(-1.23));
        assert_eq!(
            Value::try_from(json!(u64::MIN))?,
            Value::Int(u64::MIN as i32)
        );
        assert_eq!(
            Value::try_from(json!("some text"))?,
            Value::String("some text".into())
        );
        assert_eq!(
            Value::try_from(json!(["text1", "text2", "text3"]))?,
            Value::Array(vec![
                Value::String("text1".into()),
                Value::String("text2".into()),
                Value::String("text3".into())
            ])
        );
        assert_eq!(
            Value::try_from(json!({"key1": "value1", "key2": "value2"}))?,
            Value::Map(
                vec![
                    ("key1".into(), Value::String("value1".into())),
                    ("key2".into(), Value::String("value2".into()))
                ]
                .into_iter()
                .collect()
            )
        );
        Ok(())
    }

    #[test]
    fn avro_4024_resolve_double_from_unknown_string_err() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "double"}"#)?;
        let value = Value::String("unknown".to_owned());
        match value.resolve(&schema).map_err(Error::into_details) {
            Err(err @ Details::GetDouble(_)) => {
                assert_eq!(
                    format!("{err:?}"),
                    r#"Expected Value::Double, Value::Float, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: String("unknown")"#
                );
            }
            other => {
                panic!("Expected Details::GetDouble, got {other:?}");
            }
        }
        Ok(())
    }

    #[test]
    fn avro_4024_resolve_float_from_unknown_string_err() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "float"}"#)?;
        let value = Value::String("unknown".to_owned());
        match value.resolve(&schema).map_err(Error::into_details) {
            Err(err @ Details::GetFloat(_)) => {
                assert_eq!(
                    format!("{err:?}"),
                    r#"Expected Value::Float, Value::Double, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: String("unknown")"#
                );
            }
            other => {
                panic!("Expected Details::GetFloat, got {other:?}");
            }
        }
        Ok(())
    }

    #[test]
    fn avro_4029_resolve_from_unsupported_err() -> TestResult {
        let data: Vec<(&str, Value, &str)> = vec![
            (
                r#"{ "name": "NAME", "type": "int" }"#,
                Value::Float(123_f32),
                "Expected Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "fixed", "size": 3 }"#,
                Value::Float(123_f32),
                "String expected for fixed, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "bytes" }"#,
                Value::Float(123_f32),
                "Expected Value::Bytes, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "string", "logicalType": "uuid" }"#,
                Value::String("abc-1234".into()),
                "Failed to convert &str to UUID: invalid group count: expected 5, found 2",
            ),
            (
                r#"{ "name": "NAME", "type": "string", "logicalType": "uuid" }"#,
                Value::Float(123_f32),
                "Expected Value::Uuid, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "bytes", "logicalType": "big-decimal" }"#,
                Value::Float(123_f32),
                "Expected Value::BigDecimal, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "fixed", "size": 12, "logicalType": "duration" }"#,
                Value::Float(123_f32),
                "Expected Value::Duration or Value::Fixed(12), got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 3 }"#,
                Value::Float(123_f32),
                "Expected Value::Decimal, Value::Bytes or Value::Fixed, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "bytes" }"#,
                Value::Array(vec![Value::Long(256_i64)]),
                "Unable to convert to u8, got Int(256)",
            ),
            (
                r#"{ "name": "NAME", "type": "int", "logicalType": "date" }"#,
                Value::Float(123_f32),
                "Expected Value::Date or Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "int", "logicalType": "time-millis" }"#,
                Value::Float(123_f32),
                "Expected Value::TimeMillis or Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "long", "logicalType": "time-micros" }"#,
                Value::Float(123_f32),
                "Expected Value::TimeMicros, Value::Long or Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "long", "logicalType": "timestamp-millis" }"#,
                Value::Float(123_f32),
                "Expected Value::TimestampMillis, Value::Long or Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "long", "logicalType": "timestamp-micros" }"#,
                Value::Float(123_f32),
                "Expected Value::TimestampMicros, Value::Long or Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "long", "logicalType": "timestamp-nanos" }"#,
                Value::Float(123_f32),
                "Expected Value::TimestampNanos, Value::Long or Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "long", "logicalType": "local-timestamp-millis" }"#,
                Value::Float(123_f32),
                "Expected Value::LocalTimestampMillis, Value::Long or Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "long", "logicalType": "local-timestamp-micros" }"#,
                Value::Float(123_f32),
                "Expected Value::LocalTimestampMicros, Value::Long or Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "long", "logicalType": "local-timestamp-nanos" }"#,
                Value::Float(123_f32),
                "Expected Value::LocalTimestampNanos, Value::Long or Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "null" }"#,
                Value::Float(123_f32),
                "Expected Value::Null, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "boolean" }"#,
                Value::Float(123_f32),
                "Expected Value::Boolean, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "int" }"#,
                Value::Float(123_f32),
                "Expected Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "long" }"#,
                Value::Float(123_f32),
                "Expected Value::Long or Value::Int, got: Float(123.0)",
            ),
            (
                r#"{ "name": "NAME", "type": "float" }"#,
                Value::Boolean(false),
                r#"Expected Value::Float, Value::Double, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: Boolean(false)"#,
            ),
            (
                r#"{ "name": "NAME", "type": "double" }"#,
                Value::Boolean(false),
                r#"Expected Value::Double, Value::Float, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: Boolean(false)"#,
            ),
            (
                r#"{ "name": "NAME", "type": "string" }"#,
                Value::Boolean(false),
                "Expected Value::String, Value::Bytes or Value::Fixed, got: Boolean(false)",
            ),
            (
                r#"{ "name": "NAME", "type": "enum", "symbols": ["one", "two"] }"#,
                Value::Boolean(false),
                "Expected Value::Enum, got: Boolean(false)",
            ),
        ];

        for (schema_str, value, expected_error) in data {
            let schema = Schema::parse_str(schema_str)?;
            match value.resolve(&schema) {
                Err(error) => {
                    assert_eq!(format!("{error}"), expected_error);
                }
                other => {
                    panic!("Expected '{expected_error}', got {other:?}");
                }
            }
        }
        Ok(())
    }

    #[test]
    fn avro_rs_130_get_from_record() -> TestResult {
        let schema = r#"
        {
            "type": "record",
            "name": "NamespacedMessage",
            "namespace": "space",
            "fields": [
                {
                    "name": "foo",
                    "type": "string"
                },
                {
                    "name": "bar",
                    "type": "long"
                }
            ]
        }
        "#;

        let schema = Schema::parse_str(schema)?;
        let mut record = Record::new(&schema).unwrap();
        record.put("foo", "hello");
        record.put("bar", 123_i64);

        assert_eq!(
            record.get("foo").unwrap(),
            &Value::String("hello".to_string())
        );
        assert_eq!(record.get("bar").unwrap(), &Value::Long(123));

        // also make sure it doesn't fail but return None for non-existing field
        assert_eq!(record.get("baz"), None);

        Ok(())
    }

    #[test]
    fn avro_rs_392_resolve_long_to_int() {
        // Values that are valid as in i32 should work
        let value = Value::Long(0);
        value.resolve(&Schema::Int).unwrap();
        // Values that are outside the i32 range should not
        let value = Value::Long(i64::MAX);
        assert!(matches!(
            value.resolve(&Schema::Int).unwrap_err().details(),
            Details::ZagI32(_, _)
        ));
    }

    #[test]
    fn avro_rs_450_serde_json_number_u64_max() {
        assert_eq!(
            Value::try_from(json!(u64::MAX))
                .unwrap_err()
                .into_details()
                .to_string(),
            "JSON number 18446744073709551615 could not be converted into an Avro value as it's too large"
        );
    }
}
