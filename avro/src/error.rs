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

use crate::{
    schema::{Name, Schema, SchemaKind, UnionSchema},
    types::{Value, ValueKind},
};
use std::{error::Error as _, fmt};

/// Errors encounterd by Avro.
///
/// To inspect the details of the error use [`details`](Self::details) or [`into_details`](Self::into_details)
/// to get a [`Details`] which contains more precise error information.
///
/// See [`Details`] for all possible errors.
#[derive(thiserror::Error, Debug)]
#[repr(transparent)]
#[error(transparent)]
pub struct Error {
    details: Box<Details>,
}

impl Error {
    pub fn new(details: Details) -> Self {
        Self {
            details: Box::new(details),
        }
    }

    pub fn details(&self) -> &Details {
        &self.details
    }

    pub fn into_details(self) -> Details {
        *self.details
    }
}

/// Functions for constructing a specific error type.
#[allow(non_snake_case, reason = "Want to mimic the `Details` variants")]
impl Error {
    /// Construct a new [`Error`] with a [`Details::SnappyCrc32`].
    pub fn SnappyCrc32(expected: u32, actual: u32) -> Self {
        Self {
            details: Box::new(Details::SnappyCrc32 { expected, actual }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::BoolValue`].
    pub fn BoolValue(value: u8) -> Self {
        Self {
            details: Box::new(Details::BoolValue(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::FixedValue`].
    pub fn FixedValue(value: Value) -> Self {
        Self {
            details: Box::new(Details::FixedValue(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::BytesValue`].
    pub fn BytesValue(value: Value) -> Self {
        Self {
            details: Box::new(Details::BytesValue(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetUuidFromStringValue`].
    pub fn GetUuidFromStringValue(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetUuidFromStringValue(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::NameCollision`].
    pub fn NameCollision(value: String) -> Self {
        Self {
            details: Box::new(Details::NameCollision(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ResolveDecimalSchema`].
    pub fn ResolveDecimalSchema(value: SchemaKind) -> Self {
        Self {
            details: Box::new(Details::ResolveDecimalSchema(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertToUtf8`].
    pub fn ConvertToUtf8(value: std::string::FromUtf8Error) -> Self {
        Self {
            details: Box::new(Details::ConvertToUtf8(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertToUtf8Error`].
    pub fn ConvertToUtf8Error(value: std::str::Utf8Error) -> Self {
        Self {
            details: Box::new(Details::ConvertToUtf8Error(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::Validation`].
    pub fn Validation() -> Self {
        Self {
            details: Box::new(Details::Validation),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ValidationWithReason`].
    pub fn ValidationWithReason(value: Value, schema: Schema, reason: String) -> Self {
        Self {
            details: Box::new(Details::ValidationWithReason {
                value,
                schema,
                reason,
            }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::MemoryAllocation`].
    pub fn MemoryAllocation(desired: usize, maximum: usize) -> Self {
        Self {
            details: Box::new(Details::MemoryAllocation { desired, maximum }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::SignExtend`].
    pub fn SignExtend(requested: usize, needed: usize) -> Self {
        Self {
            details: Box::new(Details::SignExtend { requested, needed }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadBoolean`].
    pub fn ReadBoolean(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadBoolean(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadBytes`].
    pub fn ReadBytes(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadBytes(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadString`].
    pub fn ReadString(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadString(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadDouble`].
    pub fn ReadDouble(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadDouble(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadFloat`].
    pub fn ReadFloat(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadFloat(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadDuration`].
    pub fn ReadDuration(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadDuration(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadFixed`].
    pub fn ReadFixed(one: std::io::Error, two: usize) -> Self {
        Self {
            details: Box::new(Details::ReadFixed(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertStrToUuid`].
    pub fn ConvertStrToUuid(value: uuid::Error) -> Self {
        Self {
            details: Box::new(Details::ConvertStrToUuid(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertFixedToUuid`].
    pub fn ConvertFixedToUuid(value: usize) -> Self {
        Self {
            details: Box::new(Details::ConvertFixedToUuid(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertSliceToUuid`].
    pub fn ConvertSliceToUuid(value: uuid::Error) -> Self {
        Self {
            details: Box::new(Details::ConvertSliceToUuid(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::MapKeyType`].
    pub fn MapKeyType(value: ValueKind) -> Self {
        Self {
            details: Box::new(Details::MapKeyType(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetUnionVariant`].
    pub fn GetUnionVariant(index: i64, num_variants: usize) -> Self {
        Self {
            details: Box::new(Details::GetUnionVariant {
                index,
                num_variants,
            }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::EnumSymbolIndex`].
    pub fn EnumSymbolIndex(index: usize, num_variants: usize) -> Self {
        Self {
            details: Box::new(Details::EnumSymbolIndex {
                index,
                num_variants,
            }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetEnumSymbol`].
    pub fn GetEnumSymbol(value: String) -> Self {
        Self {
            details: Box::new(Details::GetEnumSymbol(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetEnumUnknownIndexValue`].
    pub fn GetEnumUnknownIndexValue() -> Self {
        Self {
            details: Box::new(Details::GetEnumUnknownIndexValue),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetScaleAndPrecision`].
    pub fn GetScaleAndPrecision(scale: usize, precision: usize) -> Self {
        Self {
            details: Box::new(Details::GetScaleAndPrecision { scale, precision }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetScaleWithFixedSize`].
    pub fn GetScaleWithFixedSize(size: usize, precision: usize) -> Self {
        Self {
            details: Box::new(Details::GetScaleWithFixedSize { size, precision }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetUuid`].
    pub fn GetUuid(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetUuid(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetBigDecimal`].
    pub fn GetBigDecimal(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetBigDecimal(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetDecimalFixedBytes`].
    pub fn GetDecimalFixedBytes(value: usize) -> Self {
        Self {
            details: Box::new(Details::GetDecimalFixedBytes(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ResolveDuration`].
    pub fn ResolveDuration(value: Value) -> Self {
        Self {
            details: Box::new(Details::ResolveDuration(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ResolveDecimal`].
    pub fn ResolveDecimal(value: Value) -> Self {
        Self {
            details: Box::new(Details::ResolveDecimal(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetField`].
    pub fn GetField(value: String) -> Self {
        Self {
            details: Box::new(Details::GetField(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetU8`].
    pub fn GetU8(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetU8(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ComparePrecisionAndSize`].
    pub fn ComparePrecisionAndSize(precision: usize, num_bytes: usize) -> Self {
        Self {
            details: Box::new(Details::ComparePrecisionAndSize {
                precision,
                num_bytes,
            }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertLengthToI32`].
    pub fn ConvertLengthToI32(one: std::num::TryFromIntError, two: usize) -> Self {
        Self {
            details: Box::new(Details::ConvertLengthToI32(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetDate`].
    pub fn GetDate(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetDate(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetTimeMillis`].
    pub fn GetTimeMillis(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetTimeMillis(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetTimeMicros`].
    pub fn GetTimeMicros(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetTimeMicros(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetTimestampMillis`].
    pub fn GetTimestampMillis(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetTimestampMillis(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetTimestampMicros`].
    pub fn GetTimestampMicros(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetTimestampMicros(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetTimestampNanos`].
    pub fn GetTimestampNanos(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetTimestampNanos(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetLocalTimestampMillis`].
    pub fn GetLocalTimestampMillis(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetLocalTimestampMillis(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetLocalTimestampMicros`].
    pub fn GetLocalTimestampMicros(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetLocalTimestampMicros(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetLocalTimestampNanos`].
    pub fn GetLocalTimestampNanos(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetLocalTimestampNanos(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetNull`].
    pub fn GetNull(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetNull(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetBoolean`].
    pub fn GetBoolean(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetBoolean(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetInt`].
    pub fn GetInt(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetInt(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetLong`].
    pub fn GetLong(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetLong(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetDouble`].
    pub fn GetDouble(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetDouble(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetFloat`].
    pub fn GetFloat(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetFloat(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetBytes`].
    pub fn GetBytes(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetBytes(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetString`].
    pub fn GetString(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetString(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetEnum`].
    pub fn GetEnum(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetEnum(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::CompareFixedSizes`].
    pub fn CompareFixedSizes(size: usize, n: usize) -> Self {
        Self {
            details: Box::new(Details::CompareFixedSizes { size, n }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetStringForFixed`].
    pub fn GetStringForFixed(value: Value) -> Self {
        Self {
            details: Box::new(Details::GetStringForFixed(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetEnumDefault`].
    pub fn GetEnumDefault(symbol: String, symbols: Vec<String>) -> Self {
        Self {
            details: Box::new(Details::GetEnumDefault { symbol, symbols }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetEnumValue`].
    pub fn GetEnumValue(index: usize, nsymbols: usize) -> Self {
        Self {
            details: Box::new(Details::GetEnumValue { index, nsymbols }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetDecimalMetadataFromJson`].
    pub fn GetDecimalMetadataFromJson(value: &'static str) -> Self {
        Self {
            details: Box::new(Details::GetDecimalMetadataFromJson(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::FindUnionVariant`].
    pub fn FindUnionVariant(schema: UnionSchema, value: Value) -> Self {
        Self {
            details: Box::new(Details::FindUnionVariant { schema, value }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::EmptyUnion`].
    pub fn EmptyUnion() -> Self {
        Self {
            details: Box::new(Details::EmptyUnion),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetArray`].
    pub fn GetArray(expected: SchemaKind, other: Value) -> Self {
        Self {
            details: Box::new(Details::GetArray { expected, other }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetMap`].
    pub fn GetMap(expected: SchemaKind, other: Value) -> Self {
        Self {
            details: Box::new(Details::GetMap { expected, other }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetRecord`].
    pub fn GetRecord(expected: Vec<(String, SchemaKind)>, other: Value) -> Self {
        Self {
            details: Box::new(Details::GetRecord { expected, other }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetNameField`].
    pub fn GetNameField() -> Self {
        Self {
            details: Box::new(Details::GetNameField),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetNameFieldFromRecord`].
    pub fn GetNameFieldFromRecord() -> Self {
        Self {
            details: Box::new(Details::GetNameFieldFromRecord),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetNestedUnion`].
    pub fn GetNestedUnion() -> Self {
        Self {
            details: Box::new(Details::GetNestedUnion),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetUnionDuplicate`].
    pub fn GetUnionDuplicate() -> Self {
        Self {
            details: Box::new(Details::GetUnionDuplicate),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetDefaultUnion`].
    pub fn GetDefaultUnion(one: SchemaKind, two: ValueKind) -> Self {
        Self {
            details: Box::new(Details::GetDefaultUnion(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetDefaultRecordField`].
    pub fn GetDefaultRecordField(one: String, two: String, three: String) -> Self {
        Self {
            details: Box::new(Details::GetDefaultRecordField(one, two, three)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetU64FromJson`].
    pub fn GetU64FromJson(value: serde_json::Number) -> Self {
        Self {
            details: Box::new(Details::GetU64FromJson(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetI64FromJson`].
    pub fn GetI64FromJson(value: serde_json::Number) -> Self {
        Self {
            details: Box::new(Details::GetI64FromJson(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertU64ToUsize`].
    pub fn ConvertU64ToUsize(one: std::num::TryFromIntError, two: u64) -> Self {
        Self {
            details: Box::new(Details::ConvertU64ToUsize(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertU32ToUsize`].
    pub fn ConvertU32ToUsize(one: std::num::TryFromIntError, two: u32) -> Self {
        Self {
            details: Box::new(Details::ConvertU32ToUsize(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertI64ToUsize`].
    pub fn ConvertI64ToUsize(one: std::num::TryFromIntError, two: i64) -> Self {
        Self {
            details: Box::new(Details::ConvertI64ToUsize(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertI32ToUsize`].
    pub fn ConvertI32ToUsize(one: std::num::TryFromIntError, two: i32) -> Self {
        Self {
            details: Box::new(Details::ConvertI32ToUsize(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetPrecisionOrScaleFromJson`].
    pub fn GetPrecisionOrScaleFromJson(value: serde_json::Number) -> Self {
        Self {
            details: Box::new(Details::GetPrecisionOrScaleFromJson(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ParseSchemaJson`].
    pub fn ParseSchemaJson(value: serde_json::Error) -> Self {
        Self {
            details: Box::new(Details::ParseSchemaJson(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadSchemaFromReader`].
    pub fn ReadSchemaFromReader(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadSchemaFromReader(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ParseSchemaFromValidJson`].
    pub fn ParseSchemaFromValidJson() -> Self {
        Self {
            details: Box::new(Details::ParseSchemaFromValidJson),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ParsePrimitive`].
    pub fn ParsePrimitive(value: String) -> Self {
        Self {
            details: Box::new(Details::ParsePrimitive(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetDecimalMetadataValueFromJson`].
    pub fn GetDecimalMetadataValueFromJson(key: String, value: serde_json::Value) -> Self {
        Self {
            details: Box::new(Details::GetDecimalMetadataValueFromJson { key, value }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::DecimalPrecisionLessThanScale`].
    pub fn DecimalPrecisionLessThanScale(precision: usize, scale: usize) -> Self {
        Self {
            details: Box::new(Details::DecimalPrecisionLessThanScale { precision, scale }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::DecimalPrecisionMuBePositive`].
    pub fn DecimalPrecisionMuBePositive(precision: usize) -> Self {
        Self {
            details: Box::new(Details::DecimalPrecisionMuBePositive { precision }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::BigDecimalSign`].
    pub fn BigDecimalSign() -> Self {
        Self {
            details: Box::new(Details::BigDecimalSign),
        }
    }

    /// Construct a new [`Error`] with a [`Details::BigDecimalLen`].
    pub fn BigDecimalLen(value: Self) -> Self {
        Self {
            details: Box::new(Details::BigDecimalLen(Box::new(value))),
        }
    }

    /// Construct a new [`Error`] with a [`Details::BigDecimalScale`].
    pub fn BigDecimalScale() -> Self {
        Self {
            details: Box::new(Details::BigDecimalScale),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetLogicalTypeVariant`].
    pub fn GetLogicalTypeVariant(value: serde_json::Value) -> Self {
        Self {
            details: Box::new(Details::GetLogicalTypeVariant(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetLogicalTypeField`].
    pub fn GetLogicalTypeField() -> Self {
        Self {
            details: Box::new(Details::GetLogicalTypeField),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetLogicalTypeFieldType`].
    pub fn GetLogicalTypeFieldType(value: serde_json::Value) -> Self {
        Self {
            details: Box::new(Details::GetLogicalTypeFieldType(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetComplexType`].
    pub fn GetComplexType(value: serde_json::Value) -> Self {
        Self {
            details: Box::new(Details::GetComplexType(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetComplexTypeField`].
    pub fn GetComplexTypeField() -> Self {
        Self {
            details: Box::new(Details::GetComplexTypeField),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetRecordFieldsJson`].
    pub fn GetRecordFieldsJson() -> Self {
        Self {
            details: Box::new(Details::GetRecordFieldsJson),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetEnumSymbolsField`].
    pub fn GetEnumSymbolsField() -> Self {
        Self {
            details: Box::new(Details::GetEnumSymbolsField),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetEnumSymbols`].
    pub fn GetEnumSymbols() -> Self {
        Self {
            details: Box::new(Details::GetEnumSymbols),
        }
    }

    /// Construct a new [`Error`] with a [`Details::EnumSymbolName`].
    pub fn EnumSymbolName(value: String) -> Self {
        Self {
            details: Box::new(Details::EnumSymbolName(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::FieldName`].
    pub fn FieldName(value: String) -> Self {
        Self {
            details: Box::new(Details::FieldName(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::FieldNameDuplicate`].
    pub fn FieldNameDuplicate(value: String) -> Self {
        Self {
            details: Box::new(Details::FieldNameDuplicate(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::InvalidSchemaName`].
    pub fn InvalidSchemaName(one: String, two: &'static str) -> Self {
        Self {
            details: Box::new(Details::InvalidSchemaName(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::InvalidNamespace`].
    pub fn InvalidNamespace(one: String, two: &'static str) -> Self {
        Self {
            details: Box::new(Details::InvalidNamespace(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::InvalidSchemaRecord`].
    pub fn InvalidSchemaRecord(value: String) -> Self {
        Self {
            details: Box::new(Details::InvalidSchemaRecord(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::EnumSymbolDuplicate`].
    pub fn EnumSymbolDuplicate(value: String) -> Self {
        Self {
            details: Box::new(Details::EnumSymbolDuplicate(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::EnumDefaultWrongType`].
    pub fn EnumDefaultWrongType(value: serde_json::Value) -> Self {
        Self {
            details: Box::new(Details::EnumDefaultWrongType(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetArrayItemsField`].
    pub fn GetArrayItemsField() -> Self {
        Self {
            details: Box::new(Details::GetArrayItemsField),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetMapValuesField`].
    pub fn GetMapValuesField() -> Self {
        Self {
            details: Box::new(Details::GetMapValuesField),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetFixedSizeFieldPositive`].
    pub fn GetFixedSizeFieldPositive(value: serde_json::Value) -> Self {
        Self {
            details: Box::new(Details::GetFixedSizeFieldPositive(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetFixedSizeField`].
    pub fn GetFixedSizeField() -> Self {
        Self {
            details: Box::new(Details::GetFixedSizeField),
        }
    }

    /// Construct a new [`Error`] with a [`Details::FixedDefaultLenSizeMismatch`].
    pub fn FixedDefaultLenSizeMismatch(one: usize, two: u64) -> Self {
        Self {
            details: Box::new(Details::FixedDefaultLenSizeMismatch(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::DeflateCompress`].
    pub fn DeflateCompress(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::DeflateCompress(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::DeflateDecompress`].
    pub fn DeflateDecompress(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::DeflateDecompress(value)),
        }
    }

    #[cfg(feature = "snappy")]
    /// Construct a new [`Error`] with a [`Details::SnappyCompress`].
    pub fn SnappyCompress(value: snap::Error) -> Self {
        Self {
            details: Box::new(Details::SnappyCompress(value)),
        }
    }

    #[cfg(feature = "snappy")]
    /// Construct a new [`Error`] with a [`Details::GetSnappyDecompressLen`].
    pub fn GetSnappyDecompressLen(value: snap::Error) -> Self {
        Self {
            details: Box::new(Details::GetSnappyDecompressLen(value)),
        }
    }

    #[cfg(feature = "snappy")]
    /// Construct a new [`Error`] with a [`Details::SnappyDecompress`].
    pub fn SnappyDecompress(value: snap::Error) -> Self {
        Self {
            details: Box::new(Details::SnappyDecompress(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ZstdCompress`].
    pub fn ZstdCompress(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ZstdCompress(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ZstdDecompress`].
    pub fn ZstdDecompress(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ZstdDecompress(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadHeader`].
    pub fn ReadHeader(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadHeader(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::HeaderMagic`].
    pub fn HeaderMagic() -> Self {
        Self {
            details: Box::new(Details::HeaderMagic),
        }
    }

    /// Construct a new [`Error`] with a [`Details::SingleObjectHeaderMismatch`].
    pub fn SingleObjectHeaderMismatch(one: Vec<u8>, two: Vec<u8>) -> Self {
        Self {
            details: Box::new(Details::SingleObjectHeaderMismatch(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetAvroSchemaFromMap`].
    pub fn GetAvroSchemaFromMap() -> Self {
        Self {
            details: Box::new(Details::GetAvroSchemaFromMap),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetHeaderMetadata`].
    pub fn GetHeaderMetadata() -> Self {
        Self {
            details: Box::new(Details::GetHeaderMetadata),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadMarker`].
    pub fn ReadMarker(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadMarker(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadBlockMarker`].
    pub fn ReadBlockMarker(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadBlockMarker(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadIntoBuf`].
    pub fn ReadIntoBuf(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadIntoBuf(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::GetBlockMarker`].
    pub fn GetBlockMarker() -> Self {
        Self {
            details: Box::new(Details::GetBlockMarker),
        }
    }

    /// Construct a new [`Error`] with a [`Details::IntegerOverflow`].
    pub fn IntegerOverflow() -> Self {
        Self {
            details: Box::new(Details::IntegerOverflow),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadVariableIntegerBytes`].
    pub fn ReadVariableIntegerBytes(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::ReadVariableIntegerBytes(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ZagI32`].
    pub fn ZagI32(one: std::num::TryFromIntError, two: i64) -> Self {
        Self {
            details: Box::new(Details::ZagI32(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ReadBlock`].
    pub fn ReadBlock() -> Self {
        Self {
            details: Box::new(Details::ReadBlock),
        }
    }

    /// Construct a new [`Error`] with a [`Details::SerializeValue`].
    pub fn SerializeValue(value: String) -> Self {
        Self {
            details: Box::new(Details::SerializeValue(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::SerializeValueWithSchema`].
    pub fn SerializeValueWithSchema(
        value_type: &'static str,
        value: String,
        schema: Schema,
    ) -> Self {
        Self {
            details: Box::new(Details::SerializeValueWithSchema {
                value_type,
                value,
                schema,
            }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::SerializeRecordFieldWithSchema`].
    pub fn SerializeRecordFieldWithSchema(
        field_name: &'static str,
        record_schema: Schema,
        error: Box<Error>,
    ) -> Self {
        Self {
            details: Box::new(Details::SerializeRecordFieldWithSchema {
                field_name,
                record_schema,
                error,
            }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::DeserializeValue`].
    pub fn DeserializeValue(value: String) -> Self {
        Self {
            details: Box::new(Details::DeserializeValue(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::WriteBytes`].
    pub fn WriteBytes(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::WriteBytes(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::FlushWriter`].
    pub fn FlushWriter(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::FlushWriter(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::WriteMarker`].
    pub fn WriteMarker(value: std::io::Error) -> Self {
        Self {
            details: Box::new(Details::WriteMarker(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertJsonToString`].
    pub fn ConvertJsonToString(value: serde_json::Error) -> Self {
        Self {
            details: Box::new(Details::ConvertJsonToString(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::ConvertF64ToJson`].
    pub fn ConvertF64ToJson(value: f64) -> Self {
        Self {
            details: Box::new(Details::ConvertF64ToJson(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::SchemaResolutionError`].
    pub fn SchemaResolutionError(value: Name) -> Self {
        Self {
            details: Box::new(Details::SchemaResolutionError(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::FileHeaderAlreadyWritten`].
    pub fn FileHeaderAlreadyWritten() -> Self {
        Self {
            details: Box::new(Details::FileHeaderAlreadyWritten),
        }
    }

    /// Construct a new [`Error`] with a [`Details::InvalidMetadataKey`].
    pub fn InvalidMetadataKey(value: String) -> Self {
        Self {
            details: Box::new(Details::InvalidMetadataKey(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::AmbiguousSchemaDefinition`].
    pub fn AmbiguousSchemaDefinition(value: Name) -> Self {
        Self {
            details: Box::new(Details::AmbiguousSchemaDefinition(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::EncodeDecimalAsFixedError`].
    pub fn EncodeDecimalAsFixedError(one: usize, two: usize) -> Self {
        Self {
            details: Box::new(Details::EncodeDecimalAsFixedError(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::NoEntryInLookupTable`].
    pub fn NoEntryInLookupTable(one: String, two: String) -> Self {
        Self {
            details: Box::new(Details::NoEntryInLookupTable(one, two)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::EncodeValueAsSchemaError`].
    pub fn EncodeValueAsSchemaError(
        value_kind: ValueKind,
        supported_schema: Vec<SchemaKind>,
    ) -> Self {
        Self {
            details: Box::new(Details::EncodeValueAsSchemaError {
                value_kind,
                supported_schema,
            }),
        }
    }

    /// Construct a new [`Error`] with a [`Details::IllegalSingleObjectWriterState`].
    pub fn IllegalSingleObjectWriterState() -> Self {
        Self {
            details: Box::new(Details::IllegalSingleObjectWriterState),
        }
    }

    /// Construct a new [`Error`] with a [`Details::CodecNotSupported`].
    pub fn CodecNotSupported(value: String) -> Self {
        Self {
            details: Box::new(Details::CodecNotSupported(value)),
        }
    }

    /// Construct a new [`Error`] with a [`Details::BadCodecMetadata`].
    pub fn BadCodecMetadata() -> Self {
        Self {
            details: Box::new(Details::BadCodecMetadata),
        }
    }

    /// Construct a new [`Error`] with a [`Details::UuidFromSlice`].
    pub fn UuidFromSlice(value: uuid::Error) -> Self {
        Self {
            details: Box::new(Details::UuidFromSlice(value)),
        }
    }
}

impl From<Details> for Error {
    fn from(details: Details) -> Self {
        Self::new(details)
    }
}

impl serde::ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::new(<Details as serde::ser::Error>::custom(msg))
    }
}

impl serde::de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::new(<Details as serde::de::Error>::custom(msg))
    }
}

#[derive(thiserror::Error)]
pub enum Details {
    #[error("Bad Snappy CRC32; expected {expected:x} but got {actual:x}")]
    SnappyCrc32 { expected: u32, actual: u32 },

    #[error("Invalid u8 for bool: {0}")]
    BoolValue(u8),

    #[error("Not a fixed value, required for decimal with fixed schema: {0:?}")]
    FixedValue(Value),

    #[error("Not a bytes value, required for decimal with bytes schema: {0:?}")]
    BytesValue(Value),

    #[error("Not a string value, required for uuid: {0:?}")]
    GetUuidFromStringValue(Value),

    #[error("Two schemas with the same fullname were given: {0:?}")]
    NameCollision(String),

    #[error("Not a fixed or bytes type, required for decimal schema, got: {0:?}")]
    ResolveDecimalSchema(SchemaKind),

    #[error("Invalid utf-8 string")]
    ConvertToUtf8(#[source] std::string::FromUtf8Error),

    #[error("Invalid utf-8 string")]
    ConvertToUtf8Error(#[source] std::str::Utf8Error),

    /// Describes errors happened while validating Avro data.
    #[error("Value does not match schema")]
    Validation,

    /// Describes errors happened while validating Avro data.
    #[error("Value {value:?} does not match schema {schema:?}: Reason: {reason}")]
    ValidationWithReason {
        value: Value,
        schema: Schema,
        reason: String,
    },

    #[error("Unable to allocate {desired} bytes (maximum allowed: {maximum})")]
    MemoryAllocation { desired: usize, maximum: usize },

    /// Describe a specific error happening with decimal representation
    #[error(
        "Number of bytes requested for decimal sign extension {requested} is less than the number of bytes needed to decode {needed}"
    )]
    SignExtend { requested: usize, needed: usize },

    #[error("Failed to read boolean bytes: {0}")]
    ReadBoolean(#[source] std::io::Error),

    #[error("Failed to read bytes: {0}")]
    ReadBytes(#[source] std::io::Error),

    #[error("Failed to read string: {0}")]
    ReadString(#[source] std::io::Error),

    #[error("Failed to read double: {0}")]
    ReadDouble(#[source] std::io::Error),

    #[error("Failed to read float: {0}")]
    ReadFloat(#[source] std::io::Error),

    #[error("Failed to read duration: {0}")]
    ReadDuration(#[source] std::io::Error),

    #[error("Failed to read fixed number of bytes '{1}': : {0}")]
    ReadFixed(#[source] std::io::Error, usize),

    #[error("Failed to convert &str to UUID: {0}")]
    ConvertStrToUuid(#[source] uuid::Error),

    #[error("Failed to convert Fixed bytes to UUID. It must be exactly 16 bytes, got {0}")]
    ConvertFixedToUuid(usize),

    #[error("Failed to convert Fixed bytes to UUID: {0}")]
    ConvertSliceToUuid(#[source] uuid::Error),

    #[error("Map key is not a string; key type is {0:?}")]
    MapKeyType(ValueKind),

    #[error("Union index {index} out of bounds: {num_variants}")]
    GetUnionVariant { index: i64, num_variants: usize },

    #[error("Enum symbol index out of bounds: {num_variants}")]
    EnumSymbolIndex { index: usize, num_variants: usize },

    #[error("Enum symbol not found {0}")]
    GetEnumSymbol(String),

    #[error("Unable to decode enum index")]
    GetEnumUnknownIndexValue,

    #[error("Scale {scale} is greater than precision {precision}")]
    GetScaleAndPrecision { scale: usize, precision: usize },

    #[error(
        "Fixed type number of bytes {size} is not large enough to hold decimal values of precision {precision}"
    )]
    GetScaleWithFixedSize { size: usize, precision: usize },

    #[error("Expected Value::Uuid, got: {0:?}")]
    GetUuid(Value),

    #[error("Expected Value::BigDecimal, got: {0:?}")]
    GetBigDecimal(Value),

    #[error("Fixed bytes of size 12 expected, got Fixed of size {0}")]
    GetDecimalFixedBytes(usize),

    #[error("Expected Value::Duration or Value::Fixed(12), got: {0:?}")]
    ResolveDuration(Value),

    #[error("Expected Value::Decimal, Value::Bytes or Value::Fixed, got: {0:?}")]
    ResolveDecimal(Value),

    #[error("Missing field in record: {0:?}")]
    GetField(String),

    #[error("Unable to convert to u8, got {0:?}")]
    GetU8(Value),

    #[error("Precision {precision} too small to hold decimal values with {num_bytes} bytes")]
    ComparePrecisionAndSize { precision: usize, num_bytes: usize },

    #[error("Cannot convert length to i32: {1}")]
    ConvertLengthToI32(#[source] std::num::TryFromIntError, usize),

    #[error("Expected Value::Date or Value::Int, got: {0:?}")]
    GetDate(Value),

    #[error("Expected Value::TimeMillis or Value::Int, got: {0:?}")]
    GetTimeMillis(Value),

    #[error("Expected Value::TimeMicros, Value::Long or Value::Int, got: {0:?}")]
    GetTimeMicros(Value),

    #[error("Expected Value::TimestampMillis, Value::Long or Value::Int, got: {0:?}")]
    GetTimestampMillis(Value),

    #[error("Expected Value::TimestampMicros, Value::Long or Value::Int, got: {0:?}")]
    GetTimestampMicros(Value),

    #[error("Expected Value::TimestampNanos, Value::Long or Value::Int, got: {0:?}")]
    GetTimestampNanos(Value),

    #[error("Expected Value::LocalTimestampMillis, Value::Long or Value::Int, got: {0:?}")]
    GetLocalTimestampMillis(Value),

    #[error("Expected Value::LocalTimestampMicros, Value::Long or Value::Int, got: {0:?}")]
    GetLocalTimestampMicros(Value),

    #[error("Expected Value::LocalTimestampNanos, Value::Long or Value::Int, got: {0:?}")]
    GetLocalTimestampNanos(Value),

    #[error("Expected Value::Null, got: {0:?}")]
    GetNull(Value),

    #[error("Expected Value::Boolean, got: {0:?}")]
    GetBoolean(Value),

    #[error("Expected Value::Int, got: {0:?}")]
    GetInt(Value),

    #[error("Expected Value::Long or Value::Int, got: {0:?}")]
    GetLong(Value),

    #[error(r#"Expected Value::Double, Value::Float, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: {0:?}"#)]
    GetDouble(Value),

    #[error(r#"Expected Value::Float, Value::Double, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: {0:?}"#)]
    GetFloat(Value),

    #[error("Expected Value::Bytes, got: {0:?}")]
    GetBytes(Value),

    #[error("Expected Value::String, Value::Bytes or Value::Fixed, got: {0:?}")]
    GetString(Value),

    #[error("Expected Value::Enum, got: {0:?}")]
    GetEnum(Value),

    #[error("Fixed size mismatch, expected: {size}, got: {n}")]
    CompareFixedSizes { size: usize, n: usize },

    #[error("String expected for fixed, got: {0:?}")]
    GetStringForFixed(Value),

    #[error("Enum default {symbol:?} is not among allowed symbols {symbols:?}")]
    GetEnumDefault {
        symbol: String,
        symbols: Vec<String>,
    },

    #[error("Enum value index {index} is out of bounds {nsymbols}")]
    GetEnumValue { index: usize, nsymbols: usize },

    #[error("Key {0} not found in decimal metadata JSON")]
    GetDecimalMetadataFromJson(&'static str),

    #[error("Could not find matching type in {schema:?} for {value:?}")]
    FindUnionVariant { schema: UnionSchema, value: Value },

    #[error("Union type should not be empty")]
    EmptyUnion,

    #[error("Array({expected:?}) expected, got {other:?}")]
    GetArray { expected: SchemaKind, other: Value },

    #[error("Map({expected:?}) expected, got {other:?}")]
    GetMap { expected: SchemaKind, other: Value },

    #[error("Record with fields {expected:?} expected, got {other:?}")]
    GetRecord {
        expected: Vec<(String, SchemaKind)>,
        other: Value,
    },

    #[error("No `name` field")]
    GetNameField,

    #[error("No `name` in record field")]
    GetNameFieldFromRecord,

    #[error("Unions may not directly contain a union")]
    GetNestedUnion,

    #[error("Unions cannot contain duplicate types")]
    GetUnionDuplicate,

    #[error("One union type {0:?} must match the `default`'s value type {1:?}")]
    GetDefaultUnion(SchemaKind, ValueKind),

    #[error("`default`'s value type of field {0:?} in {1:?} must be {2:?}")]
    GetDefaultRecordField(String, String, String),

    #[error("JSON value {0} claims to be u64 but cannot be converted")]
    GetU64FromJson(serde_json::Number),

    #[error("JSON value {0} claims to be i64 but cannot be converted")]
    GetI64FromJson(serde_json::Number),

    #[error("Cannot convert u64 to usize: {1}")]
    ConvertU64ToUsize(#[source] std::num::TryFromIntError, u64),

    #[error("Cannot convert u32 to usize: {1}")]
    ConvertU32ToUsize(#[source] std::num::TryFromIntError, u32),

    #[error("Cannot convert i64 to usize: {1}")]
    ConvertI64ToUsize(#[source] std::num::TryFromIntError, i64),

    #[error("Cannot convert i32 to usize: {1}")]
    ConvertI32ToUsize(#[source] std::num::TryFromIntError, i32),

    #[error("Invalid JSON value for decimal precision/scale integer: {0}")]
    GetPrecisionOrScaleFromJson(serde_json::Number),

    #[error("Failed to parse schema from JSON")]
    ParseSchemaJson(#[source] serde_json::Error),

    #[error("Failed to read schema")]
    ReadSchemaFromReader(#[source] std::io::Error),

    #[error("Must be a JSON string, object or array")]
    ParseSchemaFromValidJson,

    #[error("Unknown primitive type: {0}")]
    ParsePrimitive(String),

    #[error("invalid JSON for {key:?}: {value:?}")]
    GetDecimalMetadataValueFromJson {
        key: String,
        value: serde_json::Value,
    },

    #[error("The decimal precision ({precision}) must be bigger or equal to the scale ({scale})")]
    DecimalPrecisionLessThanScale { precision: usize, scale: usize },

    #[error("The decimal precision ({precision}) must be a positive number")]
    DecimalPrecisionMuBePositive { precision: usize },

    #[error("Unreadable big decimal sign")]
    BigDecimalSign,

    #[error("Unreadable length for big decimal inner bytes: {0}")]
    BigDecimalLen(#[source] Box<Error>),

    #[error("Unreadable big decimal scale")]
    BigDecimalScale,

    #[error("Unexpected `type` {0} variant for `logicalType`")]
    GetLogicalTypeVariant(serde_json::Value),

    #[error("No `type` field found for `logicalType`")]
    GetLogicalTypeField,

    #[error("logicalType must be a string, but is {0:?}")]
    GetLogicalTypeFieldType(serde_json::Value),

    #[error("Unknown complex type: {0}")]
    GetComplexType(serde_json::Value),

    #[error("No `type` in complex type")]
    GetComplexTypeField,

    #[error("No `fields` in record")]
    GetRecordFieldsJson,

    #[error("No `symbols` field in enum")]
    GetEnumSymbolsField,

    #[error("Unable to parse `symbols` in enum")]
    GetEnumSymbols,

    #[error("Invalid enum symbol name {0}")]
    EnumSymbolName(String),

    #[error("Invalid field name {0}")]
    FieldName(String),

    #[error("Duplicate field name {0}")]
    FieldNameDuplicate(String),

    #[error("Invalid schema name {0}. It must match the regex '{1}'")]
    InvalidSchemaName(String, &'static str),

    #[error("Invalid namespace {0}. It must match the regex '{1}'")]
    InvalidNamespace(String, &'static str),

    #[error(
        "Invalid schema: There is no type called '{0}', if you meant to define a non-primitive schema, it should be defined inside `type` attribute. Please review the specification"
    )]
    InvalidSchemaRecord(String),

    #[error("Duplicate enum symbol {0}")]
    EnumSymbolDuplicate(String),

    #[error("Default value for enum must be a string! Got: {0}")]
    EnumDefaultWrongType(serde_json::Value),

    #[error("No `items` in array")]
    GetArrayItemsField,

    #[error("No `values` in map")]
    GetMapValuesField,

    #[error("Fixed schema `size` value must be a positive integer: {0}")]
    GetFixedSizeFieldPositive(serde_json::Value),

    #[error("Fixed schema has no `size`")]
    GetFixedSizeField,

    #[error("Fixed schema's default value length ({0}) does not match its size ({1})")]
    FixedDefaultLenSizeMismatch(usize, u64),

    #[error("Failed to compress with flate: {0}")]
    DeflateCompress(#[source] std::io::Error),

    // no longer possible after migration from libflate to miniz_oxide
    #[deprecated(since = "0.19.0", note = "This error can no longer occur")]
    #[error("Failed to finish flate compressor: {0}")]
    DeflateCompressFinish(#[source] std::io::Error),

    #[error("Failed to decompress with flate: {0}")]
    DeflateDecompress(#[source] std::io::Error),

    #[cfg(feature = "snappy")]
    #[error("Failed to compress with snappy: {0}")]
    SnappyCompress(#[source] snap::Error),

    #[cfg(feature = "snappy")]
    #[error("Failed to get snappy decompression length: {0}")]
    GetSnappyDecompressLen(#[source] snap::Error),

    #[cfg(feature = "snappy")]
    #[error("Failed to decompress with snappy: {0}")]
    SnappyDecompress(#[source] snap::Error),

    #[error("Failed to compress with zstd: {0}")]
    ZstdCompress(#[source] std::io::Error),

    #[error("Failed to decompress with zstd: {0}")]
    ZstdDecompress(#[source] std::io::Error),

    #[error("Failed to read header: {0}")]
    ReadHeader(#[source] std::io::Error),

    #[error("wrong magic in header")]
    HeaderMagic,

    #[error("Message Header mismatch. Expected: {0:?}. Actual: {1:?}")]
    SingleObjectHeaderMismatch(Vec<u8>, Vec<u8>),

    #[error("Failed to get JSON from avro.schema key in map")]
    GetAvroSchemaFromMap,

    #[error("no metadata in header")]
    GetHeaderMetadata,

    #[error("Failed to read marker bytes: {0}")]
    ReadMarker(#[source] std::io::Error),

    #[error("Failed to read block marker bytes: {0}")]
    ReadBlockMarker(#[source] std::io::Error),

    #[error("Read into buffer failed: {0}")]
    ReadIntoBuf(#[source] std::io::Error),

    #[error("block marker does not match header marker")]
    GetBlockMarker,

    #[error("Overflow when decoding integer value")]
    IntegerOverflow,

    #[error("Failed to read bytes for decoding variable length integer: {0}")]
    ReadVariableIntegerBytes(#[source] std::io::Error),

    #[error("Decoded integer out of range for i32: {1}: {0}")]
    ZagI32(#[source] std::num::TryFromIntError, i64),

    #[error("unable to read block")]
    ReadBlock,

    #[error("Failed to serialize value into Avro value: {0}")]
    SerializeValue(String),

    #[error("Failed to serialize value of type {value_type} using schema {schema:?}: {value}")]
    SerializeValueWithSchema {
        value_type: &'static str,
        value: String,
        schema: Schema,
    },

    #[error("Failed to serialize field '{field_name}' for record {record_schema:?}: {error}")]
    SerializeRecordFieldWithSchema {
        field_name: &'static str,
        record_schema: Schema,
        error: Box<Error>,
    },

    #[error("Failed to deserialize Avro value into value: {0}")]
    DeserializeValue(String),

    #[error("Failed to write buffer bytes during flush: {0}")]
    WriteBytes(#[source] std::io::Error),

    #[error("Failed to flush inner writer during flush: {0}")]
    FlushWriter(#[source] std::io::Error),

    #[error("Failed to write marker: {0}")]
    WriteMarker(#[source] std::io::Error),

    #[error("Failed to convert JSON to string: {0}")]
    ConvertJsonToString(#[source] serde_json::Error),

    /// Error while converting float to json value
    #[error("failed to convert avro float to json: {0}")]
    ConvertF64ToJson(f64),

    /// Error while resolving Schema::Ref
    #[error("Unresolved schema reference: {0}")]
    SchemaResolutionError(Name),

    #[error("The file metadata is already flushed.")]
    FileHeaderAlreadyWritten,

    #[error("Metadata keys starting with 'avro.' are reserved for internal usage: {0}.")]
    InvalidMetadataKey(String),

    /// Error when two named schema have the same fully qualified name
    #[error("Two named schema defined for same fullname: {0}.")]
    AmbiguousSchemaDefinition(Name),

    #[error("Signed decimal bytes length {0} not equal to fixed schema size {1}.")]
    EncodeDecimalAsFixedError(usize, usize),

    #[error("There is no entry for '{0}' in the lookup table: {1}.")]
    NoEntryInLookupTable(String, String),

    #[error("Can only encode value type {value_kind:?} as one of {supported_schema:?}")]
    EncodeValueAsSchemaError {
        value_kind: ValueKind,
        supported_schema: Vec<SchemaKind>,
    },
    #[error("Internal buffer not drained properly. Re-initialize the single object writer struct!")]
    IllegalSingleObjectWriterState,

    #[error("Codec '{0}' is not supported/enabled")]
    CodecNotSupported(String),

    #[error("Invalid Avro data! Cannot read codec type from value that is not Value::Bytes.")]
    BadCodecMetadata,

    #[error("Cannot convert a slice to Uuid: {0}")]
    UuidFromSlice(#[source] uuid::Error),
}

#[derive(thiserror::Error, PartialEq)]
pub enum CompatibilityError {
    #[error(
        "Incompatible schema types! Writer schema is '{writer_schema_type}', but reader schema is '{reader_schema_type}'"
    )]
    WrongType {
        writer_schema_type: String,
        reader_schema_type: String,
    },

    #[error("Incompatible schema types! The {schema_type} should have been {expected_type:?}")]
    TypeExpected {
        schema_type: String,
        expected_type: Vec<SchemaKind>,
    },

    #[error(
        "Incompatible schemata! Field '{0}' in reader schema does not match the type in the writer schema"
    )]
    FieldTypeMismatch(String, #[source] Box<CompatibilityError>),

    #[error("Incompatible schemata! Field '{0}' in reader schema must have a default value")]
    MissingDefaultValue(String),

    #[error("Incompatible schemata! Reader's symbols must contain all writer's symbols")]
    MissingSymbols,

    #[error("Incompatible schemata! All elements in union must match for both schemas")]
    MissingUnionElements,

    #[error("Incompatible schemata! Name and size don't match for fixed")]
    FixedMismatch,

    #[error(
        "Incompatible schemata! The name must be the same for both schemas. Writer's name {writer_name} and reader's name {reader_name}"
    )]
    NameMismatch {
        writer_name: String,
        reader_name: String,
    },

    #[error(
        "Incompatible schemata! Unknown type for '{0}'. Make sure that the type is a valid one"
    )]
    Inconclusive(String),
}

impl serde::ser::Error for Details {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Details::SerializeValue(msg.to_string())
    }
}

impl serde::de::Error for Details {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Details::DeserializeValue(msg.to_string())
    }
}

impl fmt::Debug for Details {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut msg = self.to_string();
        if let Some(e) = self.source() {
            msg.extend([": ", &e.to_string()]);
        }
        write!(f, "{msg}")
    }
}

impl fmt::Debug for CompatibilityError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut msg = self.to_string();
        if let Some(e) = self.source() {
            msg.extend([": ", &e.to_string()]);
        }
        write!(f, "{msg}")
    }
}
