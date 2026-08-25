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

use crate::schema::{InnerDecimalSchema, NamespaceRef, UuidSchema};
use crate::{
    AvroResult, Error,
    bigdecimal::deserialize_big_decimal,
    decimal::Decimal,
    duration::Duration,
    error::Details,
    schema::{DecimalSchema, EnumSchema, FixedSchema, Name, RecordSchema, ResolvedSchema, Schema},
    types::Value,
    util::{
        DEFAULT_MAX_ALLOCATION_BYTES, decode_recursion_limit, max_allocation_bytes,
        safe_collection_len, safe_len, zag_i32, zag_i64,
    },
};
use std::{
    borrow::Borrow,
    collections::HashMap,
    io::{ErrorKind, Read},
};
use uuid::Uuid;

#[inline]
pub(crate) fn decode_long<R: Read>(reader: &mut R) -> AvroResult<Value> {
    zag_i64(reader).map(Value::Long)
}

#[inline]
fn decode_int<R: Read>(reader: &mut R) -> AvroResult<Value> {
    zag_i32(reader).map(Value::Int)
}

#[inline]
pub(crate) fn decode_len<R: Read>(reader: &mut R) -> AvroResult<usize> {
    let len = zag_i64(reader)?;
    safe_len(usize::try_from(len).map_err(|e| Details::ConvertI64ToUsize(e, len))?)
}

/// Decode the length of a sequence.
///
/// Maps and arrays are 0-terminated, 0i64 is also encoded as 0 in Avro reading a length of 0 means
/// the end of the map or array.
fn decode_seq_len<R: Read>(reader: &mut R) -> AvroResult<usize> {
    let raw_len = zag_i64(reader)?;
    safe_len(
        usize::try_from(match raw_len.cmp(&0) {
            std::cmp::Ordering::Equal => return Ok(0),
            std::cmp::Ordering::Less => {
                let _size = zag_i64(reader)?;
                raw_len.checked_neg().ok_or(Details::IntegerOverflow)?
            }
            std::cmp::Ordering::Greater => raw_len,
        })
        .map_err(|e| Details::ConvertI64ToUsize(e, raw_len))?,
    )
}

/// Per-datum decoding state.
///
/// Tracks the cumulative number of bytes allocated on behalf of a single
/// datum, so that nested collections cannot multiply the allocation budget:
/// every allocation performed while decoding one datum is debited from a
/// shared budget of [`max_allocation_bytes`] bytes, instead of each
/// collection only being checked in isolation.
#[derive(Debug)]
pub(crate) struct DecodeContext {
    /// Bytes still available for allocations while decoding the current datum.
    remaining_budget: usize,
    /// Current recursion depth of `decode_internal`.
    depth: usize,
}

impl DecodeContext {
    /// Create a new context.
    ///
    /// This should only be done when a new datum is being decoded, never during the decoding.
    pub(crate) fn new() -> Self {
        Self {
            remaining_budget: max_allocation_bytes(DEFAULT_MAX_ALLOCATION_BYTES),
            depth: 0,
        }
    }

    /// Debit `bytes` from the per-datum allocation budget
    ///
    /// # Errors
    /// `Details::MemoryAllocation` if the maximum budget is exceeded.
    fn debit_bytes(&mut self, bytes: usize) -> AvroResult<()> {
        match self.remaining_budget.checked_sub(bytes) {
            Some(remaining) => {
                self.remaining_budget = remaining;
                Ok(())
            }
            None => Err(Details::MemoryAllocation {
                desired: Some(bytes),
                maximum: max_allocation_bytes(DEFAULT_MAX_ALLOCATION_BYTES),
            }
            .into()),
        }
    }

    /// Debit the amount of bytes for `n` items of `T`.
    ///
    /// # Errors
    /// `Details::MemoryAllocation` if the maximum budget is exceeded.
    fn debit_items<T>(&mut self, n: usize) -> AvroResult<()> {
        let bytes = n
            .checked_mul(size_of::<T>())
            .ok_or(Details::IntegerOverflow)?;
        self.debit_bytes(bytes)
    }

    /// Track one level of decoding recursion, erroring once the configured
    /// maximum depth is exceeded.
    fn enter(&mut self) -> AvroResult<()> {
        self.depth += 1;
        let maximum = decode_recursion_limit();
        if self.depth > maximum {
            Err(Details::DecodeRecursionLimit { maximum }.into())
        } else {
            Ok(())
        }
    }

    /// Leave one level of decoding recursion.
    fn leave(&mut self) {
        self.depth -= 1;
    }
}

/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<R: Read>(schema: &Schema, reader: &mut R) -> AvroResult<Value> {
    let rs = ResolvedSchema::try_from(schema)?;
    decode_internal(
        schema,
        rs.get_names(),
        None,
        reader,
        &mut DecodeContext::new(),
    )
}

pub(crate) fn decode_internal<R: Read, S: Borrow<Schema>>(
    schema: &Schema,
    names: &HashMap<Name, S>,
    enclosing_namespace: NamespaceRef,
    reader: &mut R,
    ctx: &mut DecodeContext,
) -> AvroResult<Value> {
    ctx.enter()?;
    let value = decode_internal_body(schema, names, enclosing_namespace, reader, ctx);
    ctx.leave();
    value
}

fn decode_internal_body<R: Read, S: Borrow<Schema>>(
    schema: &Schema,
    names: &HashMap<Name, S>,
    enclosing_namespace: NamespaceRef,
    reader: &mut R,
    ctx: &mut DecodeContext,
) -> AvroResult<Value> {
    match schema {
        Schema::Null => Ok(Value::Null),
        Schema::Boolean => {
            let mut buf = [0u8; 1];
            match reader.read_exact(&mut buf[..]) {
                Ok(_) => match buf[0] {
                    0u8 => Ok(Value::Boolean(false)),
                    1u8 => Ok(Value::Boolean(true)),
                    _ => Err(Details::BoolValue(buf[0]).into()),
                },
                Err(io_err) => {
                    if let ErrorKind::UnexpectedEof = io_err.kind() {
                        Ok(Value::Null)
                    } else {
                        Err(Details::ReadBoolean(io_err).into())
                    }
                }
            }
        }
        Schema::Decimal(DecimalSchema { inner, .. }) => match inner {
            InnerDecimalSchema::Fixed(fixed) => {
                match decode_internal(
                    &Schema::Fixed(fixed.copy_only_size()),
                    names,
                    enclosing_namespace,
                    reader,
                    ctx,
                )? {
                    Value::Fixed(_, bytes) => Ok(Value::Decimal(Decimal::from(bytes))),
                    value => Err(Details::FixedValue(value).into()),
                }
            }
            InnerDecimalSchema::Bytes => {
                match decode_internal(&Schema::Bytes, names, enclosing_namespace, reader, ctx)? {
                    Value::Bytes(bytes) => Ok(Value::Decimal(Decimal::from(bytes))),
                    value => Err(Details::BytesValue(value).into()),
                }
            }
        },
        Schema::BigDecimal => {
            match decode_internal(&Schema::Bytes, names, enclosing_namespace, reader, ctx)? {
                Value::Bytes(bytes) => deserialize_big_decimal(&bytes).map(Value::BigDecimal),
                value => Err(Details::BytesValue(value).into()),
            }
        }
        Schema::Uuid(UuidSchema::String) => {
            let Value::String(string) =
                decode_internal(&Schema::String, names, enclosing_namespace, reader, ctx)?
            else {
                // decoding a String can also return a Null, indicating EOF
                return Err(Error::new(Details::ReadBytes(std::io::Error::from(
                    ErrorKind::UnexpectedEof,
                ))));
            };
            let uuid = Uuid::parse_str(&string).map_err(Details::ConvertStrToUuid)?;
            Ok(Value::Uuid(uuid))
        }
        Schema::Uuid(UuidSchema::Bytes) => {
            let Value::Bytes(bytes) =
                decode_internal(&Schema::Bytes, names, enclosing_namespace, reader, ctx)?
            else {
                unreachable!(
                    "decode_internal(Schema::Bytes) can only return a Value::Bytes or an error"
                )
            };
            let uuid = Uuid::from_slice(&bytes).map_err(Details::ConvertSliceToUuid)?;
            Ok(Value::Uuid(uuid))
        }
        Schema::Uuid(UuidSchema::Fixed(fixed)) => {
            let Value::Fixed(n, bytes) = decode_internal(
                &Schema::Fixed(fixed.copy_only_size()),
                names,
                enclosing_namespace,
                reader,
                ctx,
            )?
            else {
                unreachable!(
                    "decode_internal(Schema::Fixed) can only return a Value::Fixed or an error"
                )
            };
            if n != 16 {
                return Err(Details::ConvertFixedToUuid(n).into());
            }
            let uuid = Uuid::from_slice(&bytes).map_err(Details::ConvertSliceToUuid)?;
            Ok(Value::Uuid(uuid))
        }
        Schema::Int => decode_int(reader),
        Schema::Date => zag_i32(reader).map(Value::Date),
        Schema::TimeMillis => zag_i32(reader).map(Value::TimeMillis),
        Schema::Long => decode_long(reader),
        Schema::TimeMicros => zag_i64(reader).map(Value::TimeMicros),
        Schema::TimestampMillis => zag_i64(reader).map(Value::TimestampMillis),
        Schema::TimestampMicros => zag_i64(reader).map(Value::TimestampMicros),
        Schema::TimestampNanos => zag_i64(reader).map(Value::TimestampNanos),
        Schema::LocalTimestampMillis => zag_i64(reader).map(Value::LocalTimestampMillis),
        Schema::LocalTimestampMicros => zag_i64(reader).map(Value::LocalTimestampMicros),
        Schema::LocalTimestampNanos => zag_i64(reader).map(Value::LocalTimestampNanos),
        Schema::Duration(fixed_schema) => {
            if fixed_schema.size == 12 {
                let mut buf = [0u8; 12];
                reader.read_exact(&mut buf).map_err(Details::ReadDuration)?;
                Ok(Value::Duration(Duration::from(buf)))
            } else {
                Err(Details::CompareFixedSizes {
                    size: 12,
                    n: fixed_schema.size,
                }
                .into())
            }
        }
        Schema::Float => {
            let mut buf = [0u8; std::mem::size_of::<f32>()];
            reader
                .read_exact(&mut buf[..])
                .map_err(Details::ReadFloat)?;
            Ok(Value::Float(f32::from_le_bytes(buf)))
        }
        Schema::Double => {
            let mut buf = [0u8; std::mem::size_of::<f64>()];
            reader
                .read_exact(&mut buf[..])
                .map_err(Details::ReadDouble)?;
            Ok(Value::Double(f64::from_le_bytes(buf)))
        }
        Schema::Bytes => {
            let len = decode_len(reader)?;
            ctx.debit_bytes(len)?;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).map_err(Details::ReadBytes)?;
            Ok(Value::Bytes(buf))
        }
        Schema::String => {
            let len = decode_len(reader)?;
            ctx.debit_bytes(len)?;
            let mut buf = vec![0u8; len];
            match reader.read_exact(&mut buf) {
                Ok(_) => Ok(Value::String(
                    String::from_utf8(buf).map_err(Details::ConvertToUtf8)?,
                )),
                Err(io_err) => {
                    if let ErrorKind::UnexpectedEof = io_err.kind() {
                        Ok(Value::Null)
                    } else {
                        Err(Details::ReadString(io_err).into())
                    }
                }
            }
        }
        Schema::Fixed(FixedSchema { size, .. }) => {
            ctx.debit_bytes(*size)?;
            let mut buf = vec![0u8; *size];
            reader
                .read_exact(&mut buf)
                .map_err(|e| Details::ReadFixed(e, *size))?;
            Ok(Value::Fixed(*size, buf))
        }
        Schema::Array(inner) => {
            let mut items = Vec::new();

            loop {
                let len = decode_seq_len(reader)?;
                if len == 0 {
                    break;
                }

                // Check that the Vec won't grow past the max allocation size
                let total = items
                    .len()
                    .checked_add(len)
                    .ok_or(Details::IntegerOverflow)?;
                safe_collection_len::<Value>(total)?;
                ctx.debit_items::<Value>(len)?;
                // Use reserve_exact as reserve can allocate more than needed defeating the purpose
                // of the previous check
                items.reserve_exact(len);
                for _ in 0..len {
                    items.push(decode_internal(
                        &inner.items,
                        names,
                        enclosing_namespace,
                        reader,
                        ctx,
                    )?);
                }
            }

            Ok(Value::Array(items))
        }
        Schema::Map(inner) => {
            let mut items = HashMap::new();

            loop {
                let len = decode_seq_len(reader)?;
                if len == 0 {
                    break;
                }

                // Check that the HashMap won't grow past the max allocation size. This is less
                // precise than the Vec check above as HashMap allocates in buckets and doesn't have
                // a reserve_exact
                let total = items
                    .len()
                    .checked_add(len)
                    .ok_or(Details::IntegerOverflow)?;
                safe_collection_len::<(String, Value)>(total)?;
                ctx.debit_items::<(String, Value)>(len)?;

                items.reserve(len);
                for _ in 0..len {
                    match decode_internal(&Schema::String, names, enclosing_namespace, reader, ctx)?
                    {
                        Value::String(key) => {
                            let value = decode_internal(
                                &inner.types,
                                names,
                                enclosing_namespace,
                                reader,
                                ctx,
                            )?;
                            items.insert(key, value);
                        }
                        value => return Err(Details::MapKeyType(value.into()).into()),
                    }
                }
            }

            Ok(Value::Map(items))
        }
        Schema::Union(inner) => match zag_i64(reader).map_err(Error::into_details) {
            Ok(index) => {
                let variants = inner.variants();
                let variant = variants
                    .get(usize::try_from(index).map_err(|e| Details::ConvertI64ToUsize(e, index))?)
                    .ok_or(Details::GetUnionVariant {
                        index,
                        num_variants: variants.len(),
                    })?;
                let value = decode_internal(variant, names, enclosing_namespace, reader, ctx)?;
                Ok(Value::Union(index as u32, Box::new(value)))
            }
            Err(Details::ReadVariableIntegerBytes(io_err)) => {
                if let ErrorKind::UnexpectedEof = io_err.kind() {
                    Ok(Value::Union(0, Box::new(Value::Null)))
                } else {
                    Err(Details::ReadVariableIntegerBytes(io_err).into())
                }
            }
            Err(io_err) => Err(Error::new(io_err)),
        },
        Schema::Record(RecordSchema { name, fields, .. }) => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            ctx.debit_items::<(String, Value)>(fields.len())?;
            // Benchmarks indicate ~10% improvement using this method.
            let mut items = Vec::with_capacity(fields.len());
            for field in fields {
                ctx.debit_bytes(field.name.len())?;
                // TODO: This clone is also expensive. See if we can do away with it...
                items.push((
                    field.name.clone(),
                    decode_internal(
                        &field.schema,
                        names,
                        fully_qualified_name.namespace(),
                        reader,
                        ctx,
                    )?,
                ));
            }
            Ok(Value::Record(items))
        }
        Schema::Enum(EnumSchema { symbols, .. }) => {
            Ok(if let Value::Int(raw_index) = decode_int(reader)? {
                let index = usize::try_from(raw_index)
                    .map_err(|e| Details::ConvertI32ToUsize(e, raw_index))?;
                if (0..symbols.len()).contains(&index) {
                    // Cloning the symbol allocates without consuming wire
                    // bytes, so it counts against the per-datum budget.
                    ctx.debit_bytes(symbols[index].len())?;
                    let symbol = symbols[index].clone();
                    Value::Enum(raw_index as u32, symbol)
                } else {
                    return Err(Details::GetEnumValue {
                        index,
                        nsymbols: symbols.len(),
                    }
                    .into());
                }
            } else {
                return Err(Details::GetEnumUnknownIndexValue.into());
            })
        }
        Schema::Ref { name } => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            if let Some(resolved) = names.get(&fully_qualified_name) {
                decode_internal(
                    resolved.borrow(),
                    names,
                    fully_qualified_name.namespace(),
                    reader,
                    ctx,
                )
            } else {
                Err(Details::SchemaResolutionError(fully_qualified_name.into_owned()).into())
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_fun_call)]
mod tests {
    use crate::error::Details;
    use crate::schema::{InnerDecimalSchema, UuidSchema};
    use crate::util::decode_recursion_limit;
    use crate::{
        Decimal,
        decode::decode,
        encode::{encode, tests::success},
        schema::{DecimalSchema, FixedSchema, Schema},
        types::{
            Value,
            Value::{Array, Int, Map},
        },
    };
    use apache_avro_test_helper::TestResult;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn test_decode_array_without_size() -> TestResult {
        let mut input: &[u8] = &[6, 2, 4, 6, 0];
        let result = decode(&Schema::array(Schema::Int).build(), &mut input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result?);

        Ok(())
    }

    #[test]
    fn test_decode_array_with_size() -> TestResult {
        let mut input: &[u8] = &[5, 6, 2, 4, 6, 0];
        let result = decode(&Schema::array(Schema::Int).build(), &mut input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result?);

        Ok(())
    }

    // Create an array/map block with an object count of `n` and no items
    fn create_block(n: i64) -> Vec<u8> {
        // Reuse the crate's encoder so the tests cannot diverge from it.
        let mut out = Vec::new();
        crate::util::zig_i64(n, &mut out).unwrap();
        out.push(0x00);
        out
    }

    #[test]
    fn test_decode_array_of_null_huge_count_is_rejected() -> TestResult {
        // 200,000,000 nulls => ~11 GiB reserve, far above the default budget.
        let payload = create_block(200_000_000);
        let result = decode(
            &Schema::array(Schema::Null).build(),
            &mut payload.as_slice(),
        );
        assert!(
            result.is_err(),
            "a huge array<null> block count must be rejected, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn test_decode_array_of_long_huge_count_is_rejected() -> TestResult {
        // Non-zero-byte elements are also affected: the reserve happens before
        // any element is read.
        let payload = create_block(200_000_000);
        let result = decode(
            &Schema::array(Schema::Long).build(),
            &mut payload.as_slice(),
        );
        assert!(
            result.is_err(),
            "a huge array<long> block count must be rejected, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn test_decode_map_huge_count_is_rejected() -> TestResult {
        let payload = create_block(200_000_000);
        let result = decode(&Schema::map(Schema::Long).build(), &mut payload.as_slice());
        assert!(
            result.is_err(),
            "a huge map block count must be rejected, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn test_decode_small_array_of_null_still_decodes() -> TestResult {
        // A modest array of nulls within the budget must still decode.
        let payload = create_block(3);
        let result = decode(
            &Schema::array(Schema::Null).build(),
            &mut payload.as_slice(),
        )?;
        assert_eq!(Array(vec!(Value::Null, Value::Null, Value::Null)), result);

        Ok(())
    }

    #[test]
    fn avro_rs_639_test_nested_collections_share_one_allocation_budget() -> TestResult {
        use crate::util::{DEFAULT_MAX_ALLOCATION_BYTES, max_allocation_bytes};

        // Each inner array<null> block passes the per-collection check on its
        // own, but the shared per-datum budget must reject the cumulative
        // total: elements of type null cost zero wire bytes, so without a
        // cumulative budget a handful of ~10-byte inner arrays would pin an
        // unbounded multiple of the allocation limit in memory at once.
        let budget = max_allocation_bytes(DEFAULT_MAX_ALLOCATION_BYTES);
        let inner_count = (budget / 2) / size_of::<Value>() + 1;

        let inner_arrays_count = 2;
        let mut payload = Vec::new();
        // Outer array: a single block declaring two inner arrays.
        crate::util::zig_i64(inner_arrays_count, &mut payload)?;
        for _ in 0..inner_arrays_count {
            payload.extend(create_block(inner_count as i64));
        }
        // Outer array terminator.
        payload.push(0x00);

        let result = decode(
            &Schema::array(Schema::array(Schema::Null).build()).build(),
            &mut payload.as_slice(),
        );

        assert!(
            result.is_err(),
            "nested collections must share one allocation budget, got {result:?}"
        );
        let details = result.unwrap_err().into_details();
        assert!(
            matches!(details, Details::MemoryAllocation { .. }),
            "Expected memory allocation error, got: {details:?}"
        );

        Ok(())
    }

    #[test]
    fn test_decode_array_int64_min_block_count_is_rejected() -> TestResult {
        // i64::MIN as a negative block count cannot be negated (checked_neg
        // returns None); decoding must fail rather than wrap. i64::MIN zig-zag
        // encodes as the 10-byte varint below, followed by a block byte-size.
        let payload = create_block(i64::MIN);
        let result = decode(&Schema::array(Schema::Int).build(), &mut payload.as_slice());
        assert!(
            result.is_err(),
            "an i64::MIN array block count must be rejected, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_640_test_decode_fixed_size_above_budget_is_rejected() -> TestResult {
        use crate::schema::Name;

        // The schema (and with it the fixed size) can be attacker-supplied
        // via an OCF header; the decoder must refuse to allocate more than
        // the budget, before reading a single payload byte.
        let schema = Schema::Fixed(
            FixedSchema::builder()
                .name(Name::new("huge")?)
                .size(usize::MAX / 2)
                .build(),
        );
        let mut input: &[u8] = &[0u8; 4];
        let result = decode(&schema, &mut input);
        assert!(
            result.is_err(),
            "a fixed size larger than the allocation budget must be rejected, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_642_test_decode_recursion_depth_is_bounded() -> TestResult {
        // With a recursive schema, one wire byte per level drives unbounded
        // recursion; the decoder must return an error instead of overflowing
        // the stack (which would abort the process).
        let schema = Schema::parse_str(
            r#"{
                "type": "record",
                "name": "Node",
                "fields": [
                    {"name": "next", "type": ["null", "Node"]}
                ]
            }"#,
        )?;
        let recursion_depth_trigger = decode_recursion_limit() + 1;
        // Each 0x02 byte selects the "Node" union branch, one level deeper.
        let payload = vec![0x02u8; recursion_depth_trigger];
        let result = decode(&schema, &mut payload.as_slice());
        assert!(result.is_err(), "unbounded recursion must be rejected");

        Ok(())
    }

    #[test]
    fn test_decode_map_without_size() -> TestResult {
        let mut input: &[u8] = &[0x02, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = decode(&Schema::map(Schema::Int).build(), &mut input);
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result?);

        Ok(())
    }

    #[test]
    fn test_decode_map_with_size() -> TestResult {
        let mut input: &[u8] = &[0x01, 0x0C, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = decode(&Schema::map(Schema::Int).build(), &mut input);
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result?);

        Ok(())
    }

    #[test]
    fn test_negative_decimal_value() -> TestResult {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let schema = Schema::Decimal(DecimalSchema {
            inner: InnerDecimalSchema::Fixed(
                FixedSchema::builder()
                    .name(Name::new("decimal")?)
                    .size(2)
                    .build(),
            ),
            precision: 4,
            scale: 2,
        });
        let bigint = (-423).to_bigint().unwrap();
        let value = Value::Decimal(Decimal::from(bigint.to_signed_bytes_be()));

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));

        let mut bytes = &buffer[..];
        let result = decode(&schema, &mut bytes)?;
        assert_eq!(result, value);

        Ok(())
    }

    #[test]
    fn test_decode_decimal_with_bigger_than_necessary_size() -> TestResult {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let schema = Schema::Decimal(DecimalSchema {
            inner: InnerDecimalSchema::Fixed(FixedSchema {
                size: 13,
                name: Name::new("decimal")?,
                aliases: None,
                doc: None,
                attributes: Default::default(),
            }),
            precision: 4,
            scale: 2,
        });
        let value = Value::Decimal(Decimal::from(
            ((-423).to_bigint().unwrap()).to_signed_bytes_be(),
        ));
        let mut buffer = Vec::<u8>::new();

        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));
        let mut bytes: &[u8] = &buffer[..];
        let result = decode(&schema, &mut bytes)?;
        assert_eq!(result, value);

        Ok(())
    }

    #[test]
    fn test_avro_3448_recursive_definition_decode_union() -> TestResult {
        // if encoding fails in this test check the corresponding test in encode
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":[ "null", {
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
        let outer_value1 = Value::Record(vec![
            ("a".into(), Value::Union(1, Box::new(inner_value1))),
            ("b".into(), inner_value2.clone()),
        ]);
        let mut buf = Vec::new();
        encode(&outer_value1, &schema, &mut buf).expect(&success(&outer_value1, &schema));
        assert!(!buf.is_empty());
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value1,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {schema:?}\n"
            ))
        );

        let mut buf = Vec::new();
        let outer_value2 = Value::Record(vec![
            ("a".into(), Value::Union(0, Box::new(Value::Null))),
            ("b".into(), inner_value2),
        ]);
        encode(&outer_value2, &schema, &mut buf).expect(&success(&outer_value2, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value2,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {schema:?}\n"
            ))
        );

        Ok(())
    }

    #[test]
    fn test_avro_3448_recursive_definition_decode_array() -> TestResult {
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
                    "type": "Inner"
                }
            ]
        }"#,
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            ("a".into(), Value::Array(vec![inner_value1])),
            ("b".into(), inner_value2),
        ]);
        let mut buf = Vec::new();
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {schema:?}\n"
            ))
        );

        Ok(())
    }

    #[test]
    fn test_avro_3448_recursive_definition_decode_map() -> TestResult {
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
                    "type": "Inner"
                }
            ]
        }"#,
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            (
                "a".into(),
                Value::Map(vec![("akey".into(), inner_value1)].into_iter().collect()),
            ),
            ("b".into(), inner_value2),
        ]);
        let mut buf = Vec::new();
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {schema:?}\n"
            ))
        );

        Ok(())
    }

    #[test]
    fn test_avro_3448_proper_multi_level_decoding_middle_namespace() -> TestResult {
        // if encoding fails in this test check the corresponding test in encode
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

        let mut buf = Vec::new();
        encode(&outer_record_variation_1, &schema, &mut buf)
            .expect(&success(&outer_record_variation_1, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_1,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {schema:?}\n"
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_2,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {schema:?}\n"
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_3,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {schema:?}\n"
            ))
        );

        Ok(())
    }

    #[test]
    fn test_avro_3448_proper_multi_level_decoding_inner_namespace() -> TestResult {
        // if encoding fails in this test check the corresponding test in encode
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

        let mut buf = Vec::new();
        encode(&outer_record_variation_1, &schema, &mut buf)
            .expect(&success(&outer_record_variation_1, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_1,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {schema:?}\n"
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_2,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {schema:?}\n"
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_3,
            decode(&schema, &mut bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {schema:?}\n"
            ))
        );

        Ok(())
    }

    #[test]
    fn avro_3926_encode_decode_uuid_to_string() -> TestResult {
        use crate::encode::encode;

        let schema = Schema::String;
        let value = Value::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?);

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));

        let result = decode(&Schema::Uuid(UuidSchema::String), &mut &buffer[..])?;
        assert_eq!(result, value);

        Ok(())
    }

    #[test]
    fn avro_3926_encode_decode_uuid_to_fixed() -> TestResult {
        use crate::encode::encode;

        let fixed = FixedSchema {
            size: 16,
            name: "uuid".try_into()?,
            aliases: None,
            doc: None,
            attributes: Default::default(),
        };

        let schema = Schema::Fixed(fixed.clone());
        let value = Value::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?);

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));

        let result = decode(&Schema::Uuid(UuidSchema::Fixed(fixed)), &mut &buffer[..])?;
        assert_eq!(result, value);

        Ok(())
    }

    #[test]
    fn encode_decode_uuid_to_bytes() -> TestResult {
        use crate::encode::encode;

        let schema = Schema::Bytes;
        let value = Value::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?);

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));

        let result = decode(&Schema::Uuid(UuidSchema::Bytes), &mut &buffer[..])?;
        assert_eq!(result, value);

        Ok(())
    }
}
