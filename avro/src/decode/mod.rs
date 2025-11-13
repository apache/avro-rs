use crate::{
    Decimal, Duration, Error, Schema,
    bigdecimal::deserialize_big_decimal,
    decode::{
        block::BlockStateMachine, bytes::BytesStateMachine, commands::CommandTape,
        datum::DatumStateMachine, error::ValueFromTapeError, union::UnionStateMachine,
    },
    error::Details,
    schema::{
        ArraySchema, EnumSchema, FixedSchema, MapSchema, Name, Names, Namespace, RecordSchema,
        ResolvedSchema, UnionSchema,
    },
    types::Value,
    util::decode_variable,
};
use oval::Buffer;
use serde::Deserialize;
use std::{borrow::Borrow, collections::HashMap, io::Read, ops::Deref, str::FromStr};
use uuid::Uuid;

pub mod block;
pub mod bytes;
pub mod codec;
pub mod commands;
pub mod datum;
pub mod error;
pub mod object_container_file;
mod union;

pub trait StateMachine: Sized {
    type Output: Sized;

    /// Start/continue the state machine.
    ///
    /// Implementers are not allowed to return until they can't make progress anymore.
    fn parse(self, buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output>;
}

/// Indicates whether the state machine has completed or needs to be polled again.
#[must_use]
pub enum StateMachineControlFlow<StateMachine, Output> {
    /// The state machine needs more data before it can continue.
    NeedMore(StateMachine),
    /// The state machine is done and the result is returned.
    Done(Output),
}

pub type StateMachineResult<StateMachine, Output> =
    Result<StateMachineControlFlow<StateMachine, Output>, Error>;

/// The sub state machine that is currently being driven.
///
/// The `Int`, `Long`, `Float`, `Double`, and `Enum` state machines don't have state, as
/// they don't consume the buffer if there are not enough bytes. This means that the only
/// thing these state machines are keeping track of is which type we're actually decoding.
pub enum SubStateMachine {
    Null(Vec<ItemRead>),
    Bool(Vec<ItemRead>),
    Int(Vec<ItemRead>),
    Long(Vec<ItemRead>),
    Float(Vec<ItemRead>),
    Double(Vec<ItemRead>),
    Enum(Vec<ItemRead>),
    Bytes {
        fsm: BytesStateMachine,
        read: Vec<ItemRead>,
    },
    String {
        fsm: BytesStateMachine,
        read: Vec<ItemRead>,
    },
    Block(BlockStateMachine),
    Object(DatumStateMachine),
    Union(UnionStateMachine),
}

impl StateMachine for SubStateMachine {
    type Output = Vec<ItemRead>;

    fn parse(self, buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output> {
        match self {
            SubStateMachine::Null(mut read) => {
                read.push(ItemRead::Null);
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Bool(mut read) => {
                let mut byte = [0; 1];
                buffer
                    .read_exact(&mut byte)
                    .expect("Unreachable! Buffer is not empty");
                match byte {
                    [0] => read.push(ItemRead::Boolean(false)),
                    [1] => read.push(ItemRead::Boolean(true)),
                    [byte] => return Err(Details::BoolValue(byte).into()),
                }
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Int(mut read) => {
                let Some(n) = decode_zigzag_buffer(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(Self::Int(read)));
                };
                let n = i32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
                read.push(ItemRead::Int(n));
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Long(mut read) => {
                let Some(n) = decode_zigzag_buffer(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(Self::Long(read)));
                };
                read.push(ItemRead::Long(n));
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Float(mut read) => {
                let Some(bytes) = buffer.data().first_chunk().copied() else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(Self::Float(read)));
                };
                buffer.consume(4);
                read.push(ItemRead::Float(f32::from_le_bytes(bytes)));
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Double(mut read) => {
                let Some(bytes) = buffer.data().first_chunk().copied() else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(Self::Double(read)));
                };
                buffer.consume(8);
                read.push(ItemRead::Double(f64::from_le_bytes(bytes)));
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Enum(mut read) => {
                let Some(n) = decode_zigzag_buffer(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(Self::Enum(read)));
                };
                // TODO: Wrong error
                let n = u32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
                read.push(ItemRead::Enum(n));
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Bytes { fsm, mut read } => match fsm.parse(buffer)? {
                StateMachineControlFlow::NeedMore(fsm) => {
                    Ok(StateMachineControlFlow::NeedMore(Self::Bytes { fsm, read }))
                }
                StateMachineControlFlow::Done(bytes) => {
                    read.push(ItemRead::Bytes(bytes));
                    Ok(StateMachineControlFlow::Done(read))
                }
            },
            SubStateMachine::String { fsm, mut read } => match fsm.parse(buffer)? {
                StateMachineControlFlow::NeedMore(fsm) => {
                    Ok(StateMachineControlFlow::NeedMore(Self::String {
                        fsm,
                        read,
                    }))
                }
                StateMachineControlFlow::Done(bytes) => {
                    let string = String::from_utf8(bytes).map_err(Details::ConvertToUtf8)?;
                    read.push(ItemRead::String(string));
                    Ok(StateMachineControlFlow::Done(read))
                }
            },
            SubStateMachine::Block(fsm) => match fsm.parse(buffer)? {
                StateMachineControlFlow::NeedMore(fsm) => {
                    Ok(StateMachineControlFlow::NeedMore(Self::Block(fsm)))
                }
                StateMachineControlFlow::Done(read) => Ok(StateMachineControlFlow::Done(read)),
            },
            SubStateMachine::Union(fsm) => match fsm.parse(buffer)? {
                StateMachineControlFlow::NeedMore(fsm) => {
                    Ok(StateMachineControlFlow::NeedMore(Self::Union(fsm)))
                }
                StateMachineControlFlow::Done(read) => Ok(StateMachineControlFlow::Done(read)),
            },
            SubStateMachine::Object(fsm) => match fsm.parse(buffer)? {
                StateMachineControlFlow::NeedMore(fsm) => {
                    Ok(StateMachineControlFlow::NeedMore(Self::Object(fsm)))
                }
                StateMachineControlFlow::Done(read) => Ok(StateMachineControlFlow::Done(read)),
            },
        }
    }
}

/// A item that was read from the document.
#[derive(Debug)]
#[must_use]
pub enum ItemRead {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    // TODO: smollvec/hipbytes?
    Bytes(Vec<u8>),
    // TODO: smollstr/hipstr?
    String(String),
    /// The variant of the Enum that was read.
    Enum(u32),
    /// The variant of the Union that was read.
    ///
    /// The variant data is next.
    Union(u32),
    /// The start of a block of a Map or Array.
    Block(usize),
}

/// Read a zigzagged varint from the buffer.
///
/// Will only consume the buffer if a whole number has been read.
/// If insufficient bytes are available it will return `Ok(None)` to
/// indicate it needs more bytes.
pub fn decode_zigzag_buffer(buffer: &mut Buffer) -> Result<Option<i64>, Error> {
    if let Some((decoded, consumed)) = decode_variable(buffer.data())? {
        buffer.consume(consumed);
        Ok(Some(decoded))
    } else {
        Ok(None)
    }
}

/// Deserialize a tape to a [`Value`] using the provided [`Schema`].
///
/// The schema must be compatible with the schema used by the original writer.
///
/// Both `names` and `extra_names` are checked when a [`Schema::Ref`] is encountered. They're allowed
/// to have overlapping items.
///
/// # Panics
/// Can panic if the provided schema does not exactly match the schema used to create the tape. To
/// convert between the writer and reader schema use [`Value::resolve`] instead.
pub fn value_from_tape(
    tape: &mut Vec<ItemRead>,
    schema: &Schema,
    names: &Names,
) -> Result<Value, Error> {
    value_from_tape_internal(&mut tape.drain(..), schema, &None, names)
}

/// Recursively transform the `tape` into a [`Value`] according to the provided [`Schema`].
///
/// Both `names` and `extra_names` are checked when a [`Schema::Ref`] is encountered. They're allowed
/// to have overlapping items.
pub fn value_from_tape_internal(
    tape: &mut impl Iterator<Item = ItemRead>,
    schema: &Schema,
    enclosing_namespace: &Namespace,
    names: &Names,
) -> Result<Value, Error> {
    match schema {
        Schema::Null => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Null => Ok(Value::Null),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Boolean => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Boolean(bool) => Ok(Value::Boolean(bool)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Int => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Int(bool) => Ok(Value::Int(bool)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Long => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Long(long) => Ok(Value::Long(long)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Float => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Float(float) => Ok(Value::Float(float)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Double => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Double(double) => Ok(Value::Double(double)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Bytes => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Bytes(bytes) => Ok(Value::Bytes(bytes)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::String => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::String(string) => Ok(Value::String(string)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Array(ArraySchema { items, .. }) => {
            let mut collected = Vec::new();
            loop {
                let n = match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                    ItemRead::Block(n) => Ok(n),
                    item => Err(ValueFromTapeError::TapeSchemaMismatch {
                        schema: schema.clone(),
                        item,
                    }),
                }?;
                if n == 0 {
                    break;
                }
                collected.reserve(n);
                for _ in 0..n {
                    collected.push(value_from_tape_internal(
                        tape,
                        items,
                        enclosing_namespace,
                        names,
                    )?);
                }
            }
            Ok(Value::Array(collected))
        }
        Schema::Map(MapSchema { types, .. }) => {
            let mut collected = HashMap::new();
            loop {
                let n = match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                    ItemRead::Block(n) => Ok(n),
                    item => Err(ValueFromTapeError::TapeSchemaMismatch {
                        schema: schema.clone(),
                        item,
                    }),
                }?;
                if n == 0 {
                    break;
                }
                collected.reserve(n);
                for _ in 0..n {
                    let key = match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                        ItemRead::String(string) => Ok(string),
                        item => Err(ValueFromTapeError::TapeSchemaMismatch {
                            schema: Schema::String,
                            item,
                        }),
                    }?;
                    let val = value_from_tape_internal(tape, types, enclosing_namespace, names)?;
                    collected.insert(key, val);
                }
            }
            Ok(Value::Map(collected))
        }
        Schema::Union(UnionSchema { schemas, .. }) => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Union(variant) => {
                    let schema = schemas.get(usize::try_from(variant).unwrap()).ok_or(
                        Details::GetUnionVariant {
                            index: variant as i64,
                            num_variants: schemas.len(),
                        },
                    )?;
                    let value = Box::new(value_from_tape_internal(
                        tape,
                        schema,
                        enclosing_namespace,
                        names,
                    )?);
                    Ok(Value::Union(variant, value))
                }
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::Record(RecordSchema { name, fields, .. }) => {
            let fqn = name.fully_qualified_name(enclosing_namespace);
            let mut collected = Vec::with_capacity(fields.len());
            for field in fields {
                let collect = value_from_tape_internal(tape, &field.schema, &fqn.namespace, names)?;
                collected.push((field.name.clone(), collect));
            }
            Ok(Value::Record(collected))
        }
        Schema::Enum(EnumSchema { symbols, .. }) => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Enum(val) => Ok(Value::Enum(
                    val,
                    symbols.get(usize::try_from(val).unwrap()).unwrap().clone(),
                )),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::Fixed(FixedSchema { size, .. }) => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Bytes(fixed) => {
                    if *size == fixed.len() {
                        Ok(Value::Fixed(fixed.len(), fixed))
                    } else {
                        Err(ValueFromTapeError::TapeSchemaMismatchFixed {
                            expected: *size,
                            actual: fixed.len(),
                        }
                        .into())
                    }
                }
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::Decimal(_) => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Bytes(bytes) => Ok(Value::Decimal(Decimal::from(&bytes))),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::BigDecimal => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Bytes(bytes) => deserialize_big_decimal(&bytes).map(Value::BigDecimal),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Uuid(_) => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::String(string) => Uuid::from_str(&string)
                .map(Value::Uuid)
                .map_err(|e| Details::ConvertStrToUuid(e).into()),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Date => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Int(int) => Ok(Value::Date(int)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::TimeMillis => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Int(int) => Ok(Value::TimeMillis(int)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::TimeMicros => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Long(long) => Ok(Value::TimeMicros(long)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::TimestampMillis => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::TimestampMillis(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::TimestampMicros => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::TimestampMicros(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::TimestampNanos => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::TimestampNanos(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::LocalTimestampMillis => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::LocalTimestampMillis(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::LocalTimestampMicros => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::LocalTimestampMicros(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::LocalTimestampNanos => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::LocalTimestampNanos(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::Duration => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Bytes(bytes) => {
                let array: [u8; 12] = bytes.deref().try_into().unwrap();
                Ok(Value::Duration(Duration::from(array)))
            }
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Ref { name } => {
            let fqn = name.fully_qualified_name(enclosing_namespace);
            if let Some(resolved) = names.get(&fqn) {
                value_from_tape_internal(tape, resolved, &fqn.namespace, names)
            } else {
                Err(Details::SchemaResolutionError(fqn).into())
            }
        }
    }
}

/// Deserialize a tape to `T` using the provided [`Schema`].
///
/// The schema must be compatible with the schema used by the original writer.
pub fn deserialize_from_tape<'a, T: Deserialize<'a>>(
    tape: &mut Vec<ItemRead>,
    schema: &Schema,
) -> Result<T, Error> {
    let rs = ResolvedSchema::try_from(schema)?;
    deserialize_from_tape_internal(tape, schema, rs.get_names(), &None)
}

/// Recursively transform the `tape` into a `T` according to the provided [`Schema`].
fn deserialize_from_tape_internal<'a, T: Deserialize<'a>, S: Borrow<Schema>>(
    tape: &mut Vec<ItemRead>,
    _schema: &Schema,
    _names: &HashMap<Name, S>,
    _enclosing_namespace: &Namespace,
) -> Result<T, Error> {
    tape.clear();
    todo!()
}

#[cfg(test)]
#[allow(clippy::expect_fun_call)]
mod tests {
    use crate::{
        Decimal,
        encode::{encode, tests::success},
        from_avro_datum,
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
    use crate::schema::UuidSchema;

    #[test]
    fn test_decode_array_without_size() -> TestResult {
        let mut input: &[u8] = &[6, 2, 4, 6, 0];

        let result = from_avro_datum(&Schema::array(Schema::Int), &mut input, None)?;

        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result);

        Ok(())
    }

    #[test]
    fn test_decode_array_with_size() -> TestResult {
        let mut input: &[u8] = &[5, 6, 2, 4, 6, 0];
        let result = from_avro_datum(&Schema::array(Schema::Int), &mut input, None)?;
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result);

        Ok(())
    }

    #[test]
    fn test_decode_map_without_size() -> TestResult {
        let mut input: &[u8] = &[0x02, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = from_avro_datum(&Schema::map(Schema::Int), &mut input, None)?;
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result);

        Ok(())
    }

    #[test]
    fn test_decode_map_with_size() -> TestResult {
        let mut input: &[u8] = &[0x01, 0x0C, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = from_avro_datum(&Schema::map(Schema::Int), &mut input, None)?;
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result);

        Ok(())
    }

    #[test]
    fn test_negative_decimal_value() -> TestResult {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let inner = Box::new(Schema::Fixed(
            FixedSchema::builder()
                .name(Name::new("decimal")?)
                .size(2)
                .build(),
        ));
        let schema = Schema::Decimal(DecimalSchema {
            inner,
            precision: 4,
            scale: 2,
        });
        let bigint = (-423).to_bigint().unwrap();
        let value = Value::Decimal(Decimal::from(bigint.to_signed_bytes_be()));

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));

        let mut bytes = &buffer[..];
        let result = from_avro_datum(&schema, &mut bytes, None)?;
        assert_eq!(result, value);

        Ok(())
    }

    #[test]
    fn test_decode_decimal_with_bigger_than_necessary_size() -> TestResult {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let inner = Box::new(Schema::Fixed(FixedSchema {
            size: 13,
            name: Name::new("decimal")?,
            aliases: None,
            doc: None,
            default: None,
            attributes: Default::default(),
        }));
        let schema = Schema::Decimal(DecimalSchema {
            inner,
            precision: 4,
            scale: 2,
        });
        let value = Value::Decimal(Decimal::from(
            ((-423).to_bigint().unwrap()).to_signed_bytes_be(),
        ));
        let mut buffer = Vec::<u8>::new();

        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));
        let mut bytes: &[u8] = &buffer[..];
        let result = from_avro_datum(&schema, &mut bytes, None)?;
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
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
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
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
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
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
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
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
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
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_2,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_3,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
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
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_2,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_3,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
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

        let result = from_avro_datum(&Schema::Uuid(UuidSchema::String), &mut &buffer[..], None)?;
        assert_eq!(result, value);

        Ok(())
    }

    // TODO: Schema::Uuid needs a sub schema which is either String or Fixed. It's now part of the
    //       spec anyway.
    // #[test]
    // fn avro_3926_encode_decode_uuid_to_fixed() -> TestResult {
    //     use crate::encode::encode;
    //
    //     let schema = Schema::Fixed(FixedSchema {
    //         size: 16,
    //         name: "uuid".into(),
    //         aliases: None,
    //         doc: None,
    //         default: None,
    //         attributes: Default::default(),
    //     });
    //     let value = Value::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?);
    //
    //     let mut buffer = Vec::new();
    //     encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));
    //
    //     let result = from_avro_datum(&Schema::Uuid, &mut &buffer[..], None)?;
    //     assert_eq!(result, value);
    //
    //     Ok(())
    // }
}
