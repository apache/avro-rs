use crate::{
    Decimal, Duration, Error, Schema,
    bigdecimal::deserialize_big_decimal,
    decode::decode_internal,
    error::Details,
    schema::{
        ArraySchema, DecimalSchema, EnumSchema, FixedSchema, MapSchema, Name, Namespace,
        RecordSchema, UnionSchema,
    },
    state_machines::reading::{
        block::BlockStateMachine,
        bytes::BytesStateMachine,
        commands::{CommandTape, UnionVariants},
        object::ObjectStateMachine,
    },
    types::Value,
};
use oval::Buffer;
use serde::Deserialize;
use std::{borrow::Borrow, collections::HashMap, ops::Deref, str::FromStr};
use uuid::Uuid;

pub mod async_impl;
pub mod block;
pub mod bytes;
pub mod codec;
mod commands;
pub mod object;
mod object_container_file;
pub mod sync;

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
    /// The state machine is done and the result is returned.s
    Done(Output),
}

pub type StateMachineResult<StateMachine, Output> =
    Result<StateMachineControlFlow<StateMachine, Output>, Error>;

/// The sub state machine that is currently being driven.
///
/// The `Int`, `Long`, `Float`, `Double`, and `Enum` statemachines don't have state, as
/// they don't consume the buffer if there are not enough bytes. This means that the only
/// thing these statemachines are keeping track of is which type we're actually decoding.
#[derive(Default)]
pub enum SubStateMachine {
    // TODO: Remove None, replace with Option<Box<SubStateMachine>>
    #[default]
    None,
    Int,
    Long,
    Float,
    Double,
    Enum,
    Bytes(BytesStateMachine),
    String(BytesStateMachine),
    Fixed(BytesStateMachine),
    Block(BlockStateMachine),
    Object(ObjectStateMachine),
    Union(UnionVariants),
}

/// A item that was read from the document.
#[must_use]
pub enum ItemRead {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    // TODO: Maybe just make this a Vec
    Bytes(Box<[u8]>),
    // TODO: Maybe just make this a String
    String(Box<str>),
    // TODO: Maybe this can just be a bytes
    Fixed(Box<[u8]>),
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
    if let Some((decoded, consumed)) = decode_zigzag_slice(buffer.data())? {
        buffer.consume(consumed);
        Ok(Some(decoded))
    } else {
        Ok(None)
    }
}

pub fn decode_zigzag_slice(buffer: &[u8]) -> Result<Option<(i64, usize)>, Error> {
    let mut decoded = 0;
    let mut loops_done = 0;
    let mut last_byte = 0;

    for (counter, &byte) in buffer.iter().take(9).enumerate() {
        let byte = u64::from(byte);
        decoded |= (byte & 0x7F) << (counter * 7);
        loops_done = counter;
        last_byte = byte;
        if byte >> 7 == 0 {
            break;
        }
    }

    if last_byte >> 7 != 0 {
        if loops_done == 9 {
            Err(Details::IntegerOverflow.into())
        } else {
            Ok(None)
        }
    } else if decoded & 0x1 == 0 {
        Ok(Some(((decoded >> 1) as i64, loops_done)))
    } else {
        Ok(Some((!(decoded >> 1) as i64, loops_done)))
    }
}

/// Moves `src` into the referenced `dest`, dropping the previous `dest` value.
pub fn replace_drop<T>(dest: &mut T, src: T) {
    let _ = std::mem::replace(dest, src);
}

/// Deserialize a tape to a [`Value`] using the provided [`Schema`].
///
/// The schema must be compatible with the schema used by the original writer.
///
/// # Panics
/// Can panic if the provided schema does not exactly match the schema used to create the tape. To
/// convert between the writer and reader schema use [`Value::resolve`] instead.
pub fn value_from_tape<S: Borrow<Schema>>(
    tape: &mut Vec<ItemRead>,
    schema: &Schema,
    names: &HashMap<Name, S>,
    enclosing_namespace: &Namespace,
) -> Result<Value, Error> {
    match schema {
        Schema::Null => {
            if let ItemRead::Null = tape.pop().unwrap() {
                Ok(Value::Null)
            } else {
                todo!("Error")
            }
        }
        Schema::Boolean => {
            if let ItemRead::Boolean(bool) = tape.pop().unwrap() {
                Ok(Value::Boolean(bool))
            } else {
                todo!("Error")
            }
        }
        Schema::Int => {
            if let ItemRead::Int(bool) = tape.pop().unwrap() {
                Ok(Value::Int(bool))
            } else {
                todo!("Error")
            }
        }
        Schema::Long => {
            if let ItemRead::Long(long) = tape.pop().unwrap() {
                Ok(Value::Long(long))
            } else {
                todo!("Error")
            }
        }
        Schema::Float => {
            if let ItemRead::Float(float) = tape.pop().unwrap() {
                Ok(Value::Float(float))
            } else {
                todo!("Error")
            }
        }
        Schema::Double => {
            if let ItemRead::Double(double) = tape.pop().unwrap() {
                Ok(Value::Double(double))
            } else {
                todo!("Error")
            }
        }
        Schema::Bytes => {
            if let ItemRead::Bytes(bytes) = tape.pop().unwrap() {
                Ok(Value::Bytes(Vec::from(bytes)))
            } else {
                todo!("Error")
            }
        }
        Schema::String => {
            if let ItemRead::String(string) = tape.pop().unwrap() {
                Ok(Value::String(String::from(string)))
            } else {
                todo!("Error")
            }
        }
        Schema::Array(ArraySchema { items, .. }) => {
            let mut collected = Vec::new();
            loop {
                let ItemRead::Block(n) = tape.pop().unwrap() else {
                    todo!("Error")
                };
                if n == 0 {
                    break;
                }
                collected.reserve(n);
                for _ in 0..n {
                    collected.push(value_from_tape(tape, items, names, enclosing_namespace)?);
                }
            }
            Ok(Value::Array(collected))
        }
        Schema::Map(MapSchema { types, .. }) => {
            let mut collected = HashMap::new();
            loop {
                let ItemRead::Block(n) = tape.pop().unwrap() else {
                    todo!("Error")
                };
                if n == 0 {
                    break;
                }
                collected.reserve(n);
                for _ in 0..n {
                    let ItemRead::String(key) = tape.pop().unwrap() else {
                        todo!("Error")
                    };
                    let val = value_from_tape(tape, types, names, enclosing_namespace)?;
                    collected.insert(String::from(key), val);
                }
            }
            Ok(Value::Map(collected))
        }
        Schema::Union(UnionSchema { schemas, .. }) => {
            if let ItemRead::Union(variant) = tape.pop().unwrap() {
                let schema = schemas.get(usize::try_from(variant).unwrap()).unwrap();
                let value = Box::new(value_from_tape(tape, schema, names, enclosing_namespace)?);
                Ok(Value::Union(variant, value))
            } else {
                todo!("Error")
            }
        }
        Schema::Record(RecordSchema { name, fields, .. }) => {
            let fqn = name.fully_qualified_name(enclosing_namespace);
            let mut collected = Vec::with_capacity(fields.len());
            for field in fields {
                let collect = value_from_tape(tape, &field.schema, names, &fqn.namespace)?;
                collected.push((field.name.clone(), collect));
            }
            Ok(Value::Record(collected))
        }
        Schema::Enum(EnumSchema { symbols, .. }) => {
            if let ItemRead::Enum(val) = tape.pop().unwrap() {
                Ok(Value::Enum(
                    val,
                    symbols.get(usize::try_from(val).unwrap()).unwrap().clone(),
                ))
            } else {
                todo!("Error")
            }
        }
        Schema::Fixed(FixedSchema { size, .. }) => {
            if let ItemRead::Fixed(fixed) = tape.pop().unwrap() {
                // TODO: make error
                assert_eq!(*size, fixed.len());
                Ok(Value::Fixed(fixed.len(), Vec::from(fixed)))
            } else {
                todo!("Error")
            }
        }
        Schema::Decimal(_) => match tape.pop().unwrap() {
            ItemRead::Fixed(bytes) | ItemRead::Bytes(bytes) => {
                Ok(Value::Decimal(Decimal::from(&bytes)))
            }
            _ => todo!("Error"),
        },
        Schema::BigDecimal => {
            if let ItemRead::Bytes(bytes) = tape.pop().unwrap() {
                deserialize_big_decimal(&bytes).map(Value::BigDecimal)
            } else {
                todo!("Error")
            }
        }
        Schema::Uuid => {
            if let ItemRead::String(string) = tape.pop().unwrap() {
                Uuid::from_str(&string)
                    .map(Value::Uuid)
                    .map_err(|e| Details::ConvertStrToUuid(e).into())
            } else {
                todo!("Error")
            }
        }
        Schema::Date => {
            if let ItemRead::Int(int) = tape.pop().unwrap() {
                Ok(Value::Date(int))
            } else {
                todo!("Error")
            }
        }
        Schema::TimeMillis => {
            if let ItemRead::Int(int) = tape.pop().unwrap() {
                Ok(Value::TimeMillis(int))
            } else {
                todo!("Error")
            }
        }
        Schema::TimeMicros => {
            if let ItemRead::Long(long) = tape.pop().unwrap() {
                Ok(Value::TimeMicros(long))
            } else {
                todo!("Error")
            }
        }
        Schema::TimestampMillis => {
            if let ItemRead::Long(long) = tape.pop().unwrap() {
                Ok(Value::TimestampMillis(long))
            } else {
                todo!("Error")
            }
        }
        Schema::TimestampMicros => {
            if let ItemRead::Long(long) = tape.pop().unwrap() {
                Ok(Value::TimestampMicros(long))
            } else {
                todo!("Error")
            }
        }
        Schema::TimestampNanos => {
            if let ItemRead::Long(long) = tape.pop().unwrap() {
                Ok(Value::TimestampNanos(long))
            } else {
                todo!("Error")
            }
        }
        Schema::LocalTimestampMillis => {
            if let ItemRead::Long(long) = tape.pop().unwrap() {
                Ok(Value::LocalTimestampMillis(long))
            } else {
                todo!("Error")
            }
        }
        Schema::LocalTimestampMicros => {
            if let ItemRead::Long(long) = tape.pop().unwrap() {
                Ok(Value::LocalTimestampMicros(long))
            } else {
                todo!("Error")
            }
        }
        Schema::LocalTimestampNanos => {
            if let ItemRead::Long(long) = tape.pop().unwrap() {
                Ok(Value::LocalTimestampNanos(long))
            } else {
                todo!("Error")
            }
        }
        Schema::Duration => {
            if let ItemRead::Fixed(bytes) = tape.pop().unwrap() {
                let array: [u8; 12] = bytes.deref().try_into().unwrap();
                Ok(Value::Duration(Duration::from(array)))
            } else {
                todo!("Error")
            }
        }
        Schema::Ref { name } => {
            let fqn = name.fully_qualified_name(enclosing_namespace);
            if let Some(resolved) = names.get(&fqn) {
                value_from_tape(tape, resolved.borrow(), names, &fqn.namespace)
            } else {
                Err(Details::SchemaResolutionError(fqn).into())
            }
        }
    }
}

/// Deserialize a tape to `T` using the provided [`Schema`].
///
/// The schema must be compatible with the schema used by the original writer.
pub fn deserialize_from_tape<'a, T: Deserialize<'a>, S: Borrow<Schema>>(
    tape: &mut Vec<ItemRead>,
    _schema: &Schema,
    _names: &HashMap<Name, S>,
    _enclosing_namespace: &Namespace,
) -> Result<T, Error> {
    tape.clear();
    todo!()
}
