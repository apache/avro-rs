use std::{ops::RangeInclusive, sync::Arc};

use oval::Buffer;
use serde::Deserialize;

use crate::{
    Error, Schema,
    error::Details,
    state_machines::reading::{
        block::{ArrayStateMachine, MapStateMachine},
        bytes::BytesStateMachine,
        object::ObjectStateMachine,
    },
    types::Value,
};
pub mod async_impl;
pub mod block;
pub mod bytes;
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
    Array(ArrayStateMachine),
    Map(MapStateMachine),
    Object(ObjectStateMachine),
    Union(Box<[CommandTape]>),
}

/// A item that was read from the document.
#[must_use]
pub enum ItemRead {
    // TODO: This is probably not useful to have
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Box<[u8]>),
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

/// The next item type that should be read.
#[must_use]
pub enum ToRead {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Enum,
    Fixed(usize),
    Array(CommandTape),
    Map(CommandTape),
    Union(Box<[CommandTape]>),
}

/// A section of a tape of commands.
///
/// This has a reference to the entire tape, so that references to types (for Union,Map,Array) can be resolved.
#[derive(Debug, Clone)]
#[must_use]
pub struct CommandTape {
    inner: Arc<[u8]>,
    read_range: RangeInclusive<usize>,
}

impl CommandTape {
    pub const NULL: u8 = 0;
    pub const BOOLEAN: u8 = 1;
    pub const INT: u8 = 2;
    pub const LONG: u8 = 3;
    pub const FLOAT: u8 = 4;
    pub const DOUBLE: u8 = 5;
    pub const BYTES: u8 = 6;
    pub const STRING: u8 = 7;
    pub const ENUM: u8 = 8;
    pub const FIXED: u8 = 9;
    // TODO: Maybe combine Array and Map into Block
    // TODO: Add a type reference type, so that if a Block has a single type no reference is needed
    pub const ARRAY: u8 = 10;
    pub const MAP: u8 = 11;
    pub const UNION: u8 = 12;

    /// Create a new tape that will be read from start to end.
    pub fn new(command_tape: Arc<[u8]>) -> Self {
        let length = command_tape.len();
        Self {
            inner: command_tape,
            read_range: 0..=length,
        }
    }

    /// Check if the section of the tape we're reading is finished.
    pub fn is_finished(&self) -> bool {
        self.read_range.is_empty()
    }

    /// Extract a part from the tape to give to a sub state machine.
    ///
    /// The tape will run from start to end (inclusive).
    pub fn extract(&self, start: usize, end: usize) -> Self {
        assert!(
            end < self.inner.len(),
            "Reference is (partly) outside the tape"
        );
        Self {
            inner: self.inner.clone(),
            read_range: start..=end,
        }
    }

    /// Extract many parts from the tape to give to the Union state machine.
    ///
    /// The tapes will run from start to end (inclusive).
    pub fn extract_many(&self, parts: &[(usize, usize)]) -> Box<[Self]> {
        let mut vec = Vec::with_capacity(parts.len());
        for &(start, end) in parts {
            vec.push(self.extract(start, end));
        }
        vec.into_boxed_slice()
    }

    /// Read an array of bytes from the tape.
    fn read_array<const N: usize>(&mut self) -> [u8; N] {
        let start = self.read_range.next().expect("Read past the limit");
        let end = self.read_range.nth(N - 1).expect("Read past the limit");
        self.inner[start..=end].try_into().expect("Unreachable!")
    }

    /// Get the next command from the tape.
    ///
    /// # Panics
    /// Will panic if the commands are already finished, see [`Self::is_finished`].
    pub fn command(&mut self) -> ToRead {
        let position = self
            .read_range
            .next()
            .expect("The caller read past the tape");
        let byte = self.inner[position];
        match byte & 0xF {
            Self::NULL => ToRead::Null,
            Self::BOOLEAN => ToRead::Boolean,
            Self::INT => ToRead::Int,
            Self::LONG => ToRead::Long,
            Self::FLOAT => ToRead::Float,
            Self::DOUBLE => ToRead::Double,
            Self::BYTES => ToRead::Bytes,
            Self::STRING => ToRead::String,
            Self::ENUM => ToRead::Enum,
            Self::FIXED => {
                // ToRead::Fixed
                if byte >> 4 != 0 {
                    // Length is stored inine
                    ToRead::Fixed((byte >> 4) as usize)
                } else {
                    let length = usize::from_ne_bytes(self.read_array());
                    ToRead::Fixed(length)
                }
            }
            Self::ARRAY => {
                // ToRead::Array
                // TODO: If the length of the type is less than 16 we can store the length inline
                // TODO: Change end to length
                // TODO: Use varint for start and length
                let start = usize::from_ne_bytes(self.read_array());
                let end = usize::from_ne_bytes(self.read_array());
                ToRead::Array(self.extract(start, end))
            }
            Self::MAP => {
                // ToRead::Map
                // TODO: If the length of the type is less than 16 we can store the length inline
                // TODO: Change end to length
                // TODO: Use varint for start and length
                let start = usize::from_ne_bytes(self.read_array());
                let end = usize::from_ne_bytes(self.read_array());
                ToRead::Map(self.extract(start, end))
            }
            Self::UNION => {
                // ToRead::Union
                // How many variants are there?
                let number_of_options = if byte >> 4 != 0 {
                    (byte >> 4) as usize
                } else {
                    // TODO: Use varint
                    let number_of_options = u32::from_ne_bytes(self.read_array());
                    number_of_options as usize
                };

                // Where are the references to the variants? Every reference is a pair of usizes.
                // TODO: Use varint
                let position_of_options = usize::from_ne_bytes(self.read_array());

                // Assert that the references are inside the tape.
                assert!(
                    position_of_options + number_of_options * size_of::<(usize, usize)>()
                        < self.inner.len(),
                    "Options are (partly) outside the tape"
                );
                // TODO: Change options from start-end to start-length
                // TODO: Use varint for start and length
                // TODO: Find a safe way to do this
                // SAFETY: As asserted above, the references are entirely inside the slice. The ptr is not null as we've
                //         just read from the slice. We check that the options are aligned before creating the new slice.
                //         The lifetime of the slice is set to the same lifetime as the parent slice.
                let options = unsafe {
                    let options: *const (usize, usize) =
                        self.inner.as_ptr().add(position_of_options).cast();
                    assert!(options.is_aligned());
                    std::slice::from_raw_parts(options, number_of_options)
                };
                ToRead::Union(self.extract_many(options))
            }
            _ => unreachable!(), // TODO: There is room here to specialize certain types, like a Union of Null and some other type
        }
    }
}

/// Read a zigzagged varint from the buffer.
///
/// Will only consume the buffer if a whole number has been read.
/// If insufficient bytes are available it will return `Ok(None)` to
/// indicate it needs more bytes.
pub fn decode_zigzag(buffer: &mut Buffer) -> Result<Option<i64>, Error> {
    let mut decoded = 0;
    let mut loops_done = 0;
    let mut last_byte = 0;

    for (counter, &byte) in buffer.data().iter().take(9).enumerate() {
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
    } else {
        buffer.consume(loops_done);
        if decoded & 0x1 == 0 {
            Ok(Some((decoded >> 1) as i64))
        } else {
            Ok(Some(!(decoded >> 1) as i64))
        }
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
/// The tape is completely drained in the process.
pub fn value_from_tape(tape: &mut Vec<ItemRead>, _schema: &Schema) -> Result<Value, Error> {
    tape.clear();
    todo!();
}

/// Deserialize a tape to `T` using the provided [`Schema`].
///
/// The schema must be compatible with the schema used by the original writer.
///
/// The tape is completely drained in the process.
pub fn deserialize_from_tape<'a, T: Deserialize<'a>>(
    tape: &mut Vec<ItemRead>,
    _schema: &Schema,
) -> Result<T, Error> {
    tape.clear();
    todo!()
}

/// Convert a schema to a tape that can be used by the state machines.
pub fn schema_to_command_tape(_schema: &Schema) -> CommandTape {
    todo!()
}
