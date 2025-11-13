//! Low-level decoders for binary Avro data.
//!
//! # Low-level decoders
//!
//! This module contains the low-level decoders for binary Avro data. It is strongly recommended to
//! use the high-level readers in [`reader`]. This should only be used if you are decoding a custom
//! file format or are using an async runtime not supported in [`reader`].
//!
//! # Usage
//! The decoder implementations are based on finite state machines, expressed through the [`Fsm`]
//! trait. A decoder is supplied with a [`Buffer`] that has some (but not necessarily all) data. The
//! decoder will decode as far as it can. If it does not have enough data, it will return
//! [`FsmControlFlow::NeedMore`]. By filling the buffer with more data, the decoder can be resumed.
//! If the end of the data stream is reached and the decoder is still returning [`FsmControlFlow::NeedMore`]
//! then the data stream is corrupt and an error should be thrown.
//!
//! If the decoder is finished decoding, it will return a [`FsmControlFlow::Done`] which will also
//! consume the decoder, preventing accidental reuse of an already finished decoder.
//!
//! The supplied buffer must have space for at least 16 bytes, any less and some decoders won't be
//! able to make progress.
//!
//! ```no_run
//! # use apache_avro::{Schema, decode2::{DatumFsm, Fsm, FsmControlFlow}};
//! # use oval::Buffer;
//! # use std::{fs::File, io::Read};
//! # let schema = Schema::Bytes;
//!
//! let mut buffer = Buffer::with_capacity(256);
//! let mut file = File::open("some").unwrap();
//! let mut fsm = DatumFsm::new(&schema);
//!
//! // If the schema is Schema::Null, it's perfectly valid for the file to be empty
//! // so we don't check the amount of bytes filled the first time.
//! let bytes_read = file.read(buffer.space()).unwrap();
//! buffer.fill(bytes_read);
//!
//! let result = loop {
//!     match fsm.parse(&mut buffer).unwrap() {
//!         FsmControlFlow::NeedMore(new_fsm) => {
//!             fsm = new_fsm;
//!             let bytes_read = file.read(buffer.space()).unwrap();
//!             if bytes_read == 0 {
//!                 panic!("File is finished but decoder is not");
//!             }
//!             buffer.fill(bytes_read);
//!         }
//!         FsmControlFlow::Done(value) => {
//!             break value;
//!         }
//!     }
//! };
//! println!("{result:?}");
//! ```
//!
//!
//! [`reader`]: crate::reader

/// Decoder for compressed Avro data.
mod codec;
/// Decoder for the Object Container Files.
pub mod object_container;
/// Decoders for primitive types (and `fixed`).
mod primitive;
/// Decoders for complex types (excluding `fixed`).
mod complex;
/// Decoders for logical types.
mod logical;

use oval::Buffer;
use complex::union::UnionFsm;
use crate::{Error, Schema};
use complex::block::{ArrayFsm, MapFsm};
use primitive::bytes::{BytesFsm, FixedFsm, StringFsm};
use logical::decimal::{BigDecimalFsm, DecimalFsm};
use primitive::floats::{DoubleFsm, FloatFsm};
use complex::record::RecordFsm;
use logical::time::{DateFsm, DurationFsm, LocalTimestampMicrosFsm, LocalTimestampMillisFsm, LocalTimestampNanosFsm, TimeMicrosFsm, TimeMillisFsm, TimestampMicrosFsm, TimestampMillisFsm, TimestampNanosFsm};
use logical::uuid::UuidFsm;
use primitive::zigzag::{IntFsm, LongFsm};
use crate::decode2::complex::EnumFsm;
use crate::decode2::primitive::{BoolFsm, NullFsm};
use crate::schema::{ArraySchema, FixedSchema, MapSchema};
use crate::types::Value;
use crate::util::decode_variable;

/// Read a zigzagged varint from the buffer.
///
/// Will only consume the buffer if a whole number has been read.
/// If insufficient bytes are available it will return `Ok(None)` to
/// indicate it needs more bytes.
fn decode_zigzag_buffer(buffer: &mut Buffer) -> Result<Option<i64>, Error> {
    if let Some((decoded, consumed)) = decode_variable(buffer.data())? {
        buffer.consume(consumed);
        Ok(Some(decoded))
    } else {
        Ok(None)
    }
}

/// A trait for the lifecycle of a finite state machine.
pub trait Fsm: Sized {
    /// The final output of the state machine.
    type Output: Sized;

    /// Start/continue the state machine.
    ///
    /// Implementers are not allowed to return until they can't make progress anymore.
    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output>;
}

/// Indicates whether the state machine has completed or needs to be polled again.
#[must_use]
pub enum FsmControlFlow<Fsm, Output> {
    /// The state machine needs more data before it can continue.
    NeedMore(Fsm),
    /// The state machine is done and the result is returned.
    Done(Output),
}

impl<FSM1, O1> FsmControlFlow<FSM1, O1> {
    /// Map a state machine to another state machine.
    ///
    /// This function will only execute `need_more` or `done`, not both.
    pub fn map<F1, F2, FSM2, O2>(self, need_more: F1, done: F2) -> FsmControlFlow<FSM2, O2>
        where F1: FnOnce(FSM1) -> FSM2,
        F2: FnOnce(O1) -> O2, {
        match self {
            FsmControlFlow::NeedMore(fsm) => {
                FsmControlFlow::NeedMore(need_more(fsm))
            }
            FsmControlFlow::Done(fsm) => {
                FsmControlFlow::Done(done(fsm))
            }
        }
    }

    pub fn map_fallible<F1, F2, FSM2, O2>(self, need_more: F1, done: F2) -> Result<FsmControlFlow<FSM2, O2>, Error>
    where F1: FnOnce(FSM1) -> Result<FSM2, Error>,
        F2: FnOnce(O1) -> Result<O2, Error>, {
        match self {
            FsmControlFlow::NeedMore(fsm) => {
                Ok(FsmControlFlow::NeedMore(need_more(fsm)?))
            }
            FsmControlFlow::Done(fsm) => {
                Ok(FsmControlFlow::Done(done(fsm)?))
            }
        }
    }
}

pub type FsmResult<Fsm, Output> = Result<FsmControlFlow<Fsm, Output>, Error>;

pub struct DatumFsm<'a> {
    /// We wrap around inner to hide implementation details from the user.
    fsm: SubFsm<'a>
}
impl<'a> DatumFsm<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            fsm: SubFsm::from(schema)
        }
    }

    pub fn new_with_schemata(schema: &'a Schema, schemata: Vec<&'a Schema>) -> Self {
        todo!()
    }
}
impl<'a> Fsm for DatumFsm<'a> {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.fsm.parse(buffer)?.map(|fsm| Self { fsm }, |v| v))
    }
}

enum SubFsm<'a> {
    Null(NullFsm),
    Boolean(BoolFsm),
    Int(IntFsm),
    Long(LongFsm),
    Float(FloatFsm),
    Double(DoubleFsm),
    Bytes(BytesFsm),
    String(StringFsm),
    Fixed(FixedFsm),
    Enum(EnumFsm<'a>),
    Union(UnionFsm<'a>),
    Array(ArrayFsm<'a>),
    Map(MapFsm<'a>),
    Record(RecordFsm<'a>),
    Date(DateFsm),
    Decimal(DecimalFsm),
    BigDecimal(BigDecimalFsm),
    TimeMillis(TimeMillisFsm),
    TimeMicros(TimeMicrosFsm),
    TimestampMillis(TimestampMillisFsm),
    TimestampMicros(TimestampMicrosFsm),
    TimestampNanos(TimestampNanosFsm),
    LocalTimestampMillis(LocalTimestampMillisFsm),
    LocalTimestampMicros(LocalTimestampMicrosFsm),
    LocalTimestampNanos(LocalTimestampNanosFsm),
    Duration(DurationFsm),
    Uuid(UuidFsm),
}

impl<'a> Default for SubFsm<'a> {
    fn default() -> Self {
        Self::Null(NullFsm)
    }
}

impl<'a> Fsm for SubFsm<'a> {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        match self {
            Self::Null(fsm) => Ok(fsm.parse(buffer)?.map(Self::Null, |v| v)),
            Self::Boolean(fsm) => Ok(fsm.parse(buffer)?.map(Self::Boolean, |v| v)),
            Self::Int(fsm) => Ok(fsm.parse(buffer)?.map(Self::Int, |v| v)),
            Self::Long(fsm) => Ok(fsm.parse(buffer)?.map(Self::Long, |v| v)),
            Self::Float(fsm) => Ok(fsm.parse(buffer)?.map(Self::Float, |v| v)),
            Self::Double(fsm) => Ok(fsm.parse(buffer)?.map(Self::Double, |v| v)),
            Self::Bytes(fsm) => Ok(fsm.parse(buffer)?.map(Self::Bytes, |v| v)),
            Self::String(fsm) => Ok(fsm.parse(buffer)?.map(Self::String, |v| v)),
            Self::Fixed(fsm) => Ok(fsm.parse(buffer)?.map(Self::Fixed, |v| v)),
            Self::Enum(fsm) => Ok(fsm.parse(buffer)?.map(Self::Enum, |v| v)),
            Self::Union(fsm) => Ok(fsm.parse(buffer)?.map(Self::Union, |v| v)),
            Self::Array(fsm) => Ok(fsm.parse(buffer)?.map(Self::Array, |v| v)),
            Self::Map(fsm) => Ok(fsm.parse(buffer)?.map(Self::Map, |v| v)),
            Self::Record(fsm) => Ok(fsm.parse(buffer)?.map(Self::Record, |v| v)),
            Self::Date(fsm) => Ok(fsm.parse(buffer)?.map(Self::Date, |v| v)),
            Self::Decimal(fsm) => Ok(fsm.parse(buffer)?.map(Self::Decimal, |v| v)),
            Self::BigDecimal(fsm) => Ok(fsm.parse(buffer)?.map(Self::BigDecimal, |v| v)),
            Self::TimeMillis(fsm) => Ok(fsm.parse(buffer)?.map(Self::TimeMillis, |v| v)),
            Self::TimeMicros(fsm) => Ok(fsm.parse(buffer)?.map(Self::TimeMicros, |v| v)),
            Self::TimestampMillis(fsm) => Ok(fsm.parse(buffer)?.map(Self::TimestampMillis, |v| v)),
            Self::TimestampMicros(fsm) => Ok(fsm.parse(buffer)?.map(Self::TimestampMicros, |v| v)),
            Self::TimestampNanos(fsm) => Ok(fsm.parse(buffer)?.map(Self::TimestampNanos, |v| v)),
            Self::LocalTimestampMillis(fsm) => Ok(fsm.parse(buffer)?.map(Self::LocalTimestampMillis, |v| v)),
            Self::LocalTimestampMicros(fsm) => Ok(fsm.parse(buffer)?.map(Self::LocalTimestampMicros, |v| v)),
            Self::LocalTimestampNanos(fsm) => Ok(fsm.parse(buffer)?.map(Self::LocalTimestampNanos, |v| v)),
            Self::Duration(fsm) => Ok(fsm.parse(buffer)?.map(Self::Duration, |v| v)),
            Self::Uuid(fsm) => Ok(fsm.parse(buffer)?.map(Self::Uuid, |v| v)),
        }
    }
}

impl<'a> From<&'a Schema> for SubFsm<'a> {
    fn from(value: &'a Schema) -> Self {
        match value {
            Schema::Null => Self::Null(NullFsm),
            Schema::Boolean => Self::Boolean(BoolFsm),
            Schema::Int => Self::Int(IntFsm::default()),
            Schema::Long => Self::Long(LongFsm::default()),
            Schema::Float => Self::Float(FloatFsm),
            Schema::Double => Self::Double(DoubleFsm),
            Schema::Bytes => Self::Bytes(BytesFsm::default()),
            Schema::String => Self::String(StringFsm::default()),
            Schema::Array(ArraySchema { items, .. }) => Self::Array(ArrayFsm::new(items)),
            Schema::Map(MapSchema { types, .. }) => Self::Map(MapFsm::new(types)),
            Schema::Union(schema) => Self::Union(UnionFsm::new(schema)),
            Schema::Record(schema) => Self::Record(RecordFsm::new(schema)),
            Schema::Enum(schema) => Self::Enum(EnumFsm::new(schema)),
            Schema::Fixed(FixedSchema { size, .. }) => Self::Fixed(FixedFsm::new(*size)),
            Schema::Decimal(schema) => Self::Decimal(DecimalFsm::new(schema)),
            Schema::BigDecimal => Self::BigDecimal(BigDecimalFsm::default()),
            Schema::Uuid(schema) => Self::Uuid(UuidFsm::new(schema)),
            Schema::Date => Self::Date(DateFsm::default()),
            Schema::TimeMillis => Self::TimeMillis(TimeMillisFsm::default()),
            Schema::TimeMicros => Self::TimeMicros(TimeMicrosFsm::default()),
            Schema::TimestampMillis => Self::TimestampMillis(TimestampMillisFsm::default()),
            Schema::TimestampMicros => Self::TimestampMicros(TimestampMicrosFsm::default()),
            Schema::TimestampNanos => Self::TimestampNanos(TimestampNanosFsm::default()),
            Schema::LocalTimestampMillis => Self::LocalTimestampMillis(LocalTimestampMillisFsm::default()),
            Schema::LocalTimestampMicros => Self::LocalTimestampMicros(LocalTimestampMicrosFsm::default()),
            Schema::LocalTimestampNanos => Self::LocalTimestampNanos(LocalTimestampNanosFsm::default()),
            Schema::Duration => Self::Duration(DurationFsm::default()),
            Schema::Ref { .. } => todo!()
        }
    }
}


