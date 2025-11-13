use crate::{
    Error,
};
use oval::Buffer;

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
    // Null(Vec<ItemRead>),
    // Bool(Vec<ItemRead>),
    // Int(Vec<ItemRead>),
    // Long(Vec<ItemRead>),
    // Float(Vec<ItemRead>),
    // Double(Vec<ItemRead>),
    // Enum(Vec<ItemRead>),
    // Bytes {
    //     fsm: BytesStateMachine,
    //     read: Vec<ItemRead>,
    // },
    // String {
    //     fsm: BytesStateMachine,
    //     read: Vec<ItemRead>,
    // },
    // Block(BlockStateMachine),
    // Object(DatumStateMachine),
    // Union(UnionStateMachine),
}

impl StateMachine for SubStateMachine {
    type Output = ();

    fn parse(self, _buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output> {
        match self {
            // SubStateMachine::Null(mut read) => {
            //     read.push(ItemRead::Null);
            //     Ok(StateMachineControlFlow::Done(read))
            // }
            // SubStateMachine::Bool(mut read) => {
            //     let mut byte = [0; 1];
            //     buffer
            //         .read_exact(&mut byte)
            //         .expect("Unreachable! Buffer is not empty");
            //     match byte {
            //         [0] => read.push(ItemRead::Boolean(false)),
            //         [1] => read.push(ItemRead::Boolean(true)),
            //         [byte] => return Err(Details::BoolValue(byte).into()),
            //     }
            //     Ok(StateMachineControlFlow::Done(read))
            // }
            // SubStateMachine::Int(mut read) => {
            //     let Some(n) = decode_zigzag_buffer(buffer)? else {
            //         // Not enough data left in the buffer
            //         return Ok(StateMachineControlFlow::NeedMore(Self::Int(read)));
            //     };
            //     let n = i32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
            //     read.push(ItemRead::Int(n));
            //     Ok(StateMachineControlFlow::Done(read))
            // }
            // SubStateMachine::Long(mut read) => {
            //     let Some(n) = decode_zigzag_buffer(buffer)? else {
            //         // Not enough data left in the buffer
            //         return Ok(StateMachineControlFlow::NeedMore(Self::Long(read)));
            //     };
            //     read.push(ItemRead::Long(n));
            //     Ok(StateMachineControlFlow::Done(read))
            // }
            // SubStateMachine::Float(mut read) => {
            //     let Some(bytes) = buffer.data().first_chunk().copied() else {
            //         // Not enough data left in the buffer
            //         return Ok(StateMachineControlFlow::NeedMore(Self::Float(read)));
            //     };
            //     buffer.consume(4);
            //     read.push(ItemRead::Float(f32::from_le_bytes(bytes)));
            //     Ok(StateMachineControlFlow::Done(read))
            // }
            // SubStateMachine::Double(mut read) => {
            //     let Some(bytes) = buffer.data().first_chunk().copied() else {
            //         // Not enough data left in the buffer
            //         return Ok(StateMachineControlFlow::NeedMore(Self::Double(read)));
            //     };
            //     buffer.consume(8);
            //     read.push(ItemRead::Double(f64::from_le_bytes(bytes)));
            //     Ok(StateMachineControlFlow::Done(read))
            // }
            // SubStateMachine::Enum(mut read) => {
            //     let Some(n) = decode_zigzag_buffer(buffer)? else {
            //         // Not enough data left in the buffer
            //         return Ok(StateMachineControlFlow::NeedMore(Self::Enum(read)));
            //     };
            //     // TODO: Wrong error
            //     let n = u32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
            //     read.push(ItemRead::Enum(n));
            //     Ok(StateMachineControlFlow::Done(read))
            // }
            // SubStateMachine::Bytes { fsm, mut read } => match fsm.parse(buffer)? {
            //     StateMachineControlFlow::NeedMore(fsm) => {
            //         Ok(StateMachineControlFlow::NeedMore(Self::Bytes { fsm, read }))
            //     }
            //     StateMachineControlFlow::Done(bytes) => {
            //         read.push(ItemRead::Bytes(bytes));
            //         Ok(StateMachineControlFlow::Done(read))
            //     }
            // },
            // SubStateMachine::String { fsm, mut read } => match fsm.parse(buffer)? {
            //     StateMachineControlFlow::NeedMore(fsm) => {
            //         Ok(StateMachineControlFlow::NeedMore(Self::String {
            //             fsm,
            //             read,
            //         }))
            //     }
            //     StateMachineControlFlow::Done(bytes) => {
            //         let string = String::from_utf8(bytes).map_err(Details::ConvertToUtf8)?;
            //         read.push(ItemRead::String(string));
            //         Ok(StateMachineControlFlow::Done(read))
            //     }
            // },
            // SubStateMachine::Block(fsm) => match fsm.parse(buffer)? {
            //     StateMachineControlFlow::NeedMore(fsm) => {
            //         Ok(StateMachineControlFlow::NeedMore(Self::Block(fsm)))
            //     }
            //     StateMachineControlFlow::Done(read) => Ok(StateMachineControlFlow::Done(read)),
            // },
            // SubStateMachine::Union(fsm) => match fsm.parse(buffer)? {
            //     StateMachineControlFlow::NeedMore(fsm) => {
            //         Ok(StateMachineControlFlow::NeedMore(Self::Union(fsm)))
            //     }
            //     StateMachineControlFlow::Done(read) => Ok(StateMachineControlFlow::Done(read)),
            // },
            // SubStateMachine::Object(fsm) => match fsm.parse(buffer)? {
            //     StateMachineControlFlow::NeedMore(fsm) => {
            //         Ok(StateMachineControlFlow::NeedMore(Self::Object(fsm)))
            //     }
            //     StateMachineControlFlow::Done(read) => Ok(StateMachineControlFlow::Done(read)),
            // },
        }
    }
}

