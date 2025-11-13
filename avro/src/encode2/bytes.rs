use oval::Buffer;

use crate::{
    encode2::{StateMachine, StateMachineControlFlow, decode_zigzag_buffer},
    error::Details,
};

use super::StateMachineResult;

// TODO: Also make a String specific state machine. This allows checking the utf-8 while parsing
//       which would make the parser fail quicker on large invalid strings.
// TODO: This state machine could also produce inline strings (smolstr)  for strings smaller than
//       size_of::<String>, and use some extra bits to store well-known strings
//       like avro.schema and avro.codec as fixed strings.

#[derive(Default)]
pub struct BytesStateMachine {
    length: Option<usize>,
    data: Vec<u8>,
}

impl BytesStateMachine {
    pub fn new() -> Self {
        Self {
            length: None,
            data: Vec::new(),
        }
    }

    pub fn new_with_length(length: usize) -> Self {
        Self {
            length: Some(length),
            data: Vec::with_capacity(length),
        }
    }
}

impl StateMachine for BytesStateMachine {
    // This is a Vec<u8> instead of a Box<[u8]> as it's easier to create a string from a vec
    type Output = Vec<u8>;

    fn parse(mut self, buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output> {
        if self.length.is_none() {
            let Some(length) = decode_zigzag_buffer(buffer)? else {
                // Not enough data left in the buffer varint byte plus we know
                // there at least 127 bytes in the buffer now (as otherwise we wouldn't need one more varint byte).
                return Ok(StateMachineControlFlow::NeedMore(self));
            };
            let length =
                usize::try_from(length).map_err(|e| Details::ConvertI64ToUsize(e, length))?;
            self.length = Some(length);
            self.data.reserve_exact(length);
        }
        // This was just set in the previous if statement and it returns if that was not possible to do.
        let Some(length) = self.length else {
            unreachable!()
        };

        // How much more data is needed
        let remaining = length - self.data.len();
        // How much of that is available in the buffer
        let available = remaining.min(buffer.available_data());
        self.data.extend_from_slice(&buffer.data()[..available]);
        buffer.consume(available);
        if remaining - available == 0 {
            Ok(StateMachineControlFlow::Done(self.data))
        } else {
            Ok(StateMachineControlFlow::NeedMore(self))
        }
    }
}
