use oval::Buffer;

use crate::{
    error::Details,
    state_machines::reading::{DataRequest, StateMachine, StateMachineControlFlow, decode_zigzag},
};

use super::StateMachineResult;

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
            let Some(length) = decode_zigzag(buffer)? else {
                // Not enough data left in the buffer, need at least one more varint byte plus we know
                // there at least 127 bytes in the buffer now (as otherwise we wouldn't need one more varint byte).
                return Ok(StateMachineControlFlow::Continue(
                    self,
                    DataRequest::new(128),
                ));
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
        if remaining - available == 0 {
            Ok(StateMachineControlFlow::Done(self.data))
        } else {
            Ok(StateMachineControlFlow::Continue(
                self,
                DataRequest::new(remaining - available),
            ))
        }
    }
}
