use oval::Buffer;
use crate::decode2::{Fsm, FsmControlFlow, FsmResult};
use crate::decode2::decode_zigzag_buffer;
use crate::Error;
use crate::error::Details;
use crate::types::Value;

#[derive(Default)]
pub struct BytesFsm {
    inner: InnerBytesFSM,
}

impl Fsm for BytesFsm {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        match self.inner.parse(buffer)? {
            FsmControlFlow::NeedMore(new_innner) => {
                self.inner = new_innner;
                Ok(FsmControlFlow::NeedMore(self))
            }
            FsmControlFlow::Done(bytes) => {
                Ok(FsmControlFlow::Done(Value::Bytes(bytes)))
            }
        }
    }
}

pub struct FixedFsm {
    inner: InnerBytesFSM,
}

impl FixedFsm {
    pub fn new(length: usize) -> Self {
        Self {
            inner: InnerBytesFSM::new_with_length(length),
        }
    }
}

impl Fsm for FixedFsm {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        match self.inner.parse(buffer)? {
            FsmControlFlow::NeedMore(new_innner) => {
                self.inner = new_innner;
                Ok(FsmControlFlow::NeedMore(self))
            }
            FsmControlFlow::Done(bytes) => {
                Ok(FsmControlFlow::Done(Value::Fixed(bytes.len(), bytes)))
            }
        }
    }
}

#[derive(Default)]
pub struct StringFsm {
    inner: InnerBytesFSM,
    validated_up_to: usize,
}

impl StringFsm {
    /// Validate that the partially read string is valid UTF-8.
    ///
    /// This will only validate the part of the string that has not been validated yet.
    ///
    /// If `incomplete` is `true` this will allow the last 3 bytes of the string to be invalid.
    /// When the full string is read, `incomplete` should be set to `false`, which will also validate
    /// the last 3 bytes.
    fn partial_validate(data: &[u8], mut validated_up_to: usize, incomplete: bool) -> Result<usize, Error> {
        let unvalidated = &data[validated_up_to..];
        match std::str::from_utf8(unvalidated) {
            Ok(_) => {
                validated_up_to = data.len();
                Ok(validated_up_to)
            },
            Err(error) => {
                validated_up_to += error.valid_up_to();
                if incomplete && validated_up_to + 3 >= data.len() {
                    Ok(validated_up_to)
                } else {
                    Err(Details::ConvertToUtf8Error(error).into())
                }
            }
        }
    }
}

impl Fsm for StringFsm {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        match self.inner.parse(buffer)? {
            FsmControlFlow::NeedMore(new_innner) => {
                self.inner = new_innner;
                // Validate the part that was just read
                self.validated_up_to = Self::partial_validate(&self.inner.data, self.validated_up_to, true)?;
                Ok(FsmControlFlow::NeedMore(self))
            }
            FsmControlFlow::Done(bytes) => {
                // Validate the last bit
                Self::partial_validate(&bytes, self.validated_up_to, false)?;
                // SAFETY: bytes is valid, as it has been incrementally checked during read
                let string = unsafe {
                    String::from_utf8_unchecked(bytes)
                };
                Ok(FsmControlFlow::Done(Value::String(string)))
            }
        }
    }
}

#[derive(Default)]
struct InnerBytesFSM {
    length: Option<usize>,
    data: Vec<u8>,
}

impl InnerBytesFSM {
    pub fn new_with_length(length: usize) -> Self {
        Self {
            length: Some(length),
            data: Vec::with_capacity(length),
        }
    }
}

impl Fsm for InnerBytesFSM {
    type Output = Vec<u8>;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        if self.length.is_none() {
            let Some(length) = decode_zigzag_buffer(buffer)? else {
                // Not enough data left in the buffer varint byte plus we know
                // there at least 127 bytes in the buffer now (as otherwise we wouldn't need one more varint byte).
                return Ok(FsmControlFlow::NeedMore(self));
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
            Ok(FsmControlFlow::Done(self.data))
        } else {
            Ok(FsmControlFlow::NeedMore(self))
        }
    }
}