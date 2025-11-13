use std::io::Read;
use oval::Buffer;
use crate::decode2::{Fsm, FsmControlFlow, FsmResult};
use crate::error::Details;
use crate::types::Value;

pub mod zigzag;
pub mod floats;
pub mod bytes;

pub struct NullFsm;
impl Fsm for NullFsm {
    type Output = Value;

    fn parse(self, _buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(FsmControlFlow::Done(Value::Null))
    }
}

pub struct BoolFsm;
impl Fsm for BoolFsm {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        let mut byte = [0; 1];
        buffer
            .read_exact(&mut byte)
            .expect("Unreachable! Buffer is not empty");
        match byte {
            [0] => Ok(FsmControlFlow::Done(Value::Boolean(false))),
            [1] => Ok(FsmControlFlow::Done(Value::Boolean(true))),
            [byte] => Err(Details::BoolValue(byte).into()),
        }
    }
}
