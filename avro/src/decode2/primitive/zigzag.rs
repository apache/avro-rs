use oval::Buffer;
use crate::decode2::{Fsm, FsmControlFlow, FsmResult};
use crate::error::Details;
use crate::types::Value;
use crate::util::decode_variable;

#[derive(Default)]
pub struct IntFsm(ZigZagFSM);

impl Fsm for IntFsm {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        self.0.parse(buffer)?.map_fallible(|fsm| Ok(Self(fsm)), |n| {
            let n = i32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
            Ok(Value::Int(n))
        })
    }
}

#[derive(Default)]
pub struct LongFsm(ZigZagFSM);

impl Fsm for LongFsm {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.0.parse(buffer)?.map(Self, Value::Long))
    }
}

#[derive(Default)]
pub struct ZigZagFSM;
impl Fsm for ZigZagFSM {
    type Output = i64;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        if let Some((decoded, consumed)) = decode_variable(buffer.data())? {
            buffer.consume(consumed);
            Ok(FsmControlFlow::Done(decoded))
        } else {
            Ok(FsmControlFlow::NeedMore(self))
        }
    }
}