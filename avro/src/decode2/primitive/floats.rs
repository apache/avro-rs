use std::io::Read;
use oval::Buffer;
use crate::decode2::{Fsm, FsmControlFlow, FsmResult};
use crate::types::Value;

pub struct FloatFsm;
impl Fsm for FloatFsm {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        if buffer.available_data() < 4 {
            Ok(FsmControlFlow::NeedMore(self))
        } else {
            let mut bytes = [0; 4];
            buffer.read_exact(&mut bytes).unwrap_or_else(|_| unreachable!());
            let float = f32::from_le_bytes(bytes);
            Ok(FsmControlFlow::Done(Value::Float(float)))
        }
    }
}

pub struct DoubleFsm;
impl Fsm for DoubleFsm {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        if buffer.available_data() < 8 {
            Ok(FsmControlFlow::NeedMore(self))
        } else {
            let mut bytes = [0; 8];
            buffer.read_exact(&mut bytes).unwrap_or_else(|_| unreachable!());
            let double = f64::from_le_bytes(bytes);
            Ok(FsmControlFlow::Done(Value::Double(double)))
        }
    }
}