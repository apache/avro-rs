use std::ops::Deref;
use oval::Buffer;
use crate::decode2::{Fsm, FsmResult};
use crate::decode2::primitive::bytes::FixedFsm;
use crate::decode2::primitive::zigzag::ZigZagFSM;
use crate::Duration;
use crate::error::Details;
use crate::types::Value;

pub struct DurationFsm(FixedFsm);
impl DurationFsm {
    pub fn new() -> Self {
        Self(FixedFsm::new(12))
    }
}
impl Default for DurationFsm {
    fn default() -> Self {
        Self::new()
    }
}
impl Fsm for DurationFsm {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.0.parse(buffer)?.map(Self, |v| {
            let Value::Fixed(_, bytes) = v else { unreachable!() };
            let array: [u8; 12] = bytes.deref().try_into().unwrap();
            Value::Duration(Duration::from(array))
        }))
    }
}

#[derive(Default)]
pub struct DateFsm(ZigZagFSM);
impl Fsm for DateFsm {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        self.0.parse(buffer)?.map_fallible(|fsm| Ok(Self(fsm)), |n| {
            let n = i32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
            Ok(Value::Date(n))
        })
    }
}

#[derive(Default)]
pub struct TimeMillisFsm(ZigZagFSM);
impl Fsm for TimeMillisFsm {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        self.0.parse(buffer)?.map_fallible(|fsm| Ok(Self(fsm)), |n| {
            let n = i32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
            Ok(Value::TimeMillis(n))
        })
    }
}

#[derive(Default)]
pub struct TimeMicrosFsm(ZigZagFSM);
impl Fsm for TimeMicrosFsm {
    type Output = Value;
    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.0.parse(buffer)?.map(Self, Value::TimeMicros))
    }
}

#[derive(Default)]
pub struct TimestampMillisFsm(ZigZagFSM);
impl Fsm for TimestampMillisFsm {
    type Output = Value;
    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.0.parse(buffer)?.map(Self, Value::TimestampMillis))
    }
}
#[derive(Default)]
pub struct TimestampMicrosFsm(ZigZagFSM);
impl Fsm for TimestampMicrosFsm {
    type Output = Value;
    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.0.parse(buffer)?.map(Self, Value::TimestampMicros))
    }
}

#[derive(Default)]
pub struct TimestampNanosFsm(ZigZagFSM);
impl Fsm for TimestampNanosFsm {
    type Output = Value;
    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.0.parse(buffer)?.map(Self, Value::TimestampNanos))
    }
}
#[derive(Default)]
pub struct LocalTimestampMillisFsm(ZigZagFSM);
impl Fsm for LocalTimestampMillisFsm {
    type Output = Value;
    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.0.parse(buffer)?.map(Self, Value::LocalTimestampMillis))
    }
}
#[derive(Default)]
pub struct LocalTimestampMicrosFsm(ZigZagFSM);
impl Fsm for LocalTimestampMicrosFsm {
    type Output = Value;
    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.0.parse(buffer)?.map(Self, Value::LocalTimestampMicros))
    }
}
#[derive(Default)]
pub struct LocalTimestampNanosFsm(ZigZagFSM);
impl Fsm for LocalTimestampNanosFsm {
    type Output = Value;
    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.0.parse(buffer)?.map(Self, Value::LocalTimestampNanos))
    }
}