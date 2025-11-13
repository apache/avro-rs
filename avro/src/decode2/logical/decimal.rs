use oval::Buffer;
use crate::decode2::primitive::bytes::{BytesFsm, FixedFsm};
use crate::decode2::{FsmControlFlow, Fsm, FsmResult};
use crate::{Decimal, Schema};
use crate::bigdecimal::deserialize_big_decimal;
use crate::schema::DecimalSchema;
use crate::types::Value;

#[derive(Default)]
pub struct BigDecimalFsm(BytesFsm);

impl Fsm for BigDecimalFsm {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        self.0.parse(buffer)?.map_fallible(|fsm| Ok(Self(fsm)), |bytes| {
            let Value::Bytes(bytes) = bytes else { unreachable!() };
            Ok(Value::BigDecimal(deserialize_big_decimal(&bytes)?))
        })
    }
}

enum BytesOrFixedFsm {
    Bytes(BytesFsm),
    Fixed(FixedFsm),
}
impl Fsm for BytesOrFixedFsm {
    type Output = Vec<u8>;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        match self {
            BytesOrFixedFsm::Bytes(fsm) => {
                Ok(fsm.parse(buffer)?.map(BytesOrFixedFsm::Bytes, |v| {
                    let Value::Bytes(bytes) = v else { unreachable!() };
                    bytes
                }))
            }
            BytesOrFixedFsm::Fixed(fsm) => {
                Ok(fsm.parse(buffer)?.map(BytesOrFixedFsm::Fixed, |v| {
                    let Value::Fixed(_, bytes) = v else { unreachable!() };
                    bytes
                }))
            }
        }
    }
}

pub struct DecimalFsm {
    fsm: BytesOrFixedFsm,
}

impl DecimalFsm {
    pub fn new(schema: &DecimalSchema) -> Self {
        let fsm = if let Schema::Fixed(fixed) = schema.inner.as_ref() {
            BytesOrFixedFsm::Fixed(FixedFsm::new(fixed.size))
        } else if let Schema::Bytes = schema.inner.as_ref() {
            BytesOrFixedFsm::Bytes(BytesFsm::default())
        } else {
            panic!("Invalid DecimalSchema, inner schema is not Fixed or Bytes");
        };
        Self {
            fsm,
        }
    }
}
impl Fsm for DecimalFsm {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        match self.fsm.parse(buffer)? {
            FsmControlFlow::NeedMore(new_fsm) => {
                self.fsm = new_fsm;
                Ok(FsmControlFlow::NeedMore(self))
            }
            FsmControlFlow::Done(bytes) => {
                Ok(FsmControlFlow::Done(Value::Decimal(Decimal::from(bytes))))
            }
        }
    }
}
