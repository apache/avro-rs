use oval::Buffer;
use uuid::Uuid;
use crate::decode2::primitive::bytes::{FixedFsm, StringFsm};
use crate::decode2::{Fsm, FsmResult};
use crate::error::Details;
use crate::schema::UuidSchema;
use crate::types::Value;

pub enum UuidFsm {
    String(StringFsm),
    FixedFSM(FixedFsm),
}
impl UuidFsm {
    pub fn new(schema: &UuidSchema) -> Self {
        match schema {
            UuidSchema::String => {
                Self::String(StringFsm::default())
            }
            UuidSchema::Fixed(fixed_schema) => {
                assert_eq!(fixed_schema.size, 16, "Uuid(Fixed) must be 16 bytes");
                Self::FixedFSM(FixedFsm::new(16))
            }
        }
    }
}
impl Fsm for UuidFsm {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        match self {
            UuidFsm::String(fsm) => {
                fsm.parse(buffer)?.map_fallible(|fsm| Ok(Self::String(fsm)), |v| {
                    let Value::String(string) = v else { unreachable!() };
                    Ok(Value::Uuid(Uuid::parse_str(&string).map_err(Details::UuidFromSlice)?))
                })
            }
            UuidFsm::FixedFSM(fsm) => {
                fsm.parse(buffer)?.map_fallible(|fsm| Ok(Self::FixedFSM(fsm)), |v| {
                    let Value::Bytes(bytes) = v else { unreachable!() };
                    Ok(Value::Uuid(Uuid::from_slice(&bytes).map_err(Details::UuidFromSlice)?))
                })
            }
        }
    }
}
