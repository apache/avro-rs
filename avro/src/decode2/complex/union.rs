use oval::Buffer;
use crate::decode2::{Fsm, FsmControlFlow, FsmResult, SubFsm};
use crate::decode2::decode_zigzag_buffer;
use crate::error::Details;
use crate::schema::UnionSchema;
use crate::types::Value;

pub struct UnionFsm<'a> {
    schema: &'a UnionSchema,
    index: Option<u32>,
    sub_fsm: Option<Box<SubFsm<'a>>>
}

impl<'a> UnionFsm<'a> {
    pub fn new(schema: &'a UnionSchema) -> UnionFsm<'a> {
        Self {
            schema,
            index: None,
            sub_fsm: None
        }
    }
}

impl<'a> Fsm for UnionFsm<'a> {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        if self.index.is_none() {
            let Some(n) = decode_zigzag_buffer(buffer)? else {
                return Ok(FsmControlFlow::NeedMore(self));
            };
            // TODO: proper error
            self.index = Some(u32::try_from(n).map_err(|e| Details::EmptyUnion)?);
        }
        let index = self.index.unwrap_or_else(|| unreachable!());
        if let Some(sub_fsm) = self.sub_fsm.as_deref_mut() {
            let fsm = std::mem::take(sub_fsm);
            match fsm.parse(buffer)? {
                FsmControlFlow::NeedMore(fsm) => {
                    let _ = std::mem::replace(sub_fsm, fsm);
                    Ok(FsmControlFlow::NeedMore(self))
                }
                FsmControlFlow::Done(value) => {
                    Ok(FsmControlFlow::Done(Value::Union(index, Box::new(value))))
                }
            }
        } else {
            // TODO: proper error, GetUnionVariants
            let index_usize = usize::try_from(index).map_err(|e| Details::EmptyUnion)?;
            let schema = self.schema.schemas.get(index_usize).ok_or(Details::EmptyUnion)?;

            match SubFsm::from(schema).parse(buffer)? {
                FsmControlFlow::NeedMore(fsm) => {
                    self.sub_fsm = Some(Box::new(fsm));
                    Ok(FsmControlFlow::NeedMore(self))
                }
                FsmControlFlow::Done(value) => {
                    Ok(FsmControlFlow::Done(Value::Union(index, Box::new(value))))
                }
            }
        }
    }
}

