use crate::{
    error::Details,
    state_machines::reading::{
        ItemRead, StateMachine, StateMachineControlFlow, StateMachineResult, SubStateMachine,
        commands::CommandTape, decode_zigzag_buffer,
    },
};
use oval::Buffer;

enum VariantsOrFsm {
    Variants {
        variants: CommandTape,
        read: Vec<ItemRead>,
    },
    Fsm(Box<SubStateMachine>),
}

pub struct UnionStateMachine {
    variants_or_fsm: VariantsOrFsm,
    num_variants: usize,
}

impl UnionStateMachine {
    pub fn new_with_tape(variants: CommandTape, num_variants: usize, read: Vec<ItemRead>) -> Self {
        Self {
            variants_or_fsm: VariantsOrFsm::Variants { variants, read },
            num_variants,
        }
    }
}

impl StateMachine for UnionStateMachine {
    type Output = Vec<ItemRead>;

    fn parse(mut self, buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output> {
        match self.variants_or_fsm {
            VariantsOrFsm::Variants {
                mut variants,
                mut read,
            } => {
                let Some(index) = decode_zigzag_buffer(buffer)? else {
                    // Not enough data left in the buffer
                    self.variants_or_fsm = VariantsOrFsm::Variants { variants, read };
                    return Ok(StateMachineControlFlow::NeedMore(self));
                };
                let option =
                    usize::try_from(index).map_err(|e| Details::ConvertI64ToUsize(e, index))?;

                variants.skip(option).ok_or(Details::GetUnionVariant {
                    index,
                    num_variants: self.num_variants,
                })?;

                let variant = variants.command().ok_or(Details::GetUnionVariant {
                    index,
                    num_variants: self.num_variants,
                })?;

                read.push(ItemRead::Union(u32::try_from(option).unwrap()));

                match variant.into_state_machine(read).parse(buffer)? {
                    StateMachineControlFlow::NeedMore(fsm) => {
                        self.variants_or_fsm = VariantsOrFsm::Fsm(Box::new(fsm));
                        Ok(StateMachineControlFlow::NeedMore(self))
                    }
                    StateMachineControlFlow::Done(read) => Ok(StateMachineControlFlow::Done(read)),
                }
            }
            VariantsOrFsm::Fsm(fsm) => match fsm.parse(buffer)? {
                StateMachineControlFlow::NeedMore(fsm) => {
                    self.variants_or_fsm = VariantsOrFsm::Fsm(Box::new(fsm));
                    Ok(StateMachineControlFlow::NeedMore(self))
                }
                StateMachineControlFlow::Done(read) => Ok(StateMachineControlFlow::Done(read)),
            },
        }
    }
}
