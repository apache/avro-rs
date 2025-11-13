use oval::Buffer;

use crate::{
    Error,
    encode2::{
        CommandTape, ItemRead, StateMachine, StateMachineControlFlow, datum::DatumStateMachine,
        decode_zigzag_buffer,
    },
    error::Details,
};

/// Are we currently parsing an object or just finished/reading a block header
enum TapeOrFsm {
    Tape(Vec<ItemRead>),
    Fsm(DatumStateMachine),
}

pub struct BlockStateMachine {
    command_tape: CommandTape,
    tape_or_fsm: TapeOrFsm,
    left_in_current_block: usize,
    need_to_read_block_byte_size: bool,
}

impl BlockStateMachine {
    pub fn new_with_tape(command_tape: CommandTape, tape: Vec<ItemRead>) -> Self {
        Self {
            // This clone is *cheap*
            command_tape,
            tape_or_fsm: TapeOrFsm::Tape(tape),
            left_in_current_block: 0,
            need_to_read_block_byte_size: false,
        }
    }
}

impl StateMachine for BlockStateMachine {
    type Output = Vec<ItemRead>;
    fn parse(
        mut self,
        buffer: &mut Buffer,
    ) -> Result<StateMachineControlFlow<Self, Self::Output>, Error> {
        loop {
            match self.tape_or_fsm {
                TapeOrFsm::Tape(mut tape) => {
                    // If we finished the last block (or are newly created) read the block info
                    if self.left_in_current_block == 0 {
                        let Some(block) = decode_zigzag_buffer(buffer)? else {
                            // Not enough data left in the buffer
                            self.tape_or_fsm = TapeOrFsm::Tape(tape);
                            return Ok(StateMachineControlFlow::NeedMore(self));
                        };

                        // Need to read the block byte size when block is negative
                        self.need_to_read_block_byte_size = block.is_negative();

                        // We do the rest with the absolute block size
                        let abs_block = usize::try_from(block.unsigned_abs())
                            .map_err(|e| Details::ConvertU64ToUsize(e, block.unsigned_abs()))?;
                        self.left_in_current_block = abs_block;
                        tape.push(ItemRead::Block(abs_block));

                        // Done parsing the blocks
                        if abs_block == 0 {
                            return Ok(StateMachineControlFlow::Done(tape));
                        }
                    }

                    // If the block length was negative we need to read the block size
                    if self.need_to_read_block_byte_size {
                        let Some(block) = decode_zigzag_buffer(buffer)? else {
                            // Not enough data left in the buffer
                            self.tape_or_fsm = TapeOrFsm::Tape(tape);
                            return Ok(StateMachineControlFlow::NeedMore(self));
                        };

                        // Make sure the value is sane
                        // TODO: Maybe use safe_len here?
                        let _ = usize::try_from(block)
                            .map_err(|e| Details::ConvertI64ToUsize(e, block))?;

                        // This is not necessary, as it will be overwritten before being read again
                        // but it does show the intent more clearly
                        self.need_to_read_block_byte_size = false;
                    }

                    // We've either finished reading the block header or the last object was read and
                    // left_in_current_block is not zero
                    self.tape_or_fsm = TapeOrFsm::Fsm(DatumStateMachine::new_with_tape(
                        self.command_tape.clone(),
                        tape,
                    ))
                }
                TapeOrFsm::Fsm(fsm) => {
                    // (Continue) reading the object
                    match fsm.parse(buffer)? {
                        StateMachineControlFlow::NeedMore(fsm) => {
                            self.tape_or_fsm = TapeOrFsm::Fsm(fsm);
                            return Ok(StateMachineControlFlow::NeedMore(self));
                        }
                        StateMachineControlFlow::Done(tape) => {
                            self.tape_or_fsm = TapeOrFsm::Tape(tape);
                            self.left_in_current_block -= 1;
                        }
                    }
                }
            }
        }
    }
}
