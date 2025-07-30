use std::ops::DerefMut as _;

use oval::Buffer;

use crate::{
    Error,
    error::Details,
    state_machines::reading::{
        CommandTape, ItemRead, StateMachine, StateMachineControlFlow, SubStateMachine,
        bytes::BytesStateMachine, decode_zigzag, object::ObjectStateMachine, replace_drop,
    },
};

pub struct ArrayStateMachine {
    command_tape: CommandTape,
    current_sub_machine: Box<SubStateMachine>,
    tape: Vec<ItemRead>,
    left_in_current_block: usize,
    need_to_read_block_byte_size: bool,
}

impl ArrayStateMachine {
    pub fn new(command_tape: CommandTape, tape: Vec<ItemRead>) -> Self {
        Self {
            command_tape,
            current_sub_machine: Box::new(SubStateMachine::None),
            tape,
            left_in_current_block: 0,
            need_to_read_block_byte_size: false,
        }
    }
}

impl StateMachine for ArrayStateMachine {
    type Output = Vec<ItemRead>;

    fn parse(
        mut self,
        buffer: &mut Buffer,
    ) -> Result<StateMachineControlFlow<Self, Self::Output>, Error> {
        loop {
            // If we finished the last block (or are newly created) read the block info
            if self.left_in_current_block == 0 {
                let Some(block) = decode_zigzag(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(self));
                };
                self.need_to_read_block_byte_size = block.is_negative();
                let abs_block = block.unsigned_abs();
                let abs_block = usize::try_from(abs_block)
                    .map_err(|e| Details::ConvertU64ToUsize(e, abs_block))?;
                self.tape.push(ItemRead::Block(abs_block));
                if abs_block == 0 {
                    // Done parsing the array
                    return Ok(StateMachineControlFlow::Done(self.tape));
                }
            }
            // If the block length was negative we need to read the block size
            if self.need_to_read_block_byte_size {
                let Some(block) = decode_zigzag(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(self));
                };
                // Make sure the value is sane
                let _ = usize::try_from(block).map_err(|e| Details::ConvertI64ToUsize(e, block))?;
                self.need_to_read_block_byte_size = false;
            }

            // Either run the existing state machine or create a new one and run that
            match std::mem::take(self.current_sub_machine.deref_mut()) {
                SubStateMachine::None => {
                    let fsm = ObjectStateMachine::new_with_tape(
                        self.command_tape.clone(),
                        std::mem::take(&mut self.tape),
                    );
                    // Optimistically run the state machine
                    match fsm.parse(buffer)? {
                        StateMachineControlFlow::NeedMore(fsm) => {
                            replace_drop(
                                self.current_sub_machine.deref_mut(),
                                SubStateMachine::Object(fsm),
                            );
                            return Ok(StateMachineControlFlow::NeedMore(self));
                        }
                        StateMachineControlFlow::Done(tape) => {
                            self.tape = tape;
                            self.left_in_current_block -= 1;
                        }
                    }
                }
                SubStateMachine::Object(fsm) => match fsm.parse(buffer)? {
                    StateMachineControlFlow::NeedMore(fsm) => {
                        replace_drop(
                            self.current_sub_machine.deref_mut(),
                            SubStateMachine::Object(fsm),
                        );
                        return Ok(StateMachineControlFlow::NeedMore(self));
                    }
                    StateMachineControlFlow::Done(tape) => {
                        self.tape = tape;
                        replace_drop(self.current_sub_machine.deref_mut(), SubStateMachine::None);
                        self.left_in_current_block -= 1;
                    }
                },
                _ => unreachable!(),
            }
        }
    }
}

pub struct MapStateMachine {
    command_tape: CommandTape,
    current_sub_machine: Box<SubStateMachine>,
    tape: Vec<ItemRead>,
    left_in_current_block: usize,
    need_to_read_block_byte_size: bool,
}

impl MapStateMachine {
    pub fn new(command_tape: CommandTape, tape: Vec<ItemRead>) -> Self {
        Self {
            command_tape,
            current_sub_machine: Box::new(SubStateMachine::None),
            tape,
            left_in_current_block: 0,
            need_to_read_block_byte_size: false,
        }
    }
}

impl StateMachine for MapStateMachine {
    type Output = Vec<ItemRead>;

    fn parse(
        mut self,
        buffer: &mut Buffer,
    ) -> Result<StateMachineControlFlow<Self, Self::Output>, Error> {
        loop {
            // If we finished the last block (or are newly created) read the block info
            if self.left_in_current_block == 0 {
                let Some(block) = decode_zigzag(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(self));
                };
                self.need_to_read_block_byte_size = block.is_negative();
                let abs_block = block.unsigned_abs();
                let abs_block = usize::try_from(abs_block)
                    .map_err(|e| Details::ConvertU64ToUsize(e, abs_block))?;
                self.tape.push(ItemRead::Block(abs_block));
                if abs_block == 0 {
                    // Done parsing the map
                    return Ok(StateMachineControlFlow::Done(self.tape));
                }
            }
            // If the block length was negative we need to read the block size
            if self.need_to_read_block_byte_size {
                let Some(block) = decode_zigzag(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(self));
                };
                // Make sure the value is sane
                let _ = usize::try_from(block).map_err(|e| Details::ConvertI64ToUsize(e, block))?;
                self.need_to_read_block_byte_size = false;
            }

            // Either run the existing state machine or create a new one and run that
            match std::mem::take(self.current_sub_machine.deref_mut()) {
                SubStateMachine::None => {
                    let fsm = BytesStateMachine::new();
                    // Optimistically run the state machine
                    match fsm.parse(buffer)? {
                        StateMachineControlFlow::NeedMore(fsm) => {
                            replace_drop(
                                self.current_sub_machine.deref_mut(),
                                SubStateMachine::String(fsm),
                            );
                            return Ok(StateMachineControlFlow::NeedMore(self));
                        }
                        StateMachineControlFlow::Done(bytes) => {
                            let string =
                                String::from_utf8(bytes).map_err(Details::ConvertToUtf8)?;
                            self.tape.push(ItemRead::String(string.into_boxed_str()));
                            replace_drop(
                                self.current_sub_machine.deref_mut(),
                                SubStateMachine::None,
                            );
                        }
                    }
                    // Finished reading the key, start on the value
                    let fsm = ObjectStateMachine::new_with_tape(
                        self.command_tape.clone(),
                        std::mem::take(&mut self.tape),
                    );
                    // Optimistically run the state machine
                    match fsm.parse(buffer)? {
                        StateMachineControlFlow::NeedMore(fsm) => {
                            replace_drop(
                                self.current_sub_machine.deref_mut(),
                                SubStateMachine::Object(fsm),
                            );
                            return Ok(StateMachineControlFlow::NeedMore(self));
                        }
                        StateMachineControlFlow::Done(tape) => {
                            self.tape = tape;
                            self.left_in_current_block -= 1;
                        }
                    }
                }
                SubStateMachine::String(fsm) => {
                    // We didn't finish reading the key last loop
                    match fsm.parse(buffer)? {
                        StateMachineControlFlow::NeedMore(fsm) => {
                            replace_drop(
                                self.current_sub_machine.deref_mut(),
                                SubStateMachine::String(fsm),
                            );
                            return Ok(StateMachineControlFlow::NeedMore(self));
                        }
                        StateMachineControlFlow::Done(bytes) => {
                            let string =
                                String::from_utf8(bytes).map_err(Details::ConvertToUtf8)?;
                            self.tape.push(ItemRead::String(string.into_boxed_str()));
                            replace_drop(
                                self.current_sub_machine.deref_mut(),
                                SubStateMachine::None,
                            );
                        }
                    }
                    // Finished reading the key, start on the value
                    let fsm = ObjectStateMachine::new_with_tape(
                        self.command_tape.clone(),
                        std::mem::take(&mut self.tape),
                    );
                    // Optimistically run the state machine
                    match fsm.parse(buffer)? {
                        StateMachineControlFlow::NeedMore(fsm) => {
                            replace_drop(
                                self.current_sub_machine.deref_mut(),
                                SubStateMachine::Object(fsm),
                            );
                            return Ok(StateMachineControlFlow::NeedMore(self));
                        }
                        StateMachineControlFlow::Done(tape) => {
                            self.tape = tape;
                            self.left_in_current_block -= 1;
                        }
                    }
                }
                SubStateMachine::Object(fsm) => {
                    // We didn't finish reading the value last loop
                    match fsm.parse(buffer)? {
                        StateMachineControlFlow::NeedMore(fsm) => {
                            replace_drop(
                                self.current_sub_machine.deref_mut(),
                                SubStateMachine::Object(fsm),
                            );
                            return Ok(StateMachineControlFlow::NeedMore(self));
                        }
                        StateMachineControlFlow::Done(tape) => {
                            self.tape = tape;
                            replace_drop(
                                self.current_sub_machine.deref_mut(),
                                SubStateMachine::None,
                            );
                            self.left_in_current_block -= 1;
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}
