use std::{io::Read, ops::DerefMut as _};

use oval::Buffer;

use crate::{
    error::Details,
    state_machines::reading::{
        CommandTape, DataRequest, ItemRead, StateMachine, StateMachineControlFlow, SubStateMachine,
        ToRead,
        block::{ArrayStateMachine, MapStateMachine},
        bytes::BytesStateMachine,
        decode_zigzag, replace_drop,
    },
};

use super::StateMachineResult;

pub struct ObjectStateMachine {
    command_tape: CommandTape,
    current_sub_machine: Box<SubStateMachine>,
    tape: Vec<ItemRead>,
}

impl ObjectStateMachine {
    /// Create a new state machine that reads an object from the commands.
    pub fn new(command_tape: CommandTape) -> Self {
        Self::new_with_tape(command_tape, Vec::new())
    }

    /// Create a new state machine that appends to the tape (the tape is returned on completion).
    pub fn new_with_tape(command_tape: CommandTape, tape: Vec<ItemRead>) -> Self {
        Self {
            command_tape,
            current_sub_machine: Box::new(SubStateMachine::None),
            tape,
        }
    }
}

impl StateMachine for ObjectStateMachine {
    type Output = Vec<ItemRead>;

    fn parse(mut self, buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output> {
        while !buffer.empty() {
            // Only continue reading the tape if we're not currently driving a sub state machine.
            match std::mem::take(self.current_sub_machine.deref_mut()) {
                SubStateMachine::None => {
                    match self.command_tape.command() {
                        ToRead::Null => self.tape.push(ItemRead::Null),
                        ToRead::Boolean => {
                            let mut byte = [0; 1];
                            buffer
                                .read_exact(&mut byte)
                                .expect("Unreachable! Buffer is not empty");
                            match byte {
                                [0] => self.tape.push(ItemRead::Boolean(false)),
                                [1] => self.tape.push(ItemRead::Boolean(true)),
                                [byte] => return Err(Details::BoolValue(byte).into()),
                            }
                        }
                        ToRead::Int => {
                            let Some(n) = decode_zigzag(buffer)? else {
                                // Not enough data left in the buffer, need at least one more
                                replace_drop(
                                    self.current_sub_machine.deref_mut(),
                                    SubStateMachine::Int,
                                );
                                return Ok(StateMachineControlFlow::Continue(
                                    self,
                                    DataRequest::new(1),
                                ));
                            };
                            let n = i32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
                            self.tape.push(ItemRead::Int(n));
                        }
                        ToRead::Long => {
                            let Some(n) = decode_zigzag(buffer)? else {
                                // Not enough data left in the buffer, need at least one more
                                replace_drop(
                                    self.current_sub_machine.deref_mut(),
                                    SubStateMachine::Long,
                                );
                                return Ok(StateMachineControlFlow::Continue(
                                    self,
                                    DataRequest::new(1),
                                ));
                            };
                            self.tape.push(ItemRead::Long(n));
                        }
                        ToRead::Float => {
                            let Some(bytes) = buffer.data().first_chunk().copied() else {
                                // Not enough data left in the buffer
                                replace_drop(
                                    self.current_sub_machine.deref_mut(),
                                    SubStateMachine::Float,
                                );
                                return Ok(StateMachineControlFlow::Continue(
                                    self,
                                    DataRequest::new(4 - buffer.available_data()),
                                ));
                            };
                            self.tape.push(ItemRead::Float(f32::from_le_bytes(bytes)))
                        }
                        ToRead::Double => {
                            let Some(bytes) = buffer.data().first_chunk().copied() else {
                                // Not enough data left in the buffer
                                replace_drop(
                                    self.current_sub_machine.deref_mut(),
                                    SubStateMachine::Double,
                                );
                                return Ok(StateMachineControlFlow::Continue(
                                    self,
                                    DataRequest::new(8 - buffer.available_data()),
                                ));
                            };
                            self.tape.push(ItemRead::Double(f64::from_le_bytes(bytes)))
                        }
                        ToRead::Bytes => {
                            let fsm = BytesStateMachine::new();
                            // Optimistically run the state machine
                            match fsm.parse(buffer)? {
                                StateMachineControlFlow::Continue(fsm, data_request) => {
                                    replace_drop(
                                        self.current_sub_machine.deref_mut(),
                                        SubStateMachine::Bytes(fsm),
                                    );
                                    return Ok(StateMachineControlFlow::Continue(
                                        self,
                                        data_request,
                                    ));
                                }
                                StateMachineControlFlow::Done(bytes) => {
                                    self.tape.push(ItemRead::Bytes(bytes.into_boxed_slice()))
                                }
                            }
                        }
                        ToRead::String => {
                            let fsm = BytesStateMachine::new();
                            // Optimistically run the state machine
                            match fsm.parse(buffer)? {
                                StateMachineControlFlow::Continue(fsm, data_request) => {
                                    replace_drop(
                                        self.current_sub_machine.deref_mut(),
                                        SubStateMachine::String(fsm),
                                    );
                                    return Ok(StateMachineControlFlow::Continue(
                                        self,
                                        data_request,
                                    ));
                                }
                                StateMachineControlFlow::Done(bytes) => {
                                    let string =
                                        String::from_utf8(bytes).map_err(Details::ConvertToUtf8)?;
                                    self.tape.push(ItemRead::String(string.into_boxed_str()))
                                }
                            }
                        }
                        ToRead::Enum => {
                            let Some(n) = decode_zigzag(buffer)? else {
                                // Not enough data left in the buffer, need at least one more
                                replace_drop(
                                    self.current_sub_machine.deref_mut(),
                                    SubStateMachine::Enum,
                                );
                                return Ok(StateMachineControlFlow::Continue(
                                    self,
                                    DataRequest::new(1),
                                ));
                            };
                            // TODO: Wrong error
                            let n = u32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
                            self.tape.push(ItemRead::Enum(n));
                        }
                        ToRead::Fixed(length) => {
                            let fsm = BytesStateMachine::new_with_length(length);
                            // Optimistically run the state machine
                            match fsm.parse(buffer)? {
                                StateMachineControlFlow::Continue(fsm, data_request) => {
                                    replace_drop(
                                        self.current_sub_machine.deref_mut(),
                                        SubStateMachine::Bytes(fsm),
                                    );
                                    return Ok(StateMachineControlFlow::Continue(
                                        self,
                                        data_request,
                                    ));
                                }
                                StateMachineControlFlow::Done(bytes) => {
                                    self.tape.push(ItemRead::Bytes(bytes.into_boxed_slice()))
                                }
                            }
                        }
                        ToRead::Array(command_tape) => {
                            let fsm = ArrayStateMachine::new(
                                command_tape,
                                std::mem::take(&mut self.tape),
                            );
                            // Optimistically run the state machine
                            match fsm.parse(buffer)? {
                                StateMachineControlFlow::Continue(fsm, data_request) => {
                                    replace_drop(
                                        self.current_sub_machine.deref_mut(),
                                        SubStateMachine::Array(fsm),
                                    );
                                    return Ok(StateMachineControlFlow::Continue(
                                        self,
                                        data_request,
                                    ));
                                }
                                StateMachineControlFlow::Done(tape) => {
                                    self.tape = tape;
                                }
                            }
                        }
                        ToRead::Map(command_tape) => {
                            let fsm =
                                MapStateMachine::new(command_tape, std::mem::take(&mut self.tape));
                            // Optimistically run the state machine
                            match fsm.parse(buffer)? {
                                StateMachineControlFlow::Continue(fsm, data_request) => {
                                    replace_drop(
                                        self.current_sub_machine.deref_mut(),
                                        SubStateMachine::Map(fsm),
                                    );
                                    return Ok(StateMachineControlFlow::Continue(
                                        self,
                                        data_request,
                                    ));
                                }
                                StateMachineControlFlow::Done(tape) => {
                                    self.tape = tape;
                                }
                            }
                        }
                        ToRead::Union(variants) => {
                            // Optimistically try to get the variant
                            let Some(index) = decode_zigzag(buffer)? else {
                                // Not enough data left in the buffer, need at least one more
                                replace_drop(
                                    self.current_sub_machine.deref_mut(),
                                    SubStateMachine::Union(variants),
                                );
                                return Ok(StateMachineControlFlow::Continue(
                                    self,
                                    DataRequest::new(1),
                                ));
                            };
                            let option = usize::try_from(index)
                                .map_err(|e| Details::ConvertI64ToUsize(e, index))?;
                            let variant = variants.get(option).ok_or(Details::GetUnionVariant {
                                index,
                                num_variants: variants.len(),
                            })?;
                            let fsm = ObjectStateMachine::new_with_tape(
                                variant.clone(),
                                std::mem::take(&mut self.tape),
                            );

                            // Optimistically run the state machine
                            match fsm.parse(buffer)? {
                                StateMachineControlFlow::Continue(fsm, data_request) => {
                                    replace_drop(
                                        self.current_sub_machine.deref_mut(),
                                        SubStateMachine::Object(fsm),
                                    );
                                    return Ok(StateMachineControlFlow::Continue(
                                        self,
                                        data_request,
                                    ));
                                }
                                StateMachineControlFlow::Done(tape) => {
                                    self.tape = tape;
                                }
                            }
                        }
                    }
                }
                SubStateMachine::Int => {
                    let Some(n) = decode_zigzag(buffer)? else {
                        // Not enough data left in the buffer, need at least one more
                        return Ok(StateMachineControlFlow::Continue(self, DataRequest::new(1)));
                    };
                    let n = i32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
                    self.tape.push(ItemRead::Int(n));
                }
                SubStateMachine::Long => {
                    let Some(n) = decode_zigzag(buffer)? else {
                        // Not enough data left in the buffer, need at least one more
                        return Ok(StateMachineControlFlow::Continue(self, DataRequest::new(1)));
                    };
                    self.tape.push(ItemRead::Long(n));
                }
                SubStateMachine::Float => {
                    let Some(bytes) = buffer.data().first_chunk().copied() else {
                        // Not enough data left in the buffer
                        return Ok(StateMachineControlFlow::Continue(
                            self,
                            DataRequest::new(4 - buffer.available_data()),
                        ));
                    };
                    self.tape.push(ItemRead::Float(f32::from_le_bytes(bytes)))
                }
                SubStateMachine::Double => {
                    let Some(bytes) = buffer.data().first_chunk().copied() else {
                        // Not enough data left in the buffer
                        return Ok(StateMachineControlFlow::Continue(
                            self,
                            DataRequest::new(8 - buffer.available_data()),
                        ));
                    };
                    self.tape.push(ItemRead::Double(f64::from_le_bytes(bytes)))
                }
                SubStateMachine::Enum => {
                    let Some(n) = decode_zigzag(buffer)? else {
                        // Not enough data left in the buffer, need at least one more
                        return Ok(StateMachineControlFlow::Continue(self, DataRequest::new(1)));
                    };
                    // TODO: Wrong error
                    let n = u32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
                    self.tape.push(ItemRead::Enum(n));
                }
                SubStateMachine::Bytes(fsm) => match fsm.parse(buffer)? {
                    StateMachineControlFlow::Continue(fsm, data_request) => {
                        replace_drop(
                            self.current_sub_machine.deref_mut(),
                            SubStateMachine::Bytes(fsm),
                        );
                        return Ok(StateMachineControlFlow::Continue(self, data_request));
                    }
                    StateMachineControlFlow::Done(bytes) => {
                        self.tape.push(ItemRead::Bytes(bytes.into_boxed_slice()))
                    }
                },
                SubStateMachine::String(fsm) => match fsm.parse(buffer)? {
                    StateMachineControlFlow::Continue(fsm, data_request) => {
                        replace_drop(
                            self.current_sub_machine.deref_mut(),
                            SubStateMachine::String(fsm),
                        );
                        return Ok(StateMachineControlFlow::Continue(self, data_request));
                    }
                    StateMachineControlFlow::Done(bytes) => {
                        let string = String::from_utf8(bytes).map_err(Details::ConvertToUtf8)?;
                        self.tape.push(ItemRead::String(string.into_boxed_str()))
                    }
                },
                SubStateMachine::Fixed(fsm) => match fsm.parse(buffer)? {
                    StateMachineControlFlow::Continue(fsm, data_request) => {
                        replace_drop(
                            self.current_sub_machine.deref_mut(),
                            SubStateMachine::Bytes(fsm),
                        );
                        return Ok(StateMachineControlFlow::Continue(self, data_request));
                    }
                    StateMachineControlFlow::Done(bytes) => {
                        self.tape.push(ItemRead::Bytes(bytes.into_boxed_slice()))
                    }
                },
                SubStateMachine::Array(fsm) => match fsm.parse(buffer)? {
                    StateMachineControlFlow::Continue(fsm, data_request) => {
                        replace_drop(
                            self.current_sub_machine.deref_mut(),
                            SubStateMachine::Array(fsm),
                        );
                        return Ok(StateMachineControlFlow::Continue(self, data_request));
                    }
                    StateMachineControlFlow::Done(tape) => {
                        self.tape = tape;
                    }
                },
                SubStateMachine::Map(fsm) => match fsm.parse(buffer)? {
                    StateMachineControlFlow::Continue(fsm, data_request) => {
                        replace_drop(
                            self.current_sub_machine.deref_mut(),
                            SubStateMachine::Map(fsm),
                        );
                        return Ok(StateMachineControlFlow::Continue(self, data_request));
                    }
                    StateMachineControlFlow::Done(tape) => {
                        self.tape = tape;
                    }
                },
                SubStateMachine::Union(variants) => {
                    let Some(index) = decode_zigzag(buffer)? else {
                        // Not enough data left in the buffer, need at least one more
                        return Ok(StateMachineControlFlow::Continue(self, DataRequest::new(1)));
                    };
                    let option =
                        usize::try_from(index).map_err(|e| Details::ConvertI64ToUsize(e, index))?;
                    let variant = variants.get(option).ok_or(Details::GetUnionVariant {
                        index,
                        num_variants: variants.len(),
                    })?;
                    let fsm = ObjectStateMachine::new_with_tape(
                        variant.clone(),
                        std::mem::take(&mut self.tape),
                    );

                    // Optimistically run the state machine
                    match fsm.parse(buffer)? {
                        StateMachineControlFlow::Continue(fsm, data_request) => {
                            replace_drop(
                                self.current_sub_machine.deref_mut(),
                                SubStateMachine::Object(fsm),
                            );
                            return Ok(StateMachineControlFlow::Continue(self, data_request));
                        }
                        StateMachineControlFlow::Done(tape) => {
                            self.tape = tape;
                        }
                    }
                }
                SubStateMachine::Object(_) => unreachable!(),
            }
        }
        if self.command_tape.is_finished() {
            Ok(StateMachineControlFlow::Done(self.tape))
        } else {
            Ok(StateMachineControlFlow::Continue(self, DataRequest::new(1)))
        }
    }
}
