use oval::Buffer;

use super::StateMachineResult;
use crate::state_machines::reading::{
    CommandTape, ItemRead, StateMachine, StateMachineControlFlow, SubStateMachine,
};

enum TapeOrFsm {
    Tape(Vec<ItemRead>),
    Fsm(Box<SubStateMachine>),
}

pub struct DatumStateMachine {
    command_tape: CommandTape,
    tape_or_fsm: TapeOrFsm,
}

impl DatumStateMachine {
    /// Create a new state machine that reads a datum from the commands.
    pub fn new(command_tape: CommandTape) -> Self {
        Self::new_with_tape(command_tape, Vec::new())
    }

    /// Create a new state machine that appends to the tape (the tape is returned on completion).
    pub fn new_with_tape(command_tape: CommandTape, tape: Vec<ItemRead>) -> Self {
        Self {
            command_tape,
            tape_or_fsm: TapeOrFsm::Tape(tape),
        }
    }
}

impl StateMachine for DatumStateMachine {
    type Output = Vec<ItemRead>;

    fn parse(mut self, buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output> {
        // While there's data and commands to process we keep progressing the state machines
        while !buffer.data().is_empty() {
            match self.tape_or_fsm {
                TapeOrFsm::Fsm(fsm) => match fsm.parse(buffer)? {
                    StateMachineControlFlow::NeedMore(fsm) => {
                        self.tape_or_fsm = TapeOrFsm::Fsm(Box::new(fsm));
                        return Ok(StateMachineControlFlow::NeedMore(self));
                    }
                    StateMachineControlFlow::Done(read) => {
                        self.tape_or_fsm = TapeOrFsm::Tape(read);
                    }
                },
                TapeOrFsm::Tape(tape) => {
                    if let Some(command) = self.command_tape.command() {
                        let fsm = command.into_state_machine(tape);
                        // This is a duplicate of the TapeOrFsm::Fsm logic, but saves us an allocation
                        // by doing it immediately.
                        match fsm.parse(buffer)? {
                            StateMachineControlFlow::NeedMore(fsm) => {
                                self.tape_or_fsm = TapeOrFsm::Fsm(Box::new(fsm));
                                return Ok(StateMachineControlFlow::NeedMore(self));
                            }
                            StateMachineControlFlow::Done(read) => {
                                self.tape_or_fsm = TapeOrFsm::Tape(read);
                            }
                        }
                    } else {
                        self.tape_or_fsm = TapeOrFsm::Tape(tape);
                        break;
                    }
                }
            }
        }

        // Check if we're completely finished or need more data
        match (self.tape_or_fsm, self.command_tape.is_finished()) {
            (TapeOrFsm::Tape(read), true) => Ok(StateMachineControlFlow::Done(read)),
            (tape_or_fsm, _) => {
                self.tape_or_fsm = tape_or_fsm;
                Ok(StateMachineControlFlow::NeedMore(self))
            }
        }
    }
}
