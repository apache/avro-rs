use std::io::Read;

use oval::Buffer;
use serde::Deserialize;

use crate::{
    Error, Schema,
    error::Details,
    state_machines::reading::{
        ItemRead, StateMachine, StateMachineControlFlow,
        commands::CommandTape,
        deserialize_from_tape,
        object_container_file::{
            ObjectContainerFileBodyStateMachine, ObjectContainerFileHeader,
            ObjectContainerFileHeaderStateMachine,
        },
        value_from_tape,
    },
    types::Value,
};

pub struct ObjectContainerFileReader<'a, R> {
    reader_schema: Option<&'a Schema>,
    header: ObjectContainerFileHeader,
    fsm: Option<ObjectContainerFileBodyStateMachine>,
    reader: R,
    buffer: Buffer,
}

impl<'a, R: Read> ObjectContainerFileReader<'a, R> {
    /// Create a new reader for the Object Container file format.
    pub fn new(mut reader: R) -> Result<Self, Error> {
        // Read a maximum of 2Kb per read
        let mut buffer = Buffer::with_capacity(2 * 1024);

        // Parse the header
        let mut fsm = ObjectContainerFileHeaderStateMachine::new();
        let header = loop {
            // Fill the buffer
            let n = reader.read(buffer.space()).map_err(Details::ReadHeader)?;
            if n == 0 {
                return Err(Details::ReadHeader(std::io::ErrorKind::UnexpectedEof.into()).into());
            }
            buffer.fill(n);

            // Start/continue the state machine
            match fsm.parse(&mut buffer)? {
                StateMachineControlFlow::NeedMore(new_fsm) => fsm = new_fsm,
                StateMachineControlFlow::Done(header) => break header,
            }
        };

        let tape = CommandTape::build_from_schema(&header.schema)?;

        Ok(Self {
            reader_schema: None,
            fsm: Some(ObjectContainerFileBodyStateMachine::new(
                tape,
                header.sync,
                header.codec,
            )),
            header,
            reader,
            buffer,
        })
    }

    pub fn header(&self) -> &ObjectContainerFileHeader {
        &self.header
    }

    /// Get the next object in the file
    fn next_object(&mut self) -> Option<Result<Vec<ItemRead>, Error>> {
        if let Some(mut fsm) = self.fsm.take() {
            loop {
                match fsm.parse(&mut self.buffer) {
                    Ok(StateMachineControlFlow::NeedMore(new_fsm)) => {
                        fsm = new_fsm;
                        let n = match self.reader.read(self.buffer.space()) {
                            Ok(0) => {
                                return Some(Err(Details::ReadIntoBuf(
                                    std::io::ErrorKind::UnexpectedEof.into(),
                                )
                                .into()));
                            }
                            Ok(n) => n,
                            Err(e) => return Some(Err(Details::ReadIntoBuf(e).into())),
                        };
                        self.buffer.fill(n);
                    }
                    Ok(StateMachineControlFlow::Done(Some((object, fsm)))) => {
                        self.fsm.replace(fsm);
                        return Some(Ok(object));
                    }
                    Ok(StateMachineControlFlow::Done(None)) => {
                        return None;
                    }
                    Err(e) => {
                        return Some(Err(e));
                    }
                }
            }
        }
        None
    }

    pub fn next_serde<'b, T: Deserialize<'b>>(&mut self) -> Option<Result<T, Error>> {
        self.next_object().map(|r| {
            r.and_then(|_tape| {
                todo!()
                // deserialize_from_tape(&mut tape, self.reader_schema.unwrap_or(&self.header.schema), todo!(), todo!())
            })
        })
    }
}

impl<R: Read> Iterator for ObjectContainerFileReader<'_, R> {
    type Item = Result<Value, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_object().map(|r| {
            r.and_then(|_tape| {
                todo!()
                // value_from_tape(&mut tape, self.reader_schema.unwrap_or(&self.header.schema), todo!(), todo!())
            })
        })
    }
}
