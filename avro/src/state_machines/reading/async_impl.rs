use std::pin::Pin;

use async_stream::try_stream;
use futures::{AsyncRead, AsyncReadExt, Stream};
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

// This should probably also be a state machine and be wrapped in sync and async versions.
// But this suffices for the demonstration.
pub struct ObjectContainerFileReader<'a, R> {
    reader_schema: Option<&'a Schema>,
    header: ObjectContainerFileHeader,
    fsm: Option<ObjectContainerFileBodyStateMachine>,
    reader: Pin<Box<R>>,
    buffer: Buffer,
}

impl<'a, R: AsyncRead> ObjectContainerFileReader<'a, R> {
    /// Create a new reader for the Object Container file format.
    ///
    /// This will immediatly start reading the header.
    pub async fn new(mut reader: Pin<Box<R>>) -> Result<Self, Error> {
        // Read a maximum of 2Kb per read
        let mut buffer = Buffer::with_capacity(2 * 1024);

        // Parse the header
        let mut fsm = ObjectContainerFileHeaderStateMachine::new();
        let header = loop {
            // Fill the buffer
            let n = reader
                .read(buffer.space())
                .await
                .map_err(Details::ReadHeader)?;
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
    async fn next_object(&mut self) -> Option<Result<Vec<ItemRead>, Error>> {
        if let Some(mut fsm) = self.fsm.take() {
            loop {
                match fsm.parse(&mut self.buffer) {
                    Ok(StateMachineControlFlow::NeedMore(new_fsm)) => {
                        fsm = new_fsm;
                        let n = match self.reader.read(self.buffer.space()).await {
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

    pub async fn stream_serde<'b, T: Deserialize<'b>>(
        &mut self,
    ) -> impl Stream<Item = Result<T, Error>> {
        try_stream! {
            while let Some(object) = self.next_object().await {
                let _tape = object?;
                yield todo!();
                // yield deserialize_from_tape(&mut tape, self.reader_schema.unwrap_or(&self.header.schema))?;
            }
        }
    }

    pub async fn stream(&mut self) -> impl Stream<Item = Result<Value, Error>> {
        try_stream! {
            while let Some(object) = self.next_object().await {
                let _tape = object?;
                yield todo!();
                // yield value_from_tape(&mut tape, self.reader_schema.unwrap_or(&self.header.schema))?;
            }
        }
    }
}
