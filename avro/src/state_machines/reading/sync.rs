use std::{io::Read, sync::Arc};

use oval::Buffer;
use serde::Deserialize;

use crate::{
    Error, Schema,
    error::Details,
    state_machines::reading::{
        CommandTape, ItemRead, StateMachine, StateMachineControlFlow, decode_zigzag,
        deserialize_from_tape, object::ObjectStateMachine, schema_to_command_tape, value_from_tape,
    },
    types::Value,
};

// TODO: Dynamically/const construct this, this one works only on 64-bit LE
/// The tape corresponding to [`HEADER_JSON`].
///
/// ```json
/// {
///     "type": "record",
///     "name": "org.apache.avro.file.HeaderNoMagic",
///     "fields": [
///         {"name": "meta", "type": {"type": "map", "values": "bytes"}},
///         {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}
///     ]
/// }
/// ```
const HEADER_TAPE: &[u8] = &[
    CommandTape::MAP, // Starts with a map
    0x1A,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00, // The type description starts at 0x1A
    0x1A,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,               // The type description ends at 0x1A (inclusive)
    CommandTape::FIXED, // After the map there is a Fixed amount of bytes
    0x0F,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,
    0x00,               // The amount of bytes is 0x0F
    CommandTape::BYTES, // The type of the values in the map
];
const HEADER_JSON: &str = r#"{"type": "record","name": "org.apache.avro.file.HeaderNoMagic","fields": [{"name": "meta", "type": {"type": "map", "values": "bytes"}},{"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}]}"#;

// This should probably also be a state machine and be wrapped in sync and async versions.
// But this suffices for the demonstration.
pub struct ObjectContainerFileReader<'a, R> {
    reader_schema: Option<&'a Schema>,
    writer_schema: Schema,
    fsm: Option<ObjectStateMachine>,
    tape: CommandTape,
    reader: R,
    buffer: Buffer,
    left_in_block: usize,
    finished: bool,
}

impl<'a, R: Read> ObjectContainerFileReader<'a, R> {
    /// Create a new reader for the Object Container file format.
    ///
    /// This will immediatly start reading the header.
    pub fn new(mut reader: R) -> Result<Self, Error> {
        // Read a maximum of 2Kb per read
        let mut buffer = Buffer::with_capacity(2 * 1024);
        // TODO: Double check that 47 bytes is the smallest possible header
        // Read at least as many bytes as are required for the smallest header.
        // If we get an EOF, it was not a valid file anyway.
        while buffer.available_data() < 47 {
            let n = reader.read(buffer.space()).map_err(Details::ReadIntoBuf)?;
            buffer.fill(n);
        }

        // Verify the magic
        let mut magic = [0; 4];
        buffer.read_exact(&mut magic).expect("Unreachable!");
        if magic.as_slice() != b"Obj1" {
            return Err(Details::HeaderMagic.into());
        }

        // Read the rest of the header
        let mut fsm = ObjectStateMachine::new(CommandTape::new(Arc::from(HEADER_TAPE)));
        let mut decoded = loop {
            match fsm.parse(&mut buffer)? {
                StateMachineControlFlow::Continue(new_fsm, _) => {
                    fsm = new_fsm;
                    let n = reader.read(buffer.space()).map_err(Details::ReadIntoBuf)?;
                    buffer.fill(n);
                }
                StateMachineControlFlow::Done(decoded) => break decoded,
            }
        };

        let value = value_from_tape(
            &mut decoded,
            &Schema::parse_str(HEADER_JSON).expect("Unreachable!"),
        );

        let Value::Record(record) = value? else {
            unreachable!()
        };
        let (_, Value::Map(map)) = record
            .iter()
            .find(|(name, _value)| name == "meta")
            .expect("Unreachable!")
        else {
            unreachable!()
        };

        let Value::Bytes(schema) = map
            .get("avro.schema")
            .ok_or(Details::GetAvroSchemaFromMap)?
        else {
            unreachable!()
        };
        let schema =
            Schema::parse_str(std::str::from_utf8(schema).map_err(Details::ConvertToUtf8Error)?)?;

        // TODO: Codecs and such

        let command_tape = schema_to_command_tape(&schema);
        let fsm = ObjectStateMachine::new_with_tape(command_tape.clone(), decoded);

        Ok(Self {
            reader_schema: None,
            writer_schema: schema,
            fsm: Some(fsm),
            tape: command_tape,
            reader,
            buffer,
            left_in_block: 0,
            finished: false,
        })
    }

    /// Get the next object in the file
    ///
    /// # Panics
    /// Will panic if the file is already finished.
    fn next_object(&mut self) -> Result<Vec<ItemRead>, Error> {
        assert!(!self.finished, "Already finished");
        // We have finished the last block or have just been created
        if self.left_in_block == 0 {
            let left = loop {
                match decode_zigzag(&mut self.buffer)? {
                    Some(value) => break value,
                    None => {
                        let n = self
                            .reader
                            .read(self.buffer.space())
                            .map_err(Details::ReadIntoBuf)?;
                        self.buffer.fill(n);
                    }
                }
            };
            let left_abs = left.unsigned_abs();
            self.left_in_block =
                usize::try_from(left_abs).map_err(|e| Details::ConvertU64ToUsize(e, left_abs))?;
            if left.is_negative() {
                // Read the block size
                let bytes = loop {
                    match decode_zigzag(&mut self.buffer)? {
                        Some(value) => break value,
                        None => {
                            let n = self
                                .reader
                                .read(self.buffer.space())
                                .map_err(Details::ReadIntoBuf)?;
                            self.buffer.fill(n);
                        }
                    }
                };
                // Make sure the value is sane
                let _ = usize::try_from(bytes).map_err(|e| Details::ConvertI64ToUsize(e, bytes))?;
            }
        }
        let mut fsm = self.fsm.take().expect("Unreachable!");
        loop {
            match fsm.parse(&mut self.buffer)? {
                StateMachineControlFlow::Continue(new_fsm, _data_request) => {
                    fsm = new_fsm;
                    self.reader
                        .read(self.buffer.space())
                        .map_err(Details::ReadIntoBuf)?;
                }
                StateMachineControlFlow::Done(value) => {
                    self.fsm = Some(ObjectStateMachine::new(self.tape.clone()));
                    return Ok(value);
                }
            }
        }
    }

    pub fn next_serde<'b, T: Deserialize<'b>>(&mut self) -> Option<Result<T, Error>> {
        if self.finished {
            return None;
        }
        let next = self.next_object().and_then(|mut tape| {
            deserialize_from_tape(&mut tape, self.reader_schema.unwrap_or(&self.writer_schema))
        });
        if next.is_err() {
            self.finished = true;
        }
        Some(next)
    }
}

impl<R: Read> Iterator for ObjectContainerFileReader<'_, R> {
    type Item = Result<Value, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let next = self.next_object().and_then(|mut tape| {
            value_from_tape(&mut tape, self.reader_schema.unwrap_or(&self.writer_schema))
        });
        if next.is_err() {
            self.finished = true;
        }
        Some(next)
    }
}
