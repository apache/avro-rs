use log::warn;
use oval::Buffer;
use std::{collections::HashMap, io::Read, str::FromStr, sync::Arc};

use crate::{
    Codec, Error, Schema,
    error::Details,
    state_machines::reading::{
        CommandTape, ItemRead, StateMachine, StateMachineControlFlow, StateMachineResult,
        codec::CodecStateMachine, decode_zigzag_buffer, object::ObjectStateMachine,
    },
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
#[rustfmt::skip]
const HEADER_TAPE: &[u8] = &[
    CommandTape::BLOCK | 2 << 4,                    // Starts with a map
    CommandTape::STRING,                            // The keys are strings
    CommandTape::BYTES,                             // The values are bytes
    CommandTape::FIXED,                             // After the map there is a Fixed amount of bytes
    0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // The amount of bytes is 0x0F
];
const HEADER_JSON: &str = r#"{"type": "record","name": "org.apache.avro.file.HeaderNoMagic","fields": [{"name": "meta", "type": {"type": "map", "values": "bytes"}},{"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}]}"#;

/// The header as read from an Object Container file format.
pub struct ObjectContainerFileHeader {
    /// The schema used to write the file.
    pub schema: Schema,
    /// The compression used.
    // TODO: Add decompression
    pub codec: Codec,
    /// The sync marker used between blocks
    pub sync: [u8; 16],
    /// User metadata in the header
    pub metadata: HashMap<Box<str>, Box<[u8]>>,
}

/// A state machine for parsing the header of the Object Container file format.
///
/// After finishing this state machine the body can be read with [`ObjectContainerFileBodyStateMachine`].
pub struct ObjectContainerFileHeaderStateMachine {
    /// The actual state machine used to parse the header.
    ///
    /// This doesn't actually need to be an [`Option`] as it's constructed in [`Self::new`]. However,
    /// as [`StateMachine::parse`] takes `self` we need it in an `Option` so we can do [`Option::take`].
    fsm: Option<ObjectStateMachine>,
    read_magic: bool,
}

impl ObjectContainerFileHeader {
    pub fn command_tape() -> CommandTape {
        CommandTape::new(Arc::from(HEADER_TAPE))
    }

    /// Create the header from an output tape.
    ///
    /// # Panics
    /// Will panic if the tape was not produced from [`Self::command_tape()`].
    pub fn from_tape(mut tape: Vec<ItemRead>) -> Result<Self, Error> {
        // We want to read the tape from front to back
        let mut tape = tape.drain(..);

        let mut schema = None;
        let mut codec = None;
        let mut found_compression_level = false;
        let mut metadata = HashMap::new();

        while let Some(ItemRead::Block(items_left)) = tape.next() {
            if items_left == 0 {
                // Got to the end of the map
                break;
            }
            let Some(ItemRead::String(key)) = tape.next() else {
                panic!("The input does not correspond to the command tape");
            };
            let Some(ItemRead::Bytes(value)) = tape.next() else {
                panic!("The input does not correspond to the command tape");
            };

            match key.as_ref() {
                "avro.schema" => {
                    let string = String::from_utf8(value.into()).map_err(Details::ConvertToUtf8)?;
                    let parsed_schema = Schema::parse_str(&string)?;
                    if schema.replace(parsed_schema).is_some() {
                        // Duplicate key
                        return Err(Details::GetHeaderMetadata.into());
                    }
                }
                "avro.codec" => {
                    let string = String::from_utf8(value.into()).map_err(Details::ConvertToUtf8)?;
                    let parsed_codec =
                        Codec::from_str(&string).map_err(|_| Details::CodecNotSupported(string))?;
                    if codec.replace(parsed_codec).is_some() {
                        // Duplicate key
                        return Err(Details::GetHeaderMetadata.into());
                    }
                }
                "avro.codec.compression_level" => {
                    // Compression level is not useful for decoding
                    if found_compression_level {
                        // Duplicate key
                        return Err(Details::GetHeaderMetadata.into());
                    }
                    found_compression_level = true;
                }
                _ => {
                    if key.starts_with("avro.") {
                        warn!("Ignoring unknown metadata key: {key}");
                    }
                    if metadata.insert(key, value).is_some() {
                        // Duplicate key
                        return Err(Details::GetHeaderMetadata.into());
                    }
                }
            }
        }
        let Some(schema) = schema else {
            return Err(Details::GetHeaderMetadata.into());
        };
        let codec = codec.unwrap_or(Codec::Null);
        let Some(ItemRead::Fixed(raw_sync)) = tape.next() else {
            panic!("The input does not correspond to the command tape");
        };
        let sync = raw_sync
            .as_ref()
            .try_into()
            .expect("The input does not correspond to the command tape");
        Ok(ObjectContainerFileHeader {
            schema,
            codec,
            sync,
            metadata,
        })
    }
}

impl ObjectContainerFileHeaderStateMachine {
    pub fn new() -> Self {
        Self {
            fsm: Some(ObjectStateMachine::new(CommandTape::new(Arc::from(
                HEADER_TAPE,
            )))),
            read_magic: false,
        }
    }
}

impl StateMachine for ObjectContainerFileHeaderStateMachine {
    type Output = ObjectContainerFileHeader;

    fn parse(mut self, buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output> {
        while !self.read_magic {
            if buffer.available_data() < 4 {
                return Ok(StateMachineControlFlow::NeedMore(self));
            }
            if &buffer.data()[0..4] != b"Obj1" {
                return Err(Details::HeaderMagic.into());
            }
            buffer.consume(4);
            self.read_magic = true;
        }
        match self.fsm.take().expect("Unreachable!").parse(buffer)? {
            StateMachineControlFlow::NeedMore(fsm) => {
                let _ = self.fsm.insert(fsm);
                Ok(StateMachineControlFlow::NeedMore(self))
            }
            StateMachineControlFlow::Done(tape) => Ok(StateMachineControlFlow::Done(
                ObjectContainerFileHeader::from_tape(tape)?,
            )),
        }
    }
}

pub struct ObjectContainerFileBodyStateMachine {
    fsm: Option<CodecStateMachine<ObjectStateMachine>>,
    tape: CommandTape,
    sync: [u8; 16],
    left_in_block: usize,
    need_to_read_block_byte_size: bool,
    need_to_read_sync: bool,
}

impl ObjectContainerFileBodyStateMachine {
    pub fn new(tape: CommandTape, sync: [u8; 16], codec: Codec) -> Self {
        Self {
            fsm: Some(CodecStateMachine::new(
                ObjectStateMachine::new(tape.clone()),
                codec,
            )),
            tape,
            sync,
            left_in_block: 0,
            need_to_read_block_byte_size: false,
            need_to_read_sync: false,
        }
    }
}

impl StateMachine for ObjectContainerFileBodyStateMachine {
    type Output = Option<(Vec<ItemRead>, Self)>;

    fn parse(mut self, buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output> {
        if self.left_in_block == 0 {
            if self.need_to_read_sync {
                if buffer.available_data() < 16 {
                    return Ok(StateMachineControlFlow::NeedMore(self));
                }
                let mut sync = [0; 16];
                assert_eq!(
                    buffer.read(&mut sync).expect("Unreachable!"),
                    16,
                    "Did not read enough data!"
                );
                if sync != self.sync {
                    return Err(Details::GetBlockMarker.into());
                }
                self.need_to_read_sync = false;
            }
            let Some(block) = decode_zigzag_buffer(buffer)? else {
                // Not enough data left in the buffer
                return Ok(StateMachineControlFlow::NeedMore(self));
            };
            let abs_block = block.unsigned_abs();
            let abs_block =
                usize::try_from(abs_block).map_err(|e| Details::ConvertU64ToUsize(e, abs_block))?;
            if abs_block == 0 {
                // Done parsing the array
                return Ok(StateMachineControlFlow::Done(None));
            }
            self.need_to_read_block_byte_size = true;
            // This will only be done after this block is finished
            self.need_to_read_sync = true;
            self.left_in_block = abs_block;
        }
        if self.need_to_read_block_byte_size {
            let Some(block) = decode_zigzag_buffer(buffer)? else {
                // Not enough data left in the buffer
                return Ok(StateMachineControlFlow::NeedMore(self));
            };
            // Make sure the value is sane
            let _ = usize::try_from(block).map_err(|e| Details::ConvertI64ToUsize(e, block))?;
            self.need_to_read_block_byte_size = false;
        }

        match self.fsm.take().expect("Unreachable!").parse(buffer)? {
            StateMachineControlFlow::NeedMore(fsm) => {
                self.fsm.replace(fsm);
                Ok(StateMachineControlFlow::NeedMore(self))
            }
            StateMachineControlFlow::Done((result, mut codec)) => {
                codec.reset(ObjectStateMachine::new(self.tape.clone()));
                self.fsm.replace(codec);
                self.left_in_block -= 1;
                Ok(StateMachineControlFlow::Done(Some((result, self))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        Schema,
        state_machines::reading::{
            commands::CommandTape,
            object_container_file::{HEADER_JSON, HEADER_TAPE},
        },
    };

    #[test]
    pub fn header_tape() {
        let schema = Schema::parse_str(HEADER_JSON).unwrap();
        let tape = CommandTape::build_from_schema(&schema).unwrap();
        assert_eq!(tape, CommandTape::new(Arc::from(HEADER_TAPE)));
    }
}
