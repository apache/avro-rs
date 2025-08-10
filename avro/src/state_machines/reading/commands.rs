use crate::{
    Error, Schema,
    error::Details,
    schema::{
        ArraySchema, DecimalSchema, EnumSchema, FixedSchema, MapSchema, Name, RecordSchema,
        UnionSchema,
    },
};
use std::{collections::HashMap, ops::Range, sync::Arc};

/// The next item type that should be read.
#[must_use]
pub enum ToRead {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Enum,
    Ref(CommandTape),
    Fixed(usize),
    Block(CommandTape),
    Union(UnionVariants),
}

pub struct UnionVariants {
    tape: CommandTape,
    num_variants: usize,
}

impl UnionVariants {
    pub fn get(mut self, index: usize) -> Result<CommandTape, Error> {
        if index >= self.num_variants {
            return Err(Details::GetUnionVariant {
                index: index as i64,
                num_variants: self.num_variants,
            }
            .into());
        }
        // Skip past the other variants
        self.tape.skip(index);
        let offset = self.tape.read_range.next().expect("Unreachable!");
        // Return the next command as the command tape
        Ok(self.tape.extract(offset, 1))
    }
}

/// A section of a tape of commands.
///
/// This has a reference to the entire tape, so that references to types (for Union,Map,Array) can be resolved.
#[derive(Debug, Clone, PartialEq)]
#[must_use]
pub struct CommandTape {
    inner: Arc<[u8]>,
    read_range: Range<usize>,
}

impl CommandTape {
    pub const NULL: u8 = 0;
    pub const BOOLEAN: u8 = 1;
    pub const INT: u8 = 2;
    pub const LONG: u8 = 3;
    pub const FLOAT: u8 = 4;
    pub const DOUBLE: u8 = 5;
    pub const BYTES: u8 = 6;
    pub const STRING: u8 = 7;
    pub const ENUM: u8 = 8;
    /// A fixed amount of bytes.
    ///
    /// If the amount of bytes is smaller than or equal to `0xF`, the amount is stored in the four
    /// most significant bits of the byte. Otherwise, it's stored as a native endian usize directly
    /// after the command byte.
    pub const FIXED: u8 = 9;
    /// A block based format follows (i.e. Map or Array).
    ///
    /// The command sequence of the type in the block follows immediately after the command byte.
    /// The length of the sequence is stored in the most significant four bits of the command byte.
    /// If the sequence is larger than `0xF`, then either the entire sequence or part of it is
    /// put behind a [`Self::REF`].
    pub const BLOCK: u8 = 10;
    pub const UNION: u8 = 11;
    /// A reference to a command sequence somewhere else in the tape.
    ///
    /// If the length of the sequence is smaller than or equal to `0xF`, the length is stored in the
    /// four most significant bits of the byte. Otherwise, it's stored as a native endian usize
    /// directly after the command byte. After the length follows the offset as a native endian
    /// usize.
    pub const REF: u8 = 12;
    /// Skip the next `n` commands.
    ///
    /// A SKIP command is not counted as a command.
    ///
    /// If `n` is smaller than or equal to `0xF`, the amount is stored in the four most significant
    /// bits of the byte. Otherwise, it's stored as a native endian usize directly after the command
    /// byte.
    pub const SKIP: u8 = 13;

    /// Create a new tape that will be read from start to end.
    pub fn new(command_tape: Arc<[u8]>) -> Self {
        let length = command_tape.len();
        Self {
            inner: command_tape,
            read_range: 0..length,
        }
    }

    pub fn build_from_schema(schema: &Schema) -> Result<Self, Error> {
        CommandTapeBuilder::build(schema)
    }

    /// Check if the section of the tape we're reading is finished.
    pub fn is_finished(&self) -> bool {
        self.read_range.is_empty()
    }

    /// Extract a part from the tape to give to a sub-state machine.
    ///
    /// The tape will run from offset for the given amount of commands.
    pub fn extract(&self, offset: usize, commands: usize) -> Self {
        let mut temp = Self {
            inner: self.inner.clone(),
            read_range: offset..self.inner.len(),
        };
        temp.skip(commands);
        let size = temp.read_range.next().unwrap_or(self.inner.len());

        assert!(
            offset + size <= self.inner.len(),
            "Reference is (partly) outside the tape"
        );
        Self {
            inner: self.inner.clone(),
            read_range: offset..(offset + size),
        }
    }

    /// Extract many parts from the tape to give to the Union state machine.
    ///
    /// The tapes will run from start to end (inclusive).
    pub fn extract_many(&self, parts: &[(usize, usize)]) -> Box<[Self]> {
        let mut vec = Vec::with_capacity(parts.len());
        for &(start, end) in parts {
            vec.push(self.extract(start, end));
        }
        vec.into_boxed_slice()
    }

    /// Read an array of bytes from the tape.
    fn read_array<const N: usize>(&mut self) -> [u8; N] {
        let start = self.read_range.next().expect("Read past the limit");
        let end = self.read_range.nth(N - 1).expect("Read past the limit");
        self.inner[start..=end].try_into().expect("Unreachable!")
    }

    /// Get the next command from the tape.
    ///
    /// Will return `None` if exhausted.
    pub fn command(&mut self) -> Option<ToRead> {
        if let Some(position) = self.read_range.next() {
            let byte = self.inner[position];
            match byte & 0xF {
                Self::NULL => Some(ToRead::Null),
                Self::BOOLEAN => Some(ToRead::Boolean),
                Self::INT => Some(ToRead::Int),
                Self::LONG => Some(ToRead::Long),
                Self::FLOAT => Some(ToRead::Float),
                Self::DOUBLE => Some(ToRead::Double),
                Self::BYTES => Some(ToRead::Bytes),
                Self::STRING => Some(ToRead::String),
                Self::ENUM => Some(ToRead::Enum),
                Self::FIXED => {
                    // ToRead::Fixed
                    if byte >> 4 != 0 {
                        // Length is stored inline
                        Some(ToRead::Fixed((byte >> 4) as usize))
                    } else {
                        let length = usize::from_ne_bytes(self.read_array());
                        Some(ToRead::Fixed(length))
                    }
                }
                Self::BLOCK => {
                    // ToRead::Block
                    let size = (byte >> 4) as usize;
                    self.skip(size);
                    Some(ToRead::Block(self.extract(position + 1, size)))
                }
                Self::UNION => {
                    // ToRead::Union
                    // How many variants are there?
                    let num_variants = if byte >> 4 != 0 {
                        (byte >> 4) as usize
                    } else {
                        // TODO: Use varint
                        let num_variants = u32::from_ne_bytes(self.read_array());
                        num_variants as usize
                    };
                    let variants = UnionVariants {
                        tape: self.clone(),
                        num_variants,
                    };

                    // Skip over the commands for the variants
                    self.skip(num_variants);

                    Some(ToRead::Union(variants))
                }
                Self::REF => {
                    // ToRead::Ref
                    let size = if byte >> 4 != 0 {
                        // Length is stored inline
                        (byte >> 4) as usize
                    } else {
                        usize::from_ne_bytes(self.read_array())
                    };
                    let offset = usize::from_ne_bytes(self.read_array());
                    Some(ToRead::Ref(self.extract(offset, size)))
                }
                Self::SKIP => {
                    let commands = if byte >> 4 != 0 {
                        (byte >> 4) as usize
                    } else {
                        usize::from_ne_bytes(self.read_array())
                    };
                    self.skip(commands);

                    self.command()
                }
                _ => unreachable!(), // TODO: There is room here to specialize certain types, like a Union of Null and some other type
            }
        } else {
            None
        }
    }

    /// Skip `amount` commands.
    ///
    /// If a command contains subcommands, these will also be skipped.
    fn skip(&mut self, mut amount: usize) {
        let mut i = 0;
        while i < amount {
            let position = self
                .read_range
                .next()
                .expect("The caller read past the tape");
            let byte = self.inner[position];
            match byte & 0xF {
                CommandTape::REF => {
                    if byte >> 4 == 0 {
                        let _size = usize::from_ne_bytes(self.read_array());
                    }
                    let _offset = usize::from_ne_bytes(self.read_array());
                }
                CommandTape::BOOLEAN
                | CommandTape::INT
                | CommandTape::LONG
                | CommandTape::FLOAT
                | CommandTape::DOUBLE
                | CommandTape::BYTES
                | CommandTape::STRING
                | CommandTape::ENUM
                | CommandTape::NULL => {}
                CommandTape::FIXED => {
                    if byte >> 4 == 0 {
                        let _size = usize::from_ne_bytes(self.read_array());
                    }
                }
                CommandTape::UNION | CommandTape::BLOCK | CommandTape::SKIP => {
                    // These commands can inline other commands, so add them to the skip list
                    let num_variants = if byte >> 4 != 0 {
                        (byte >> 4) as usize
                    } else {
                        // TODO: Use varint
                        let num_variants = u32::from_ne_bytes(self.read_array());
                        num_variants as usize
                    };
                    amount += num_variants;
                }
                _ => unreachable!(),
            }
            i += 1;
        }
    }
}

struct CommandTapeBuilder<'a> {
    tape: Vec<u8>,
    references: HashMap<&'a Name, (usize, usize)>,
}

impl<'a> CommandTapeBuilder<'a> {
    pub fn new() -> Self {
        Self {
            tape: Vec::new(),
            references: HashMap::new(),
        }
    }

    fn add_schema(&mut self, schema: &'a Schema, inline_up_to: usize) -> Result<usize, Error> {
        match schema {
            Schema::Null => {
                self.tape.push(CommandTape::NULL);
                Ok(1)
            }
            Schema::Boolean => {
                self.tape.push(CommandTape::BOOLEAN);
                Ok(1)
            }
            Schema::Int | Schema::Date | Schema::TimeMillis => {
                self.tape.push(CommandTape::INT);
                Ok(1)
            }
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => {
                self.tape.push(CommandTape::LONG);
                Ok(1)
            }
            Schema::Float => {
                self.tape.push(CommandTape::FLOAT);
                Ok(1)
            }
            Schema::Double => {
                self.tape.push(CommandTape::DOUBLE);
                Ok(1)
            }
            Schema::Bytes | Schema::BigDecimal => {
                self.tape.push(CommandTape::BYTES);
                Ok(1)
            }
            Schema::String | Schema::Uuid => {
                self.tape.push(CommandTape::STRING);
                Ok(1)
            }
            Schema::Array(ArraySchema { items, .. }) => {
                let block_offset = self.tape.len();
                self.tape.push(CommandTape::BLOCK);
                let commands = self.add_schema(items, 16)?;
                self.tape[block_offset] = CommandTape::BLOCK | (commands << 4) as u8;
                Ok(1)
            }
            Schema::Map(MapSchema { types, .. }) => {
                let block_offset = self.tape.len();
                self.tape.push(CommandTape::BLOCK);
                self.tape.push(CommandTape::STRING);
                let commands = self.add_schema(types, 15)?;
                self.tape[block_offset] = CommandTape::BLOCK | ((commands + 1) << 4) as u8;
                Ok(1)
            }
            Schema::Union(UnionSchema { schemas, .. }) => {
                let schema_len = schemas.len();
                if 0 < schema_len && schema_len <= 0xF {
                    self.tape.push(CommandTape::UNION | (schema_len << 4) as u8);
                } else {
                    self.tape.push(CommandTape::UNION);
                    self.tape.extend_from_slice(&schema_len.to_ne_bytes());
                }
                for schema in schemas {
                    self.add_schema(schema, 1)?;
                }
                Ok(1)
            }
            Schema::Record(RecordSchema { name, fields, .. }) => {
                if let Some(&(offset, commands)) = self.references.get(name) {
                    self.add_reference(offset, commands);
                    Ok(1)
                } else if fields.is_empty() {
                    panic!("Record has no fields! {schema:?}");
                } else {
                    let commands = fields.len();
                    if commands > inline_up_to {
                        // If this record is larger than the amount we're allowed to inline, inject
                        // a SKIP command.
                        if commands <= 0xF {
                            self.tape.push(CommandTape::SKIP | (commands << 4) as u8);
                        } else {
                            self.tape.push(CommandTape::SKIP);
                            self.tape.extend_from_slice(&commands.to_ne_bytes());
                        }
                    }
                    let offset = self.tape.len();
                    self.references.insert(name, (offset, commands));
                    for field in fields {
                        let _commands = self.add_schema(&field.schema, 1)?;
                    }
                    if commands > inline_up_to {
                        // Now refer back to the skip block
                        self.add_reference(offset, commands);
                        Ok(1)
                    } else {
                        Ok(commands)
                    }
                }
            }
            Schema::Enum(EnumSchema { name, .. }) => {
                let offset = self.tape.len();
                let commands = 1;
                self.tape.push(CommandTape::ENUM);
                self.references.insert(name, (offset, commands));
                Ok(1)
            }
            Schema::Fixed(FixedSchema { name, size, .. }) => {
                let offset = self.tape.len();
                if 0 < *size && *size <= 0xF {
                    self.tape.push(CommandTape::FIXED | (*size << 4) as u8);
                } else {
                    self.tape.push(CommandTape::FIXED);
                    self.tape.extend_from_slice(&size.to_ne_bytes());
                }
                self.references.entry(name).or_insert((offset, 1));
                Ok(1)
            }
            Schema::Decimal(DecimalSchema { inner, .. }) => self.add_schema(inner, inline_up_to),
            Schema::Duration => {
                self.tape.push(CommandTape::FIXED | 12 << 4);
                Ok(1)
            }
            Schema::Ref { name } => {
                let &(offset, commands) = self
                    .references
                    .get(name)
                    .ok_or_else(|| Details::SchemaResolutionError(name.clone()))?;
                self.add_reference(offset, commands);
                Ok(1)
            }
        }
    }

    fn add_reference(&mut self, offset: usize, commands: usize) {
        if commands == 0 {
            self.tape.push(CommandTape::NULL);
        } else if commands <= 0xF {
            self.tape.push(CommandTape::REF | (commands << 4) as u8);
        } else {
            self.tape.push(CommandTape::REF);
            self.tape.extend_from_slice(&commands.to_ne_bytes());
        }
        self.tape.extend_from_slice(&offset.to_ne_bytes());
    }

    pub fn build(schema: &Schema) -> Result<CommandTape, Error> {
        let mut builder = Self::new();

        builder.add_schema(schema, usize::MAX)?;

        let tape_len = builder.tape.len();
        Ok(CommandTape {
            inner: Arc::from(builder.tape),
            read_range: 0..tape_len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn command_tape_simple() {
        assert_eq!(
            CommandTape::build_from_schema(&Schema::Null)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::NULL]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::Boolean)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::BOOLEAN]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::Int)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::INT]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::Date)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::INT]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::TimeMillis)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::INT]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::Long)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::LONG]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::TimeMicros)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::LONG]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::TimestampMillis)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::LONG]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::TimestampMicros)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::LONG]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::TimestampNanos)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::LONG]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::LocalTimestampMillis)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::LONG]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::LocalTimestampMicros)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::LONG]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::LocalTimestampNanos)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::LONG]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::Float)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::FLOAT]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::Double)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::DOUBLE]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::Bytes)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::BYTES]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::String)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::STRING]
        );
        assert_eq!(
            CommandTape::build_from_schema(&Schema::Uuid)
                .unwrap()
                .inner
                .as_ref(),
            &[CommandTape::STRING]
        );
    }
}
