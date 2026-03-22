// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{borrow::Borrow, io::Write};

use serde::{
    Serialize,
    ser::{SerializeMap, SerializeSeq},
};

use super::{Config, SchemaAwareSerializer};
use crate::{
    Error, Schema,
    encode::encode_int,
    error::Details,
    schema::{ArraySchema, MapSchema},
    util::zig_i32,
};

#[expect(
    private_interfaces,
    reason = "Direct and Buffered should not be used directly"
)]
pub enum BlockSerializer<'s, 'w, W: Write, S: Borrow<Schema>> {
    Direct(DirectBlockSerializer<'s, 'w, W, S>),
    Buffered(BufferedBlockSerializer<'s, 'w, W, S>),
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> BlockSerializer<'s, 'w, W, S> {
    pub fn array(
        writer: &'w mut W,
        schema: &'s ArraySchema,
        config: Config<'s, S>,
        len: Option<usize>,
        bytes_written: Option<usize>,
    ) -> Result<Self, Error> {
        let schema = if let Schema::Ref { name } = schema.items.as_ref() {
            config.get_schema(name)?
        } else {
            &schema.items
        };

        Self::new(writer, schema, config, len, bytes_written)
    }

    pub fn map(
        writer: &'w mut W,
        schema: &'s MapSchema,
        config: Config<'s, S>,
        len: Option<usize>,
        bytes_written: Option<usize>,
    ) -> Result<Self, Error> {
        let schema = if let Schema::Ref { name } = schema.types.as_ref() {
            config.get_schema(name)?
        } else {
            &schema.types
        };

        Self::new(writer, schema, config, len, bytes_written)
    }

    fn new(
        writer: &'w mut W,
        schema: &'s Schema,
        config: Config<'s, S>,
        len: Option<usize>,
        bytes_written: Option<usize>,
    ) -> Result<Self, Error> {
        let bytes_written = bytes_written.unwrap_or(0);
        if let Some(len) = len
            && config.target_block_size.is_none()
        {
            Ok(Self::Direct(DirectBlockSerializer::new(
                writer,
                schema,
                config,
                len,
                bytes_written,
            )?))
        } else {
            let target_block_size = config.target_block_size.unwrap_or(1024);
            Ok(Self::Buffered(BufferedBlockSerializer::new(
                writer,
                schema,
                config,
                target_block_size,
                bytes_written,
            )))
        }
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeSeq for BlockSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self {
            BlockSerializer::Direct(direct) => direct.serialize_element(value),
            BlockSerializer::Buffered(buffered) => buffered.serialize_element(value),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self {
            BlockSerializer::Direct(direct) => direct.end(),
            BlockSerializer::Buffered(buffered) => buffered.end(),
        }
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeMap for BlockSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self {
            BlockSerializer::Direct(direct) => direct.serialize_key(key),
            BlockSerializer::Buffered(buffered) => buffered.serialize_key(key),
        }
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self {
            BlockSerializer::Direct(direct) => direct.serialize_value(value),
            BlockSerializer::Buffered(buffered) => buffered.serialize_value(value),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self {
            BlockSerializer::Direct(direct) => direct.end(),
            BlockSerializer::Buffered(buffered) => buffered.end(),
        }
    }
}

struct DirectBlockSerializer<'s, 'w, W: Write, S: Borrow<Schema>> {
    writer: &'w mut W,
    schema: &'s Schema,
    config: Config<'s, S>,
    bytes_written: usize,
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> DirectBlockSerializer<'s, 'w, W, S> {
    pub fn new(
        writer: &'w mut W,
        schema: &'s Schema,
        config: Config<'s, S>,
        len: usize,
        mut bytes_written: usize,
    ) -> Result<Self, Error> {
        if len != 0 {
            // .end() always writes the zero block, so we only write the size for arrays
            // that have at least one element
            bytes_written += zig_i32(len as i32, &mut *writer)?;
        }
        Ok(Self {
            writer,
            schema,
            config,
            bytes_written,
        })
    }

    fn end(self) -> Result<usize, Error> {
        // Write the zero directly instead of through zig_i32 which does a lot of extra work
        self.writer.write_all(&[0]).map_err(Details::WriteBytes)?;

        Ok(self.bytes_written + 1)
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeSeq for DirectBlockSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.bytes_written += value.serialize(SchemaAwareSerializer::new(
            self.writer,
            self.schema,
            self.config,
        )?)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeMap for DirectBlockSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.bytes_written += key.serialize(SchemaAwareSerializer::new(
            self.writer,
            &Schema::String,
            self.config,
        )?)?;
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_element(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

struct BufferedBlockSerializer<'s, 'w, W: Write, S: Borrow<Schema>> {
    writer: &'w mut W,
    buffer: Vec<u8>,
    schema: &'s Schema,
    config: Config<'s, S>,
    bytes_written: usize,
    items_in_buffer: i32,
    target_block_size: usize,
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> BufferedBlockSerializer<'s, 'w, W, S> {
    pub fn new(
        writer: &'w mut W,
        schema: &'s Schema,
        config: Config<'s, S>,
        target_block_size: usize,
        bytes_written: usize,
    ) -> Self {
        Self {
            writer,
            buffer: Vec::with_capacity(target_block_size),
            schema,
            config,
            bytes_written,
            items_in_buffer: 0,
            target_block_size,
        }
    }

    /// Write a block including the items and size header.
    fn write_block(&mut self) -> Result<(), Error> {
        // Write the header, the negative item count indicates that the next value is the size of the
        // block in bytes
        self.bytes_written += encode_int(0 - self.items_in_buffer, &mut *self.writer)?;
        self.bytes_written += encode_int(self.buffer.len() as i32, &mut *self.writer)?;

        // Write the actual data
        self.writer
            .write_all(&self.buffer)
            .map_err(Details::WriteBytes)?;
        self.bytes_written += self.buffer.len();

        // Reset the buffer
        self.items_in_buffer = 0;
        self.buffer.clear();

        Ok(())
    }

    fn end(mut self) -> Result<usize, Error> {
        // Write any items remaining in the buffer
        if self.items_in_buffer > 0 {
            self.write_block()?;
        }
        debug_assert_eq!(self.buffer.len(), 0, "Buffer must be empty at this point");

        // Write the zero directly instead of through zig_i32 which does a lot of extra work
        self.writer.write_all(&[0]).map_err(Details::WriteBytes)?;

        Ok(self.bytes_written + 1)
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeSeq for BufferedBlockSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(SchemaAwareSerializer::new(
            &mut self.buffer,
            self.schema,
            self.config,
        )?)?;
        self.items_in_buffer += 1;

        if self.buffer.len() >= self.target_block_size {
            self.write_block()?;
        }
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeMap for BufferedBlockSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        key.serialize(SchemaAwareSerializer::new(
            &mut self.buffer,
            &Schema::String,
            self.config,
        )?)?;
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_element(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}
