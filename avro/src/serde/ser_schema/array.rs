use std::io::Write;

use serde::ser::SerializeSeq;

use super::{Config, SchemaAwareSerializer};
use crate::{Error, encode::encode_int, error::Details, schema::ArraySchema};

#[expect(
    private_interfaces,
    reason = "Direct and Buffered should not be used directly"
)]
pub enum ArraySerializer<'s, 'w, W: Write> {
    Direct(DirectArraySerializer<'s, 'w, W>),
    Buffered(BufferedArraySerializer<'s, 'w, W>),
}

impl<'s, 'w, W: Write> ArraySerializer<'s, 'w, W> {
    pub fn new(
        writer: &'w mut W,
        schema: &'s ArraySchema,
        config: Config<'s>,
        len: Option<usize>,
        bytes_written: Option<usize>,
    ) -> Result<Self, Error> {
        if let Some(len) = len
            && config.target_block_size.is_none()
        {
            Ok(Self::Direct(DirectArraySerializer::new(
                writer,
                schema,
                config,
                len,
                bytes_written.unwrap_or(0),
            )?))
        } else {
            let target_block_size = config.target_block_size.unwrap_or(1024);
            Ok(Self::Buffered(BufferedArraySerializer::new(
                writer,
                schema,
                config,
                target_block_size,
                bytes_written.unwrap_or(0),
            )))
        }
    }
}

impl<'s, 'w, W: Write> SerializeSeq for ArraySerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        match self {
            ArraySerializer::Direct(direct) => direct.serialize_element(value),
            ArraySerializer::Buffered(buffered) => buffered.serialize_element(value),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self {
            ArraySerializer::Direct(direct) => direct.end(),
            ArraySerializer::Buffered(buffered) => buffered.end(),
        }
    }
}

struct DirectArraySerializer<'s, 'w, W: Write> {
    writer: &'w mut W,
    array: &'s ArraySchema,
    config: Config<'s>,
    bytes_written: usize,
}

impl<'s, 'w, W: Write> DirectArraySerializer<'s, 'w, W> {
    pub fn new(
        mut writer: &'w mut W,
        array: &'s ArraySchema,
        config: Config<'s>,
        len: usize,
        mut bytes_written: usize,
    ) -> Result<Self, Error> {
        bytes_written += encode_int(len as i32, &mut writer)?;
        Ok(Self {
            writer,
            array,
            config,
            bytes_written,
        })
    }
}

impl<'s, 'w, W: Write> SerializeSeq for DirectArraySerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        let ser = SchemaAwareSerializer::new(self.writer, &self.array.items, self.config)?;
        self.bytes_written += value.serialize(ser)?;
        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(&[0]).map_err(Details::WriteBytes)?;
        self.bytes_written += 1;

        Ok(self.bytes_written)
    }
}
struct BufferedArraySerializer<'s, 'w, W: Write> {
    writer: &'w mut W,
    buffer: Vec<u8>,
    array: &'s ArraySchema,
    config: Config<'s>,
    bytes_written: usize,
    items_written: usize,
    target_block_size: usize,
}

impl<'s, 'w, W: Write> BufferedArraySerializer<'s, 'w, W> {
    pub fn new(
        writer: &'w mut W,
        array: &'s ArraySchema,
        config: Config<'s>,
        target_block_size: usize,
        bytes_written: usize,
    ) -> Self {
        Self {
            writer,
            buffer: Vec::new(),
            array,
            config,
            bytes_written,
            items_written: 0,
            target_block_size,
        }
    }

    fn write_block(&mut self) -> Result<(), Error> {
        self.bytes_written += encode_int(0 - (self.items_written as i32), &mut *self.writer)?;
        self.bytes_written += encode_int(self.buffer.len() as i32, &mut *self.writer)?;
        self.writer
            .write_all(&self.buffer)
            .map_err(Details::WriteBytes)?;
        self.bytes_written += self.buffer.len();
        self.buffer.clear();
        Ok(())
    }
}

impl<'s, 'w, W: Write> SerializeSeq for BufferedArraySerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        let ser = SchemaAwareSerializer::new(&mut self.buffer, &self.array.items, self.config)?;
        value.serialize(ser)?;
        self.items_written += 1;

        if self.buffer.len() >= self.target_block_size {
            self.write_block()?;
        }

        Ok(())
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        if self.items_written > 0 {
            self.write_block()?;
        }
        assert_eq!(self.buffer.len(), 0, "Buffer must be empty at this point");

        encode_int(0, self.writer)?;
        self.bytes_written += 1;

        Ok(self.bytes_written)
    }
}
