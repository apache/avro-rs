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

use crate::serde::deser_schema::{Config, SchemaAwareDeserializer};
use crate::{
    AvroResult, Codec, Error,
    decode::{decode, decode_internal},
    error::Details,
    schema::{Names, Schema, resolve_names, resolve_names_with_schemata},
    types::Value,
    util,
};
use log::warn;
use serde::de::DeserializeOwned;
use serde_json::from_slice;
use std::{
    collections::HashMap,
    io::{ErrorKind, Read},
    str::FromStr,
};

/// Internal Block reader.
#[derive(Debug, Clone)]
pub(super) struct Block<'r, R> {
    reader: R,
    /// Internal buffering to reduce allocation.
    buf: Vec<u8>,
    buf_idx: usize,
    /// Number of elements expected to exist within this block.
    message_count: usize,
    marker: [u8; 16],
    codec: Codec,
    pub(super) writer_schema: Schema,
    schemata: Vec<&'r Schema>,
    pub(super) user_metadata: HashMap<String, Vec<u8>>,
    names_refs: Names,
}

impl<'r, R: Read> Block<'r, R> {
    pub(super) fn new(reader: R, schemata: Vec<&'r Schema>) -> AvroResult<Block<'r, R>> {
        let mut block = Block {
            reader,
            codec: Codec::Null,
            writer_schema: Schema::Null,
            schemata,
            buf: vec![],
            buf_idx: 0,
            message_count: 0,
            marker: [0; 16],
            user_metadata: Default::default(),
            names_refs: Default::default(),
        };

        block.read_header()?;
        Ok(block)
    }

    /// Try to read the header and to set the writer `Schema`, the `Codec` and the marker based on
    /// its content.
    fn read_header(&mut self) -> AvroResult<()> {
        let mut buf = [0u8; 4];
        self.reader
            .read_exact(&mut buf)
            .map_err(Details::ReadHeader)?;

        if buf != [b'O', b'b', b'j', 1u8] {
            return Err(Details::HeaderMagic.into());
        }

        let meta_schema = Schema::map(Schema::Bytes).build();
        match decode(&meta_schema, &mut self.reader)? {
            Value::Map(metadata) => {
                self.read_writer_schema(&metadata)?;
                self.codec = read_codec(&metadata)?;

                for (key, value) in metadata {
                    if key == "avro.schema"
                        || key == "avro.codec"
                        || key == "avro.codec.compression_level"
                    {
                        // already processed
                    } else if key.starts_with("avro.") {
                        warn!("Ignoring unknown metadata key: {key}");
                    } else {
                        self.read_user_metadata(key, value);
                    }
                }
            }
            _ => {
                return Err(Details::GetHeaderMetadata.into());
            }
        }

        self.reader
            .read_exact(&mut self.marker)
            .map_err(|e| Details::ReadMarker(e).into())
    }

    fn fill_buf(&mut self, n: usize) -> AvroResult<()> {
        // The buffer needs to contain exactly `n` elements, otherwise codecs will potentially read
        // invalid bytes.
        //
        // The are two cases to handle here:
        //
        // 1. `n > self.buf.len()`:
        //    In this case we call `Vec::resize`, which guarantees that `self.buf.len() == n`.
        // 2. `n < self.buf.len()`:
        //    We need to resize to ensure that the buffer len is safe to read `n` elements.
        //
        // TODO: Figure out a way to avoid having to truncate for the second case.
        self.buf.resize(util::safe_len(n)?, 0);
        self.reader
            .read_exact(&mut self.buf)
            .map_err(Details::ReadIntoBuf)?;
        self.buf_idx = 0;
        Ok(())
    }

    /// Try to read a data block, also performing schema resolution for the objects contained in
    /// the block. The objects are stored in an internal buffer to the `Reader`.
    fn read_block_next(&mut self) -> AvroResult<()> {
        assert!(self.is_empty(), "Expected self to be empty!");
        match util::read_long(&mut self.reader).map_err(Error::into_details) {
            Ok(block_len) => {
                self.message_count = block_len as usize;
                let block_bytes = util::read_long(&mut self.reader)?;
                self.fill_buf(block_bytes as usize)?;
                let mut marker = [0u8; 16];
                self.reader
                    .read_exact(&mut marker)
                    .map_err(Details::ReadBlockMarker)?;

                if marker != self.marker {
                    return Err(Details::GetBlockMarker.into());
                }

                // NOTE (JAB): This doesn't fit this Reader pattern very well.
                // `self.buf` is a growable buffer that is reused as the reader is iterated.
                // For non `Codec::Null` variants, `decompress` will allocate a new `Vec`
                // and replace `buf` with the new one, instead of reusing the same buffer.
                // We can address this by using some "limited read" type to decode directly
                // into the buffer. But this is fine, for now.
                self.codec.decompress(&mut self.buf)
            }
            Err(Details::ReadVariableIntegerBytes(io_err)) => {
                if let ErrorKind::UnexpectedEof = io_err.kind() {
                    // to not return any error in case we only finished to read cleanly from the stream
                    Ok(())
                } else {
                    Err(Details::ReadVariableIntegerBytes(io_err).into())
                }
            }
            Err(e) => Err(Error::new(e)),
        }
    }

    fn len(&self) -> usize {
        self.message_count
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn read_next(&mut self, read_schema: Option<&Schema>) -> AvroResult<Option<Value>> {
        if self.is_empty() {
            self.read_block_next()?;
            if self.is_empty() {
                return Ok(None);
            }
        }

        let mut block_bytes = &self.buf[self.buf_idx..];
        let b_original = block_bytes.len();

        let item = decode_internal(
            &self.writer_schema,
            &self.names_refs,
            None,
            &mut block_bytes,
        )?;
        let item = match read_schema {
            Some(schema) => item.resolve(schema)?,
            None => item,
        };

        if b_original != 0 && b_original == block_bytes.len() {
            // from_avro_datum did not consume any bytes, so return an error to avoid an infinite loop
            return Err(Details::ReadBlock.into());
        }
        self.buf_idx += b_original - block_bytes.len();
        self.message_count -= 1;
        Ok(Some(item))
    }

    pub(super) fn read_next_deser<T: DeserializeOwned>(&mut self) -> AvroResult<Option<T>> {
        if self.is_empty() {
            self.read_block_next()?;
            if self.is_empty() {
                return Ok(None);
            }
        }

        let mut block_bytes = &self.buf[self.buf_idx..];
        let b_original = block_bytes.len();

        let deserializer = SchemaAwareDeserializer::new(
            &mut block_bytes,
            &self.writer_schema,
            Config {
                names: &self.names_refs,
                human_readable: false,
            },
        )?;
        let item = T::deserialize(deserializer)?;

        if b_original != 0 && b_original == block_bytes.len() {
            // from_avro_datum did not consume any bytes, so return an error to avoid an infinite loop
            return Err(Details::ReadBlock.into());
        }
        self.buf_idx += b_original - block_bytes.len();
        self.message_count -= 1;
        Ok(Some(item))
    }

    fn read_writer_schema(&mut self, metadata: &HashMap<String, Value>) -> AvroResult<()> {
        let json: serde_json::Value = metadata
            .get("avro.schema")
            .and_then(|bytes| {
                if let Value::Bytes(ref bytes) = *bytes {
                    from_slice(bytes.as_ref()).ok()
                } else {
                    None
                }
            })
            .ok_or(Details::GetAvroSchemaFromMap)?;
        if !self.schemata.is_empty() {
            let mut names = HashMap::new();
            resolve_names_with_schemata(
                self.schemata.iter().copied(),
                &mut names,
                None,
                &HashMap::new(),
            )?;
            self.names_refs = names.into_iter().map(|(n, s)| (n, s.clone())).collect();
            self.writer_schema = Schema::parse_with_names(&json, self.names_refs.clone())?;
        } else {
            self.writer_schema = Schema::parse(&json)?;
            let mut names = HashMap::new();
            resolve_names(&self.writer_schema, &mut names, None, &HashMap::new())?;
            self.names_refs = names.into_iter().map(|(n, s)| (n, s.clone())).collect();
        }
        Ok(())
    }

    fn read_user_metadata(&mut self, key: String, value: Value) {
        match value {
            Value::Bytes(ref vec) => {
                self.user_metadata.insert(key, vec.clone());
            }
            wrong => {
                warn!("User metadata values must be Value::Bytes, found {wrong:?}");
            }
        }
    }
}

fn read_codec(metadata: &HashMap<String, Value>) -> AvroResult<Codec> {
    let result = metadata
        .get("avro.codec")
        .map(|codec| {
            if let Value::Bytes(ref bytes) = *codec {
                match std::str::from_utf8(bytes.as_ref()) {
                    Ok(utf8) => Ok(utf8),
                    Err(utf8_error) => Err(Details::ConvertToUtf8Error(utf8_error).into()),
                }
            } else {
                Err(Details::BadCodecMetadata.into())
            }
        })
        .map(|codec_res| match codec_res {
            Ok(codec) => match Codec::from_str(codec) {
                Ok(codec) => match codec {
                    #[cfg(feature = "bzip")]
                    Codec::Bzip2(_) => {
                        use crate::Bzip2Settings;
                        if let Some(Value::Bytes(bytes)) =
                            metadata.get("avro.codec.compression_level")
                        {
                            Ok(Codec::Bzip2(Bzip2Settings::new(bytes[0])))
                        } else {
                            Ok(codec)
                        }
                    }
                    #[cfg(feature = "xz")]
                    Codec::Xz(_) => {
                        use crate::XzSettings;
                        if let Some(Value::Bytes(bytes)) =
                            metadata.get("avro.codec.compression_level")
                        {
                            Ok(Codec::Xz(XzSettings::new(bytes[0])))
                        } else {
                            Ok(codec)
                        }
                    }
                    #[cfg(feature = "zstandard")]
                    Codec::Zstandard(_) => {
                        use crate::ZstandardSettings;
                        if let Some(Value::Bytes(bytes)) =
                            metadata.get("avro.codec.compression_level")
                        {
                            Ok(Codec::Zstandard(ZstandardSettings::new(bytes[0])))
                        } else {
                            Ok(codec)
                        }
                    }
                    _ => Ok(codec),
                },
                Err(_) => Err(Details::CodecNotSupported(codec.to_owned()).into()),
            },
            Err(err) => Err(err),
        });

    result.unwrap_or(Ok(Codec::Null))
}
