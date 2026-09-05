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

use std::{
    collections::HashMap,
    io::{ErrorKind, Read},
    str::FromStr,
};

use log::warn;
use serde::de::DeserializeOwned;
use serde_json::from_slice;

use crate::{
    AvroResult, Codec, Error,
    decode::{DecodeContext, decode, decode_internal},
    error::Details,
    schema::{Names, Schema, resolve_names, resolve_names_with_schemata},
    serde::deser_schema::{Config, SchemaAwareDeserializer},
    types::Value,
    util,
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
    human_readable: bool,
}

impl<'r, R: Read> Block<'r, R> {
    pub(super) fn new(
        reader: R,
        schemata: Vec<&'r Schema>,
        human_readable: bool,
    ) -> AvroResult<Block<'r, R>> {
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
            human_readable,
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
        match util::read_usize(&mut self.reader).map_err(Error::into_details) {
            Ok(block_len) => {
                self.message_count = block_len;
                let block_bytes = util::read_usize(&mut self.reader)?;
                self.fill_buf(block_bytes)?;
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
                self.codec.decompress(&mut self.buf)?;

                // The declared object count is attacker-controlled header data
                // and is not otherwise required to be backed by actual bytes:
                // with a zero-width writer schema (e.g. "null") every object
                // decodes without consuming input, so a ~80-byte file could
                // declare 2^62 objects and make readers spin or OOM on
                // collect(). A count larger than the decompressed block size
                // is only possible for such zero-width datums; bound it by
                // the allocation budget instead of trusting the header.
                if self.message_count > self.buf.len() {
                    util::safe_collection_len::<Value>(self.message_count)?;
                }
                Ok(())
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
            &mut DecodeContext::new(),
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

    pub(super) fn read_next_deser<T: DeserializeOwned>(
        &mut self,
        reader_schema: Option<&Schema>,
    ) -> AvroResult<Option<T>> {
        if self.is_empty() {
            self.read_block_next()?;
            if self.is_empty() {
                return Ok(None);
            }
        }

        let mut block_bytes = &self.buf[self.buf_idx..];
        let b_original = block_bytes.len();

        let item = if reader_schema.is_some() {
            // TODO: Implement SchemaAwareResolvingDeserializer
            panic!("Schema aware deserialisation does not resolve schemas yet");
        } else {
            let config = Config {
                names: &self.names_refs,
                human_readable: self.human_readable,
                recursion_depth: 0,
            };
            T::deserialize(SchemaAwareDeserializer::new(
                &mut block_bytes,
                &self.writer_schema,
                config,
            )?)?
        };

        if b_original != 0 && b_original == block_bytes.len() {
            // No bytes were read, return an error to avoid an infinite loop
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
            self.writer_schema = Schema::parse_with_names(json, self.names_refs.clone())?;
        } else {
            self.writer_schema = Schema::parse(json)?;
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
                            match bytes.first() {
                                Some(&level) => Ok(Codec::Bzip2(Bzip2Settings::new(level))),
                                None => Err(Details::BadCodecMetadata.into()),
                            }
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
                            match bytes.first() {
                                Some(&level) => Ok(Codec::Xz(XzSettings::new(level))),
                                None => Err(Details::BadCodecMetadata.into()),
                            }
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
                            match bytes.first() {
                                Some(&level) => Ok(Codec::Zstandard(ZstandardSettings::new(level))),
                                None => Err(Details::BadCodecMetadata.into()),
                            }
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

#[cfg(test)]
mod tests {
    use super::Block;
    use crate::error::Details;
    use crate::{Codec, Schema};
    use apache_avro_test_helper::TestResult;

    #[cfg(any(feature = "bzip", feature = "xz", feature = "zstandard"))]
    fn empty_compression_level_errors_for(codec_name: &str) {
        use crate::types::Value;
        use std::collections::HashMap;

        let mut metadata = HashMap::new();
        metadata.insert(
            "avro.codec".to_string(),
            Value::Bytes(codec_name.as_bytes().to_vec()),
        );
        metadata.insert(
            "avro.codec.compression_level".to_string(),
            Value::Bytes(vec![]),
        );

        // An empty compression_level in attacker-controlled metadata must be
        // a clean error, not an index-out-of-bounds panic.
        let err = super::read_codec(&metadata).unwrap_err().into_details();
        assert!(matches!(err, Details::BadCodecMetadata), "{err:?}");
    }

    #[cfg(feature = "bzip")]
    #[test]
    fn avro_rs_644_empty_bzip2_compression_level_is_rejected() {
        empty_compression_level_errors_for("bzip2");
    }

    #[cfg(feature = "xz")]
    #[test]
    fn avro_rs_644_empty_xz_compression_level_is_rejected() {
        empty_compression_level_errors_for("xz");
    }

    #[cfg(feature = "zstandard")]
    #[test]
    fn avro_rs_644_empty_zstandard_compression_level_is_rejected() {
        empty_compression_level_errors_for("zstandard");
    }

    #[test]
    fn avro_rs_643_huge_message_count_on_empty_block_is_rejected() -> TestResult {
        // Block header claiming 2^62 objects with a block size of 0: with a
        // zero-width writer schema (e.g. "null") each object decodes without
        // consuming input, so the declared count must be bounded by the
        // allocation budget instead of being trusted.
        let mut data = Vec::new();
        crate::util::zig_i64(1i64 << 62, &mut data)?;
        data.push(0x00); // block size 0
        data.extend([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]); // sync marker

        let mut block = Block::<'_, &[u8]> {
            reader: &data[..],
            buf: vec![],
            buf_idx: 0,
            message_count: 0,
            marker: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            codec: Codec::Null,
            writer_schema: Schema::Null,
            schemata: vec![],
            user_metadata: Default::default(),
            names_refs: Default::default(),
            human_readable: false,
        };

        let err = block.read_block_next().unwrap_err().into_details();
        // 2^62 * size_of::<Value>() overflows usize, so this trips the
        // checked_mul overflow arm inside safe_collection_len.
        assert!(matches!(err, Details::IntegerOverflow), "{err:?}");
        Ok(())
    }

    #[test]
    fn avro_rs_643_over_budget_message_count_on_empty_block_is_rejected() -> TestResult {
        // A count that multiplies cleanly by size_of::<Value>() but exceeds
        // the allocation budget must trip the budget check itself, pinning
        // the MemoryAllocation path alongside the overflow one above.
        let budget = crate::util::DEFAULT_MAX_ALLOCATION_BYTES;
        let count = budget / size_of::<crate::types::Value>() + 1;

        let mut data = Vec::new();
        crate::util::zig_i64(count as i64, &mut data)?;
        data.push(0x00); // block size 0
        data.extend([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]); // sync marker

        let mut block = Block::<'_, &[u8]> {
            reader: &data[..],
            buf: vec![],
            buf_idx: 0,
            message_count: 0,
            marker: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            codec: Codec::Null,
            writer_schema: Schema::Null,
            schemata: vec![],
            user_metadata: Default::default(),
            names_refs: Default::default(),
            human_readable: false,
        };

        let err = block.read_block_next().unwrap_err().into_details();
        assert!(matches!(err, Details::MemoryAllocation { .. }), "{err:?}");

        Ok(())
    }

    #[test]
    fn avro_rs_643_small_message_count_on_empty_block_is_accepted() -> TestResult {
        // The crate's own writer produces count > 0 with an empty block body
        // for zero-width datums (e.g. schema "null"); those must keep working.
        let null_values_count = 3;
        let mut data = Vec::new();
        crate::util::zig_i64(null_values_count, &mut data)?;
        data.push(0x00); // block size 0
        data.extend([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]); // sync marker

        let mut block = Block::<'_, &[u8]> {
            reader: &data[..],
            buf: vec![],
            buf_idx: 0,
            message_count: 0,
            marker: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            codec: Codec::Null,
            writer_schema: Schema::Null,
            schemata: vec![],
            user_metadata: Default::default(),
            names_refs: Default::default(),
            human_readable: false,
        };

        block.read_block_next()?;
        assert_eq!(block.message_count, null_values_count as usize);
        Ok(())
    }

    #[test]
    fn avro_rs_586_negative_block_size() {
        let mut block = Block::<'_, &[u8]> {
            // Block header with an object count of 1 and a block size of -1
            reader: &[0x02, 0x01],
            buf: vec![],
            buf_idx: 0,
            message_count: 0,
            marker: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            codec: Codec::Null,
            writer_schema: Schema::Null,
            schemata: vec![],
            user_metadata: Default::default(),
            names_refs: Default::default(),
            human_readable: false,
        };

        let err = block.read_block_next().unwrap_err().into_details();
        assert!(matches!(err, Details::ConvertI64ToUsize(_, _)));
    }

    #[test]
    fn avro_rs_586_negative_block_len() {
        let mut block = Block::<'_, &[u8]> {
            // Block header with an object count of -1 and a block size of 0
            reader: &[0x01, 0x00],
            buf: vec![],
            buf_idx: 0,
            message_count: 0,
            marker: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            codec: Codec::Null,
            writer_schema: Schema::Null,
            schemata: vec![],
            user_metadata: Default::default(),
            names_refs: Default::default(),
            human_readable: false,
        };

        let err = block.read_block_next().unwrap_err().into_details();
        assert!(matches!(err, Details::ConvertI64ToUsize(_, _)));
    }
}
