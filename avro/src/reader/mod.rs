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

//! Logic handling reading from Avro format at user level.

mod block;
pub mod datum;
pub mod single_object;

use std::{
    collections::HashMap,
    io::{Read, Seek},
    marker::PhantomData,
};

use block::Block;
pub use block::BlockPosition;
use bon::bon;
use serde::de::DeserializeOwned;

use crate::{AvroResult, schema::Schema, types::Value, util::is_human_readable};

/// Main interface for reading Avro formatted values.
///
/// To be used as an iterator:
///
/// ```no_run
/// # use apache_avro::Reader;
/// # use std::io::Cursor;
/// # let input = Cursor::new(Vec::<u8>::new());
/// for value in Reader::new(input).unwrap() {
///     match value {
///         Ok(v) => println!("{:?}", v),
///         Err(e) => println!("Error: {}", e),
///     };
/// }
/// ```
pub struct Reader<'a, R> {
    block: Block<'a, R>,
    reader_schema: Option<&'a Schema>,
    errored: bool,
    should_resolve_schema: bool,
}

#[bon]
impl<'a, R: Read> Reader<'a, R> {
    /// Creates a `Reader` given something implementing the `io::Read` trait to read from.
    /// No reader `Schema` will be set.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    pub fn new(reader: R) -> AvroResult<Reader<'a, R>> {
        Reader::builder(reader).build()
    }

    /// Creates a `Reader` given something implementing the `io::Read` trait to read from.
    /// With an optional reader `Schema` and optional schemata to use for resolving schema
    /// references.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    #[builder(finish_fn = build)]
    pub fn builder(
        #[builder(start_fn)] reader: R,
        /// The schema the written data needs to be adapted to.
        ///
        /// This is currently not compatible with [`ReaderDeser`] and will panic during reading if the
        /// writer schema is not the same as the reader schema.
        reader_schema: Option<&'a Schema>,
        schemata: Option<Vec<&'a Schema>>,
        #[builder(default = is_human_readable())] human_readable: bool,
    ) -> AvroResult<Reader<'a, R>> {
        let schemata =
            schemata.unwrap_or_else(|| reader_schema.map(|rs| vec![rs]).unwrap_or_default());

        let block = Block::new(reader, schemata, human_readable)?;
        let mut reader = Reader {
            block,
            reader_schema,
            errored: false,
            should_resolve_schema: false,
        };
        // Check if the reader and writer schemas disagree.
        reader.should_resolve_schema =
            reader_schema.is_some_and(|reader_schema| reader.writer_schema() != reader_schema);
        Ok(reader)
    }

    /// Get a reference to the writer `Schema`.
    #[inline]
    pub fn writer_schema(&self) -> &Schema {
        &self.block.writer_schema
    }

    /// Get a reference to the optional reader `Schema`.
    #[inline]
    pub fn reader_schema(&self) -> Option<&Schema> {
        self.reader_schema
    }

    /// Get a reference to the user metadata.
    #[inline]
    pub fn user_metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.block.user_metadata
    }

    /// Convert this reader into an iterator that deserializes to `T`.
    pub fn into_deser_iter<T: DeserializeOwned>(self) -> ReaderDeser<'a, R, T> {
        ReaderDeser {
            inner: self,
            phantom: PhantomData,
        }
    }

    #[inline]
    fn read_next(&mut self) -> AvroResult<Option<Value>> {
        let read_schema = if self.should_resolve_schema {
            self.reader_schema
        } else {
            None
        };

        self.block.read_next(read_schema)
    }

    fn read_next_deser<T: DeserializeOwned>(&mut self) -> AvroResult<Option<T>> {
        // TODO: Implement SchemaAwareResolvingDeserializer
        assert!(
            !self.should_resolve_schema,
            "Schema aware deserialisation does not resolve schemas yet"
        );

        self.block.read_next_deser(self.reader_schema)
    }
}

impl<R: Read> Iterator for Reader<'_, R> {
    type Item = AvroResult<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        // to prevent keep on reading after the first error occurs
        if self.errored {
            return None;
        };
        match self.read_next() {
            Ok(opt) => opt.map(Ok),
            Err(e) => {
                self.errored = true;
                Some(Err(e))
            }
        }
    }
}

impl<R: Read + Seek> Reader<'_, R> {
    /// The currently loaded block's position and record count.
    ///
    /// Returns `None` before the first record has been read.
    /// Updated automatically each time a new block is loaded during iteration.
    pub fn current_block(&self) -> Option<BlockPosition> {
        self.block.current_block_info
    }

    /// Byte offset where data blocks begin (right after the file header).
    ///
    /// This is the offset of the first data block — equivalent to the position
    /// that would be returned by `current_block().offset` for block 0.
    pub fn data_start(&self) -> u64 {
        self.block.data_start
    }

    /// Seek to the data block at the given byte offset and load it.
    ///
    /// The offset must point to the start of a valid data block (before its
    /// object-count varint). The block is read, decompressed, and its sync
    /// marker is validated against the file header. After this call, [`Iterator::next`]
    /// yields the first record in that block.
    ///
    /// Typically the caller saves offsets from [`current_block`](Self::current_block)
    /// during forward iteration and later passes them here to jump back.
    pub fn seek_to_block(&mut self, offset: u64) -> AvroResult<()> {
        self.block.seek_to_block(offset)?;
        self.errored = false;
        Ok(())
    }
}

/// Wrapper around [`Reader`] where the iterator deserializes `T`.
pub struct ReaderDeser<'a, R, T> {
    inner: Reader<'a, R>,
    phantom: PhantomData<T>,
}

impl<R: Read, T: DeserializeOwned> Iterator for ReaderDeser<'_, R, T> {
    type Item = AvroResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        // Don't continue when we've errored before
        if self.inner.errored {
            return None;
        }
        match self.inner.read_next_deser::<T>() {
            Ok(opt) => opt.map(Ok),
            Err(e) => {
                self.inner.errored = true;
                Some(Err(e))
            }
        }
    }
}

/// Reads the marker bytes from Avro bytes generated earlier by a `Writer`
///
/// # Panics
/// Will panic if `bytes` does not contain at least 16 bytes.
pub fn read_marker(bytes: &[u8]) -> [u8; 16] {
    assert!(
        bytes.len() > 16,
        "The bytes are too short to read a marker from them"
    );
    let mut marker = [0_u8; 16];
    marker.clone_from_slice(&bytes[(bytes.len() - 16)..]);
    marker
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use apache_avro_test_helper::TestResult;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::types::Record;

    const SCHEMA: &str = r#"
    {
      "type": "record",
      "name": "test",
      "fields": [
        {
          "name": "a",
          "type": "long",
          "default": 42
        },
        {
          "name": "b",
          "type": "string"
        }
      ]
    }
    "#;
    const ENCODED: &[u8] = &[
        79u8, 98u8, 106u8, 1u8, 4u8, 22u8, 97u8, 118u8, 114u8, 111u8, 46u8, 115u8, 99u8, 104u8,
        101u8, 109u8, 97u8, 222u8, 1u8, 123u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8,
        114u8, 101u8, 99u8, 111u8, 114u8, 100u8, 34u8, 44u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8,
        58u8, 34u8, 116u8, 101u8, 115u8, 116u8, 34u8, 44u8, 34u8, 102u8, 105u8, 101u8, 108u8,
        100u8, 115u8, 34u8, 58u8, 91u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8,
        97u8, 34u8, 44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 108u8, 111u8, 110u8,
        103u8, 34u8, 44u8, 34u8, 100u8, 101u8, 102u8, 97u8, 117u8, 108u8, 116u8, 34u8, 58u8, 52u8,
        50u8, 125u8, 44u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8, 98u8, 34u8,
        44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 115u8, 116u8, 114u8, 105u8,
        110u8, 103u8, 34u8, 125u8, 93u8, 125u8, 20u8, 97u8, 118u8, 114u8, 111u8, 46u8, 99u8, 111u8,
        100u8, 101u8, 99u8, 8u8, 110u8, 117u8, 108u8, 108u8, 0u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8, 4u8, 20u8, 54u8,
        6u8, 102u8, 111u8, 111u8, 84u8, 6u8, 98u8, 97u8, 114u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8,
    ];

    #[test]
    fn test_reader_iterator() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let reader = Reader::builder(ENCODED).reader_schema(&schema).build()?;

        let mut record1 = Record::new(&schema).unwrap();
        record1.put("a", 27i64);
        record1.put("b", "foo");

        let mut record2 = Record::new(&schema).unwrap();
        record2.put("a", 42i64);
        record2.put("b", "bar");

        let expected = [record1.into(), record2.into()];

        for (i, value) in reader.enumerate() {
            assert_eq!(value?, expected[i]);
        }

        Ok(())
    }

    #[test]
    fn test_reader_invalid_header() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut invalid = &ENCODED[1..];
        assert!(
            Reader::builder(&mut invalid)
                .reader_schema(&schema)
                .build()
                .is_err()
        );

        Ok(())
    }

    #[test]
    fn test_reader_invalid_block() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut invalid = &ENCODED[0..ENCODED.len() - 19];
        let reader = Reader::builder(&mut invalid)
            .reader_schema(&schema)
            .build()?;
        for value in reader {
            assert!(value.is_err());
        }

        Ok(())
    }

    #[test]
    fn test_reader_empty_buffer() -> TestResult {
        let empty = Cursor::new(Vec::new());
        assert!(Reader::new(empty).is_err());

        Ok(())
    }

    #[test]
    fn test_reader_only_header() -> TestResult {
        let mut invalid = &ENCODED[..165];
        let reader = Reader::new(&mut invalid)?;
        for value in reader {
            assert!(value.is_err());
        }

        Ok(())
    }

    #[test]
    fn test_avro_3405_read_user_metadata_success() -> TestResult {
        use crate::writer::Writer;

        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;

        let mut user_meta_data: HashMap<String, Vec<u8>> = HashMap::new();
        user_meta_data.insert(
            "stringKey".to_string(),
            "stringValue".to_string().into_bytes(),
        );
        user_meta_data.insert("bytesKey".to_string(), b"bytesValue".to_vec());
        user_meta_data.insert("vecKey".to_string(), vec![1, 2, 3]);

        for (k, v) in user_meta_data.iter() {
            writer.add_user_metadata(k.to_string(), v)?;
        }

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        writer.append_value(record.clone())?;
        writer.append_value(record.clone())?;
        writer.flush()?;
        let result = writer.into_inner()?;

        let reader = Reader::new(&result[..])?;
        assert_eq!(reader.user_metadata(), &user_meta_data);

        Ok(())
    }

    #[cfg(not(feature = "snappy"))]
    #[test]
    fn test_avro_3549_read_not_enabled_codec() {
        let snappy_compressed_avro = vec![
            79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 210, 1, 123,
            34, 102, 105, 101, 108, 100, 115, 34, 58, 91, 123, 34, 110, 97, 109, 101, 34, 58, 34,
            110, 117, 109, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 115, 116, 114, 105, 110,
            103, 34, 125, 93, 44, 34, 110, 97, 109, 101, 34, 58, 34, 101, 118, 101, 110, 116, 34,
            44, 34, 110, 97, 109, 101, 115, 112, 97, 99, 101, 34, 58, 34, 101, 120, 97, 109, 112,
            108, 101, 110, 97, 109, 101, 115, 112, 97, 99, 101, 34, 44, 34, 116, 121, 112, 101, 34,
            58, 34, 114, 101, 99, 111, 114, 100, 34, 125, 20, 97, 118, 114, 111, 46, 99, 111, 100,
            101, 99, 12, 115, 110, 97, 112, 112, 121, 0, 213, 209, 241, 208, 200, 110, 164, 47,
            203, 25, 90, 235, 161, 167, 195, 177, 2, 20, 4, 12, 6, 49, 50, 51, 115, 38, 58, 0, 213,
            209, 241, 208, 200, 110, 164, 47, 203, 25, 90, 235, 161, 167, 195, 177,
        ];

        if let Err(err) = Reader::new(snappy_compressed_avro.as_slice()) {
            assert_eq!("Codec 'snappy' is not supported/enabled", err.to_string());
        } else {
            panic!("Expected an error in the reading of the codec!");
        }
    }

    /// Write an Avro file with multiple blocks and verify we can seek between them.
    #[test]
    fn test_seek_to_block() -> TestResult {
        use crate::writer::Writer;

        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;

        // Block 0: records with a=10, a=20
        let mut r = Record::new(&schema).unwrap();
        r.put("a", 10i64);
        r.put("b", "b0_r0");
        writer.append_value(r)?;
        let mut r = Record::new(&schema).unwrap();
        r.put("a", 20i64);
        r.put("b", "b0_r1");
        writer.append_value(r)?;
        writer.flush()?;

        // Block 1: records with a=30, a=40
        let mut r = Record::new(&schema).unwrap();
        r.put("a", 30i64);
        r.put("b", "b1_r0");
        writer.append_value(r)?;
        let mut r = Record::new(&schema).unwrap();
        r.put("a", 40i64);
        r.put("b", "b1_r1");
        writer.append_value(r)?;
        writer.flush()?;

        // Block 2: records with a=50
        let mut r = Record::new(&schema).unwrap();
        r.put("a", 50i64);
        r.put("b", "b2_r0");
        writer.append_value(r)?;
        writer.flush()?;

        let data = writer.into_inner()?;

        // Read forward and collect block positions
        let mut reader = Reader::new(Cursor::new(&data))?;
        let mut block_offsets: Vec<BlockPosition> = Vec::new();
        let mut all_values: Vec<Value> = Vec::new();

        assert!(reader.current_block().is_none());

        while let Some(value) = reader.next() {
            all_values.push(value?);
            let pos = reader.current_block().expect("block info after read");
            if block_offsets
                .last()
                .is_none_or(|last| last.offset != pos.offset)
            {
                block_offsets.push(pos);
            }
        }

        assert_eq!(all_values.len(), 5);
        assert_eq!(block_offsets.len(), 3);
        assert_eq!(block_offsets[0].message_count, 2);
        assert_eq!(block_offsets[1].message_count, 2);
        assert_eq!(block_offsets[2].message_count, 1);
        assert_eq!(reader.data_start(), block_offsets[0].offset);

        // Seek back to block 1 and read its records
        reader.seek_to_block(block_offsets[1].offset)?;
        let v1 = reader.next().unwrap()?;
        assert_eq!(v1, all_values[2]);
        let v2 = reader.next().unwrap()?;
        assert_eq!(v2, all_values[3]);

        // Seek back to block 0
        reader.seek_to_block(block_offsets[0].offset)?;
        let v0 = reader.next().unwrap()?;
        assert_eq!(v0, all_values[0]);

        // Seek to block 2
        reader.seek_to_block(block_offsets[2].offset)?;
        let v4 = reader.next().unwrap()?;
        assert_eq!(v4, all_values[4]);

        assert!(reader.next().is_none());

        Ok(())
    }

    /// Seeking to an invalid offset should fail with a sync marker error.
    #[test]
    fn test_seek_to_invalid_offset() -> TestResult {
        use crate::writer::Writer;

        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;
        let mut r = Record::new(&schema).unwrap();
        r.put("a", 1i64);
        r.put("b", "x");
        writer.append_value(r)?;
        writer.flush()?;
        let data = writer.into_inner()?;

        let mut reader = Reader::new(Cursor::new(&data))?;
        let result = reader.seek_to_block(7);
        assert!(result.is_err());

        Ok(())
    }
}
