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

//! Regression: the OCF block reader must reject a negative block size with a
//! conversion error instead of wrapping it via `as usize`.

use apache_avro::{Error, Reader, Schema, Writer, error::Details, types::Record};
use apache_avro_test_helper::TestResult;

#[test]
fn negative_block_size_returns_conversion_error() -> TestResult {
    let schema = Schema::parse_str(
        r#"{"type": "record", "name": "T", "fields": [{"name": "a", "type": "int"}]}"#,
    )?;
    let mut writer = Writer::new(&schema, Vec::new())?;
    let mut record = Record::new(&schema).unwrap();
    record.put("a", 1i32);
    writer.append_value(record)?;
    writer.flush()?;
    let valid = writer.into_inner()?;

    // The sync marker written in the header is also the last 16 bytes of the file.
    let marker = &valid[valid.len() - 16..];

    // Locate the header sync marker (the first occurrence).
    let header_marker_pos = valid
        .windows(16)
        .position(|w| w == marker)
        .expect("sync marker not found in header");
    let block_start = header_marker_pos + 16;

    // The first data block is: block_count(varint) | block_size(varint) | data | sync.
    // For one small record both varints are single-byte (high bit clear).
    assert!(
        valid[block_start] & 0x80 == 0,
        "block_count varint is unexpectedly multi-byte"
    );
    let block_size_offset = block_start + 1;
    assert!(
        valid[block_size_offset] & 0x80 == 0,
        "block_size varint is unexpectedly multi-byte"
    );

    // Replace the single-byte block_size with zigzag-encoded -1 (== 0x01).
    let mut corrupted = valid.clone();
    corrupted[block_size_offset] = 0x01; // zigzag(-1)

    let mut reader = Reader::new(&corrupted[..])?;

    let value = reader
        .next()
        .expect("Reader produced no values and no errors");

    match value.map_err(Error::into_details) {
        Err(Details::ConvertI64ToUsize(_, -1)) => Ok(()),
        other => panic!("Expected Details::ConvertI64ToUsize(_, -1), got {other:?}"),
    }
}
