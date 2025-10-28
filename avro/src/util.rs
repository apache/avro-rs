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

//! Utility functions, like configuring various global settings.

use crate::{AvroResult, Error, error::Details, schema::Documentation};
use serde_json::{Map, Value};
use std::{io::Write, sync::OnceLock};

/// Maximum number of bytes that can be allocated when decoding
/// Avro-encoded values. This is a protection against ill-formed
/// data, whose length field might be interpreted as enormous.
/// See max_allocation_bytes to change this limit.
pub const DEFAULT_MAX_ALLOCATION_BYTES: usize = 512 * 1024 * 1024;
static MAX_ALLOCATION_BYTES: OnceLock<usize> = OnceLock::new();

/// Whether to set serialization & deserialization traits
/// as `human_readable` or not.
/// See [set_serde_human_readable] to change this value.
// crate-visible for testing
pub(crate) static SERDE_HUMAN_READABLE: OnceLock<bool> = OnceLock::new();
/// Whether the serializer and deserializer should indicate to types that the format is human-readable.
pub const DEFAULT_SERDE_HUMAN_READABLE: bool = false;

pub(crate) trait MapHelper {
    fn string(&self, key: &str) -> Option<String>;

    fn name(&self) -> Option<String> {
        self.string("name")
    }

    fn doc(&self) -> Documentation {
        self.string("doc")
    }

    fn aliases(&self) -> Option<Vec<String>>;
}

impl MapHelper for Map<String, Value> {
    fn string(&self, key: &str) -> Option<String> {
        self.get(key)
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    }

    fn aliases(&self) -> Option<Vec<String>> {
        // FIXME no warning when aliases aren't a json array of json strings
        self.get("aliases")
            .and_then(|aliases| aliases.as_array())
            .and_then(|aliases| {
                aliases
                    .iter()
                    .map(|alias| alias.as_str())
                    .map(|alias| alias.map(|a| a.to_string()))
                    .collect::<Option<_>>()
            })
    }
}

pub(crate) fn zig_i32<W: Write>(n: i32, buffer: W) -> AvroResult<usize> {
    zig_i64(n as i64, buffer)
}

pub(crate) fn zig_i64<W: Write>(n: i64, writer: W) -> AvroResult<usize> {
    encode_variable(((n << 1) ^ (n >> 63)) as u64, writer)
}

fn encode_variable<W: Write>(mut z: u64, mut writer: W) -> AvroResult<usize> {
    let mut buffer = [0u8; 10];
    let mut i: usize = 0;
    loop {
        if z <= 0x7F {
            buffer[i] = (z & 0x7F) as u8;
            i += 1;
            break;
        } else {
            buffer[i] = (0x80 | (z & 0x7F)) as u8;
            i += 1;
            z >>= 7;
        }
    }
    writer
        .write(&buffer[..i])
        .map_err(|e| Details::WriteBytes(e).into())
}

/// Decode a zigzag encoded length.
///
/// This version of [`decode_len`] will return a [`Details::ReadVariableIntegerBytes`] error if there are not
/// enough bytes and does not return the amount of bytes read.
///
/// See [`decode_len`] for more details.
pub(crate) fn decode_len_simple(buffer: &[u8]) -> AvroResult<(usize, usize)> {
    decode_len(buffer)?.ok_or_else(|| {
        Details::ReadVariableIntegerBytes(std::io::ErrorKind::UnexpectedEof.into()).into()
    })
}

/// Decode a zigzag encoded length.
///
/// This will use [`safe_len`] to check if the length is in allowed bounds.
///
/// # Returns
/// `Some(integer, bytes read)` if it completely read an integer, `None` if it did not have enough
/// bytes in the slice.
pub(crate) fn decode_len(buffer: &[u8]) -> AvroResult<Option<(usize, usize)>> {
    if let Some((integer, bytes)) = decode_variable(buffer)? {
        let length =
            usize::try_from(integer).map_err(|e| Details::ConvertI64ToUsize(e, integer))?;
        let safe = safe_len(length)?;
        Ok(Some((safe, bytes)))
    } else {
        Ok(None)
    }
}

/// Decode a zigzag encoded integer.
///
/// # Returns
/// `Some(integer, bytes read)` if it completely read an integer, `None` if it did not have enough
/// bytes in the slice.
pub(crate) fn decode_variable(buffer: &[u8]) -> Result<Option<(i64, usize)>, Error> {
    let mut decoded = 0;
    let mut loops_done = 0;
    let mut last_byte = 0;

    for (counter, &byte) in buffer.iter().take(10).enumerate() {
        decoded |= u64::from(byte & 0x7F) << (counter * 7);
        loops_done = counter;
        last_byte = byte;
        if byte >> 7 == 0 {
            break;
        }
    }

    if last_byte >> 7 != 0 {
        if loops_done == 10 {
            Err(Details::IntegerOverflow.into())
        } else {
            Ok(None)
        }
    } else if decoded & 0x1 == 0 {
        Ok(Some(((decoded >> 1) as i64, loops_done + 1)))
    } else {
        Ok(Some((!(decoded >> 1) as i64, loops_done + 1)))
    }
}

/// Set the maximum number of bytes that can be allocated when decoding data.
///
/// This function only changes the setting once. On subsequent calls the value will stay the same
/// as the first time it is called. It is automatically called on first allocation and defaults to
/// [`DEFAULT_MAX_ALLOCATION_BYTES`].
///
/// # Returns
/// The configured maximum, which might be different from what the function was called with if the
/// value was already set before.
pub fn max_allocation_bytes(num_bytes: usize) -> usize {
    *MAX_ALLOCATION_BYTES.get_or_init(|| num_bytes)
}

pub(crate) fn safe_len(len: usize) -> AvroResult<usize> {
    let max_bytes = max_allocation_bytes(DEFAULT_MAX_ALLOCATION_BYTES);

    if len <= max_bytes {
        Ok(len)
    } else {
        Err(Details::MemoryAllocation {
            desired: len,
            maximum: max_bytes,
        }
        .into())
    }
}

/// Set whether the serializer and deserializer should indicate to types that the format is human-readable.
///
/// This function only changes the setting once. On subsequent calls the value will stay the same
/// as the first time it is called. It is automatically called on first allocation and defaults to
/// [`DEFAULT_SERDE_HUMAN_READABLE`].
///
/// *NOTE*: Changing this setting can change the output of [`from_value`](crate::from_value) and the
/// accepted input of [`to_value`](crate::to_value).
///
/// # Returns
/// The configured human-readable value, which might be different from what the function was called
/// with if the value was already set before.
pub fn set_serde_human_readable(human_readable: bool) -> bool {
    *SERDE_HUMAN_READABLE.get_or_init(|| human_readable)
}

pub(crate) fn is_human_readable() -> bool {
    *SERDE_HUMAN_READABLE.get_or_init(|| DEFAULT_SERDE_HUMAN_READABLE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro_test_helper::TestResult;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_zigzag() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        zig_i32(42i32, &mut a).unwrap();
        zig_i64(42i64, &mut b).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_zig_i64() {
        let mut s = Vec::new();

        zig_i64(0, &mut s).unwrap();
        assert_eq!(s, [0]);

        s.clear();
        zig_i64(-1, &mut s).unwrap();
        assert_eq!(s, [1]);

        s.clear();
        zig_i64(1, &mut s).unwrap();
        assert_eq!(s, [2]);

        s.clear();
        zig_i64(-64, &mut s).unwrap();
        assert_eq!(s, [127]);

        s.clear();
        zig_i64(64, &mut s).unwrap();
        assert_eq!(s, [128, 1]);

        s.clear();
        zig_i64(i32::MAX as i64, &mut s).unwrap();
        assert_eq!(s, [254, 255, 255, 255, 15]);

        s.clear();
        zig_i64(i32::MAX as i64 + 1, &mut s).unwrap();
        assert_eq!(s, [128, 128, 128, 128, 16]);

        s.clear();
        zig_i64(i32::MIN as i64, &mut s).unwrap();
        assert_eq!(s, [255, 255, 255, 255, 15]);

        s.clear();
        zig_i64(i32::MIN as i64 - 1, &mut s).unwrap();
        assert_eq!(s, [129, 128, 128, 128, 16]);

        s.clear();
        zig_i64(i64::MAX, &mut s).unwrap();
        assert_eq!(s, [254, 255, 255, 255, 255, 255, 255, 255, 255, 1]);

        s.clear();
        zig_i64(i64::MIN, &mut s).unwrap();
        assert_eq!(s, [255, 255, 255, 255, 255, 255, 255, 255, 255, 1]);
    }

    #[test]
    fn test_zig_i32() {
        let mut s = Vec::new();
        zig_i32(i32::MAX / 2, &mut s).unwrap();
        assert_eq!(s, [254, 255, 255, 255, 7]);

        s.clear();
        zig_i32(i32::MIN / 2, &mut s).unwrap();
        assert_eq!(s, [255, 255, 255, 255, 7]);

        s.clear();
        zig_i32(-(i32::MIN / 2), &mut s).unwrap();
        assert_eq!(s, [128, 128, 128, 128, 8]);

        s.clear();
        zig_i32(i32::MIN / 2 - 1, &mut s).unwrap();
        assert_eq!(s, [129, 128, 128, 128, 8]);

        s.clear();
        zig_i32(i32::MAX, &mut s).unwrap();
        assert_eq!(s, [254, 255, 255, 255, 15]);

        s.clear();
        zig_i32(i32::MIN, &mut s).unwrap();
        assert_eq!(s, [255, 255, 255, 255, 15]);
    }

    #[test]
    fn test_overflow() {
        let not_enough_bytes: &[u8] = &[0xe1, 0xe1, 0xe1, 0xe1, 0xe1];
        assert!(decode_variable(not_enough_bytes).unwrap().is_none());
    }

    #[test]
    fn test_safe_len() -> TestResult {
        assert_eq!(42usize, safe_len(42usize)?);
        assert!(safe_len(1024 * 1024 * 1024).is_err());

        Ok(())
    }
}
