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

use crate::{AvroResult, error::Details, schema::Documentation};
use serde_json::{Map, Value};
use std::{
    io::{Read, Write},
    sync::OnceLock,
};

/// Maximum number of bytes that can be allocated when decoding Avro-encoded values.
///
/// This is a protection against ill-formed data, whose length field might be interpreted as enormous.
///
/// See [`max_allocation_bytes`] to change this limit.
pub const DEFAULT_MAX_ALLOCATION_BYTES: usize = 512 * 1024 * 1024;
static MAX_ALLOCATION_BYTES: OnceLock<usize> = OnceLock::new();

/// Whether to set serialization & deserialization traits as `human_readable` or not.
///
/// See [`set_serde_human_readable`] to change this value.
pub const DEFAULT_SERDE_HUMAN_READABLE: bool = false;
/// Whether the serializer and deserializer should indicate to types that the format is human-readable.
// crate-visible for testing
pub(crate) static SERDE_HUMAN_READABLE: OnceLock<bool> = OnceLock::new();

pub(crate) trait MapHelper {
    fn string(&self, key: &str) -> Option<&str>;

    fn name(&self) -> Option<&str> {
        self.string("name")
    }

    fn doc(&self) -> Documentation {
        self.string("doc").map(Into::into)
    }

    fn aliases(&self) -> Option<Vec<String>>;
}

impl MapHelper for Map<String, Value> {
    fn string(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.as_str())
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

/// Decode a long from the reader and convert it to a usize.
pub(crate) fn read_usize<R: Read>(reader: &mut R) -> AvroResult<usize> {
    let long = zag_i64(reader)?;
    usize::try_from(long).map_err(|e| Details::ConvertI64ToUsize(e, long).into())
}

/// Write the number as a zigzagged varint to the writer.
pub(crate) fn zig_i32<W: Write>(n: i32, buffer: W) -> AvroResult<usize> {
    zig_i64(n as i64, buffer)
}

/// Write the number as a zigzagged varint to the writer.
pub(crate) fn zig_i64<W: Write>(n: i64, writer: W) -> AvroResult<usize> {
    let zigzagged = ((n << 1) ^ (n >> 63)) as u64;
    encode_variable(zigzagged, writer)
}

/// Decode a zigzagged varint from the reader.
pub(crate) fn zag_i32<R: Read>(reader: &mut R) -> AvroResult<i32> {
    let i = zag_i64(reader)?;
    i32::try_from(i).map_err(|e| Details::ZagI32(e, i).into())
}

/// Decode a zigzagged varint from the reader.
pub(crate) fn zag_i64<R: Read>(reader: &mut R) -> AvroResult<i64> {
    let z = decode_variable(reader)?;
    Ok(if z & 0x1 == 0 {
        (z >> 1) as i64
    } else {
        !(z >> 1) as i64
    })
}

/// Write the number as a varint to the writer.
///
/// Note: this function does not do zigzag encoding, for that see [`zig_i32`] and [`zig_i64`].
fn encode_variable<W: Write>(mut zigzagged: u64, mut writer: W) -> AvroResult<usize> {
    // Ensure the number is little endian for the varint encoding (no-op on LE systems)
    zigzagged = zigzagged.to_le();
    // Encode the number as a varint
    let mut buffer = [0u8; 10];
    let mut i: usize = 0;
    loop {
        if zigzagged <= 0x7F {
            buffer[i] = (zigzagged & 0x7F) as u8;
            i += 1;
            break;
        } else {
            buffer[i] = (0x80 | (zigzagged & 0x7F)) as u8;
            i += 1;
            zigzagged >>= 7;
        }
    }
    writer
        .write_all(&buffer[..i])
        .map_err(Details::WriteBytes)?;
    Ok(i)
}

/// Read a varint from the reader.
///
/// Note: this function does not do zigzag decoding, for that see [`zag_i32`] and [`zag_i64`].
fn decode_variable<R: Read>(reader: &mut R) -> AvroResult<u64> {
    let mut i = 0u64;
    let mut buf = [0u8; 1];

    let mut j = 0;
    loop {
        if j > 9 {
            // if j * 7 > 64
            return Err(Details::IntegerOverflow.into());
        }
        reader
            .read_exact(&mut buf[..])
            .map_err(Details::ReadVariableIntegerBytes)?;
        i |= (u64::from(buf[0] & 0x7F)) << (j * 7);
        if (buf[0] >> 7) == 0 {
            break;
        } else {
            j += 1;
        }
    }

    Ok(u64::from_le(i))
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
            desired: Some(len),
            maximum: max_bytes,
        }
        .into())
    }
}

/// Bound the cumulative number of elements a collection (array or map) may hold.
///
/// [`safe_len`] guards byte-length allocations, but an array or map block is a
/// count of elements, and reserving capacity for `n` elements allocates at
/// least `n * item_size` bytes (`item_size` being the in-memory size of a
/// decoded element; a collection may over-allocate beyond that for growth or
/// internal metadata, so this is a lower-bound estimate). A malicious or
/// truncated input can declare a huge block count in a few bytes, so the count
/// must be validated against the allocation budget, accounting for the
/// per-element size, before reserving or decoding. This also bounds zero-byte
/// on-wire elements (e.g. `null`), which consume no input and so cannot be
/// bounded by the bytes remaining, since the limit is on the decoded element
/// count rather than the bytes read.
pub(crate) fn safe_collection_len(total_items: usize, item_size: usize) -> AvroResult<()> {
    let max_bytes = max_allocation_bytes(DEFAULT_MAX_ALLOCATION_BYTES);
    // Use checked_mul (not saturating_mul): saturating to usize::MAX could pass
    // the check below when max_bytes is configured to usize::MAX, letting the
    // subsequent reserve() hit a capacity-overflow panic instead of erroring.
    let desired = total_items
        .checked_mul(item_size.max(1))
        .ok_or(Details::IntegerOverflow)?;

    if desired <= max_bytes {
        Ok(())
    } else {
        Err(Details::MemoryAllocation {
            desired,
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
        let causes_left_shift_overflow: &[u8] = &[0xe1; 10];
        assert!(matches!(
            decode_variable(&mut &*causes_left_shift_overflow)
                .unwrap_err()
                .details(),
            Details::IntegerOverflow
        ));
    }

    #[test]
    fn test_safe_len() -> TestResult {
        assert_eq!(42usize, safe_len(42usize)?);
        assert!(safe_len(1024 * 1024 * 1024).is_err());

        Ok(())
    }
}
