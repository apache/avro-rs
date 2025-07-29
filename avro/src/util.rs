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

use crate::AvroResult;
use crate::error::sync::Details;
use std::sync::atomic::Ordering;
use std::sync::{
    Once,
    atomic::{AtomicBool, AtomicUsize},
};

/// Maximum number of bytes that can be allocated when decoding
/// Avro-encoded values. This is a protection against ill-formed
/// data, whose length field might be interpreted as enormous.
/// See max_allocation_bytes to change this limit.
pub const DEFAULT_MAX_ALLOCATION_BYTES: usize = 512 * 1024 * 1024;
static MAX_ALLOCATION_BYTES: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_ALLOCATION_BYTES);
static MAX_ALLOCATION_BYTES_ONCE: Once = Once::new();

/// Whether to set serialization & deserialization traits
/// as `human_readable` or not.
/// See [set_serde_human_readable] to change this value.
// crate-visible for testing
pub(crate) static SERDE_HUMAN_READABLE: AtomicBool = AtomicBool::new(true);
static SERDE_HUMAN_READABLE_ONCE: Once = Once::new();

pub(crate) fn is_human_readable() -> bool {
    SERDE_HUMAN_READABLE.load(Ordering::Acquire)
}

/// Set a new maximum number of bytes that can be allocated when decoding data.
/// Once called, the limit cannot be changed.
///
/// **NOTE** This function must be called before decoding **any** data. The
/// library leverages [`std::sync::Once`](https://doc.rust-lang.org/std/sync/struct.Once.html)
/// to set the limit either when calling this method, or when decoding for
/// the first time.
pub fn max_allocation_bytes(num_bytes: usize) -> usize {
    MAX_ALLOCATION_BYTES_ONCE.call_once(|| {
        MAX_ALLOCATION_BYTES.store(num_bytes, Ordering::Release);
    });
    MAX_ALLOCATION_BYTES.load(Ordering::Acquire)
}
pub fn safe_len(len: usize) -> AvroResult<usize> {
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

/// Set whether serializing/deserializing is marked as human readable in serde traits.
/// This will adjust the return value of `is_human_readable()` for both.
/// Once called, the value cannot be changed.
///
/// **NOTE** This function must be called before serializing/deserializing **any** data. The
/// library leverages [`std::sync::Once`](https://doc.rust-lang.org/std/sync/struct.Once.html)
/// to set the limit either when calling this method, or when decoding for
/// the first time.
pub fn set_serde_human_readable(human_readable: bool) {
    SERDE_HUMAN_READABLE_ONCE.call_once(|| {
        SERDE_HUMAN_READABLE.store(human_readable, Ordering::Release);
    });
}

#[synca::synca(
  #[cfg(feature = "tokio")]
  pub mod tokio { },
  #[cfg(feature = "sync")]
  pub mod sync {
    sync!();
    replace!(
      bigdecimal::tokio => bigdecimal::sync,
      decode::tokio => decode::sync,
      encode::tokio => encode::sync,
      error::tokio => error::sync,
      schema::tokio => schema::sync,
      util::tokio => util::sync,
      #[tokio::test] => #[test]
    );
  }
)]
mod util {
    #[cfg(feature = "tokio")]
    use futures::future::TryFutureExt;
    #[cfg(feature = "sync")]
    use std::io::Read as AvroRead;
    #[cfg(feature = "tokio")]
    use tokio::io::AsyncRead as AvroRead;
    #[cfg(feature = "tokio")]
    use tokio::io::AsyncReadExt;

    use crate::AvroResult;
    use crate::error::tokio::Details;
    use crate::schema::tokio::Documentation;
    use serde_json::{Map, Value};
    use std::io::Write;

    pub trait MapHelper {
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

    pub async fn read_long<R: AvroRead + Unpin>(reader: &mut R) -> AvroResult<i64> {
        zag_i64(reader).await
    }

    pub fn zig_i32<W: Write>(n: i32, buffer: W) -> AvroResult<usize> {
        zig_i64(n as i64, buffer)
    }

    pub fn zig_i64<W: Write>(n: i64, writer: W) -> AvroResult<usize> {
        encode_variable(((n << 1) ^ (n >> 63)) as u64, writer)
    }

    pub async fn zag_i32<R: AvroRead + Unpin>(reader: &mut R) -> AvroResult<i32> {
        let i = zag_i64(reader).await?;
        i32::try_from(i).map_err(|e| Details::ZagI32(e, i).into())
    }

    pub async fn zag_i64<R: AvroRead + Unpin>(reader: &mut R) -> AvroResult<i64> {
        let z = decode_variable(reader).await?;
        Ok(if z & 0x1 == 0 {
            (z >> 1) as i64
        } else {
            !(z >> 1) as i64
        })
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

    async fn decode_variable<R: AvroRead + Unpin>(reader: &mut R) -> AvroResult<u64> {
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
                .map_err(Details::ReadVariableIntegerBytes)
                .await?;
            i |= (u64::from(buf[0] & 0x7F)) << (j * 7);
            if (buf[0] >> 7) == 0 {
                break;
            } else {
                j += 1;
            }
        }

        Ok(i)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::util::safe_len;
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
            let causes_left_shift_overflow: &[u8] = &[0xe1, 0xe1, 0xe1, 0xe1, 0xe1];
            assert!(decode_variable(&mut &*causes_left_shift_overflow).is_err());
        }

        #[test]
        fn test_safe_len() -> TestResult {
            assert_eq!(42usize, safe_len(42usize)?);
            assert!(safe_len(1024 * 1024 * 1024).is_err());

            Ok(())
        }
    }
}
