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

//! Decompression must be bounded by `max_allocation_bytes` so a small
//! compressed block cannot inflate to an enormous buffer and exhaust memory.
//!
//! This is a dedicated integration test so it runs in its own process, where
//! the (write-once) allocation limit can be set to a small value; the payloads
//! are then only a few MiB, keeping the test cheap and deterministic.

use apache_avro::Codec;
use apache_avro::error::Details;

const BUDGET: usize = 1024 * 1024; // 1 MiB
const BOMB_PLAINTEXT: usize = 8 * 1024 * 1024; // 8 MiB, well over the budget

fn set_budget() {
    apache_avro::util::max_allocation_bytes(BUDGET);
}

/// Compresses `plaintext` bytes with `codec` and asserts the decompressed
/// output (larger than the budget) is rejected rather than allocated, then that
/// a small payload still round-trips.
fn assert_bounded(codec: Codec) {
    set_budget();

    let mut bomb = vec![0u8; BOMB_PLAINTEXT];
    codec.compress(&mut bomb).expect("compress bomb");
    assert!(
        bomb.len() <= BUDGET,
        "compressed bomb ({} bytes) should be far smaller than the budget",
        bomb.len()
    );
    let result = codec.decompress(&mut bomb);
    // Assert it fails *specifically* because the output exceeded the budget, so
    // a regression that fails decompression for another reason cannot pass.
    match result {
        Err(e) => assert!(
            matches!(e.details(), Details::MemoryAllocation { .. }),
            "expected a MemoryAllocation error, got {e:?}"
        ),
        Ok(()) => panic!("a decompressed output exceeding the budget must be rejected"),
    }

    // A payload within the budget must still round-trip.
    let original = b"apache avro decompression within budget".to_vec();
    let mut data = original.clone();
    codec.compress(&mut data).expect("compress small");
    codec.decompress(&mut data).expect("decompress small");
    assert_eq!(data, original);
}

#[test]
fn deflate_decompression_is_bounded() {
    use apache_avro::DeflateSettings;
    assert_bounded(Codec::Deflate(DeflateSettings::default()));
}

#[test]
#[cfg(feature = "snappy")]
fn snappy_decompression_is_bounded() {
    assert_bounded(Codec::Snappy);
}

#[test]
#[cfg(feature = "zstandard")]
fn zstandard_decompression_is_bounded() {
    use apache_avro::ZstandardSettings;
    assert_bounded(Codec::Zstandard(ZstandardSettings::default()));
}

#[test]
#[cfg(feature = "bzip")]
fn bzip2_decompression_is_bounded() {
    use apache_avro::Bzip2Settings;
    assert_bounded(Codec::Bzip2(Bzip2Settings::default()));
}

#[test]
#[cfg(feature = "xz")]
fn xz_decompression_is_bounded() {
    use apache_avro::XzSettings;
    assert_bounded(Codec::Xz(XzSettings::default()));
}
