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

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::{hint::black_box, time::Duration};

const BLOCK_SIZES: &[usize] = &[128, 1024, 16 * 1024, 1024 * 1024];

fn input(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| {
            let word = (i as u64)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .rotate_left((i % 63) as u32);
            (word ^ (word >> 32)) as u8
        })
        .collect()
}

fn crc32fast_checksum(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

fn crc_fast_checksum(data: &[u8]) -> u32 {
    crc_fast::crc32_iso_hdlc(data)
}

fn compress_with_crc32fast(data: &[u8]) -> Vec<u8> {
    compress_with(data, crc32fast_checksum)
}

fn compress_with_crc_fast(data: &[u8]) -> Vec<u8> {
    compress_with(data, crc_fast_checksum)
}

fn compress_with(data: &[u8], checksum: fn(&[u8]) -> u32) -> Vec<u8> {
    let mut encoded = vec![0; snap::raw::max_compress_len(data.len())];
    let compressed_size = snap::raw::Encoder::new()
        .compress(data, &mut encoded)
        .expect("snappy compression should succeed");

    encoded.truncate(compressed_size + 4);
    encoded[compressed_size..].copy_from_slice(&checksum(data).to_be_bytes());
    encoded
}

fn decompress_with_crc32fast(compressed: &[u8]) -> Vec<u8> {
    decompress_with(compressed, crc32fast_checksum)
}

fn decompress_with_crc_fast(compressed: &[u8]) -> Vec<u8> {
    decompress_with(compressed, crc_fast_checksum)
}

fn decompress_with(compressed: &[u8], checksum: fn(&[u8]) -> u32) -> Vec<u8> {
    let data_end = compressed.len() - 4;
    let decompressed_size = snap::raw::decompress_len(&compressed[..data_end])
        .expect("snappy decompressed length should be readable");
    let mut decoded = vec![0; decompressed_size];

    snap::raw::Decoder::new()
        .decompress(&compressed[..data_end], &mut decoded)
        .expect("snappy decompression should succeed");

    let expected = u32::from_be_bytes(compressed[data_end..].try_into().unwrap());
    assert_eq!(expected, checksum(&decoded));
    decoded
}

fn bench_crc32(c: &mut Criterion) {
    let mut group = c.benchmark_group("crc32_iso_hdlc");
    for &size in BLOCK_SIZES {
        let data = input(size);
        assert_eq!(crc32fast_checksum(&data), crc_fast_checksum(&data));
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(format!("crc32fast/{size}"), &data, |b, data| {
            b.iter(|| crc32fast_checksum(black_box(data)));
        });
        group.bench_with_input(format!("crc-fast/{size}"), &data, |b, data| {
            b.iter(|| crc_fast_checksum(black_box(data)));
        });
    }
    group.finish();
}

fn bench_snappy_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("snappy_compress_with_crc32");
    for &size in BLOCK_SIZES {
        let data = input(size);
        assert_eq!(
            compress_with_crc32fast(&data)[..],
            compress_with_crc_fast(&data)[..]
        );
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(format!("crc32fast/{size}"), &data, |b, data| {
            b.iter(|| compress_with_crc32fast(black_box(data)));
        });
        group.bench_with_input(format!("crc-fast/{size}"), &data, |b, data| {
            b.iter(|| compress_with_crc_fast(black_box(data)));
        });
    }
    group.finish();
}

fn bench_snappy_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("snappy_decompress_with_crc32");
    for &size in BLOCK_SIZES {
        let data = input(size);
        let compressed = compress_with_crc_fast(&data);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(format!("crc32fast/{size}"), &compressed, |b, compressed| {
            b.iter_batched(
                || compressed.clone(),
                |compressed| decompress_with_crc32fast(black_box(&compressed)),
                BatchSize::SmallInput,
            );
        });
        group.bench_with_input(format!("crc-fast/{size}"), &compressed, |b, compressed| {
            b.iter_batched(
                || compressed.clone(),
                |compressed| decompress_with_crc_fast(black_box(&compressed)),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(5));
    targets = bench_crc32, bench_snappy_compress, bench_snappy_decompress
);
criterion_main!(benches);
