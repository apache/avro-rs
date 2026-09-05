<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# CRC32 Snappy Benchmark Results

This benchmark compares the current Snappy checksum replacement against the old
`crc32fast` call pattern. It exercises three paths:

- raw CRC-32/ISO-HDLC over contiguous blocks;
- Snappy compression plus Avro's trailing CRC32;
- Snappy decompression plus Avro's trailing CRC32 verification.

Run command:

```bash
cargo bench -p apache-avro --bench crc32_snappy --features snappy,derive
```

The benchmark asserts that `crc32fast` and `crc-fast` produce identical CRC32
values before timing the cases. The Snappy compression benchmark also asserts
that both variants produce identical compressed blocks, including the big-endian
CRC trailer.

## Environments

| Label | Host | CPU / arch | Rust |
| --- | --- | --- | --- |
| aarch64 | local | Darwin arm64, Mac17,6 | rustc 1.98.1 |
| x86_64 | azure-4074205200 | AMD EPYC 9V45, x86_64, `pclmulqdq`, `sse4_1`, `sse4_2`, `avx2`, `avx512f`, `avx512vl`, `vpclmulqdq` | rustc 1.98.0 |

## aarch64 Results

Throughput values are Criterion midpoint estimates.

| Path | Block size | crc32fast | crc-fast | Speedup |
| --- | ---: | ---: | ---: | ---: |
| CRC only | 128 B | 22.95 GiB/s | 29.00 GiB/s | 1.26x |
| CRC only | 1 KiB | 12.68 GiB/s | 66.72 GiB/s | 5.26x |
| CRC only | 16 KiB | 22.34 GiB/s | 78.37 GiB/s | 3.51x |
| CRC only | 1 MiB | 33.64 GiB/s | 100.17 GiB/s | 2.98x |
| Snappy compress + CRC | 128 B | 1.64 GiB/s | 1.68 GiB/s | 1.03x |
| Snappy compress + CRC | 1 KiB | 4.47 GiB/s | 6.18 GiB/s | 1.38x |
| Snappy compress + CRC | 16 KiB | 7.27 GiB/s | 9.54 GiB/s | 1.31x |
| Snappy compress + CRC | 1 MiB | 16.55 GiB/s | 26.11 GiB/s | 1.58x |
| Snappy decompress + CRC | 128 B | 4.68 GiB/s | 4.69 GiB/s | 1.00x |
| Snappy decompress + CRC | 1 KiB | 8.53 GiB/s | 19.51 GiB/s | 2.29x |
| Snappy decompress + CRC | 16 KiB | 12.15 GiB/s | 21.99 GiB/s | 1.81x |
| Snappy decompress + CRC | 1 MiB | 17.42 GiB/s | 25.64 GiB/s | 1.47x |

On aarch64, `crc32fast` uses the ARM CRC extension instructions such as
`__crc32d` and `__crc32b`, with multi-stream combining for large buffers to
reduce dependency-chain stalls. `crc-fast` can use PMULL/AES folding for
CRC-32/ISO-HDLC, and can select a SHA3/EOR3-assisted variant on newer aarch64
CPUs. That explains why the raw CRC win is strongest once the input is large
enough to amortize the wider folding setup, and why the win still appears in
Snappy encode/decode once block sizes grow.

## x86_64 Results

Throughput values are Criterion midpoint estimates.

| Path | Block size | crc32fast | crc-fast | Speedup |
| --- | ---: | ---: | ---: | ---: |
| CRC only | 128 B | 6.74 GiB/s | 5.92 GiB/s | 0.88x |
| CRC only | 1 KiB | 6.18 GiB/s | 26.30 GiB/s | 4.26x |
| CRC only | 16 KiB | 6.01 GiB/s | 64.49 GiB/s | 10.74x |
| CRC only | 1 MiB | 5.98 GiB/s | 66.72 GiB/s | 11.15x |
| Snappy compress + CRC | 128 B | 1.19 GiB/s | 1.38 GiB/s | 1.16x |
| Snappy compress + CRC | 1 KiB | 3.37 GiB/s | 6.04 GiB/s | 1.79x |
| Snappy compress + CRC | 16 KiB | 4.50 GiB/s | 15.32 GiB/s | 3.40x |
| Snappy compress + CRC | 1 MiB | 5.04 GiB/s | 21.76 GiB/s | 4.32x |
| Snappy decompress + CRC | 128 B | 2.60 GiB/s | 3.20 GiB/s | 1.23x |
| Snappy decompress + CRC | 1 KiB | 4.07 GiB/s | 10.20 GiB/s | 2.51x |
| Snappy decompress + CRC | 16 KiB | 4.75 GiB/s | 16.31 GiB/s | 3.44x |
| Snappy decompress + CRC | 1 MiB | 4.83 GiB/s | 17.94 GiB/s | 3.72x |

On x86/x86_64, the SSE4.2 CRC32 instruction is CRC32C/ISCSI, not
CRC-32/ISO-HDLC, so it is not the direct instruction for Avro's Snappy
checksum. The relevant accelerated family for this checksum is carry-less
multiply: PCLMULQDQ and, where available, wider VPCLMULQDQ/AVX-512 paths.
On the EPYC 9V45 test host, `crc-fast` scales much better for 1 KiB and larger
blocks. The 128 B raw CRC case is the one exception, where fixed overhead makes
`crc32fast` slightly faster, but the Snappy encode/decode paths still favor
`crc-fast` even at 128 B.

## Conclusion

The replacement is supported by both isolated checksum measurements and
Snappy-shaped encode/decode measurements. The improvement is not just a raw
microbenchmark artifact: it remains visible when the checksum is placed next to
Snappy compression/decompression, especially for 1 KiB and larger Avro blocks.

The main caveat remains MSRV. `crc-fast` 1.10.0 requires Rust 1.89, so this
replacement should be accepted only together with the workspace and CI MSRV bump
from 1.88.0 to 1.89.0.
