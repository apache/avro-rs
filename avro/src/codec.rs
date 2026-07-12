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

//! Logic for all supported compression codecs in Avro.
use crate::{AvroResult, Error, error::Details, types::Value};
use strum::{EnumIter, EnumString, IntoStaticStr};

/// Settings for the `Deflate` codec.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct DeflateSettings {
    compression_level: miniz_oxide::deflate::CompressionLevel,
}

impl DeflateSettings {
    pub fn new(compression_level: miniz_oxide::deflate::CompressionLevel) -> Self {
        DeflateSettings { compression_level }
    }

    /// Get the compression level as a `u8`, note that this means the [`miniz_oxide::deflate::CompressionLevel::DefaultCompression`] variant
    /// will appear as `255`, this is normalized by the [`miniz_oxide`] crate later.
    pub fn compression_level(&self) -> u8 {
        self.compression_level as u8
    }
}

impl Default for DeflateSettings {
    /// Default compression level is `miniz_oxide::deflate::CompressionLevel::DefaultCompression`.
    fn default() -> Self {
        Self::new(miniz_oxide::deflate::CompressionLevel::DefaultCompression)
    }
}

/// The compression codec used to compress blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumIter, EnumString, IntoStaticStr)]
#[strum(serialize_all = "kebab_case")]
pub enum Codec {
    /// The `Null` codec simply passes through data uncompressed.
    Null,
    /// The `Deflate` codec writes the data block using the deflate algorithm
    /// as specified in RFC 1951, and typically implemented using the zlib library.
    /// Note that this format (unlike the "zlib format" in RFC 1950) does not have a checksum.
    Deflate(DeflateSettings),
    #[cfg(feature = "snappy")]
    /// The `Snappy` codec uses Google's [Snappy](http://google.github.io/snappy/)
    /// compression library. Each compressed block is followed by the 4-byte, big-endian
    /// CRC32 checksum of the uncompressed data in the block.
    Snappy,
    #[cfg(feature = "zstandard")]
    /// The `Zstandard` codec uses Facebook's [Zstandard](https://facebook.github.io/zstd/)
    Zstandard(zstandard::ZstandardSettings),
    #[cfg(feature = "bzip")]
    /// The `BZip2` codec uses [BZip2](https://sourceware.org/bzip2/)
    /// compression library.
    Bzip2(bzip::Bzip2Settings),
    #[cfg(feature = "xz")]
    /// The `Xz` codec uses [Xz utils](https://tukaani.org/xz/)
    /// compression library.
    Xz(xz::XzSettings),
}

impl From<Codec> for Value {
    fn from(value: Codec) -> Self {
        Self::Bytes(<&str>::from(value).as_bytes().to_vec())
    }
}

impl Codec {
    /// Compress a stream of bytes in-place.
    pub fn compress(self, stream: &mut Vec<u8>) -> AvroResult<()> {
        match self {
            Codec::Null => (),
            Codec::Deflate(settings) => {
                let compressed =
                    miniz_oxide::deflate::compress_to_vec(stream, settings.compression_level());
                *stream = compressed;
            }
            #[cfg(feature = "snappy")]
            Codec::Snappy => {
                let mut encoded: Vec<u8> = vec![0; snap::raw::max_compress_len(stream.len())];
                let compressed_size = snap::raw::Encoder::new()
                    .compress(&stream[..], &mut encoded[..])
                    .map_err(Details::SnappyCompress)?;

                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&stream[..]);
                let checksum = hasher.finalize();
                let checksum_as_bytes = checksum.to_be_bytes();
                let checksum_len = checksum_as_bytes.len();
                encoded.truncate(compressed_size + checksum_len);
                encoded[compressed_size..].copy_from_slice(&checksum_as_bytes);

                *stream = encoded;
            }
            #[cfg(feature = "zstandard")]
            Codec::Zstandard(settings) => {
                use std::io::Write;
                let mut encoder = zstd::Encoder::new(Vec::new(), settings.compression_level as i32)
                    .map_err(Details::ZstdCompress)?;
                encoder.write_all(stream).map_err(Details::ZstdCompress)?;
                *stream = encoder.finish().map_err(Details::ZstdCompress)?;
            }
            #[cfg(feature = "bzip")]
            Codec::Bzip2(settings) => {
                use bzip2::read::BzEncoder;
                use std::io::Read;

                let mut encoder = BzEncoder::new(&stream[..], settings.compression());
                let mut buffer = Vec::new();
                encoder
                    .read_to_end(&mut buffer)
                    .unwrap_or_else(|_| unreachable!("No I/O errors possible with Vec<u8>"));
                *stream = buffer;
            }
            #[cfg(feature = "xz")]
            Codec::Xz(settings) => {
                use liblzma::read::XzEncoder;
                use std::io::Read;

                let mut encoder = XzEncoder::new(&stream[..], settings.compression_level as u32);
                let mut buffer = Vec::new();
                encoder
                    .read_to_end(&mut buffer)
                    .unwrap_or_else(|_| unreachable!("No I/O errors possible with Vec<u8>"));
                *stream = buffer;
            }
        };

        Ok(())
    }

    /// Decompress a stream of bytes in-place.
    pub fn decompress(self, stream: &mut Vec<u8>) -> AvroResult<()> {
        // Cap the decompressed output at the configured allocation budget so a
        // small compressed block cannot inflate to an enormous buffer (a
        // "decompression bomb") and exhaust memory.
        let max_bytes =
            crate::util::max_allocation_bytes(crate::util::DEFAULT_MAX_ALLOCATION_BYTES);
        *stream = match self {
            Codec::Null => return Ok(()),
            Codec::Deflate(_settings) => miniz_oxide::inflate::decompress_to_vec_with_limit(stream, max_bytes).map_err(|e| {
                use miniz_oxide::inflate::TINFLStatus::{FailedCannotMakeProgress, BadParam, Adler32Mismatch, Failed, Done, NeedsMoreInput, HasMoreOutput};
                // The output would grow past the allocation budget: reject it as
                // a decompression bomb rather than allocating without bound.
                if let HasMoreOutput = e.status {
                    return Error::new(Details::MemoryAllocation {
                        desired: max_bytes.saturating_add(1),
                        maximum: max_bytes,
                    });
                }
                let err = {
                    use std::io::{Error as IoError, ErrorKind};
                    match e.status {
                        FailedCannotMakeProgress => IoError::from(ErrorKind::UnexpectedEof),
                        BadParam => IoError::other("miniz_oxide reported an invalid parameter while decompressing"),
                        Adler32Mismatch => IoError::from(ErrorKind::InvalidData),
                        Failed => IoError::from(ErrorKind::InvalidData),
                        Done => IoError::other("Unexpected error: miniz_oxide reported an error with a success status. Please report this to avro-rs developers."),
                        NeedsMoreInput => IoError::from(ErrorKind::UnexpectedEof),
                        HasMoreOutput => unreachable!("handled above"),
                        other => IoError::other(format!("Unexpected error: {other:?}"))
                    }
                };
                Error::new(Details::DeflateDecompress(err))
            })?,
            #[cfg(feature = "snappy")]
            Codec::Snappy => {
                let decompressed_size = snap::raw::decompress_len(&stream[..stream.len() - 4])
                    .map_err(Details::GetSnappyDecompressLen)?;
                // The decompressed size is taken from the (untrusted) block
                // header, so bound it before allocating for it.
                let decompressed_size = crate::util::safe_len(decompressed_size)?;
                let mut decoded = vec![0; decompressed_size];
                snap::raw::Decoder::new()
                    .decompress(&stream[..stream.len() - 4], &mut decoded[..])
                    .map_err(Details::SnappyDecompress)?;

                let mut last_four: [u8; 4] = [0; 4];
                last_four.copy_from_slice(&stream[(stream.len() - 4)..]);
                let expected: u32 = u32::from_be_bytes(last_four);

                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&decoded);
                let actual = hasher.finalize();

                if expected != actual {
                    return Err(Details::SnappyCrc32{expected, actual}.into());
                }
                decoded
            }
            #[cfg(feature = "zstandard")]
            Codec::Zstandard(_settings) => {
                use std::io::{BufReader, Read};
                use zstd::zstd_safe;

                let mut decoded = Vec::new();
                let buffer_size = zstd_safe::DCtx::in_size();
                let buffer = BufReader::with_capacity(buffer_size, &stream[..]);
                let decoder = zstd::Decoder::new(buffer).map_err(Details::ZstdDecompress)?;
                // Read one byte past the budget so an output that exactly fills
                // it is allowed, while a larger (bomb) output is detected.
                decoder
                    .take((max_bytes as u64).saturating_add(1))
                    .read_to_end(&mut decoded)
                    .map_err(Details::ZstdDecompress)?;
                if decoded.len() > max_bytes {
                    return Err(Details::MemoryAllocation { desired: decoded.len(), maximum: max_bytes }.into());
                }
                decoded
            }
            #[cfg(feature = "bzip")]
            Codec::Bzip2(_) => {
                use bzip2::read::BzDecoder;
                use std::io::Read;

                let mut decoded = Vec::new();
                BzDecoder::new(&stream[..])
                    .take((max_bytes as u64).saturating_add(1))
                    .read_to_end(&mut decoded)
                    .map_err(Details::Bzip2Decompress)?;
                if decoded.len() > max_bytes {
                    return Err(Details::MemoryAllocation { desired: decoded.len(), maximum: max_bytes }.into());
                }
                decoded
            }
            #[cfg(feature = "xz")]
            Codec::Xz(_) => {
                use liblzma::read::XzDecoder;
                use std::io::Read;

                let mut decoded: Vec<u8> = Vec::new();
                XzDecoder::new(&stream[..])
                    .take((max_bytes as u64).saturating_add(1))
                    .read_to_end(&mut decoded)
                    .map_err(Details::XzDecompress)?;
                if decoded.len() > max_bytes {
                    return Err(Details::MemoryAllocation { desired: decoded.len(), maximum: max_bytes }.into());
                }
                decoded
            }
        };
        Ok(())
    }
}

#[cfg(feature = "bzip")]
pub mod bzip {
    use bzip2::Compression;

    #[derive(Clone, Copy, Eq, PartialEq, Debug)]
    pub struct Bzip2Settings {
        pub compression_level: u8,
    }

    impl Bzip2Settings {
        pub fn new(compression_level: u8) -> Self {
            Self { compression_level }
        }

        pub(crate) fn compression(&self) -> Compression {
            Compression::new(self.compression_level as u32)
        }
    }

    impl Default for Bzip2Settings {
        fn default() -> Self {
            Bzip2Settings::new(Compression::best().level() as u8)
        }
    }
}

#[cfg(feature = "zstandard")]
pub mod zstandard {
    #[derive(Clone, Copy, Eq, PartialEq, Debug)]
    pub struct ZstandardSettings {
        pub compression_level: u8,
    }

    impl ZstandardSettings {
        pub fn new(compression_level: u8) -> Self {
            Self { compression_level }
        }
    }

    impl Default for ZstandardSettings {
        fn default() -> Self {
            Self::new(0)
        }
    }
}

#[cfg(feature = "xz")]
pub mod xz {
    #[derive(Clone, Copy, Eq, PartialEq, Debug)]
    pub struct XzSettings {
        pub compression_level: u8,
    }

    impl XzSettings {
        pub fn new(compression_level: u8) -> Self {
            Self { compression_level }
        }
    }

    impl Default for XzSettings {
        fn default() -> Self {
            XzSettings::new(9)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro_test_helper::TestResult;
    use miniz_oxide::deflate::CompressionLevel;
    use pretty_assertions::{assert_eq, assert_ne};

    const INPUT: &[u8] = b"theanswertolifetheuniverseandeverythingis42theanswertolifetheuniverseandeverythingis4theanswertolifetheuniverseandeverythingis2";

    #[test]
    fn null_compress_and_decompress() -> TestResult {
        let codec = Codec::Null;
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream)?;
        assert_eq!(INPUT, stream.as_slice());
        codec.decompress(&mut stream)?;
        assert_eq!(INPUT, stream.as_slice());
        Ok(())
    }

    #[test]
    fn deflate_compress_and_decompress() -> TestResult {
        compress_and_decompress(Codec::Deflate(DeflateSettings::new(
            CompressionLevel::BestCompression,
        )))
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn snappy_compress_and_decompress() -> TestResult {
        compress_and_decompress(Codec::Snappy)
    }

    #[cfg(feature = "zstandard")]
    #[test]
    fn zstd_compress_and_decompress() -> TestResult {
        compress_and_decompress(Codec::Zstandard(zstandard::ZstandardSettings::default()))
    }

    #[cfg(feature = "bzip")]
    #[test]
    fn bzip_compress_and_decompress() -> TestResult {
        compress_and_decompress(Codec::Bzip2(bzip::Bzip2Settings::default()))
    }

    #[cfg(feature = "xz")]
    #[test]
    fn xz_compress_and_decompress() -> TestResult {
        compress_and_decompress(Codec::Xz(xz::XzSettings::default()))
    }

    fn compress_and_decompress(codec: Codec) -> TestResult {
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream)?;
        assert_ne!(INPUT, stream.as_slice());
        assert!(INPUT.len() > stream.len());
        codec.decompress(&mut stream)?;
        assert_eq!(INPUT, stream.as_slice());
        Ok(())
    }

    #[test]
    fn codec_to_str() {
        assert_eq!(<&str>::from(Codec::Null), "null");
        assert_eq!(
            <&str>::from(Codec::Deflate(DeflateSettings::default())),
            "deflate"
        );

        #[cfg(feature = "snappy")]
        assert_eq!(<&str>::from(Codec::Snappy), "snappy");

        #[cfg(feature = "zstandard")]
        assert_eq!(
            <&str>::from(Codec::Zstandard(zstandard::ZstandardSettings::default())),
            "zstandard"
        );

        #[cfg(feature = "bzip")]
        assert_eq!(
            <&str>::from(Codec::Bzip2(bzip::Bzip2Settings::default())),
            "bzip2"
        );

        #[cfg(feature = "xz")]
        assert_eq!(<&str>::from(Codec::Xz(xz::XzSettings::default())), "xz");
    }

    #[test]
    fn codec_from_str() {
        use std::str::FromStr;

        assert_eq!(Codec::from_str("null").unwrap(), Codec::Null);
        assert_eq!(
            Codec::from_str("deflate").unwrap(),
            Codec::Deflate(DeflateSettings::default())
        );

        #[cfg(feature = "snappy")]
        assert_eq!(Codec::from_str("snappy").unwrap(), Codec::Snappy);

        #[cfg(feature = "zstandard")]
        assert_eq!(
            Codec::from_str("zstandard").unwrap(),
            Codec::Zstandard(zstandard::ZstandardSettings::default())
        );

        #[cfg(feature = "bzip")]
        assert_eq!(
            Codec::from_str("bzip2").unwrap(),
            Codec::Bzip2(bzip::Bzip2Settings::default())
        );

        #[cfg(feature = "xz")]
        assert_eq!(
            Codec::from_str("xz").unwrap(),
            Codec::Xz(xz::XzSettings::default())
        );

        assert!(Codec::from_str("not a codec").is_err());
    }
}
