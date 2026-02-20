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

//! **[Apache Avro](https://avro.apache.org/)** is a data serialization system which provides rich
//! data structures and a compact, fast, binary data format. If you are not familiar with the data
//! format, please read [`documentation::primer`] first.
//!
//! There are two ways of working with Avro data in this crate:
//!
//! 1. Via the generic [`Value`](types::Value) type.
//! 2. Via types implementing [`AvroSchema`] and Serde's [`Serialize`] and [`Deserialize`].
//!
//! The first option is great for dealing with Avro data in a dynamic way. For example, when working
//! with unknown or rapidly changing schemas or when you don't want or need to map to Rust types. The
//! module documentation of [`documentation::dynamic`] explains how to work in this dynamic way.
//!
//! The second option is great when dealing with static schemas that should be decoded to and encoded
//! from Rust types. The module documentation of [`serde`] explains how to work in this static way.
//!
//! # Features
//!
//! - `derive`: enable support for deriving [`AvroSchema`]
//! - `snappy`: enable support for the Snappy codec
//! - `zstandard`: enable support for the Zstandard codec
//! - `bzip`: enable support for the Bzip2 codec
//! - `xz`: enable support for the Xz codec
//!
//! # MSRV
//!
//! The current MSRV is 1.88.0.
//!
//! The MSRV may be bumped in minor releases.
//!
// These are links because otherwise `cargo rdme` gets angry
//! [`Serialize`]: https://docs.rs/serde/latest/serde/trait.Serialize.html
//! [`Deserialize`]: https://docs.rs/serde/latest/serde/trait.Deserialize.html

mod bigdecimal;
mod bytes;
mod codec;
mod decimal;
mod decode;
mod duration;
mod encode;
mod reader;
mod writer;

#[cfg(doc)]
pub mod documentation;
pub mod error;
pub mod headers;
pub mod rabin;
pub mod schema;
pub mod schema_compatibility;
pub mod schema_equality;
pub mod serde;
pub mod types;
pub mod util;
pub mod validator;

#[expect(deprecated)]
pub use crate::{
    bigdecimal::BigDecimal,
    bytes::{
        serde_avro_bytes, serde_avro_bytes_opt, serde_avro_fixed, serde_avro_fixed_opt,
        serde_avro_slice, serde_avro_slice_opt,
    },
};
#[cfg(feature = "bzip")]
pub use codec::bzip::Bzip2Settings;
#[cfg(feature = "xz")]
pub use codec::xz::XzSettings;
#[cfg(feature = "zstandard")]
pub use codec::zstandard::ZstandardSettings;
pub use codec::{Codec, DeflateSettings};
pub use decimal::Decimal;
pub use duration::{Days, Duration, Millis, Months};
pub use error::Error;
pub use reader::{
    Reader, from_avro_datum, from_avro_datum_reader_schemata, from_avro_datum_schemata,
    read_marker,
    single_object::{GenericSingleObjectReader, SpecificSingleObjectReader},
};
pub use schema::Schema;
pub use serde::{AvroSchema, AvroSchemaComponent, from_value, to_value};
pub use uuid::Uuid;
pub use writer::{
    Clearable, GenericSingleObjectWriter, SpecificSingleObjectWriter, Writer, WriterBuilder,
    to_avro_datum, to_avro_datum_schemata, write_avro_datum_ref,
};

#[cfg(feature = "derive")]
pub use apache_avro_derive::AvroSchema;

/// A convenience type alias for `Result`s with `Error`s.
pub type AvroResult<T> = Result<T, Error>;

/// Set the maximum number of bytes that can be allocated when decoding data.
///
/// This function only changes the setting once. On subsequent calls the value will stay the same
/// as the first time it is called. It is automatically called on first allocation and defaults to
/// [`util::DEFAULT_MAX_ALLOCATION_BYTES`].
///
/// # Returns
/// The configured maximum, which might be different from what the function was called with if the
/// value was already set before.
#[deprecated(
    since = "0.21.0",
    note = "Please use apache_avro::util::max_allocation_bytes"
)]
pub fn max_allocation_bytes(num_bytes: usize) -> usize {
    util::max_allocation_bytes(num_bytes)
}

/// Set whether the serializer and deserializer should indicate to types that the format is human-readable.
///
/// This function only changes the setting once. On subsequent calls the value will stay the same
/// as the first time it is called. It is automatically called on first allocation and defaults to
/// [`util::DEFAULT_SERDE_HUMAN_READABLE`].
///
/// *NOTE*: Changing this setting can change the output of [`from_value`] and the
/// accepted input of [`to_value`].
///
/// # Returns
/// The configured human-readable value, which might be different from what the function was called
/// with if the value was already set before.
#[deprecated(
    since = "0.21.0",
    note = "Please use apache_avro::util::set_serde_human_readable"
)]
pub fn set_serde_human_readable(human_readable: bool) -> bool {
    util::set_serde_human_readable(human_readable)
}

#[cfg(test)]
mod tests {
    use crate::{
        Codec, Reader, Schema, Writer, from_avro_datum,
        types::{Record, Value},
    };
    use pretty_assertions::assert_eq;

    //TODO: move where it fits better
    #[test]
    fn test_enum_default() {
        let writer_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#;
        let reader_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let writer_schema = Schema::parse_str(writer_raw_schema).unwrap();
        let reader_schema = Schema::parse_str(reader_raw_schema).unwrap();
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null).unwrap();
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        writer.append_value(record).unwrap();
        let input = writer.into_inner().unwrap();
        let mut reader = Reader::builder(&input[..])
            .reader_schema(&reader_schema)
            .build()
            .unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(1, "spades".to_string())),
            ])
        );
        assert!(reader.next().is_none());
    }

    //TODO: move where it fits better
    #[test]
    fn test_enum_string_value() {
        let raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let schema = Schema::parse_str(raw_schema).unwrap();
        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null).unwrap();
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "clubs");
        writer.append_value(record).unwrap();
        let input = writer.into_inner().unwrap();
        let mut reader = Reader::builder(&input[..])
            .reader_schema(&schema)
            .build()
            .unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(2, "clubs".to_string())),
            ])
        );
        assert!(reader.next().is_none());
    }

    //TODO: move where it fits better
    #[test]
    fn test_enum_no_reader_schema() {
        let writer_raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let writer_schema = Schema::parse_str(writer_raw_schema).unwrap();
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null).unwrap();
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "clubs");
        writer.append_value(record).unwrap();
        let input = writer.into_inner().unwrap();
        let mut reader = Reader::new(&input[..]).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(2, "clubs".to_string())),
            ])
        );
    }

    #[test]
    fn test_illformed_length() {
        let raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#;

        let schema = Schema::parse_str(raw_schema).unwrap();

        // Would allocate 18446744073709551605 bytes
        let illformed: &[u8] = &[0x3e, 0x15, 0xff, 0x1f, 0x15, 0xff];

        let value = from_avro_datum(&schema, &mut &*illformed, None);
        assert!(value.is_err());
    }
}
