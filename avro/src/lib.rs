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

// Enable using `fake_variadic` on docs.rs
#![cfg_attr(docsrs, feature(rustdoc_internals))]
#![cfg_attr(docsrs, allow(internal_features))]

mod bigdecimal;
mod codec;
mod decimal;
mod decode;
mod duration;
mod encode;

#[cfg(doc)]
pub mod documentation;
pub mod error;
pub mod headers;
pub mod rabin;
pub mod reader;
pub mod schema;
pub mod schema_compatibility;
pub mod schema_equality;
pub mod serde;
pub mod types;
pub mod util;
pub mod validator;
pub mod writer;

// Re-export types from private module
pub use crate::{
    bigdecimal::BigDecimal,
    codec::{Codec, DeflateSettings},
    decimal::Decimal,
    duration::{Days, Duration, Millis, Months},
};

// Re-export types from private modules that need to be behind feature gates
#[cfg(feature = "bzip")]
pub use crate::codec::bzip::Bzip2Settings;
#[cfg(feature = "xz")]
pub use crate::codec::xz::XzSettings;
#[cfg(feature = "zstandard")]
pub use crate::codec::zstandard::ZstandardSettings;

// Re-exports that are available at their respective paths but are made available in the root for
// convenience.
#[doc(inline)]
pub use crate::{
    error::Error,
    reader::{
        Reader,
        single_object::{GenericSingleObjectReader, SpecificSingleObjectReader},
    },
    schema::Schema,
    serde::{AvroSchema, AvroSchemaComponent, from_value, to_value},
    writer::{
        Clearable, Writer, WriterBuilder,
        single_object::{GenericSingleObjectWriter, SpecificSingleObjectWriter},
    },
};

// Re-exports from other crates
pub use uuid::Uuid;

#[cfg(feature = "derive")]
pub use apache_avro_derive::AvroSchema;

/// A convenience type alias for `Result`s with `Error`s.
pub type AvroResult<T> = Result<T, Error>;

#[cfg(test)]
mod tests {
    use crate::{
        Codec, Reader, Schema, Writer,
        reader::datum::GenericDatumReader,
        types::{Record, Value},
    };
    use apache_avro_test_helper::TestResult;
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
    fn test_illformed_length() -> TestResult {
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

        let value = GenericDatumReader::builder(&schema)
            .build()?
            .read_value(&mut &*illformed);
        assert!(value.is_err());

        Ok(())
    }
}
