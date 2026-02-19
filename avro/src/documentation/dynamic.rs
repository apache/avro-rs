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

//! # Using Avro in Rust, the dynamic way.
//!
//! ## Creating a schema
//!
//! An Avro data cannot exist without an Avro schema. Schemas **must** be used while writing and
//! **can** be used while reading and they carry the information regarding the type of data we are
//! handling. Avro schemas are used for both schema validation and resolution of Avro data.
//!
//! Avro schemas are defined in **JSON** format and can just be parsed out of a raw string:
//!
//! ```
//! use apache_avro::Schema;
//!
//! let raw_schema = r#"
//!     {
//!         "type": "record",
//!         "name": "test",
//!         "fields": [
//!             {"name": "a", "type": "long", "default": 42},
//!             {"name": "b", "type": "string"}
//!         ]
//!     }
//! "#;
//!
//! // if the schema is not valid, this function will return an error
//! let schema = Schema::parse_str(raw_schema).unwrap();
//!
//! // schemas can be printed for debugging
//! println!("{:?}", schema);
//! ```
//!
//! Additionally, a list of definitions (which may depend on each other) can be given and all of
//! them will be parsed into the corresponding schemas.
//!
//! ```
//! use apache_avro::Schema;
//!
//! let raw_schema_1 = r#"{
//!         "name": "A",
//!         "type": "record",
//!         "fields": [
//!             {"name": "field_one", "type": "float"}
//!         ]
//!     }"#;
//!
//! // This definition depends on the definition of A above
//! let raw_schema_2 = r#"{
//!         "name": "B",
//!         "type": "record",
//!         "fields": [
//!             {"name": "field_one", "type": "A"}
//!         ]
//!     }"#;
//!
//! // if the schemas are not valid, this function will return an error
//! let schemas = Schema::parse_list(&[raw_schema_1, raw_schema_2]).unwrap();
//!
//! // schemas can be printed for debugging
//! println!("{:?}", schemas);
//! ```
//!
//! ## Writing data
//!
//! Once we have defined a schema, we are ready to serialize data in Avro, validating them against
//! the provided schema in the process. As mentioned before, there are two ways of handling Avro
//! data in Rust.
//!
//! Given that the schema we defined above is that of an Avro *Record*, we are going to use the
//! associated type provided by the library to specify the data we want to serialize:
//!
//! ```
//! # use apache_avro::Schema;
//! use apache_avro::types::Record;
//! use apache_avro::Writer;
//! #
//! # let raw_schema = r#"
//! #     {
//! #         "type": "record",
//! #         "name": "test",
//! #         "fields": [
//! #             {"name": "a", "type": "long", "default": 42},
//! #             {"name": "b", "type": "string"}
//! #         ]
//! #     }
//! # "#;
//! # let schema = Schema::parse_str(raw_schema).unwrap();
//! // a writer needs a schema and something to write to
//! let mut writer = Writer::new(&schema, Vec::new()).unwrap();
//!
//! // the Record type models our Record schema
//! let mut record = Record::new(writer.schema()).unwrap();
//! record.put("a", 27i64);
//! record.put("b", "foo");
//!
//! // schema validation happens here
//! writer.append_value(record).unwrap();
//!
//! // this is how to get back the resulting Avro bytecode
//! // this performs a flush operation to make sure data has been written, so it can fail
//! // you can also call `writer.flush()` yourself without consuming the writer
//! let encoded = writer.into_inner().unwrap();
//! ```
//!
//! The vast majority of the times, schemas tend to define a record as a top-level container
//! encapsulating all the values to convert as fields and providing documentation for them, but in
//! case we want to directly define an Avro value, the library offers that capability via the
//! `Value` interface.
//!
//! ```
//! use apache_avro::types::Value;
//!
//! let mut value = Value::String("foo".to_string());
//! ```
//!
//! ## Reading data
//!
//! As far as reading Avro encoded data goes, we can just use the schema encoded with the data to
//! read them. The library will do it automatically for us, as it already does for the compression
//! codec:
//!
//! ```
//! use apache_avro::Reader;
//! # use apache_avro::Schema;
//! # use apache_avro::types::Record;
//! # use apache_avro::Writer;
//! #
//! # let raw_schema = r#"
//! #     {
//! #         "type": "record",
//! #         "name": "test",
//! #         "fields": [
//! #             {"name": "a", "type": "long", "default": 42},
//! #             {"name": "b", "type": "string"}
//! #         ]
//! #     }
//! # "#;
//! # let schema = Schema::parse_str(raw_schema).unwrap();
//! # let mut writer = Writer::new(&schema, Vec::new()).unwrap();
//! # let mut record = Record::new(writer.schema()).unwrap();
//! # record.put("a", 27i64);
//! # record.put("b", "foo");
//! # writer.append_value(record).unwrap();
//! # let input = writer.into_inner().unwrap();
//! // reader creation can fail in case the input to read from is not Avro-compatible or malformed
//! let reader = Reader::new(&input[..]).unwrap();
//!
//! // value is a Result of an Avro Value in case the read operation fails
//! for value in reader {
//!     println!("{:?}", value.unwrap());
//! }
//! ```
//!
//! In case, instead, we want to specify a different (but compatible) reader schema from the schema
//! the data has been written with, we can just do as the following:
//! ```
//! use apache_avro::Schema;
//! use apache_avro::Reader;
//! # use apache_avro::types::Record;
//! # use apache_avro::Writer;
//! #
//! # let writer_raw_schema = r#"
//! #     {
//! #         "type": "record",
//! #         "name": "test",
//! #         "fields": [
//! #             {"name": "a", "type": "long", "default": 42},
//! #             {"name": "b", "type": "string"}
//! #         ]
//! #     }
//! # "#;
//! # let writer_schema = Schema::parse_str(writer_raw_schema).unwrap();
//! # let mut writer = Writer::new(&writer_schema, Vec::new()).unwrap();
//! # let mut record = Record::new(writer.schema()).unwrap();
//! # record.put("a", 27i64);
//! # record.put("b", "foo");
//! # writer.append_value(record).unwrap();
//! # let input = writer.into_inner().unwrap();
//!
//! let reader_raw_schema = r#"
//!     {
//!         "type": "record",
//!         "name": "test",
//!         "fields": [
//!             {"name": "a", "type": "long", "default": 42},
//!             {"name": "b", "type": "string"},
//!             {"name": "c", "type": "long", "default": 43}
//!         ]
//!     }
//! "#;
//!
//! let reader_schema = Schema::parse_str(reader_raw_schema).unwrap();
//!
//! // reader creation can fail in case the input to read from is not Avro-compatible or malformed
//! let reader = Reader::builder(&input[..]).schema(&reader_schema).build().unwrap();
//!
//! // value is a Result of an Avro Value in case the read operation fails
//! for value in reader {
//!     println!("{:?}", value.unwrap());
//! }
//! ```
//!
//! The library will also automatically perform schema resolution while reading the data.
//!
//! For more information about schema compatibility and resolution, please refer to the
//! [Avro Specification](https://avro.apache.org/docs/++version++/specification/#schema-declaration).
//!
//! ## Putting everything together
//!
//! The following is an example of how to combine everything showed so far and it is meant to be a
//! quick reference of the [`Value`](crate::types::Value) interface:
//!
//! ```
//! use apache_avro::{Codec, DeflateSettings, Reader, Schema, Writer, from_value, types::Record, Error};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Deserialize, Serialize)]
//! struct Test {
//!     a: i64,
//!     b: String,
//! }
//!
//! fn main() -> Result<(), Error> {
//!     let raw_schema = r#"
//!         {
//!             "type": "record",
//!             "name": "test",
//!             "fields": [
//!                 {"name": "a", "type": "long", "default": 42},
//!                 {"name": "b", "type": "string"}
//!             ]
//!         }
//!     "#;
//!
//!     let schema = Schema::parse_str(raw_schema)?;
//!
//!     println!("{:?}", schema);
//!
//!     let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate(DeflateSettings::default())).unwrap();
//!
//!     let mut record = Record::new(writer.schema()).unwrap();
//!     record.put("a", 27i64);
//!     record.put("b", "foo");
//!
//!     writer.append_value(record)?;
//!
//!     let test = Test {
//!         a: 27,
//!         b: "foo".to_owned(),
//!     };
//!
//!     writer.append_ser(test)?;
//!
//!     let input = writer.into_inner()?;
//!     let reader = Reader::builder(&input[..]).schema(&schema).build()?;
//!
//!     for record in reader {
//!         println!("{:?}", from_value::<Test>(&record?));
//!     }
//!     Ok(())
//! }
//! ```
//!
