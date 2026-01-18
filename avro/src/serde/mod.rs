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

//! Everything needed to use this crate with Serde.
//!
//! # Using `apache-avro` for `serde`
//!
//! Avro is a schema-based format, this means it requires a few extra steps to use compared to
//! a data format like JSON.
//!
//! ## Schemas
//! It's strongly recommended to derive the schemas for your types using the [`AvroSchema`] derive macro.
//! The macro uses the Serde attributes to generate a matching schema and checks that no attributes are
//! used that are incompatible with the Serde implementation in this crate. See [the trait documentation] for
//! details on how to change the generated schema.
//!
//! Alternatively, you can write your own schema. If you go down this path, it is recommended you start with
//! the schema derived by [`AvroSchema`] and then modify it to fit your needs.
//!
//! ### Using existing schemas
//! If you have schemas that are already being used in other parts of your software stack, generating types
//! from the schema can be very useful. There is a **third-party** crate [`rsgen-avro`] that implements this.
//!
//! ## Reading and writing data
//!
//! ```
//! # use std::io::Cursor;
//! # use serde::{Serialize, Deserialize};
//! # use apache_avro::{AvroSchema, Error, Reader, Writer, serde::{from_value, to_value}};
//!
//! #[derive(AvroSchema, Serialize, Deserialize, PartialEq, Debug)]
//! struct Foo {
//!     a: i64,
//!     b: String,
//! }
//!
//! let schema = Foo::get_schema();
//! // A writer needs the schema of the type that is going to be written
//! let mut writer = Writer::new(&schema, Vec::new())?;
//!
//! let foo = Foo {
//!     a: 42,
//!     b: "Hello".to_string(),
//! };
//!
//! // There are two ways to serialize data.
//! // 1: Serialize directly to the writer:
//! writer.append_ser(&foo)?;
//! // 2: First serialize to an Avro `Value` then write that:
//! let foo_value = to_value(&foo)?;
//! writer.append(foo_value)?;
//!
//! // Always flush or consume the writer
//! let data = writer.into_inner()?;
//!
//! // The reader does not need a schema as it's included in the data
//! let reader = Reader::new(Cursor::new(data))?;
//! // The reader is an iterator
//! for result in reader {
//!     let value = result?;
//!     let new_foo: Foo = from_value(&value)?;
//!     assert_eq!(new_foo, foo);
//! }
//! # Ok::<(), Error>(())
//! ```
//!
//! [`rsgen-avro`]: https://docs.rs/rsgen-avro/latest/rsgen_avro/
//! [the trait documentation]: AvroSchema

mod derive;
mod util;
mod with;

pub mod de;
pub mod ser;
pub mod ser_schema;

#[doc(inline)]
pub use derive::{AvroSchema, AvroSchemaComponent};
#[doc(inline)]
pub use with::{bytes, bytes_opt, fixed, fixed_opt, slice, slice_opt};
