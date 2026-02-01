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

//! # Using Avro in Rust, the Serde way.
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
//! #### Performance pitfall
//! One performance pitfall with Serde is (de)serializing bytes. The implementation of [`Serialize`][`serde::Serialize`]
//! and [`Deserialize`][`serde::Deserialize`] for types as `Vec<u8>`, `&[u8]` and `Cow<[u8]>` will
//! all use the array of integers representation. This can normally be fixed using the [`serde_bytes`]
//! crate, however this crate also needs some extra information. Therefore, you need to use the
//! [`bytes`], [`bytes_opt`], [`fixed`], [`fixed_opt`], [`mod@slice`], and [`slice_opt`] modules of
//! this crate instead.
//!
//! #### Using existing schemas
//! If you have schemas that are already being used in other parts of your software stack, generating types
//! from the schema can be very useful. There is a **third-party** crate [`rsgen-avro`] that implements this.
//!
//! ## Serializing data
//! Writing data is very simple. Use [`T::get_schema()`](AvroSchema::get_schema()) to get the schema
//! for the type you want to serialize. It is recommended to keep this schema around as long as possible
//! as generating the schema is quite expensive. Then create a [`Writer`](crate::Writer) with your schema
//! and use the [`append_ser()`](crate::Writer::append_ser()) function to serialize your data.
//!
//! ## Deserializing data
//! Reading data is both simpler and more complex than writing. On the one hand, you don't need to
//! generate a schema, as the Avro file has it embedded. But you can't directly deserialize from a
//! [`Reader`](crate::Reader). Instead, you have to iterate over the [`Value`](crate::types::Value)s
//! in the reader and deserialize from those via [`from_value`].
//!
//! ## Putting it all together
//!
//! The following is an example of how to combine everything showed so far and it is meant to be a
//! quick reference of the Serde interface:
//!
//! ```
//! # use std::io::Cursor;
//! # use serde::{Serialize, Deserialize};
//! # use apache_avro::{AvroSchema, Error, Reader, Writer, serde::{from_value, to_value}};
//! #[derive(AvroSchema, Serialize, Deserialize, PartialEq, Debug)]
//! struct Foo {
//!     a: i64,
//!     b: String,
//!     // Otherwise it will be serialized as an array of integers
//!     #[avro(with)]
//!     #[serde(with = "apache_avro::serde::bytes")]
//!     c: Vec<u8>,
//! }
//!
//! // Creating this schema is expensive, reuse it as much as possible
//! let schema = Foo::get_schema();
//! // A writer needs the schema of the type that is going to be written
//! let mut writer = Writer::new(&schema, Vec::new())?;
//!
//! let foo = Foo {
//!     a: 42,
//!     b: "Hello".to_string(),
//!     c: b"Data".to_vec()
//! };
//!
//! // Serialize as many items as you want.
//! writer.append_ser(&foo)?;
//! writer.append_ser(&foo)?;
//! writer.append_ser(&foo)?;
//!
//! // Always flush
//! writer.flush()?;
//! // Or consume the writer
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

mod de;
mod derive;
mod ser;
pub(crate) mod ser_schema;
mod util;
mod with;

pub use de::from_value;
pub use derive::{AvroSchema, AvroSchemaComponent};
pub use ser::to_value;
pub use with::{bytes, bytes_opt, fixed, fixed_opt, slice, slice_opt};

#[doc(hidden)]
pub use derive::get_record_fields_in_ctxt;
