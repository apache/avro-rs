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

//! # Mapping the Avro data model to the Serde data model
//!
//! When manually mapping an Avro schema to Rust types it is important to understand
//! how the different data models are mapped. When mapping from Rust types to an Avro schema,
//! see [the documentation for the reverse](super::serde_data_model_to_avro).
//!
//! Only the the mapping as defined here is supported. Any other behavior might change in a minor version.
//!
//! ## Primitive Types
//! - `null`: `()`
//! - `boolean`: [`bool`]
//! - `int`: [`i32`] (or [`i16`], [`i8`], [`u16`], [`u8`])
//! - `long`: [`i64`] (or [`u32`])
//! - `float`: [`f32`]
//! - `double`: [`f64`]
//! - `bytes`: [`Vec<u8>`](std::vec::Vec) (or any type that uses [`Serialize::serialize_bytes`](serde::Serialize), [`Deserialize::deserialize_bytes`](serde::Deserialize), [`Deserialize::deserialize_byte_buf`](serde::Deserialize))
//!     - It is required to use [`apache_avro::serde::bytes`] as otherwise Serde will (de)serialize a `Vec` as an array of integers instead.
//! - `string`: [`String`] (or any type that uses [`Serialize::serialize_str`](serde::Serialize), [`Deserialize::deserialize_str`](serde::Deserialize), [`Deserialize::deserialize_string`](serde::Deserialize))
//!
//! ## Complex Types
//! - `records`: A struct with the same name and fields or a tuple with the same fields.
//!     - Extra fields can be added to the struct if they are marked with `#[serde(skip)]`
//! - `enums`: A enum with the same name and unit variants for every symbol
//!     - The index of the symbol most match the index of the variant
//! - `arrays`: [`Vec`] (or any type that uses [`Serialize::serialize_seq`](serde::Serialize), [`Deserialize::deserialize_seq`](serde::Deserialize))
//!     - `[T; N]` is (de)serialized as a tuple by Serde, to (de)serialize them as an `array` use [`apache_avro::serde::array`]
//! - `maps`: [`HashMap<String, _>`](std::collections::HashMap) (or any type that uses [`Serialize::serialize_map`](serde::Serialize), [`Deserialize::deserialize_map`](serde::Deserialize))
//! - `unions`: A enum with a variant for each variant
//!     - The index of the union variant must match the enum variant
//!     - A `null` can be a unit variant or a newtype variant with a unit type
//!     - All other variants must be newtype variants, struct variants, or tuple variants.
//! - `fixed`: [`Vec<u8>`](std::vec::Vec) (or any type that uses [`Serialize::serialize_bytes`](serde::Serialize), [`Deserialize::deserialize_bytes`](serde::Deserialize), [`Deserialize::deserialize_byte_buf`](serde::Deserialize))
//!     - It is required to use [`apache_avro::serde::bytes`] as otherwise Serde will (de)serialize a `Vec` as an array of integers instead.
//!
//! [`apache_avro::serde::array`]: crate::serde::array
//! [`apache_avro::serde::bytes`]: crate::serde::bytes
//! [`apache_avro::serde::fixed`]: crate::serde::fixed
