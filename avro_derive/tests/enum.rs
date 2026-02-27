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

use apache_avro::{AvroSchema, Error, Reader, Schema, Writer};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Takes in a type that implements the right combination of traits and runs it through a Serde
/// round-trip and asserts the result is the same.
#[track_caller]
fn serde_assert<T>(obj: T)
where
    T: std::fmt::Debug + Serialize + DeserializeOwned + AvroSchema + Clone + PartialEq,
{
    assert_eq!(obj, serde(obj.clone()).unwrap());
}

// /// Takes in a type that implements the right combination of traits and runs it through a Serde
// /// round-trip and asserts that the error matches the expected string.
// fn serde_assert_err<T>(obj: T, expected: &str)
// where
//     T: std::fmt::Debug + Serialize + DeserializeOwned + AvroSchema + Clone + PartialEq,
// {
//     let error = serde(obj).unwrap_err().to_string();
//     assert!(
//         error.contains(expected),
//         "Error `{error}` does not contain `{expected}`"
//     );
// }

fn serde<T>(obj: T) -> Result<T, Error>
where
    T: Serialize + DeserializeOwned + AvroSchema,
{
    de(ser(obj)?)
}

fn ser<T>(obj: T) -> Result<Vec<u8>, Error>
where
    T: Serialize + AvroSchema,
{
    let schema = T::get_schema();
    let mut writer = Writer::new(&schema, Vec::new())?;
    writer.append_ser(obj)?;
    writer.into_inner()
}

fn de<T>(encoded: Vec<u8>) -> Result<T, Error>
where
    T: DeserializeOwned + AvroSchema,
{
    assert!(!encoded.is_empty());
    let schema = T::get_schema();
    let mut reader = Reader::builder(&encoded[..])
        .reader_schema(&schema)
        .build()?;
    Ok(reader.next_deser::<T>()?.unwrap())
}

#[test]
fn avro_rs_xxx_enum_repr_default() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    enum Foo {
        A,
        B,
        C,
    }

    assert!(matches!(Foo::get_schema(), Schema::Enum(_)));
    serde_assert(Foo::A);
}

#[test]
fn avro_rs_xxx_enum_repr_enum() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "enum")]
    enum Foo {
        A,
        B,
        C,
    }

    assert!(matches!(Foo::get_schema(), Schema::Enum(_)));
    serde_assert(Foo::A);
}

#[test]
fn avro_rs_xxx_enum_repr_discriminator_value_plain() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "record_tag_content")]
    #[serde(tag = "type", content = "value")]
    enum Foo {
        A,
        B,
        C,
    }

    assert!(matches!(Foo::get_schema(), Schema::Record(_)));
    serde_assert(Foo::A);
}

#[test]
fn avro_rs_xxx_enum_repr_discriminator_value_tuple() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "record_tag_content")]
    #[serde(tag = "type", content = "value")]
    enum Foo {
        A(),
        B(String),
        C(String, bool),
    }

    assert!(matches!(Foo::get_schema(), Schema::Record(_)));
    serde_assert(Foo::A());
    serde_assert(Foo::B("Something".to_string()));
    serde_assert(Foo::C("Something".to_string(), true));
}

#[test]
fn avro_rs_xxx_enum_repr_bare_union_plain() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "bare_union")]
    #[serde(untagged)]
    enum Foo {
        A,
    }

    assert!(matches!(Foo::get_schema(), Schema::Union(_)));
    serde_assert(Foo::A);
}

#[test]
fn avro_rs_xxx_enum_repr_bare_union_tuple() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "bare_union")]
    #[serde(untagged)]
    enum Foo {
        B(String),
        C(String, bool),
    }

    assert!(matches!(Foo::get_schema(), Schema::Union(_)));
    serde_assert(Foo::B("Something".to_string()));
    serde_assert(Foo::C("Something".to_string(), true));
}
