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
use pretty_assertions::assert_eq;
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
        #[serde(rename = "D")]
        C,
    }

    let schema = Schema::parse_str(
        r#"{
        "type": "enum",
        "name": "Foo",
        "symbols": ["A", "B", "D"]
    }"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A);
    serde_assert(Foo::B);
    serde_assert(Foo::C);
}

#[test]
fn avro_rs_xxx_enum_repr_enum() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "enum")]
    enum Foo {
        A,
        B,
        #[serde(rename = "D")]
        C,
    }

    let schema = Schema::parse_str(
        r#"{
        "type": "enum",
        "name": "Foo",
        "symbols": ["A", "B", "D"]
    }"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A);
    serde_assert(Foo::B);
    serde_assert(Foo::C);
}

#[test]
fn avro_rs_xxx_enum_repr_record_tag_content_plain() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "record_tag_content")]
    #[serde(tag = "type", content = "value")]
    enum Foo {
        A,
        B,
        #[serde(rename = "D")]
        C,
    }

    let schema = Schema::parse_str(
        r#"{
        "type": "record",
        "name": "Foo",
        "fields": [
            {
                "name": "type",
                "type": {
                    "type": "enum",
                    "name": "type",
                    "symbols": ["A", "B", "D"]
                }
            },
            {
                "name": "value",
                "type": [
                    "null"
                ]
            }
        ]
    }"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A);
    serde_assert(Foo::B);
    serde_assert(Foo::C);
}

#[test]
fn avro_rs_xxx_enum_repr_record_tag_content_tuple() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "record_tag_content")]
    #[serde(tag = "type", content = "value")]
    enum Foo {
        A(),
        B(String),
        #[serde(rename = "D")]
        C(
            String,
            #[serde(rename = "is_it_true", alias = "is_it_false")] bool,
        ),
    }

    let schema = Schema::parse_str(
        r#"{
        "type": "record",
        "name": "Foo",
        "fields": [
            {
                "name": "type",
                "type": {
                    "type": "enum",
                    "name": "type",
                    "symbols": ["A", "B", "D"]
                }
            },
            {
                "name": "value",
                "type": [
                    "null",
                    "string",
                    {
                        "type": "record",
                        "name": "D",
                        "default": "null",
                        "fields": [
                            { "name": "field_0", "type": "string" },
                            { "name": "is_it_true", "aliases": ["is_it_false"], "type": "boolean" }
                        ],
                        "org.apache.avro.rust.tuple": true
                    }
                ]
            }
        ]
    }"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A());
    serde_assert(Foo::B("Something".to_string()));
    serde_assert(Foo::C("Something".to_string(), true));
}

#[test]
fn avro_rs_xxx_enum_repr_record_tag_content_struct() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "record_tag_content")]
    #[serde(tag = "type", content = "value")]
    enum Foo {
        A {},
        B {
            spam: String,
        },
        #[serde(rename = "D")]
        C {
            bar: String,
            #[serde(rename = "is_it_true", alias = "is_it_false")]
            other: bool,
        },
    }

    let schema = Schema::parse_str(
        r#"{
        "type": "record",
        "name": "Foo",
        "fields": [
            {
                "name": "type",
                "type": {
                    "type": "enum",
                    "name": "type",
                    "symbols": ["A", "B", "D"]
                }
            },
            {
                "name": "value",
                "type": [
                    {"type": "record", "name": "A", "fields": []},
                    {"type": "record", "name": "B", "fields": [{"name": "spam", "type": "string"}]},
                    {
                        "type": "record",
                        "name": "D",
                        "default": "null",
                        "fields": [
                            { "name": "bar", "type": "string" },
                            { "name": "is_it_true", "aliases": ["is_it_false"], "type": "boolean" }
                        ]
                    }
                ]
            }
        ]
    }"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A {});
    serde_assert(Foo::B {
        spam: "Something".to_string(),
    });
    serde_assert(Foo::C {
        bar: "Something".to_string(),
        other: true,
    });
}

#[test]
fn avro_rs_xxx_enum_repr_bare_union_plain() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "bare_union")]
    #[serde(untagged)]
    enum Foo {
        A,
    }

    let schema = Schema::parse_str(r#"["null"]"#).unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A);
}

#[test]
fn avro_rs_xxx_enum_repr_bare_union_tuple() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "bare_union")]
    #[serde(untagged)]
    enum Foo {
        B(String),
        #[serde(rename = "D")]
        C(
            String,
            #[serde(rename = "is_it_true", alias = "is_it_false")] bool,
        ),
    }

    let schema = Schema::parse_str(
        r#"[
        "string",
        {
            "type": "record",
            "name": "D",
            "default": "null",
            "fields": [
                { "name": "field_0", "type": "string" },
                { "name": "is_it_true", "aliases": ["is_it_false"], "type": "boolean" }
            ],
            "org.apache.avro.rust.tuple": true
        }
    ]"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::B("Something".to_string()));
    serde_assert(Foo::C("Something".to_string(), true));
}

#[test]
fn avro_rs_xxx_enum_repr_record_internally_tagged_plain() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "record_internally_tagged")]
    #[serde(tag = "type")]
    enum Foo {
        A,
        B,
        #[serde(rename = "D")]
        C,
    }

    let schema = Schema::parse_str(
        r#"{
        "type": "record",
        "name": "Foo",
        "fields": [
            {
                "name": "type",
                "type": "string"
            }
        ]
    }"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A);
    serde_assert(Foo::B);
    serde_assert(Foo::C);
}

#[test]
#[should_panic(
    expected = "Newtype variant type must implement `get_record_fields` for internally tagged enums"
)]
fn avro_rs_xxx_enum_repr_record_internally_tagged_tuple_no_get_record_fields() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "record_internally_tagged")]
    #[serde(tag = "type")]
    enum Foo {
        B(String),
    }

    let _ = Foo::get_schema();
}

#[test]
fn avro_rs_xxx_enum_repr_record_internally_tagged_tuple() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "record_internally_tagged")]
    #[serde(tag = "type")]
    enum Foo {
        B(Bar),
    }

    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    struct Bar {
        spam: String,
    }

    let schema = Schema::parse_str(
        r#"{
        "type": "record",
        "name": "Foo",
        "fields": [
            {
                "name": "type",
                "type": "string"
            },
            {
                "name": "spam",
                "type": "string"
            }
        ]
    }"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::B(Bar {
        spam: "Something".to_string(),
    }));
}

#[test]
fn avro_rs_xxx_enum_repr_record_internally_tagged_struct() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "record_internally_tagged")]
    #[serde(tag = "type")]
    enum Foo {
        A {},
        B {
            #[avro(default = r#""spam""#)]
            spam: String,
        },
        #[serde(rename = "D")]
        C {
            #[avro(default = r#""bar""#)]
            bar: String,
            #[serde(rename = "is_it_true", alias = "is_it_false")]
            #[avro(default = "true")]
            other: bool,
        },
    }

    let schema = Schema::parse_str(
        r#"{
        "type": "record",
        "name": "Foo",
        "fields": [
            {
                "name": "type",
                "type": "string"
            },
            {
                "name": "spam",
                "type": "string"
            },
            {
                "name": "bar",
                "type": "string"
            },
            {
                "name": "is_it_true",
                "aliases": ["is_it_false"],
                "type": "boolean"
            }
        ]
    }"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A {});
    serde_assert(Foo::B {
        spam: "Something".to_string(),
    });
    serde_assert(Foo::C {
        bar: "Something".to_string(),
        other: true,
    });
}

#[test]
#[should_panic(expected = "Missing default for skipped field 'spam' for schema")]
fn avro_rs_xxx_enum_repr_record_internally_tagged_struct_no_defaults() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "record_internally_tagged")]
    #[serde(tag = "type")]
    enum Foo {
        A {},
        B {
            spam: String,
        },
        #[serde(rename = "D")]
        C {
            bar: String,
            #[serde(rename = "is_it_true", alias = "is_it_false")]
            other: bool,
        },
    }

    serde_assert(Foo::A {});
}

#[test]
fn avro_rs_xxx_enum_repr_union_of_records_plain() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "union_of_records")]
    enum Foo {
        A,
        B,
        #[serde(rename = "D")]
        C,
    }

    let schema = Schema::parse_str(
        r#"[
        {"type": "record", "name": "A", "fields": []},
        {"type": "record", "name": "B", "fields": []},
        {"type": "record", "name": "D", "fields": []}
    ]"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A);
    serde_assert(Foo::B);
    serde_assert(Foo::C);
}

#[test]
fn avro_rs_xxx_enum_repr_union_of_records_tuple() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "union_of_records")]
    enum Foo {
        A(),
        B(String),
        #[serde(rename = "D")]
        C(
            String,
            #[serde(rename = "is_it_true", alias = "is_it_false")] bool,
        ),
    }

    let schema = Schema::parse_str(
        r#"[
        {"type": "record", "name": "A", "fields": [], "org.apache.avro.rust.tuple": true},
        {"type": "record", "name": "B", "fields": [{
            "name": "field_0", "type": "string"
        }], "org.apache.avro.rust.tuple": true, "org.apache.avro.rust.union_of_records": true },
        {"type": "record", "name": "D", "fields": [
            {"name": "field_0", "type": "string"},
            {"name": "is_it_true", "aliases": ["is_it_false"], "type": "boolean"}
        ], "org.apache.avro.rust.tuple": true}
    ]"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A());
    serde_assert(Foo::B("Something".to_string()));
    serde_assert(Foo::C("Something".to_string(), true));
}

#[test]
fn avro_rs_xxx_enum_repr_union_of_records_struct() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "union_of_records")]
    enum Foo {
        A {},
        B {
            spam: String,
        },
        #[serde(rename = "D")]
        C {
            bar: String,
            #[serde(rename = "is_it_true", alias = "is_it_false")]
            other: bool,
        },
    }

    let schema = Schema::parse_str(
        r#"[
        {"type": "record", "name": "A", "fields": []},
        {"type": "record", "name": "B", "fields": [{
            "name": "spam", "type": "string"
        }], "org.apache.avro.rust.union_of_records": true },
        {"type": "record", "name": "D", "fields": [
            {"name": "bar", "type": "string"},
            {"name": "is_it_true", "aliases": ["is_it_false"], "type": "boolean"}
        ]}
    ]"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A {});
    serde_assert(Foo::B {
        spam: "Something".to_string(),
    });
    serde_assert(Foo::C {
        bar: "Something".to_string(),
        other: true,
    });
}

#[test]
fn avro_rs_xxx_enum_repr_bare_union_without_untagged_plain() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "bare_union")]
    enum Foo {
        A,
    }

    let schema = Schema::parse_str(r#"["null"]"#).unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::A);
}

#[test]
fn avro_rs_xxx_enum_repr_bare_union_without_untagged_tuple() {
    #[derive(AvroSchema, Debug, Serialize, Deserialize, Clone, PartialEq)]
    #[avro(repr = "bare_union")]
    enum Foo {
        B(String),
        #[serde(rename = "D")]
        C(
            String,
            #[serde(rename = "is_it_true", alias = "is_it_false")] bool,
        ),
    }

    let schema = Schema::parse_str(
        r#"[
        "string",
        {
            "type": "record",
            "name": "D",
            "default": "null",
            "fields": [
                { "name": "field_0", "type": "string" },
                { "name": "is_it_true", "aliases": ["is_it_false"], "type": "boolean" }
            ],
            "org.apache.avro.rust.tuple": true
        }
    ]"#,
    )
    .unwrap();

    assert_eq!(Foo::get_schema(), schema);
    serde_assert(Foo::B("Something".to_string()));
    serde_assert(Foo::C("Something".to_string(), true));
}
