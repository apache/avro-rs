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

use apache_avro::{AvroSchema, Error, Reader, Schema, Writer, from_value};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Takes in a type that implements the right combination of traits and runs it through a Serde
/// round-trip and asserts the result is the same.
fn serde_assert<T>(obj: T)
where
    T: std::fmt::Debug + Serialize + DeserializeOwned + AvroSchema + Clone + PartialEq,
{
    assert_eq!(obj, serde(obj.clone()).unwrap());
}

/// Takes in a type that implements the right combination of traits and runs it through a Serde
/// round-trip and asserts that the error matches the expected string.
fn serde_assert_err<T>(obj: T, expected: &str)
where
    T: std::fmt::Debug + Serialize + DeserializeOwned + AvroSchema + Clone + PartialEq,
{
    let error = serde(obj).unwrap_err().to_string();
    assert!(
        error.contains(expected),
        "Error `{error}` does not contain `{expected}`"
    );
}

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
    let mut reader = Reader::with_schema(&schema, &encoded[..])?;
    if let Some(res) = reader.next() {
        return res.and_then(|v| from_value::<T>(&v));
    }
    panic!("Nothing was encoded!")
}

mod container_attributes {
    use super::*;

    #[test]
    fn avro_rs_373_rename() {
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        #[serde(rename = "Bar")]
        struct Foo {
            a: String,
            b: i32,
        }

        let schema = r#"
        {
            "type":"record",
            "name":"Bar",
            "fields": [
                {
                    "name":"a",
                    "type":"string"
                },
                {
                    "name":"b",
                    "type":"int"
                }
            ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        assert_eq!(schema, Foo::get_schema());

        serde_assert(Foo {
            a: "spam".to_string(),
            b: 321,
        });
    }

    #[test]
    fn avro_rs_373_nested_rename() {
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        #[serde(rename = "Bar")]
        struct Foo {
            a: String,
            b: i32,
        }

        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        struct Outer {
            bar: Foo,
        }

        let schema = r#"
        {
            "type":"record",
            "name":"Outer",
            "fields": [
                {
                    "name":"bar",
                    "type": {
                        "type":"record",
                        "name":"Bar",
                        "fields": [
                            {
                                "name":"a",
                                "type":"string"
                            },
                            {
                                "name":"b",
                                "type":"int"
                            }
                        ]
                    }
                }
            ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        assert_eq!(schema, Outer::get_schema());

        serde_assert(Outer {
            bar: Foo {
                a: "spam".to_string(),
                b: 321,
            },
        });
    }

    #[test]
    fn avro_rs_373_rename_all() {
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        #[serde(rename_all = "UPPERCASE")]
        struct Foo {
            a: String,
            b: i32,
        }

        let schema = r#"
        {
            "type":"record",
            "name":"Foo",
            "fields": [
                {
                    "name":"A",
                    "type":"string"
                },
                {
                    "name":"B",
                    "type":"int"
                }
            ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        assert_eq!(schema, Foo::get_schema());

        serde_assert(Foo {
            a: "spam".to_string(),
            b: 321,
        });
    }

    #[test]
    fn avro_rs_373_from_into() {
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        #[serde(from = "FooFromInto", into = "FooFromInto")]
        struct Foo {
            a: String,
            b: i32,
        }
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        struct FooFromInto {
            a: String,
            b: i32,
        }

        impl From<FooFromInto> for Foo {
            fn from(value: FooFromInto) -> Self {
                Self {
                    a: value.a,
                    b: value.b,
                }
            }
        }

        impl From<Foo> for FooFromInto {
            fn from(value: Foo) -> Self {
                Self {
                    a: value.a,
                    b: value.b,
                }
            }
        }

        let schema = r#"
        {
            "type":"record",
            "name":"Foo",
            "fields": [
                {
                    "name":"a",
                    "type":"string"
                },
                {
                    "name":"b",
                    "type":"int"
                }
            ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        assert_eq!(schema, Foo::get_schema());

        serde_assert(Foo {
            a: "spam".to_string(),
            b: 321,
        });
    }

    #[test]
    fn avro_rs_373_from_into_different() {
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        #[serde(from = "FooFromInto", into = "FooFromInto")]
        struct Foo {
            a: String,
            b: i32,
        }
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        struct FooFromInto {
            a: String,
            b: i32,
            c: bool,
        }

        impl From<FooFromInto> for Foo {
            fn from(value: FooFromInto) -> Self {
                Self {
                    a: value.a,
                    b: value.b,
                }
            }
        }

        impl From<Foo> for FooFromInto {
            fn from(value: Foo) -> Self {
                Self {
                    a: value.a,
                    b: value.b,
                    c: true,
                }
            }
        }

        let schema = r#"
        {
            "type":"record",
            "name":"Foo",
            "fields": [
                {
                    "name":"a",
                    "type":"string"
                },
                {
                    "name":"b",
                    "type":"int"
                }
            ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        assert_eq!(schema, Foo::get_schema());

        serde_assert_err(
            Foo {
                a: "spam".to_string(),
                b: 321,
            },
            "Invalid field name c",
        );
    }
}

mod variant_attributes {
    use super::*;

    #[test]
    fn avro_rs_373_rename() {
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        enum Foo {
            #[serde(rename = "Three")]
            One,
            Two,
        }

        let schema = r#"
        {
            "type":"enum",
            "name":"Foo",
            "symbols": [
                "Three", "Two"
            ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        assert_eq!(schema, Foo::get_schema());

        serde_assert(Foo::One);
    }

    #[test]
    fn avro_rs_373_alias() {
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        enum Foo {
            #[serde(rename = "Three", alias = "One")]
            One,
            Two,
        }

        let schema = r#"
        {
            "type":"enum",
            "name":"Foo",
            "symbols": [
                "Three", "Two"
            ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        assert_eq!(schema, Foo::get_schema());

        serde_assert(Foo::One);
    }

    #[test]
    fn avro_rs_373_skip() {
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        enum Foo {
            #[allow(dead_code)]
            #[serde(skip)]
            One,
            Two,
        }

        let schema = r#"
        {
            "type":"enum",
            "name":"Foo",
            "symbols": [
                "One", "Two"
            ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        assert_eq!(schema, Foo::get_schema());

        serde_assert(Foo::Two);
    }
}

mod field_attributes {
    use super::*;

    #[test]
    fn avro_rs_373_rename() {
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        struct Foo {
            #[serde(rename = "c")]
            a: String,
            b: i32,
        }

        let schema = r#"
        {
            "type":"record",
            "name":"Foo",
            "fields": [
                {
                    "name":"c",
                    "type":"string"
                },
                {
                    "name":"b",
                    "type":"int"
                }
            ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        assert_eq!(schema, Foo::get_schema());

        serde_assert(Foo {
            a: "spam".to_string(),
            b: 321,
        });
    }

    #[test]
    fn avro_rs_373_flatten() {
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        struct Nested {
            a: bool,
        }

        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        struct Foo {
            #[serde(flatten)]
            nested: Nested,
            b: i32,
        }

        let schema = r#"
        {
            "type":"record",
            "name":"Foo",
            "fields": [
                {
                    "name":"a",
                    "type":"boolean"
                },
                {
                    "name":"b",
                    "type":"int"
                }
            ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        assert_eq!(schema, Foo::get_schema());

        serde_assert(Foo {
            nested: Nested { a: true },
            b: 321,
        });
    }

    #[test]
    fn avro_rs_373_skip() {
        #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
        struct Foo {
            #[serde(skip)]
            a: String,
            b: i32,
        }

        let schema = r#"
        {
            "type":"record",
            "name":"Foo",
            "fields": [
                {
                    "name":"b",
                    "type":"int"
                }
            ]
        }
        "#;

        let schema = Schema::parse_str(schema).unwrap();
        assert_eq!(schema, Foo::get_schema());

        serde_assert(Foo {
            a: "".to_string(),
            b: 321,
        });
    }

    #[test]
    fn avro_rs_397_avroschema_with_bytes() {
        use apache_avro::{
            serde_avro_bytes, serde_avro_bytes_opt, serde_avro_fixed, serde_avro_fixed_opt,
            serde_avro_slice, serde_avro_slice_opt,
        };

        #[expect(dead_code, reason = "We only care about the schema")]
        #[derive(AvroSchema)]
        struct TestStructWithBytes<'a> {
            #[avro(with)]
            #[serde(with = "serde_avro_bytes")]
            vec_field: Vec<u8>,
            #[avro(with)]
            #[serde(with = "serde_avro_bytes_opt")]
            vec_field_opt: Option<Vec<u8>>,

            #[avro(with = serde_avro_fixed::get_schema_in_ctxt::<6>)]
            #[serde(with = "serde_avro_fixed")]
            fixed_field: [u8; 6],
            #[avro(with = serde_avro_fixed_opt::get_schema_in_ctxt::<7>)]
            #[serde(with = "serde_avro_fixed_opt")]
            fixed_field_opt: Option<[u8; 7]>,

            #[avro(with)]
            #[serde(with = "serde_avro_slice")]
            slice_field: &'a [u8],
            #[avro(with)]
            #[serde(with = "serde_avro_slice_opt")]
            slice_field_opt: Option<&'a [u8]>,
        }

        let schema = Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "TestStructWithBytes",
              "fields": [ {
                "name": "vec_field",
                "type": "bytes"
              }, {
                "name": "vec_field_opt",
                "type": ["null", "bytes"]
              }, {
                "name": "fixed_field",
                "type": {
                  "name": "serde_avro_fixed_6",
                  "type": "fixed",
                  "size": 6
                }
              }, {
                "name": "fixed_field_opt",
                "type": ["null", {
                  "name": "serde_avro_fixed_7",
                  "type": "fixed",
                  "size": 7
                } ]
              }, {
                "name": "slice_field",
                "type": "bytes"
              }, {
                "name": "slice_field_opt",
                "type": ["null", "bytes"]
              } ]
            }"#,
        )
        .unwrap();
        assert_eq!(schema, TestStructWithBytes::get_schema())
    }
}
