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

use apache_avro::{
    AvroSchema, AvroSchemaComponent, Reader, Schema, Writer, from_value,
    schema::{Alias, EnumSchema, FixedSchema, Name, Names, Namespace, RecordSchema},
};
use proptest::prelude::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{borrow::Cow, collections::HashMap, sync::Mutex};

use pretty_assertions::assert_eq;

/// Takes in a type that implements the right combination of traits and runs it through a Serde Cycle and asserts the result is the same
fn serde_assert<T>(obj: T)
where
    T: std::fmt::Debug + Serialize + DeserializeOwned + AvroSchema + Clone + PartialEq,
{
    assert_eq!(obj, serde(obj.clone()));
}

fn serde<T>(obj: T) -> T
where
    T: Serialize + DeserializeOwned + AvroSchema,
{
    de(ser(obj))
}

fn ser<T>(obj: T) -> Vec<u8>
where
    T: Serialize + AvroSchema,
{
    let schema = T::get_schema();
    let mut writer = Writer::new(&schema, Vec::new()).unwrap();
    if let Err(e) = writer.append_ser(obj) {
        panic!("{e:?}");
    }
    writer.into_inner().unwrap()
}

fn de<T>(encoded: Vec<u8>) -> T
where
    T: DeserializeOwned + AvroSchema,
{
    assert!(!encoded.is_empty());
    let schema = T::get_schema();
    let mut reader = Reader::with_schema(&schema, &encoded[..]).unwrap();
    if let Some(res) = reader.next() {
        match res {
            Ok(value) => {
                return from_value::<T>(&value).unwrap();
            }
            Err(e) => panic!("{e:?}"),
        }
    }
    unreachable!()
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
struct TestBasic {
    a: i32,
    b: String,
}

proptest! {
#[test]
fn test_smoke_test(a: i32, b: String) {
    let schema = r#"
    {
        "type":"record",
        "name":"TestBasic",
        "fields":[
            {
                "name":"a",
                "type":"int"
            },
            {
                "name":"b",
                "type":"string"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestBasic::get_schema());
    let test = TestBasic {
        a,
        b,
    };
    serde_assert(test);
}}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
#[avro(namespace = "com.testing.namespace")]
struct TestBasicNamespace {
    a: i32,
    b: String,
}

#[test]
fn test_basic_namespace() {
    let schema = r#"
    {
        "type":"record",
        "name":"com.testing.namespace.TestBasicNamespace",
        "fields":[
            {
                "name":"a",
                "type":"int"
            },
            {
                "name":"b",
                "type":"string"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestBasicNamespace::get_schema());
    if let Schema::Record(RecordSchema { name, .. }) = TestBasicNamespace::get_schema() {
        assert_eq!("com.testing.namespace".to_owned(), name.namespace.unwrap())
    } else {
        panic!("TestBasicNamespace schema must be a record schema")
    }
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
#[avro(namespace = "com.testing.complex.namespace")]
struct TestComplexNamespace {
    a: TestBasicNamespace,
    b: String,
}

#[test]
fn test_complex_namespace() {
    let schema = r#"
    {
        "type":"record",
        "name":"com.testing.complex.namespace.TestComplexNamespace",
        "fields":[
            {
                "name":"a",
                "type":{
                    "type":"record",
                    "name":"com.testing.namespace.TestBasicNamespace",
                    "fields":[
                        {
                            "name":"a",
                            "type":"int"
                        },
                        {
                            "name":"b",
                            "type":"string"
                        }
                    ]
                }
            },
            {
                "name":"b",
                "type":"string"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestComplexNamespace::get_schema());
    if let Schema::Record(RecordSchema { name, fields, .. }) = TestComplexNamespace::get_schema() {
        assert_eq!(
            "com.testing.complex.namespace".to_owned(),
            name.namespace.unwrap()
        );
        let inner_schema = fields
            .iter()
            .filter(|field| field.name == "a")
            .map(|field| &field.schema)
            .next();
        if let Some(Schema::Record(RecordSchema { name, .. })) = inner_schema {
            assert_eq!(
                "com.testing.namespace".to_owned(),
                name.namespace.clone().unwrap()
            )
        } else {
            panic!("Field 'a' must have a record schema")
        }
    } else {
        panic!("TestComplexNamespace schema must be a record schema")
    }
}

#[test]
fn avro_rs_239_test_named_record() {
    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
    #[avro(namespace = "com.testing.namespace")]
    #[serde(rename = "Other")]
    struct TestNamedRecord {
        a: i32,
        b: String,
    }

    let schema = r#"
    {
        "type":"record",
        "name":"com.testing.namespace.Other",
        "fields":[
            {
                "name":"a",
                "type":"int"
            },
            {
                "name":"b",
                "type":"string"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestNamedRecord::get_schema());
    if let Schema::Record(RecordSchema { name, .. }) = TestNamedRecord::get_schema() {
        assert_eq!("Other", name.name.as_str());
        assert_eq!(Some("com.testing.namespace"), name.namespace.as_deref())
    } else {
        panic!("TestNamedRecord schema must be a record schema")
    }
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
struct TestAllSupportedBaseTypes {
    //Basics test
    a: bool,
    b: i8,
    c: i16,
    d: i32,
    e: u8,
    f: u16,
    g: i64,
    h: f32,
    i: f64,
    j: String,
}

proptest! {
#[test]
fn test_basic_types(a: bool, b: i8, c: i16, d: i32, e: u8, f: u16, g: i64, h: f32, i: f64, j: String) {
    let schema = r#"
    {
        "type":"record",
        "name":"TestAllSupportedBaseTypes",
        "fields":[
            {
                "name":"a",
                "type": "boolean"
            },
            {
                "name":"b",
                "type":"int"
            },
            {
                "name":"c",
                "type":"int"
            },
            {
                "name":"d",
                "type":"int"
            },
            {
                "name":"e",
                "type":"int"
            },
            {
                "name":"f",
                "type":"int"
            },
            {
                "name":"g",
                "type":"long"
            },
            {
                "name":"h",
                "type":"float"
            },
            {
                "name":"i",
                "type":"double"
            },
            {
                "name":"j",
                "type":"string"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestAllSupportedBaseTypes::get_schema());
    let all_basic = TestAllSupportedBaseTypes {
        a,
        b,
        c,
        d,
        e,
        f,
        g,
        h,
        i,
        j,
    };
    serde_assert(all_basic);
}}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
struct TestNested {
    a: i32,
    b: TestAllSupportedBaseTypes,
}

proptest! {
#[test]
fn test_inner_struct(a: bool, b: i8, c: i16, d: i32, e: u8, f: u16, g: i64, h: f32, i: f64, j: String, aa: i32) {
    let schema = r#"
    {
        "type":"record",
        "name":"TestNested",
        "fields":[
            {
                "name":"a",
                "type":"int"
            },
            {
                "name":"b",
                "type":{
                    "type":"record",
                    "name":"TestAllSupportedBaseTypes",
                    "fields":[
                        {
                            "name":"a",
                            "type": "boolean"
                        },
                        {
                            "name":"b",
                            "type":"int"
                        },
                        {
                            "name":"c",
                            "type":"int"
                        },
                        {
                            "name":"d",
                            "type":"int"
                        },
                        {
                            "name":"e",
                            "type":"int"
                        },
                        {
                            "name":"f",
                            "type":"int"
                        },
                        {
                            "name":"g",
                            "type":"long"
                        },
                        {
                            "name":"h",
                            "type":"float"
                        },
                        {
                            "name":"i",
                            "type":"double"
                        },
                        {
                            "name":"j",
                            "type":"string"
                        }
                    ]
                }
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestNested::get_schema());
    let all_basic = TestAllSupportedBaseTypes {
        a,
        b,
        c,
        d,
        e,
        f,
        g,
        h,
        i,
        j,
    };
    let inner_struct = TestNested {
        a: aa,
        b: all_basic,
    };
    serde_assert(inner_struct);
}}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
struct TestOptional {
    a: Option<i32>,
}

proptest! {
#[test]
fn test_optional_field_some(a: i32) {
    let schema = r#"
    {
        "type":"record",
        "name":"TestOptional",
        "fields":[
            {
                "name":"a",
                "type":["null","int"]
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestOptional::get_schema());
    let optional_field = TestOptional { a: Some(a) };
    serde_assert(optional_field);
}}

#[test]
fn test_optional_field_none() {
    let optional_field = TestOptional { a: None };
    serde_assert(optional_field);
}

/// Generic Containers
#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
struct TestGeneric<T: AvroSchemaComponent> {
    a: String,
    b: Vec<T>,
    c: HashMap<String, T>,
}

proptest! {
#[test]
fn test_generic_container_1(a: String, b: Vec<i32>, c: HashMap<String, i32>) {
    let schema = r#"
    {
        "type":"record",
        "name":"TestGeneric",
        "fields":[
            {
                "name":"a",
                "type":"string"
            },
            {
                "name":"b",
                "type": {
                    "type":"array",
                    "items":"int"
                }
            },
            {
                "name":"c",
                "type": {
                    "type":"map",
                    "values":"int"
                }
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestGeneric::<i32>::get_schema());
    let test_generic = TestGeneric::<i32> {
        a,
        b,
        c,
    };
    serde_assert(test_generic);
}}

proptest! {
#[test]
fn test_generic_container_2(a: bool, b: i8, c: i16, d: i32, e: u8, f: u16, g: i64, h: f32, i: f64, j: String) {
    let schema = r#"
    {
        "type":"record",
        "name":"TestGeneric",
        "fields":[
            {
                "name":"a",
                "type":"string"
            },
            {
                "name":"b",
                "type": {
                    "type":"array",
                    "items":{
                        "type":"record",
                        "name":"TestAllSupportedBaseTypes",
                        "fields":[
                            {
                                "name":"a",
                                "type": "boolean"
                            },
                            {
                                "name":"b",
                                "type":"int"
                            },
                            {
                                "name":"c",
                                "type":"int"
                            },
                            {
                                "name":"d",
                                "type":"int"
                            },
                            {
                                "name":"e",
                                "type":"int"
                            },
                            {
                                "name":"f",
                                "type":"int"
                            },
                            {
                                "name":"g",
                                "type":"long"
                            },
                            {
                                "name":"h",
                                "type":"float"
                            },
                            {
                                "name":"i",
                                "type":"double"
                            },
                            {
                                "name":"j",
                                "type":"string"
                            }
                        ]
                    }
                }
            },
            {
                "name":"c",
                "type": {
                    "type":"map",
                    "values":"TestAllSupportedBaseTypes"
                }
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(
        schema,
        TestGeneric::<TestAllSupportedBaseTypes>::get_schema()
    );
    let test_generic = TestGeneric::<TestAllSupportedBaseTypes> {
        a: "testing".to_owned(),
        b: vec![TestAllSupportedBaseTypes {
            a,
            b,
            c,
            d,
            e,
            f,
            g,
            h,
            i,
            j: j.clone(),
        }],
        c: vec![(
            "key".to_owned(),
            TestAllSupportedBaseTypes {
                a,
                b,
                c,
                d,
                e,
                f,
                g,
                h,
                i,
                j,
            },
        )]
        .into_iter()
        .collect(),
    };
    serde_assert(test_generic);
}}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
enum TestAllowedEnum {
    A,
    B,
    C,
    D,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
struct TestAllowedEnumNested {
    a: TestAllowedEnum,
    b: String,
}

#[test]
fn test_enum() {
    let schema = r#"
    {
        "type":"record",
        "name":"TestAllowedEnumNested",
        "fields":[
            {
                "name":"a",
                "type": {
                    "type":"enum",
                    "name":"TestAllowedEnum",
                    "symbols":["A","B","C","D"]
                }
            },
            {
                "name":"b",
                "type":"string"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestAllowedEnumNested::get_schema());
    let enum_included = TestAllowedEnumNested {
        a: TestAllowedEnum::B,
        b: "hey".to_owned(),
    };
    serde_assert(enum_included);
}

#[test]
fn avro_rs_239_test_enum_named() {
    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
    #[serde(rename = "Other", rename_all = "snake_case")]
    enum TestNamedEnum {
        A,
        B,
        C,
        #[serde(rename = "e")]
        D,
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
    struct TestNamedEnumNested {
        a: TestNamedEnum,
        b: String,
    }

    let schema = r#"
    {
        "type":"record",
        "name":"TestNamedEnumNested",
        "fields":[
            {
                "name":"a",
                "type": {
                    "type":"enum",
                    "name":"Other",
                    "symbols":["a","b","c","e"]
                }
            },
            {
                "name":"b",
                "type":"string"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestNamedEnumNested::get_schema());
    let enum_included = TestNamedEnumNested {
        a: TestNamedEnum::B,
        b: "hey".to_owned(),
    };
    serde_assert(enum_included);
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
struct ConsList {
    value: i32,
    next: Option<Box<ConsList>>,
}

#[test]
fn test_cons() {
    let schema = r#"
    {
        "type":"record",
        "name":"ConsList",
        "fields":[
            {
                "name":"value",
                "type":"int"
            },
            {
                "name":"next",
                "type":["null","ConsList"]
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, ConsList::get_schema());
    let list = ConsList {
        value: 34,
        next: Some(Box::new(ConsList {
            value: 42,
            next: None,
        })),
    };
    serde_assert(list)
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
struct ConsListGeneric<T: AvroSchemaComponent> {
    value: T,
    next: Option<Box<ConsListGeneric<T>>>,
}

#[test]
fn test_cons_generic() {
    let schema = r#"
    {
        "type":"record",
        "name":"ConsListGeneric",
        "fields":[
            {
                "name":"value",
                "type":{
                    "type":"record",
                    "name":"TestAllowedEnumNested",
                    "fields":[
                        {
                            "name":"a",
                            "type": {
                                "type":"enum",
                                "name":"TestAllowedEnum",
                                "symbols":["A","B","C","D"]
                            }
                        },
                        {
                            "name":"b",
                            "type":"string"
                        }
                    ]
                }
            },
            {
                "name":"next",
                "type":["null","ConsListGeneric"]
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(
        schema,
        ConsListGeneric::<TestAllowedEnumNested>::get_schema()
    );
    let list = ConsListGeneric::<TestAllowedEnumNested> {
        value: TestAllowedEnumNested {
            a: TestAllowedEnum::B,
            b: "testing".into(),
        },
        next: Some(Box::new(ConsListGeneric::<TestAllowedEnumNested> {
            value: TestAllowedEnumNested {
                a: TestAllowedEnum::D,
                b: "testing2".into(),
            },
            next: None,
        })),
    };
    serde_assert(list)
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
struct TestSimpleArray {
    a: [i32; 4],
}

proptest! {
#[test]
fn test_simple_array(a: [i32; 4]) {
    let schema = r#"
    {
        "type":"record",
        "name":"TestSimpleArray",
        "fields":[
            {
                "name":"a",
                "type": {
                    "type":"array",
                    "items":"int"
                }
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestSimpleArray::get_schema());
    let test = TestSimpleArray { a };
    serde_assert(test)
}}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
struct TestComplexArray<T: AvroSchemaComponent> {
    a: [T; 2],
}

#[test]
fn test_complex_array() {
    let schema = r#"
    {
        "type":"record",
        "name":"TestComplexArray",
        "fields":[
            {
                "name":"a",
                "type": {
                    "type":"array",
                    "items":{
                        "type":"record",
                        "name":"TestBasic",
                        "fields":[
                            {
                                "name":"a",
                                "type":"int"
                            },
                            {
                                "name":"b",
                                "type":"string"
                            }
                        ]
                    }
                }
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestComplexArray::<TestBasic>::get_schema());
    let test = TestComplexArray::<TestBasic> {
        a: [
            TestBasic {
                a: 27,
                b: "foo".to_owned(),
            },
            TestBasic {
                a: 28,
                b: "bar".to_owned(),
            },
        ],
    };
    serde_assert(test)
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
struct Testu8 {
    a: Vec<u8>,
    b: [u8; 2],
}

proptest! {
#[test]
fn test_bytes_handled(a: Vec<u8>, b: [u8; 2]) {
    let test = Testu8 {
        a,
        b,
    };
    serde_assert(test)
    // don't check for schema equality to allow for transitioning to bytes or fixed types in the future
}}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
struct TestSmartPointers<'a> {
    a: String,
    b: Mutex<Vec<i64>>,
    c: Cow<'a, i32>,
}

#[test]
fn test_smart_pointers() {
    let schema = r#"
    {
        "type":"record",
        "name":"TestSmartPointers",
        "fields":[
            {
                "name":"a",
                "type": "string"
            },
            {
                "name":"b",
                "type":{
                    "type":"array",
                    "items":"long"
                }
            },
            {
                "name":"c",
                "type":"int"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestSmartPointers::get_schema());
    let test = TestSmartPointers {
        a: "hey".into(),
        b: Mutex::new(vec![42]),
        c: Cow::Owned(32),
    };
    // test serde with manual equality for mutex
    let test = serde(test);
    assert_eq!("hey", test.a);
    assert_eq!(vec![42], *test.b.lock().unwrap());
    assert_eq!(Cow::Owned::<i32>(32), test.c);
}

#[derive(Debug, Serialize, AvroSchema, Clone, PartialEq)]
struct TestReference<'a> {
    a: &'a Vec<i32>,
    b: &'static str,
    c: &'a f64,
}

proptest! {
#[test]
fn test_reference_struct(a: Vec<i32>, c: f64) {
    let schema = r#"
    {
        "type":"record",
        "name":"TestReference",
        "fields":[
            {
                "name":"a",
                "type": {
                    "type":"array",
                    "items":"int"
                }
            },
            {
                "name":"b",
                "type":"string"
            },
            {
                "name":"c",
                "type":"double"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, TestReference::get_schema());
    // let a = vec![34];
    // let c = 4.55555555_f64;
    let test = TestReference {
        a: &a,
        b: "testing_static",
        c: &c,
    };
    ser(test);
}}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
#[avro(namespace = "com.testing.namespace", doc = "A Documented Record")]
struct TestBasicWithAttributes {
    #[avro(doc = "Milliseconds since Queen released Bohemian Rhapsody")]
    a: i32,
    #[avro(doc = "Full lyrics of Bohemian Rhapsody")]
    b: String,
}

#[test]
fn test_basic_with_attributes() {
    let schema = r#"
    {
        "type":"record",
        "name":"com.testing.namespace.TestBasicWithAttributes",
        "doc":"A Documented Record",
        "fields":[
            {
                "name":"a",
                "type":"int",
                "doc":"Milliseconds since Queen released Bohemian Rhapsody"
            },
            {
                "name":"b",
                "type": "string",
                "doc": "Full lyrics of Bohemian Rhapsody"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    if let Schema::Record(RecordSchema { name, doc, .. }) = TestBasicWithAttributes::get_schema() {
        assert_eq!("com.testing.namespace".to_owned(), name.namespace.unwrap());
        assert_eq!("A Documented Record", doc.unwrap())
    } else {
        panic!("TestBasicWithAttributes schema must be a record schema")
    }
    assert_eq!(schema, TestBasicWithAttributes::get_schema());
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
#[avro(namespace = "com.testing.namespace")]
/// A Documented Record
struct TestBasicWithOuterDocAttributes {
    #[avro(doc = "Milliseconds since Queen released Bohemian Rhapsody")]
    a: i32,
    #[avro(doc = "Full lyrics of Bohemian Rhapsody")]
    b: String,
}

#[test]
fn test_basic_with_out_doc_attributes() {
    let schema = r#"
    {
        "type":"record",
        "name":"com.testing.namespace.TestBasicWithOuterDocAttributes",
        "doc":"A Documented Record",
        "fields":[
            {
                "name":"a",
                "type":"int",
                "doc":"Milliseconds since Queen released Bohemian Rhapsody"
            },
            {
                "name":"b",
                "type": "string",
                "doc": "Full lyrics of Bohemian Rhapsody"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    let derived_schema = TestBasicWithOuterDocAttributes::get_schema();
    assert_eq!(&schema, &derived_schema);
    if let Schema::Record(RecordSchema { name, doc, .. }) = derived_schema {
        assert_eq!("com.testing.namespace".to_owned(), name.namespace.unwrap());
        assert_eq!("A Documented Record", doc.unwrap())
    } else {
        panic!("TestBasicWithOuterDocAttributes schema must be a record schema")
    }
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
#[avro(namespace = "com.testing.namespace")]
/// A Documented Record
/// that spans
/// multiple lines
struct TestBasicWithLargeDoc {
    #[avro(doc = "Milliseconds since Queen released Bohemian Rhapsody")]
    a: i32,
    #[avro(doc = "Full lyrics of Bohemian Rhapsody")]
    b: String,
}

#[test]
fn test_basic_with_large_doc() {
    let schema = r#"
    {
        "type":"record",
        "name":"com.testing.namespace.TestBasicWithLargeDoc",
        "doc":"A Documented Record",
        "fields":[
            {
                "name":"a",
                "type":"int",
                "doc":"Milliseconds since Queen released Bohemian Rhapsody"
            },
            {
                "name":"b",
                "type": "string",
                "doc": "Full lyrics of Bohemian Rhapsody"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    if let Schema::Record(RecordSchema { name, doc, .. }) = TestBasicWithLargeDoc::get_schema() {
        assert_eq!("com.testing.namespace".to_owned(), name.namespace.unwrap());
        assert_eq!(
            "A Documented Record\nthat spans\nmultiple lines",
            doc.unwrap()
        )
    } else {
        panic!("TestBasicWithLargeDoc schema must be a record schema")
    }
    assert_eq!(schema, TestBasicWithLargeDoc::get_schema());
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
struct TestBasicWithBool {
    a: bool,
    b: Option<bool>,
}

proptest! {
#[test]
fn avro_3634_test_basic_with_bool(a in any::<bool>(), b in any::<Option<bool>>()) {
    let schema = r#"
    {
        "type":"record",
        "name":"TestBasicWithBool",
        "fields":[
            {
                "name":"a",
                "type":"boolean"
            },
            {
                "name":"b",
                "type":["null","boolean"]
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    let derived_schema = TestBasicWithBool::get_schema();

    if let Schema::Record(RecordSchema { name, .. }) = derived_schema {
        assert_eq!("TestBasicWithBool", name.fullname(None))
    } else {
        panic!("TestBasicWithBool schema must be a record schema")
    }
    assert_eq!(schema, TestBasicWithBool::get_schema());

    serde_assert(TestBasicWithBool { a, b });
}}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
struct TestBasicWithU32 {
    a: u32,
}

proptest! {
#[test]
fn test_basic_with_u32(a in any::<u32>()) {
    let schema = r#"
    {
        "type":"record",
        "name":"TestBasicWithU32",
        "fields":[
            {
                "name":"a",
                "type":"long"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    if let Schema::Record(RecordSchema { name, .. }) = TestBasicWithU32::get_schema() {
        assert_eq!("TestBasicWithU32", name.fullname(None))
    } else {
        panic!("TestBasicWithU32 schema must be a record schema")
    }
    assert_eq!(schema, TestBasicWithU32::get_schema());

    serde_assert(TestBasicWithU32 { a });
}}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
#[avro(alias = "a", alias = "b", alias = "c")]
struct TestBasicStructWithAliases {
    a: i32,
}

#[test]
fn test_basic_struct_with_aliases() {
    let schema = r#"
    {
        "type":"record",
        "name":"TestBasicStructWithAliases",
        "aliases":["a", "b", "c"],
        "fields":[
            {
                "name":"a",
                "type":"int"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    if let Schema::Record(RecordSchema { name, aliases, .. }) =
        TestBasicStructWithAliases::get_schema()
    {
        assert_eq!("TestBasicStructWithAliases", name.fullname(None));
        assert_eq!(
            Some(vec![
                Alias::new("a").unwrap(),
                Alias::new("b").unwrap(),
                Alias::new("c").unwrap()
            ]),
            aliases
        );
    } else {
        panic!("TestBasicStructWithAliases schema must be a record schema")
    }
    assert_eq!(schema, TestBasicStructWithAliases::get_schema());

    serde_assert(TestBasicStructWithAliases { a: i32::MAX });
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
#[avro(alias = "d")]
#[avro(alias = "e")]
#[avro(alias = "f")]
struct TestBasicStructWithAliases2 {
    a: i32,
}

#[test]
fn test_basic_struct_with_aliases2() {
    let schema = r#"
    {
        "type":"record",
        "name":"TestBasicStructWithAliases2",
        "aliases":["d", "e", "f"],
        "fields":[
            {
                "name":"a",
                "type":"int"
            }
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    if let Schema::Record(RecordSchema { name, aliases, .. }) =
        TestBasicStructWithAliases2::get_schema()
    {
        assert_eq!("TestBasicStructWithAliases2", name.fullname(None));
        assert_eq!(
            Some(vec![
                Alias::new("d").unwrap(),
                Alias::new("e").unwrap(),
                Alias::new("f").unwrap()
            ]),
            aliases
        );
    } else {
        panic!("TestBasicStructWithAliases2 schema must be a record schema")
    }
    assert_eq!(schema, TestBasicStructWithAliases2::get_schema());

    serde_assert(TestBasicStructWithAliases2 { a: i32::MAX });
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
#[avro(alias = "a", alias = "b", alias = "c")]
enum TestBasicEnumWithAliases {
    A,
    B,
}

#[test]
fn test_basic_enum_with_aliases() {
    let schema = r#"
    {
        "type":"enum",
        "name":"TestBasicEnumWithAliases",
        "aliases":["a", "b", "c"],
        "symbols":[
            "A",
            "B"
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    if let Schema::Enum(EnumSchema { name, aliases, .. }) = TestBasicEnumWithAliases::get_schema() {
        assert_eq!("TestBasicEnumWithAliases", name.fullname(None));
        assert_eq!(
            Some(vec![
                Alias::new("a").unwrap(),
                Alias::new("b").unwrap(),
                Alias::new("c").unwrap()
            ]),
            aliases
        );
    } else {
        panic!("TestBasicEnumWithAliases schema must be an enum schema")
    }
    assert_eq!(schema, TestBasicEnumWithAliases::get_schema());

    serde_assert(TestBasicEnumWithAliases::A);
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq, Eq)]
#[avro(alias = "d")]
#[avro(alias = "e")]
#[avro(alias = "f")]
enum TestBasicEnumWithAliases2 {
    A,
    B,
}

#[test]
fn test_basic_enum_with_aliases2() {
    let schema = r#"
    {
        "type":"enum",
        "name":"TestBasicEnumWithAliases2",
        "aliases":["d", "e", "f"],
        "symbols":[
            "A",
            "B"
        ]
    }
    "#;
    let schema = Schema::parse_str(schema).unwrap();
    if let Schema::Enum(EnumSchema { name, aliases, .. }) = TestBasicEnumWithAliases2::get_schema()
    {
        assert_eq!("TestBasicEnumWithAliases2", name.fullname(None));
        assert_eq!(
            Some(vec![
                Alias::new("d").unwrap(),
                Alias::new("e").unwrap(),
                Alias::new("f").unwrap()
            ]),
            aliases
        );
    } else {
        panic!("TestBasicEnumWithAliases2 schema must be an enum schema")
    }
    assert_eq!(schema, TestBasicEnumWithAliases2::get_schema());

    serde_assert(TestBasicEnumWithAliases2::B);
}

#[test]
fn test_basic_struct_with_defaults() {
    #[derive(Debug, Deserialize, Serialize, AvroSchema, Clone, PartialEq, Eq)]
    enum MyEnum {
        Foo,
        Bar,
        Baz,
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestBasicStructWithDefaultValues {
        #[avro(default = "123")]
        a: i32,
        #[avro(default = r#""The default value for 'b'""#)]
        b: String,
        #[avro(default = "true")]
        condition: bool,
        // no default value for 'c'
        c: f64,
        #[avro(default = r#"{"a": 1, "b": 2}"#)]
        map: HashMap<String, i32>,

        #[avro(default = "[1, 2, 3]")]
        array: Vec<i32>,

        #[avro(default = r#""Foo""#)]
        myenum: MyEnum,

        #[avro(default = "null")]
        optional: Option<String>,
    }

    let schema = r#"
    {
        "type":"record",
        "name":"TestBasicStructWithDefaultValues",
        "fields": [
            {
                "name":"a",
                "type":"int",
                "default":123
            },
            {
                "name":"b",
                "type":"string",
                "default": "The default value for 'b'"
            },
            {
                "name":"condition",
                "type":"boolean",
                "default":true
            },
            {
                "name":"c",
                "type":"double"
            },
            {
                "name":"map",
                "type":{
                    "type":"map",
                    "values":"int"
                },
                "default": {
                    "a": 1,
                    "b": 2
                }
            },
            {
                "name":"array",
                "type":{
                    "type":"array",
                    "items":"int"
                },
                "default": [1, 2, 3]
            },
            {
                "name":"myenum",
                "type":{
                    "type":"enum",
                    "name":"MyEnum",
                    "symbols":["Foo", "Bar", "Baz"]
                },
                "default":"Foo"
            },
            {
                "name":"optional",
                "type": ["null", "string"],
                "default": null
            }
        ]
    }
    "#;

    let schema = Schema::parse_str(schema).unwrap();
    if let Schema::Record(RecordSchema { name, fields, .. }) =
        TestBasicStructWithDefaultValues::get_schema()
    {
        assert_eq!("TestBasicStructWithDefaultValues", name.fullname(None));
        use serde_json::json;
        for field in fields {
            match field.name.as_str() {
                "a" => assert_eq!(Some(json!(123_i32)), field.default),
                "b" => assert_eq!(
                    Some(json!(r#"The default value for 'b'"#.to_owned())),
                    field.default
                ),
                "condition" => assert_eq!(Some(json!(true)), field.default),
                "array" => assert_eq!(Some(json!([1, 2, 3])), field.default),
                "map" => assert_eq!(
                    Some(json!({
                        "a": 1,
                        "b": 2
                    })),
                    field.default
                ),
                "c" => assert_eq!(None, field.default),
                "myenum" => assert_eq!(Some(json!("Foo")), field.default),
                "optional" => assert_eq!(Some(json!(null)), field.default),
                _ => panic!("Unexpected field name"),
            }
        }
    } else {
        panic!("TestBasicStructWithDefaultValues schema must be a record schema")
    }
    assert_eq!(schema, TestBasicStructWithDefaultValues::get_schema());

    serde_assert(TestBasicStructWithDefaultValues {
        a: 321,
        b: "A custom value for 'b'".to_owned(),
        condition: false,
        c: 987.654,
        map: [("a".to_owned(), 1), ("b".to_owned(), 2)]
            .iter()
            .cloned()
            .collect(),
        array: vec![4, 5, 6],
        myenum: MyEnum::Bar,
        optional: None,
    });
}

#[test]
fn avro_3633_test_basic_struct_with_skip_attribute() {
    // Note: If using the skip attribute together with serialization,
    // the serde's skip attribute needs also to be added

    #[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
    struct TestBasicStructNoSchema {
        field: bool,
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestBasicStructWithSkipAttribute {
        #[serde(skip)]
        condition: bool,
        a: f64,
        #[serde(skip)]
        map: HashMap<String, i32>,
        array: Vec<i32>,
        #[serde(skip)]
        mystruct: TestBasicStructNoSchema,
        b: i32,
    }

    let schema = r#"
    {
        "type":"record",
        "name":"TestBasicStructWithSkipAttribute",
        "fields": [
            {
                "name":"a",
                "type":"double"
            },
            {
                "name":"array",
                "type":{
                    "type":"array",
                    "items":"int"
                }
            },
            {
                "name":"b",
                "type":"int"
            }
        ]
    }
    "#;

    let schema = Schema::parse_str(schema).unwrap();
    let derived_schema = TestBasicStructWithSkipAttribute::get_schema();
    if let Schema::Record(RecordSchema { name, fields, .. }) = &derived_schema {
        assert_eq!("TestBasicStructWithSkipAttribute", name.fullname(None));
        for field in fields {
            match field.name.as_str() {
                "condition" => panic!("Unexpected field 'condition'"),
                "mystruct" => panic!("Unexpected field 'mystruct'"),
                "map" => panic!("Unexpected field 'map'"),
                _ => {}
            }
        }
    } else {
        panic!(
            "TestBasicStructWithSkipAttribute schema must be a record schema: {derived_schema:?}"
        )
    }
    assert_eq!(schema, derived_schema);

    // Note: If serde's `skip` attribute is used on a field, the field's type
    // needs the trait 'Default' to be implemented, since it is skipping the serialization process.
    // Copied or cloned objects within 'serde_assert()' doesn't "copy" (serialize/deserialze)
    // these fields, so no values are initialized here for skipped fields.
    serde_assert(TestBasicStructWithSkipAttribute {
        condition: bool::default(), // <- skipped
        a: 987.654,
        map: HashMap::default(), // <- skipped
        array: vec![4, 5, 6],
        mystruct: TestBasicStructNoSchema::default(), // <- skipped
        b: 321,
    });
}

#[test]
fn avro_3633_test_basic_struct_with_rename_attribute() {
    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestBasicStructWithRenameAttribute {
        #[serde(rename = "a1")]
        a: bool,
        b: i32,
        #[serde(rename = "c1")]
        c: f32,
    }

    let schema = r#"
    {
        "type":"record",
        "name":"TestBasicStructWithRenameAttribute",
        "fields": [
            {
                "name":"a1",
                "type":"boolean"
            },
            {
                "name":"b",
                "type":"int"
            },
            {
                "name":"c1",
                "type":"float"
            }
        ]
    }
    "#;

    let schema = Schema::parse_str(schema).unwrap();
    let derived_schema = TestBasicStructWithRenameAttribute::get_schema();
    if let Schema::Record(RecordSchema { name, fields, .. }) = &derived_schema {
        assert_eq!("TestBasicStructWithRenameAttribute", name.fullname(None));
        for field in fields {
            match field.name.as_str() {
                "a" => panic!("Unexpected field name 'a': must be 'a1'"),
                "c" => panic!("Unexpected field name 'c': must be 'c1'"),
                _ => {}
            }
        }
    } else {
        panic!(
            "TestBasicStructWithRenameAttribute schema must be a record schema: {derived_schema:?}"
        )
    }
    assert_eq!(schema, derived_schema);

    serde_assert(TestBasicStructWithRenameAttribute {
        a: true,
        b: 321,
        c: 987.654,
    });
}

#[test]
fn test_avro_3663_raw_identifier_field_name() {
    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct TestRawIdent {
        r#type: bool,
    }

    let derived_schema = TestRawIdent::get_schema();
    if let Schema::Record(RecordSchema { fields, .. }) = derived_schema {
        let field = fields.first().expect("TestRawIdent must contain a field");
        assert_eq!(field.name, "type");
    } else {
        panic!("Unexpected schema type for {derived_schema:?}")
    }
}

#[test]
fn avro_3962_fields_documentation() {
    /// Foo docs
    #[derive(AvroSchema)]
    #[allow(dead_code)]
    struct Foo {
        /// a's Rustdoc
        a: i32,
        /// b's Rustdoc
        #[avro(doc = "attribute doc has priority over Rustdoc")]
        b: i32,
    }

    if let Schema::Record(RecordSchema { fields, .. }) = Foo::get_schema() {
        assert_eq!(fields[0].doc, Some("a's Rustdoc".to_string()));
        assert_eq!(
            fields[1].doc,
            Some("attribute doc has priority over Rustdoc".to_string())
        );
    } else {
        panic!("Unexpected schema type for Foo")
    }
}

#[test]
fn avro_rs_247_serde_flatten_support() {
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
fn avro_rs_247_serde_nested_flatten_support() {
    use apache_avro::AvroSchema;
    use serde::{Deserialize, Serialize};

    #[derive(AvroSchema, Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct NestedFoo {
        one: u32,
    }

    #[derive(AvroSchema, Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct Foo {
        #[serde(flatten)]
        nested_foo: NestedFoo,
    }

    #[derive(AvroSchema, Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Bar {
        foo: Foo,
        two: u32,
    }

    let schema = r#"
    {
        "type":"record",
        "name":"Bar",
        "fields": [
            {
                "name":"foo",
                "type": {
                    "type": "record",
                    "name": "Foo",
                    "fields": [
                        {
                            "name": "one",
                            "type": "long"
                        }
                    ]
                }
            },
            {
                "name":"two",
                "type":"long"
            }
        ]
    }
    "#;

    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, Bar::get_schema());

    serde_assert(Bar {
        foo: Foo {
            nested_foo: NestedFoo { one: 42 },
        },
        two: 2,
    });
}

#[test]
#[should_panic(expected = "Duplicate field names found")]
fn avro_rs_247_serde_flatten_support_duplicate_field_name() {
    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct Nested {
        a: i32,
    }

    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct Foo {
        #[serde(flatten)]
        nested: Nested,
        a: i32,
    }

    Foo::get_schema();
}

#[test]
fn avro_rs_247_serde_flatten_support_with_skip() {
    #[derive(Debug, Serialize, Deserialize, AvroSchema, Clone, PartialEq)]
    struct Nested {
        a: bool,
        #[serde(skip)]
        c: f64,
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
        nested: Nested { a: true, c: 0.0 },
        b: 321,
    });
}

#[test]
fn avro_rs_397_with() {
    let schema = Schema::parse_str(
        r#"
    {
        "type":"record",
        "name":"Foo",
        "fields": [
            {
                "name":"a",
                "type":"bytes"
            },
            {
                "name":"b",
                "type":"long"
            },
            {
                "name":"c",
                "type":"bytes"
            }
        ]
    }
    "#,
    )
    .unwrap();

    fn long_schema(_named_schemas: &mut Names, _enclosing_namespace: &Namespace) -> Schema {
        Schema::Long
    }

    mod module {
        use super::*;
        pub fn get_schema_in_ctxt(
            _named_schemas: &mut Names,
            _enclosing_namespace: &Namespace,
        ) -> Schema {
            Schema::Bytes
        }
    }

    #[allow(dead_code)]
    #[derive(AvroSchema)]
    struct Foo {
        #[avro(with)]
        #[serde(with = "module")]
        a: String,
        #[avro(with = long_schema)]
        b: i32,
        #[avro(with = module::get_schema_in_ctxt)]
        c: String,
    }

    assert_eq!(schema, Foo::get_schema());
}

#[test]
fn avro_rs_397_with_generic() {
    let schema = Schema::parse_str(
        r#"
    {
        "type":"record",
        "name":"Foo",
        "fields": [
            {
                "name":"a",
                "type": {
                    "type": "fixed",
                    "size": 15,
                    "name": "fixed_15"
                }
            }
        ]
    }
    "#,
    )
    .unwrap();

    fn generic<const N: usize>(
        _named_schemas: &mut Names,
        _enclosing_namespace: &Namespace,
    ) -> Schema {
        Schema::Fixed(FixedSchema {
            name: Name::new(&format!("fixed_{N}")).unwrap(),
            aliases: None,
            doc: None,
            size: N,
            attributes: Default::default(),
        })
    }

    #[allow(dead_code)]
    #[derive(AvroSchema)]
    struct Foo {
        #[avro(with = generic::<15>)]
        a: [u8; 15],
    }

    assert_eq!(schema, Foo::get_schema());
}

#[test]
fn avro_rs_397_uuid() {
    let schema = Schema::parse_str(
        r#"
    {
        "type":"record",
        "name":"Foo",
        "fields": [
            {
                "name":"baz",
                "type":{
                    "type":"fixed",
                    "logicalType":"uuid",
                    "name":"uuid",
                    "size":16
                }
            }
        ]
    }
    "#,
    )
    .unwrap();

    #[derive(AvroSchema, Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Foo {
        #[serde(rename = "baz")]
        bar: uuid::Uuid,
    }

    assert_eq!(schema, Foo::get_schema());
    serde_assert(Foo {
        bar: uuid::Uuid::nil(),
    });
}

#[test]
fn avro_rs_397_derive_with_expr_lambda() {
    let schema = r#"
  {
    "type":"record",
    "name":"Foo",
    "fields": [
      {
        "name": "_a",
        "type": "bytes"
      },
      {
        "name": "_b",
        "type": "int"
      }
    ]
  }"#;

    let expected_schema = Schema::parse_str(schema).unwrap();

    #[derive(AvroSchema)]
    struct Foo {
        #[avro(with = || Schema::Bytes)]
        _a: String,
        _b: i32,
    }

    let derived_schema = Foo::get_schema();

    assert_eq!(expected_schema, derived_schema);
}

#[test]
fn avro_rs_398_transparent_with_skip() {
    fn long_schema(_named_schemas: &mut Names, _enclosing_namespace: &Namespace) -> Schema {
        Schema::Long
    }

    #[allow(dead_code)]
    #[derive(AvroSchema)]
    #[serde(transparent)]
    struct Foo {
        #[serde(skip)]
        a: String,
        #[avro(with = long_schema)]
        b: i32,
        #[serde(skip)]
        c: String,
    }

    assert_eq!(Schema::Long, Foo::get_schema());
}

#[test]
fn avro_rs_401_do_not_match_typename() {
    #[expect(nonstandard_style, reason = "It needs to be exactly this")]
    type f32 = f64;

    #[expect(dead_code, reason = "We only check the schema")]
    #[derive(AvroSchema)]
    struct Foo {
        field: f32,
    }

    let schema = r#"
    {
        "type":"record",
        "name":"Foo",
        "fields": [
            {
                "name":"field",
                "type":"double"
            }
        ]
    }
    "#;

    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, Foo::get_schema());
}

#[test]
fn avro_rs_401_supported_type_variants() {
    #[expect(dead_code, reason = "We only check the schema")]
    #[derive(AvroSchema)]
    struct Foo<'a> {
        one: f32,
        two: std::string::String,
        three: &'static i32,
        four: &'a str,
        five: &'a mut f64,
        six: [u8; 5],
        seven: [u8],
    }

    let schema = r#"
    {
        "type":"record",
        "name":"Foo",
        "fields": [
            {
                "name":"one",
                "type":"float"
            },
            {
                "name":"two",
                "type":"string"
            },
            {
                "name":"three",
                "type":"int"
            },
            {
                "name":"four",
                "type":"string"
            },
            {
                "name":"five",
                "type":"double"
            },
            {
                "name":"six",
                "type":"array",
                "items":"int"
            },
            {
                "name":"seven",
                "type":"array",
                "items":"int"
            }
        ]
    }
    "#;

    let schema = Schema::parse_str(schema).unwrap();
    assert_eq!(schema, Foo::get_schema());
}

#[test]
fn avro_rs_414_round_trip_char_u64_u128_i128() {
    let schema = Schema::parse_str(
        r#"
      {
        "type":"record",
        "name":"Foo",
        "fields": [
          {
            "name": "a",
            "type": "string"
          },
          {
            "name": "b",
            "type": {
                "name": "u64",
                "type": "fixed",
                "size": 8
            }
          },
          {
            "name": "c",
            "type": {
                "name": "u128",
                "type": "fixed",
                "size": 16
            }
          },
          {
            "name": "d",
            "type": {
                "name": "i128",
                "type": "fixed",
                "size": 16
            }
          }
        ]
      }"#,
    )
    .unwrap();

    #[derive(AvroSchema, Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct Foo {
        a: char,
        b: u64,
        c: u128,
        d: i128,
    }

    assert_eq!(schema, Foo::get_schema());
    serde_assert(Foo {
        a: '👺',
        b: u64::MAX,
        c: u128::MAX,
        d: i128::MAX,
    });
}

#[test]
fn avro_rs_448_flatten_recurring_type() {
    #[derive(AvroSchema)]
    #[expect(dead_code, reason = "Only testing derived schema")]
    pub enum Color {
        G,
    }

    #[derive(AvroSchema)]
    pub struct A {
        pub _color: Color,
    }

    #[derive(AvroSchema)]
    pub struct C {
        #[serde(flatten)]
        pub _a: A,
    }

    #[derive(AvroSchema)]
    pub struct TestStruct {
        pub _a: Color,
        pub _c: C,
    }

    let schema = Schema::parse_str(
        r#"{
        "name": "TestStruct",
        "type":"record",
        "fields": [
            {
                "name":"_a",
                "type": {
                    "name": "Color",
                    "type": "enum",
                    "symbols": ["G"]
                }
            },
            {
                "name":"_c",
                "type": {
                    "name":"C",
                    "type":"record",
                    "fields": [
                        {
                            "name": "_color",
                            "type": "Color"
                        }
                    ]
                }
            }
        ]
    }"#,
    )
    .unwrap();

    assert_eq!(TestStruct::get_schema(), schema);
}

#[test]
fn avro_rs_448_flatten_transparent_sandwich() {
    #[derive(AvroSchema)]
    #[expect(dead_code, reason = "Only testing derived schema")]
    pub enum Color {
        G,
    }

    #[derive(AvroSchema)]
    pub struct A {
        pub _color: Color,
    }

    #[derive(AvroSchema)]
    pub struct C {
        #[serde(flatten)]
        pub _a: A,
    }

    #[derive(AvroSchema)]
    #[serde(transparent)]
    pub struct B {
        pub _c: C,
    }

    #[derive(AvroSchema)]
    pub struct TestStruct {
        pub _a: Color,
        pub _b: B,
        pub _c: C,
    }

    let schema = Schema::parse_str(
        r#"{
        "name": "TestStruct",
        "type":"record",
        "fields": [
            {
                "name":"_a",
                "type": {
                    "name": "Color",
                    "type": "enum",
                    "symbols": ["G"]
                }
            },
            {
                "name":"_b",
                "type": {
                    "name":"C",
                    "type":"record",
                    "fields": [
                        {
                            "name": "_color",
                            "type": "Color"
                        }
                    ]
                }
            },
            {
                "name":"_c",
                "type": "C"
            }
        ]
    }"#,
    )
    .unwrap();

    assert_eq!(TestStruct::get_schema(), schema);
}

#[test]
fn avro_rs_448_transparent_with() {
    #[derive(AvroSchema)]
    #[serde(transparent)]
    pub struct TestStruct {
        #[avro(with = || Schema::Long)]
        pub _a: i32,
    }

    let mut named_schemas = HashMap::new();
    assert_eq!(
        TestStruct::get_record_fields_in_ctxt(0, &mut named_schemas, &None),
        None
    );
    assert!(
        named_schemas.is_empty(),
        "No name should've been added: {named_schemas:?}"
    );
}

#[test]
fn avro_rs_448_transparent_with_2() {
    #[derive(AvroSchema)]
    pub struct Foo {
        _field: i32,
        _a: String,
    }

    #[derive(AvroSchema)]
    #[serde(transparent)]
    pub struct TestStruct {
        #[avro(with = Foo::get_schema_in_ctxt)]
        pub _a: Foo,
    }

    let mut named_schemas = HashMap::new();
    let fields = TestStruct::get_record_fields_in_ctxt(0, &mut named_schemas, &None).unwrap();
    assert!(
        named_schemas.is_empty(),
        "No name should've been added: {named_schemas:?}"
    );
    assert_eq!(fields.len(), 2);

    TestStruct::get_schema_in_ctxt(&mut named_schemas, &None);
    assert_eq!(
        named_schemas.len(),
        1,
        "One name should've been added: {named_schemas:?}"
    );

    let fields = TestStruct::get_record_fields_in_ctxt(0, &mut named_schemas, &None).unwrap();
    assert_eq!(
        named_schemas.len(),
        1,
        "No name should've been added: {named_schemas:?}"
    );
    assert_eq!(fields.len(), 2);
}

#[test]
fn avro_rs_448_flatten_field_positions() {
    #[derive(AvroSchema)]
    struct Foo {
        _a: i32,
        _b: String,
    }

    #[derive(AvroSchema)]
    struct Bar {
        _c: Vec<i64>,
        #[serde(flatten)]
        _d: Foo,
        _e: bool,
    }

    let Schema::Record(schema) = Bar::get_schema() else {
        panic!("Structs should generate records")
    };

    let positions = schema
        .fields
        .into_iter()
        .map(|f| f.position)
        .collect::<Vec<_>>();
    assert_eq!(positions.as_slice(), &[0, 1, 2, 3][..]);
}
