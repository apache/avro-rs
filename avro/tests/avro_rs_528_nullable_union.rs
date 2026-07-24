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

use std::fmt::Debug;

use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::writer::datum::GenericDatumWriter;
use pretty_assertions::assert_eq;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[track_caller]
fn assert_roundtrip<T>(value: &T, schema: &Schema)
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    let serialized = GenericDatumWriter::builder(schema)
        .build()
        .unwrap()
        .write_ser_to_vec(&value)
        .unwrap();
    let deserialized: T = GenericDatumReader::builder(schema)
        .build()
        .unwrap()
        .read_deser(&mut &serialized[..])
        .unwrap();

    assert_eq!(&deserialized, value);
}

mod nullable_enum {
    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyEnum {
        A,
        B,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyUnionNullable {
        Null,
        MyEnum(MyEnum),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyUnionAvroJsonEncoding {
        MyEnum(MyEnum),
    }

    fn schema() -> Schema {
        Schema::parse_str(
            r#"
        [
            "null",
            {
                "type": "enum",
                "name": "MyEnum",
                "symbols": ["A", "B"]
            }
        ]"#,
        )
        .unwrap()
    }

    #[test]
    fn null_variant_enum() {
        let schema = schema();
        assert_roundtrip(&MyUnionNullable::Null, &schema);
        assert_roundtrip(&MyUnionNullable::MyEnum(MyEnum::A), &schema);
        assert_roundtrip(&MyUnionNullable::MyEnum(MyEnum::B), &schema);
    }

    #[test]
    fn option_enum() {
        let schema = schema();
        assert_roundtrip(&None::<MyEnum>, &schema);
        assert_roundtrip(&Some(MyEnum::A), &schema);
        assert_roundtrip(&Some(MyEnum::B), &schema);
    }

    #[test]
    fn avro_json_encoding_compatible_null() {
        assert_roundtrip(&None::<MyUnionAvroJsonEncoding>, &schema());
    }

    #[test]
    // TODO: A (union) enum with only one variant is incorrectly seen as an option by the serializer
    //   and fails to serialize
    fn avro_json_encoding_compatible_my_enum_a() {
        // I think this should work
        assert_roundtrip(&Some(MyUnionAvroJsonEncoding::MyEnum(MyEnum::A)), &schema());
    }
}

mod nullable_primitive_int {
    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyUnionNullable {
        Null,
        Int(i32),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyUnionAvroJsonEncoding {
        #[serde(rename = "int")]
        Int(i32),
    }

    fn schema() -> Schema {
        Schema::parse_str(
            r#"
        [
            "null",
            "int"
        ]
        "#,
        )
        .unwrap()
    }

    #[test]
    fn null_variant_enum() {
        let schema = schema();
        assert_roundtrip(&MyUnionNullable::Null, &schema);
        assert_roundtrip(&MyUnionNullable::Int(42), &schema);
    }

    #[test]
    fn option_i32() {
        let schema = schema();
        assert_roundtrip(&None::<i32>, &schema);
        assert_roundtrip(&Some(42_i32), &schema);
    }

    #[test]
    fn avro_json_encoding_compatible_null() {
        assert_roundtrip(&None::<MyUnionAvroJsonEncoding>, &schema());
    }

    #[test]
    // TODO: A (union) enum with only one variant is incorrectly seen as an option by the serializer
    //   and fails to serialize
    fn avro_json_encoding_compatible_int_42() {
        assert_roundtrip(&Some(MyUnionAvroJsonEncoding::Int(42)), &schema());
    }
}

mod nullable_record {
    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct MyRecord {
        a: i32,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyUnionNullable {
        Null,
        MyRecord(MyRecord),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyUnionAvroJsonEncoding {
        MyRecord(MyRecord),
    }

    fn schema() -> Schema {
        Schema::parse_str(
            r#"
        [
            "null",
            {
                "type": "record",
                "name": "MyRecord",
                "fields": [
                    {"name": "a", "type": "int"}
                ]
            }
        ]
        "#,
        )
        .unwrap()
    }

    #[test]
    fn null_variant_enum() {
        let schema = schema();
        assert_roundtrip(&MyUnionNullable::Null, &schema);
        assert_roundtrip(&MyUnionNullable::MyRecord(MyRecord { a: 27 }), &schema);
    }

    #[test]
    fn option_record() {
        let schema = schema();
        assert_roundtrip(&None::<MyRecord>, &schema);
        assert_roundtrip(&Some(MyRecord { a: 27 }), &schema);
    }

    #[test]
    fn avro_json_encoding_compatible_null() {
        assert_roundtrip(&None::<MyUnionAvroJsonEncoding>, &schema());
    }

    #[test]
    // TODO: A (union) enum with only one variant is incorrectly seen as an option by the serializer
    //   and fails to serialize
    fn avro_json_encoding_compatible_my_record_a_27() {
        assert_roundtrip(
            &Some(MyUnionAvroJsonEncoding::MyRecord(MyRecord { a: 27 })),
            &schema(),
        );
    }
}

mod nullable_int_enum_record {
    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyEnum {
        A,
        B,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct MyRecord {
        a: i32,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyUnionNullable {
        Null,
        Int(i32),
        MyEnum(MyEnum),
        MyRecord(MyRecord),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(untagged)]
    enum MyUnionUntagged {
        Int(i32),
        MyEnum(MyEnum),
        MyRecord(MyRecord),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyUnionAvroJsonEncoding {
        Int(i32),
        MyEnum(MyEnum),
        MyRecord(MyRecord),
    }

    fn schema() -> Schema {
        Schema::parse_str(
            r#"
        [
            "null",
            "int",
            {
                "type": "enum",
                "name": "MyEnum",
                "symbols": ["A", "B"]
            },
            {
                "type": "record",
                "name": "MyRecord",
                "fields": [
                    {"name": "a", "type": "int"}
                ]
            }
        ]
        "#,
        )
        .unwrap()
    }

    #[test]
    fn null_variant_enum() {
        let schema = schema();
        assert_roundtrip(&MyUnionNullable::Null, &schema);
        assert_roundtrip(&MyUnionNullable::Int(42), &schema);
        assert_roundtrip(&MyUnionNullable::MyEnum(MyEnum::A), &schema);
        assert_roundtrip(&MyUnionNullable::MyEnum(MyEnum::B), &schema);
        assert_roundtrip(&MyUnionNullable::MyRecord(MyRecord { a: 27 }), &schema);
    }

    #[test]
    fn option_enum() {
        let schema = schema();
        assert_roundtrip(&None::<MyUnionAvroJsonEncoding>, &schema);
        assert_roundtrip(&Some(MyUnionAvroJsonEncoding::Int(42)), &schema);
        assert_roundtrip(&Some(MyUnionAvroJsonEncoding::MyEnum(MyEnum::A)), &schema);
        assert_roundtrip(
            &Some(MyUnionAvroJsonEncoding::MyRecord(MyRecord { a: 27 })),
            &schema,
        );
    }

    #[test]
    fn option_enum_untagged() {
        let schema = schema();
        assert_roundtrip(&None::<MyUnionUntagged>, &schema);
        assert_roundtrip(&Some(MyUnionUntagged::Int(42)), &schema);
        assert_roundtrip(
            &Some(MyUnionUntagged::MyRecord(MyRecord { a: 27 })),
            &schema,
        );
    }

    #[test]
    #[should_panic(
        expected = "If the type has a plain enum inside a untagged enum, this is not supported by Serde."
    )]
    fn rusty_untagged_my_enum_a() {
        // This cannot work. Because the parent enum is untagged, the serializer will get the index
        // of the child enum. This makes it impossible for the serializer to pick the right variant.
        assert_roundtrip(&Some(MyUnionUntagged::MyEnum(MyEnum::A)), &schema());
    }
}

mod nullable_untagged_pitfall {
    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct MyRecordA {
        a: i32,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct MyRecordB {
        a: i32,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyUnionNullable {
        Null,
        MyRecordA(MyRecordA),
        MyRecordB(MyRecordB),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(untagged)]
    enum MyUnionUntagged {
        MyRecordA(MyRecordA),
        MyRecordB(MyRecordB),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum MyUnionAvroJsonEncoding {
        MyRecordA(MyRecordA),
        MyRecordB(MyRecordB),
    }

    fn schema() -> Schema {
        Schema::parse_str(
            r#"
        [
            "null",
            {
                "type": "record",
                "name": "MyRecordA",
                "fields": [
                    {"name": "a", "type": "int"}
                ]
            },
            {
                "type": "record",
                "name": "MyRecordB",
                "fields": [
                    {"name": "a", "type": "int"}
                ]
            }
        ]
        "#,
        )
        .unwrap()
    }

    #[test]
    fn null_variant_enum_my_record_a_27() {
        let schema = schema();
        assert_roundtrip(&MyUnionNullable::Null, &schema);
        assert_roundtrip(&MyUnionNullable::MyRecordA(MyRecordA { a: 27 }), &schema);
        assert_roundtrip(&MyUnionNullable::MyRecordB(MyRecordB { a: 27 }), &schema);
    }

    #[test]
    fn rusty_my_record_a_27() {
        let schema = schema();
        assert_roundtrip(&None::<MyUnionAvroJsonEncoding>, &schema);
        assert_roundtrip(
            &Some(MyUnionAvroJsonEncoding::MyRecordA(MyRecordA { a: 27 })),
            &schema,
        );
        assert_roundtrip(
            &Some(MyUnionAvroJsonEncoding::MyRecordB(MyRecordB { a: 27 })),
            &schema,
        );
    }

    #[test]
    fn rusty_untagged_my_record_a_27() {
        let schema = schema();
        assert_roundtrip(&None::<MyUnionUntagged>, &schema);
        assert_roundtrip(
            &Some(MyUnionUntagged::MyRecordA(MyRecordA { a: 27 })),
            &schema,
        );
    }

    #[test]
    #[should_panic(expected = "assertion failed: `(left == right)`")]
    fn rusty_untagged_my_record_b_27() {
        // Because the untagged enum has two exactly the same fields, this will be correctly serialized
        // as MyRecordB, but incorrectly deserialized as MyRecordA. This is a limitation of Serde untagged.
        assert_roundtrip(
            &Some(MyUnionUntagged::MyRecordB(MyRecordB { a: 27 })),
            &schema(),
        );
    }
}
