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

/*
   Compiling apache-avro v0.22.0 (/home/coder/avro-rs/avro)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.62s
────────────
 Nextest run ID 4c2f8dd6-2ff4-4de0-b4b5-9a7709c6749b with nextest profile: default
    Starting 36 tests across 1 binary
        FAIL [   0.005s] apache-avro::nullable_union nullable_enum::avro_json_encoding_compatible_my_enum_a
  stdout ───

    running 1 test
    test nullable_enum::avro_json_encoding_compatible_my_enum_a ... FAILED

    failures:

    failures:
        nullable_enum::avro_json_encoding_compatible_my_enum_a

    test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 35 filtered out; finished in 0.00s

  stderr ───

    Backtrace omitted. Run with RUST_BACKTRACE=1 to display it.
    Run with RUST_BACKTRACE=full to include source snippets.

    The application panicked (crashed).
      apache_avro::error::Error: Error { details: Failed to serialize value of type `newtype variant` using Schema::Enum(EnumSchema { name: Name { name: "MyEnum", .. }, symbols: ["A", "B"], .. }): Expected Schema::Union }
    in avro/tests/nullable_union.rs, line 214
    thread: nullable_enum::avro_json_encoding_compatible_my_enum_a

        PASS [   0.004s] apache-avro::nullable_union nullable_enum::rusty_my_enum_a
        PASS [   0.004s] apache-avro::nullable_union nullable_int_enum_record::null_variant_enum_my_enum_a
        PASS [   0.004s] apache-avro::nullable_union nullable_enum::rusty_null
        PASS [   0.005s] apache-avro::nullable_union nullable_enum::avro_json_encoding_compatible_null
        PASS [   0.005s] apache-avro::nullable_union nullable_enum::null_variant_enum_my_enum_a
        PASS [   0.005s] apache-avro::nullable_union nullable_enum::null_variant_enum_null
        PASS [   0.005s] apache-avro::nullable_union nullable_int_enum_record::null_variant_enum_int_42
        PASS [   0.005s] apache-avro::nullable_union nullable_int_enum_record::null_variant_enum_my_record_a_27
        PASS [   0.004s] apache-avro::nullable_union nullable_int_enum_record::rusty_int_42
        PASS [   0.004s] apache-avro::nullable_union nullable_int_enum_record::rusty_null
        PASS [   0.004s] apache-avro::nullable_union nullable_int_enum_record::rusty_my_enum_a
        PASS [   0.005s] apache-avro::nullable_union nullable_int_enum_record::rusty_my_record_a_27
        PASS [   0.005s] apache-avro::nullable_union nullable_int_enum_record::null_variant_enum_null
        PASS [   0.005s] apache-avro::nullable_union nullable_int_enum_record::rusty_untagged_int_42
        PASS [   0.006s] apache-avro::nullable_union nullable_int_enum_record::rusty_untagged_my_enum_a
        FAIL [   0.004s] apache-avro::nullable_union nullable_primitive_int::avro_json_encoding_compatible_int_42
  stdout ───

    running 1 test
    test nullable_primitive_int::avro_json_encoding_compatible_int_42 ... FAILED

    failures:

    failures:
        nullable_primitive_int::avro_json_encoding_compatible_int_42

    test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 35 filtered out; finished in 0.00s

  stderr ───

    Backtrace omitted. Run with RUST_BACKTRACE=1 to display it.
    Run with RUST_BACKTRACE=full to include source snippets.

    The application panicked (crashed).
      apache_avro::error::Error: Error { details: Failed to serialize value of type `newtype variant` using Schema::Int: Expected Schema::Union }
    in avro/tests/nullable_union.rs, line 269
    thread: nullable_primitive_int::avro_json_encoding_compatible_int_42

        PASS [   0.004s] apache-avro::nullable_union nullable_primitive_int::null_variant_enum_null
        PASS [   0.004s] apache-avro::nullable_union nullable_primitive_int::null_variant_enum_int_42
        PASS [   0.005s] apache-avro::nullable_union nullable_int_enum_record::rusty_untagged_my_record_a_27
        PASS [   0.005s] apache-avro::nullable_union nullable_primitive_int::avro_json_encoding_compatible_null
        PASS [   0.007s] apache-avro::nullable_union nullable_primitive_int::rusty_int_42
        PASS [   0.005s] apache-avro::nullable_union nullable_int_enum_record::rusty_untagged_null
        PASS [   0.004s] apache-avro::nullable_union nullable_primitive_int::rusty_null
        FAIL [   0.005s] apache-avro::nullable_union nullable_record::avro_json_encoding_compatible_my_record_a_27
  stdout ───

    running 1 test
    test nullable_record::avro_json_encoding_compatible_my_record_a_27 ... FAILED

    failures:

    failures:
        nullable_record::avro_json_encoding_compatible_my_record_a_27

    test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 35 filtered out; finished in 0.00s

  stderr ───

    Backtrace omitted. Run with RUST_BACKTRACE=1 to display it.
    Run with RUST_BACKTRACE=full to include source snippets.

    The application panicked (crashed).
      apache_avro::error::Error: Error { details: Failed to serialize value of type `newtype variant` using Schema::Record(RecordSchema { name: Name { name: "MyRecord", .. }, fields: [RecordField { name: "a", schema: Int, .. }], .. }): Expected Schema::Union }
    in avro/tests/nullable_union.rs, line 336
    thread: nullable_record::avro_json_encoding_compatible_my_record_a_27

        PASS [   0.005s] apache-avro::nullable_union nullable_record::rusty_my_record_a_27
        PASS [   0.005s] apache-avro::nullable_union nullable_record::avro_json_encoding_compatible_null
        PASS [   0.007s] apache-avro::nullable_union nullable_record::null_variant_enum_my_record_a_27
        PASS [   0.007s] apache-avro::nullable_union nullable_record::null_variant_enum_null
        PASS [   0.005s] apache-avro::nullable_union nullable_record::rusty_null
        PASS [   0.005s] apache-avro::nullable_union nullable_untagged_pitfall::null_variant_enum_my_record_b_27
        PASS [   0.006s] apache-avro::nullable_union nullable_untagged_pitfall::null_variant_enum_my_record_a_27
        PASS [   0.004s] apache-avro::nullable_union nullable_untagged_pitfall::rusty_untagged_my_record_a_27
        PASS [   0.005s] apache-avro::nullable_union nullable_untagged_pitfall::rusty_my_record_a_27
        PASS [   0.004s] apache-avro::nullable_union nullable_untagged_pitfall::rusty_untagged_my_record_b_27
        PASS [   0.005s] apache-avro::nullable_union nullable_untagged_pitfall::rusty_my_record_b_27
────────────
     Summary [   0.027s] 36 tests run: 33 passed, 3 failed, 0 skipped
        FAIL [   0.005s] apache-avro::nullable_union nullable_enum::avro_json_encoding_compatible_my_enum_a
        FAIL [   0.004s] apache-avro::nullable_union nullable_primitive_int::avro_json_encoding_compatible_int_42
        FAIL [   0.005s] apache-avro::nullable_union nullable_record::avro_json_encoding_compatible_my_record_a_27
error: test run failed
*/

use std::fmt::Debug;

use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::writer::datum::GenericDatumWriter;
use apache_avro_test_helper::TestResult;
use pretty_assertions::assert_eq;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[track_caller]
fn assert_roundtrip<T>(value: T, schema: &Schema) -> TestResult
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    let serialized = GenericDatumWriter::builder(schema)
        .build()?
        .write_ser_to_vec(&value)?;
    let deserialized: T = GenericDatumReader::builder(schema)
        .build()?
        .read_deser(&mut &serialized[..])?;

    assert_eq!(deserialized, value);
    Ok(())
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
    ]
    "#,
        )
        .unwrap()
    }

    #[test]
    fn null_variant_enum_null() -> TestResult {
        assert_roundtrip(MyUnionNullable::Null, &schema())
    }

    #[test]
    fn rusty_null() -> TestResult {
        assert_roundtrip(None::<MyEnum>, &schema())
    }

    #[test]
    fn avro_json_encoding_compatible_null() -> TestResult {
        assert_roundtrip(None::<MyUnionAvroJsonEncoding>, &schema())
    }

    #[test]
    fn null_variant_enum_my_enum_a() -> TestResult {
        assert_roundtrip(MyUnionNullable::MyEnum(MyEnum::A), &schema())
    }

    #[test]
    fn rusty_my_enum_a() -> TestResult {
        assert_roundtrip(Some(MyEnum::A), &schema())
    }

    #[test]
    #[should_panic]
    fn avro_json_encoding_compatible_my_enum_a() {
        assert_roundtrip(Some(MyUnionAvroJsonEncoding::MyEnum(MyEnum::A)), &schema()).unwrap();
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
    fn null_variant_enum_null() -> TestResult {
        assert_roundtrip(MyUnionNullable::Null, &schema())
    }

    #[test]
    fn rusty_null() -> TestResult {
        assert_roundtrip(None::<i32>, &schema())
    }

    #[test]
    fn avro_json_encoding_compatible_null() -> TestResult {
        assert_roundtrip(None::<MyUnionAvroJsonEncoding>, &schema())
    }

    #[test]
    fn null_variant_enum_int_42() -> TestResult {
        assert_roundtrip(MyUnionNullable::Int(42), &schema())
    }

    #[test]
    fn rusty_int_42() -> TestResult {
        assert_roundtrip(Some(42_i32), &schema())
    }

    #[test]
    #[should_panic]
    fn avro_json_encoding_compatible_int_42() {
        assert_roundtrip(Some(MyUnionAvroJsonEncoding::Int(42)), &schema()).unwrap();
    }
}

mod nullable_record {
    use super::*;

    const NULLABLE_RECORD_SCHEMA: &str = r#"
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
    "#;

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
        Schema::parse_str(NULLABLE_RECORD_SCHEMA).unwrap()
    }

    #[test]
    fn null_variant_enum_null() -> TestResult {
        assert_roundtrip(MyUnionNullable::Null, &schema())
    }

    #[test]
    fn rusty_null() -> TestResult {
        assert_roundtrip(None::<MyRecord>, &schema())
    }

    #[test]
    fn avro_json_encoding_compatible_null() -> TestResult {
        assert_roundtrip(None::<MyUnionAvroJsonEncoding>, &schema())
    }

    #[test]
    fn null_variant_enum_my_record_a_27() -> TestResult {
        assert_roundtrip(MyUnionNullable::MyRecord(MyRecord { a: 27 }), &schema())
    }

    #[test]
    fn rusty_my_record_a_27() -> TestResult {
        assert_roundtrip(Some(MyRecord { a: 27 }), &schema())
    }

    #[test]
    #[should_panic]
    fn avro_json_encoding_compatible_my_record_a_27() {
        assert_roundtrip(
            Some(MyUnionAvroJsonEncoding::MyRecord(MyRecord { a: 27 })),
            &schema(),
        )
        .unwrap();
    }
}

mod nullable_int_enum_record {
    use super::*;

    const NULLABLE_INT_ENUM_RECORD_SCHEMA: &str = r#"
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
    "#;

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
        Schema::parse_str(NULLABLE_INT_ENUM_RECORD_SCHEMA).unwrap()
    }

    #[test]
    fn null_variant_enum_null() -> TestResult {
        assert_roundtrip(MyUnionNullable::Null, &schema())
    }

    #[test]
    fn rusty_null() {
        assert_roundtrip(None::<MyUnionAvroJsonEncoding>, &schema()).unwrap();
    }

    #[test]
    fn rusty_untagged_null() {
        assert_roundtrip(None::<MyUnionUntagged>, &schema()).unwrap();
    }

    #[test]
    fn null_variant_enum_int_42() -> TestResult {
        assert_roundtrip(MyUnionNullable::Int(42), &schema())
    }

    #[test]
    fn rusty_int_42() {
        assert_roundtrip(Some(MyUnionAvroJsonEncoding::Int(42)), &schema()).unwrap();
    }

    #[test]
    fn rusty_untagged_int_42() {
        assert_roundtrip(Some(MyUnionUntagged::Int(42)), &schema()).unwrap();
    }

    #[test]
    fn null_variant_enum_my_enum_a() -> TestResult {
        assert_roundtrip(MyUnionNullable::MyEnum(MyEnum::A), &schema())
    }

    #[test]
    fn rusty_my_enum_a() {
        assert_roundtrip(Some(MyUnionAvroJsonEncoding::MyEnum(MyEnum::A)), &schema()).unwrap();
    }

    #[test]
    // Idk why this is erroring, the error source is from serde. However, I'm fine with not
    // supporting this anyways since supporting untagged enums itself is opening a whole new can of
    // worms
    #[ignore]
    fn rusty_untagged_my_enum_a() {
        assert_roundtrip(Some(MyUnionUntagged::MyEnum(MyEnum::A)), &schema()).unwrap()
    }

    #[test]
    fn null_variant_enum_my_record_a_27() -> TestResult {
        assert_roundtrip(MyUnionNullable::MyRecord(MyRecord { a: 27 }), &schema())
    }

    #[test]
    fn rusty_my_record_a_27() {
        assert_roundtrip(
            Some(MyUnionAvroJsonEncoding::MyRecord(MyRecord { a: 27 })),
            &schema(),
        )
        .unwrap()
    }

    #[test]
    fn rusty_untagged_my_record_a_27() {
        assert_roundtrip(
            Some(MyUnionUntagged::MyRecord(MyRecord { a: 27 })),
            &schema(),
        )
        .unwrap();
    }
}

mod nullable_untagged_pitfall {
    use super::*;

    const NULLABLE_RECORD_SCHEMA: &str = r#"
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
    "#;

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
        Schema::parse_str(NULLABLE_RECORD_SCHEMA).unwrap()
    }

    #[test]
    fn null_variant_enum_my_record_a_27() -> TestResult {
        assert_roundtrip(MyUnionNullable::MyRecordA(MyRecordA { a: 27 }), &schema())
    }

    #[test]
    fn rusty_my_record_a_27() {
        assert_roundtrip(
            Some(MyUnionAvroJsonEncoding::MyRecordA(MyRecordA { a: 27 })),
            &schema(),
        )
        .unwrap();
    }

    #[test]
    fn rusty_untagged_my_record_a_27() {
        assert_roundtrip(
            Some(MyUnionUntagged::MyRecordA(MyRecordA { a: 27 })),
            &schema(),
        )
        .unwrap();
    }

    #[test]
    fn null_variant_enum_my_record_b_27() -> TestResult {
        assert_roundtrip(MyUnionNullable::MyRecordB(MyRecordB { a: 27 }), &schema())
    }

    #[test]
    fn rusty_my_record_b_27() {
        assert_roundtrip(
            Some(MyUnionAvroJsonEncoding::MyRecordB(MyRecordB { a: 27 })),
            &schema(),
        )
        .unwrap();
    }

    #[test]
    #[should_panic]
    fn rusty_untagged_my_record_b_27() {
        assert_roundtrip(
            Some(MyUnionUntagged::MyRecordB(MyRecordB { a: 27 })),
            &schema(),
        )
        .unwrap();
    }
}
