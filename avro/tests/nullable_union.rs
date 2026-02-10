use apache_avro::{AvroResult, Reader, Schema, Writer, from_value, types::Value};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

struct TestCase<'a, T> {
    input: T,
    schema: &'a Schema,
    expected_avro: &'a Value,
}

fn test_serialize<T>(test_case: TestCase<T>) -> AvroResult<()>
where
    T: Serialize + std::fmt::Debug,
{
    let mut writer = Writer::new(test_case.schema, Vec::new())?;
    writer.append_ser(&test_case.input)?;
    let bytes = writer.into_inner()?;
    let mut reader = Reader::with_schema(test_case.schema, &bytes[..])?;
    let read_avro_value = reader.next().unwrap()?;
    assert_eq!(
        &read_avro_value, test_case.expected_avro,
        "serialization is not correct: expected: {:?}, got: {:?}, input: {:?}",
        test_case.expected_avro, read_avro_value, test_case.input
    );
    Ok(())
}

fn test_deserialize<T>(test_case: TestCase<T>) -> AvroResult<()>
where
    T: DeserializeOwned + std::fmt::Debug + std::cmp::PartialEq,
{
    let deserialized: T = from_value(test_case.expected_avro)?;
    assert_eq!(
        deserialized, test_case.input,
        "deserialization is not correct: expected: {:?}, got: {:?}",
        test_case.input, deserialized,
    );
    Ok(())
}

mod nullable_enum {
    use super::*;

    const NULLABLE_ENUM_SCHEMA: &str = r#"
    {
        "name": "MyUnion",
        "type": [
            "null",
            {
                "type": "enum",
                "name": "MyEnum",
                "symbols": ["A", "B"]
            }
        ]
    }
    "#;

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
        Schema::parse_str(NULLABLE_ENUM_SCHEMA).unwrap()
    }

    fn null_variant_expected_avro() -> Value {
        Value::Union(0, Box::new(Value::Null))
    }

    fn a_variant_expected_avro() -> Value {
        Value::Union(1, Box::new(Value::Enum(0, "A".to_string())))
    }

    #[test]
    fn serialize_null_variant_enum_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::Null,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::Null,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_rusty_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: None::<MyEnum>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_rusty_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: None::<MyEnum>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_avro_json_encoding_compatible_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: None::<MyUnionAvroJsonEncoding>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_avro_json_encoding_compatible_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: None::<MyUnionAvroJsonEncoding>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_null_variant_enum_my_enum_a() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::MyEnum(MyEnum::A),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_my_enum_a() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::MyEnum(MyEnum::A),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_rusty_my_enum_a() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyEnum::A),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_rusty_my_enum_a() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyEnum::A),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_avro_json_encoding_compatible_my_enum_a() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyEnum(MyEnum::A)),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_avro_json_encoding_compatible_my_enum_a() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyEnum(MyEnum::A)),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }
}

mod nullable_primitive_int {
    use super::*;

    const NULLABLE_INT_SCHEMA: &str = r#"
    {
        "name": "MyUnion",
        "type": [
            "null",
            "int"
        ]
    }
    "#;

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
        Schema::parse_str(NULLABLE_INT_SCHEMA).unwrap()
    }

    fn null_variant_expected_avro() -> Value {
        Value::Union(0, Box::new(Value::Null))
    }

    fn int_variant_expected_avro(v: i32) -> Value {
        Value::Union(1, Box::new(Value::Int(v)))
    }

    #[test]
    fn serialize_null_variant_enum_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::Null,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::Null,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_rusty_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: None::<i32>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_rusty_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: None::<i32>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_avro_json_encoding_compatible_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: None::<MyUnionAvroJsonEncoding>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_avro_json_encoding_compatible_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: None::<MyUnionAvroJsonEncoding>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_null_variant_enum_int_42() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::Int(42),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_int_42() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::Int(42),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }

    #[test]
    fn serialize_rusty_int_42() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(42_i32),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }

    #[test]
    fn deserialize_rusty_int_42() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(42_i32),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }

    #[test]
    fn serialize_avro_json_encoding_compatible_int_42() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::Int(42)),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }

    #[test]
    fn deserialize_avro_json_encoding_compatible_int_42() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::Int(42)),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }
}

mod nullable_record {
    use super::*;

    const NULLABLE_RECORD_SCHEMA: &str = r#"
    {
        "name": "MyUnion",
        "type": [
            "null",
            {
                "type": "record",
                "name": "MyRecord",
                "fields": [
                    {"name": "a", "type": "int"}
                ]
            }
        ]
    }
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

    fn null_variant_expected_avro() -> Value {
        Value::Union(0, Box::new(Value::Null))
    }

    fn record_variant_expected_avro(a: i32) -> Value {
        Value::Union(
            1,
            Box::new(Value::Record(vec![("a".to_string(), Value::Int(a))])),
        )
    }

    #[test]
    fn serialize_null_variant_enum_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::Null,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::Null,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_rusty_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: None::<MyRecord>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_rusty_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: None::<MyRecord>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_avro_json_encoding_compatible_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: None::<MyUnionAvroJsonEncoding>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_avro_json_encoding_compatible_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: None::<MyUnionAvroJsonEncoding>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_null_variant_enum_my_record_a_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::MyRecord(MyRecord { a: 27 }),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_my_record_a_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::MyRecord(MyRecord { a: 27 }),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }

    #[test]
    fn serialize_rusty_my_record_a_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyRecord { a: 27 }),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }

    #[test]
    fn deserialize_rusty_my_record_a_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyRecord { a: 27 }),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }

    #[test]
    fn serialize_avro_json_encoding_compatible_my_record_a_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyRecord(MyRecord { a: 27 })),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }

    #[test]
    fn deserialize_avro_json_encoding_compatible_my_record_a_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyRecord(MyRecord { a: 27 })),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }
}

mod nullable_int_enum_record {
    use super::*;

    const NULLABLE_INT_ENUM_RECORD_SCHEMA: &str = r#"
    {
        "name": "MyUnion",
        "type": [
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
    }
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

    fn null_variant_expected_avro() -> Value {
        Value::Union(0, Box::new(Value::Null))
    }

    fn int_variant_expected_avro(v: i32) -> Value {
        Value::Union(1, Box::new(Value::Int(v)))
    }

    fn a_variant_expected_avro() -> Value {
        Value::Union(2, Box::new(Value::Enum(0, "A".to_string())))
    }

    fn record_variant_expected_avro(a: i32) -> Value {
        Value::Union(
            3,
            Box::new(Value::Record(vec![("a".to_string(), Value::Int(a))])),
        )
    }

    #[test]
    fn serialize_null_variant_enum_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::Null,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::Null,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_rusty_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: None::<MyUnionAvroJsonEncoding>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_rusty_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: None::<MyUnionAvroJsonEncoding>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_rusty_untagged_null() -> AvroResult<()> {
        test_serialize(TestCase {
            input: None::<MyUnionUntagged>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_rusty_untagged_null() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: None::<MyUnionUntagged>,
            schema: &schema(),
            expected_avro: &null_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_null_variant_enum_int_42() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::Int(42),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_int_42() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::Int(42),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }

    #[test]
    fn serialize_rusty_int_42() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::Int(42)),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }

    #[test]
    fn deserialize_rusty_int_42() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::Int(42)),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }

    #[test]
    fn serialize_rusty_untagged_int_42() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionUntagged::Int(42)),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }

    #[test]
    fn deserialize_rusty_untagged_int_42() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionUntagged::Int(42)),
            schema: &schema(),
            expected_avro: &int_variant_expected_avro(42),
        })
    }

    #[test]
    fn serialize_null_variant_enum_my_enum_a() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::MyEnum(MyEnum::A),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_my_enum_a() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::MyEnum(MyEnum::A),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_rusty_my_enum_a() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyEnum(MyEnum::A)),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_rusty_my_enum_a() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyEnum(MyEnum::A)),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_rusty_untagged_my_enum_a() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionUntagged::MyEnum(MyEnum::A)),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }

    #[test]
    fn deserialize_rusty_untagged_my_enum_a() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionUntagged::MyEnum(MyEnum::A)),
            schema: &schema(),
            expected_avro: &a_variant_expected_avro(),
        })
    }

    #[test]
    fn serialize_null_variant_enum_my_record_a_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::MyRecord(MyRecord { a: 27 }),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_my_record_a_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::MyRecord(MyRecord { a: 27 }),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }

    #[test]
    fn serialize_rusty_my_record_a_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyRecord(MyRecord { a: 27 })),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }

    #[test]
    fn deserialize_rusty_my_record_a_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyRecord(MyRecord { a: 27 })),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }

    #[test]
    fn serialize_rusty_untagged_my_record_a_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionUntagged::MyRecord(MyRecord { a: 27 })),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }

    #[test]
    fn deserialize_rusty_untagged_my_record_a_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionUntagged::MyRecord(MyRecord { a: 27 })),
            schema: &schema(),
            expected_avro: &record_variant_expected_avro(27),
        })
    }
}

mod nullable_untagged_pitfall {
    use super::*;

    const NULLABLE_RECORD_SCHEMA: &str = r#"
    {
        "name": "MyUnion",
        "type": [
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
    }
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

    fn record_a_variant_expected_avro(a: i32) -> Value {
        Value::Union(
            1,
            Box::new(Value::Record(vec![("a".to_string(), Value::Int(a))])),
        )
    }

    fn record_b_variant_expected_avro(a: i32) -> Value {
        Value::Union(
            2,
            Box::new(Value::Record(vec![("a".to_string(), Value::Int(a))])),
        )
    }

    #[test]
    fn serialize_null_variant_enum_my_record_a_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::MyRecordA(MyRecordA { a: 27 }),
            schema: &schema(),
            expected_avro: &record_a_variant_expected_avro(27),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_my_record_a_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::MyRecordA(MyRecordA { a: 27 }),
            schema: &schema(),
            expected_avro: &record_a_variant_expected_avro(27),
        })
    }

    #[test]
    fn serialize_rusty_my_record_a_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyRecordA(MyRecordA { a: 27 })),
            schema: &schema(),
            expected_avro: &record_a_variant_expected_avro(27),
        })
    }

    #[test]
    fn deserialize_rusty_my_record_a_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyRecordA(MyRecordA { a: 27 })),
            schema: &schema(),
            expected_avro: &record_a_variant_expected_avro(27),
        })
    }

    #[test]
    fn serialize_rusty_untagged_my_record_a_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionUntagged::MyRecordA(MyRecordA { a: 27 })),
            schema: &schema(),
            expected_avro: &record_a_variant_expected_avro(27),
        })
    }

    #[test]
    fn deserialize_rusty_untagged_my_record_a_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionUntagged::MyRecordA(MyRecordA { a: 27 })),
            schema: &schema(),
            expected_avro: &record_a_variant_expected_avro(27),
        })
    }

    #[test]
    fn serialize_null_variant_enum_my_record_b_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: MyUnionNullable::MyRecordB(MyRecordB { a: 27 }),
            schema: &schema(),
            expected_avro: &record_b_variant_expected_avro(27),
        })
    }

    #[test]
    fn deserialize_null_variant_enum_my_record_b_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: MyUnionNullable::MyRecordB(MyRecordB { a: 27 }),
            schema: &schema(),
            expected_avro: &record_b_variant_expected_avro(27),
        })
    }

    #[test]
    fn serialize_rusty_my_record_b_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyRecordB(MyRecordB { a: 27 })),
            schema: &schema(),
            expected_avro: &record_b_variant_expected_avro(27),
        })
    }

    #[test]
    fn deserialize_rusty_my_record_b_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionAvroJsonEncoding::MyRecordB(MyRecordB { a: 27 })),
            schema: &schema(),
            expected_avro: &record_b_variant_expected_avro(27),
        })
    }

    #[test]
    fn serialize_rusty_untagged_my_record_b_27() -> AvroResult<()> {
        test_serialize(TestCase {
            input: Some(MyUnionUntagged::MyRecordB(MyRecordB { a: 27 })),
            schema: &schema(),
            expected_avro: &record_b_variant_expected_avro(27),
        })
    }

    #[test]
    #[ignore]
    fn deserialize_rusty_untagged_my_record_b_27() -> AvroResult<()> {
        test_deserialize(TestCase {
            input: Some(MyUnionUntagged::MyRecordB(MyRecordB { a: 27 })),
            schema: &schema(),
            expected_avro: &record_b_variant_expected_avro(27),
        })
    }
}
