use apache_avro::{AvroResult, Schema};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct Root {
    field_union: Enum,
    field_f: String,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
enum Enum {
    A {},
    B {},
    C {
        field_a: i64,
        field_b: Option<String>,
    },
    D {
        field_a: f32,
        field_b: i32,
    },
}

const SCHEMA_STR: &str = r#"{
    "name": "Root",
    "type": "record",
    "fields": [
        {"name": "field_union", "type": [
            {
                "name": "A",
                "type": "record",
                "fields": []
            },
            {
                "name": "B",
                "type": "record",
                "fields": []
            },
            {
                "name": "C",
                "type": "record",
                "fields": [
                    {"name": "field_a", "type": "long"},
                    {"name": "field_b", "type": ["null", "string"]}
                ]
            },
            {
                "name": "D",
                "type": "record",
                "fields": [
                    {"name": "field_a", "type": "float"},
                    {"name": "field_b", "type": "int"}
                ]
            }
        ]},
        {"name": "field_f", "type": "string"}
    ]
}"#;

#[test]
fn test_union_variants_serialization() -> AvroResult<()> {
    let schema = Schema::parse_str(SCHEMA_STR)?;

    // Test variant 0
    {
        let input = Root {
            field_union: Enum::A {},
            field_f: "test1".to_owned(),
        };

        #[rustfmt::skip]
        let expected_bytes: [u8; 7] = [
            // Root {
                // field_union:
                    0x00, // variant 0 (Enum::A) {
                    // }
                // field_f:
                    0x0A, // string length = 5
                    0x74, 0x65, 0x73, 0x74, 0x31, // UTF-8 string "test1"
            // }
        ];

        let value = apache_avro::to_value(&input)?.resolve(&schema)?;
        let encoded = apache_avro::to_avro_datum(&schema, value)?;

        assert_eq!(encoded, expected_bytes);

        let value = apache_avro::from_avro_datum(&schema, &mut encoded.as_slice(), None)?;
        let output: Root = apache_avro::from_value(&value)?;

        assert_eq!(input, output);
    }

    // Test variant 1
    {
        let input = Root {
            field_union: Enum::B {},
            field_f: "test2".to_owned(),
        };

        #[rustfmt::skip]
        let expected_bytes: [u8; 7] = [
            // Root {
                // field_union:
                    0x02, // variant 1 (Enum::B) {
                    // }
                // field_f:
                    0x0A, // string length = 5
                    0x74, 0x65, 0x73, 0x74, 0x32, // UTF-8 string "test2"
            // }
        ];

        let value = apache_avro::to_value(&input)?.resolve(&schema)?;
        let encoded = apache_avro::to_avro_datum(&schema, value)?;

        assert_eq!(encoded, expected_bytes);

        let value = apache_avro::from_avro_datum(&schema, &mut encoded.as_slice(), None)?;
        let output: Root = apache_avro::from_value(&value)?;

        assert_eq!(input, output);
    }

    // Test variant 2
    {
        let input = Root {
            field_union: Enum::C {
                field_a: 3,
                field_b: Some("test3".to_owned()),
            },
            field_f: "test4".to_owned(),
        };

        #[rustfmt::skip]
        let expected_bytes: [u8; 15] = [
            // Root {
                // field_union:
                    0x04, // variant 2 (Enum::C) {
                        // field_a:
                            0x06, // 3
                        // field_b:
                            0x02, // variant 1 (Some) {
                                0x0A, // string length = 5
                                0x74, 0x65, 0x73, 0x74, 0x33, // UTF-8 string "test3"
                            // }
                    // }
                // field_f:
                    0x0A, // string length = 5
                    0x74, 0x65, 0x73, 0x74, 0x34, // UTF-8 string "test4"
            // }
        ];

        let value = apache_avro::to_value(&input)?.resolve(&schema)?;
        let encoded = apache_avro::to_avro_datum(&schema, value)?;

        assert_eq!(encoded, expected_bytes);

        let value = apache_avro::from_avro_datum(&schema, &mut encoded.as_slice(), None)?;
        let output: Root = apache_avro::from_value(&value)?;

        assert_eq!(input, output);
    }

    // Test variant 3
    {
        let input = Root {
            field_union: Enum::D {
                field_a: 0.0,
                field_b: 4,
            },
            field_f: "test5".to_owned(),
        };

        #[rustfmt::skip]
        let expected_bytes: [u8; 12] = [
            // Root {
                // field_union:
                    0x06, // variant 3 (Enum::D) {
                        // field_a:
                            0x00, 0x00, 0x00, 0x00, // 0.0
                        // field_b:
                            0x08, // 4
                    // }
                // field_f:
                    0x0A, // string length = 5
                    0x74, 0x65, 0x73, 0x74, 0x35, // UTF-8 string "test5"
            // }
        ];

        let value = apache_avro::to_value(&input)?.resolve(&schema)?;
        let encoded = apache_avro::to_avro_datum(&schema, value)?;

        assert_eq!(encoded, expected_bytes);

        let value = apache_avro::from_avro_datum(&schema, &mut encoded.as_slice(), None)?;
        let output: Root = apache_avro::from_value(&value)?;

        assert_eq!(input, output);
    }

    Ok(())
}
