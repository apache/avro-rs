use apache_avro::{AvroSchema, Schema, SpecificSingleObjectWriter};
use apache_avro_test_helper::TestResult;
use serde::{Deserialize, Serialize};

#[test]
fn avro_rs_53_uuid_with_string_true() -> TestResult {
    #[derive(Debug, Serialize, Deserialize)]
    struct Comment {
        id: apache_avro::Uuid,
    }

    impl AvroSchema for Comment {
        fn get_schema() -> Schema {
            Schema::parse_str(
                r#"{
                        "type" : "record",
                        "name" : "Comment",
                        "fields" : [ {
                          "name" : "id",
                          "type" : {
                            "type" : "string",
                            "logicalType" : "uuid",
                            "name": "StringUUID"
                          }
                        } ]
                     }"#,
            )
            .expect("Invalid Comment Avro schema")
        }
    }

    let payload = Comment {
        id: "de2df598-9948-4988-b00a-a41c0e287398".parse()?,
    };
    let mut buffer = Vec::new();

    // serialize the Uuid as String
    assert!(apache_avro::util::set_serde_human_readable(true));
    let bytes = SpecificSingleObjectWriter::<Comment>::with_capacity(64)?
        .write_ref(&payload, &mut buffer)?;
    assert_eq!(bytes, 47);

    Ok(())
}
