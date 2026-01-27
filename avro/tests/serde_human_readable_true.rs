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

use apache_avro::{AvroSchema, Schema, SpecificSingleObjectWriter, schema::UuidSchema};
use apache_avro_test_helper::TestResult;
use pretty_assertions::assert_eq;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    let bytes = SpecificSingleObjectWriter::<Comment>::new()?.write_ref(&payload, &mut buffer)?;
    assert_eq!(bytes, 47);

    Ok(())
}

#[test]
fn avro_rs_440_uuid_string() -> TestResult {
    #[derive(apache_avro_derive::AvroSchema, Serialize, Deserialize)]
    #[serde(transparent)]
    struct CustomUuid {
        #[avro(with = || Schema::Uuid(UuidSchema::String))]
        inner: Uuid,
    }
    let uuid = CustomUuid {
        inner: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?,
    };
    let mut buffer = Vec::new();

    assert!(apache_avro::util::set_serde_human_readable(true));
    let writer = SpecificSingleObjectWriter::new()?;
    writer.write(uuid, &mut buffer)?;

    assert_eq!(
        String::from_utf8_lossy(&buffer),
        "�\u{1}'G�8�[\u{4}�H550e8400-e29b-41d4-a716-446655440000"
    );

    Ok(())
}

#[test]
fn avro_rs_440_uuid_bytes() -> TestResult {
    #[derive(apache_avro_derive::AvroSchema, Serialize, Deserialize)]
    #[serde(transparent)]
    struct CustomUuid {
        #[avro(with = || Schema::Uuid(UuidSchema::Bytes))]
        inner: Uuid,
    }
    let uuid = CustomUuid {
        inner: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?,
    };
    let mut buffer = Vec::new();

    assert!(apache_avro::util::set_serde_human_readable(true));
    let writer = SpecificSingleObjectWriter::new()?;
    assert_eq!(
        writer.write(uuid, &mut buffer).unwrap_err().to_string(),
        "Failed to serialize value of type string using schema Uuid(Bytes): 550e8400-e29b-41d4-a716-446655440000. Cause: Expected bytes but got a string. Did you mean to use `Schema::Uuid(UuidSchema::String)` or `utils::serde_set_human_readable(false)`?"
    );

    Ok(())
}

#[test]
fn avro_rs_440_uuid_fixed() -> TestResult {
    #[derive(apache_avro_derive::AvroSchema, Serialize, Deserialize)]
    #[serde(transparent)]
    struct CustomUuid {
        inner: Uuid,
    }
    assert!(matches!(
        CustomUuid::get_schema(),
        Schema::Uuid(UuidSchema::Fixed(_))
    ));
    let uuid = CustomUuid {
        inner: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?,
    };
    let mut buffer = Vec::new();

    assert!(apache_avro::util::set_serde_human_readable(true));
    let writer = SpecificSingleObjectWriter::new()?;
    assert_eq!(
        writer.write(uuid, &mut buffer).unwrap_err().to_string(),
        r#"Failed to serialize value of type string using schema Uuid(Fixed(FixedSchema { name: Name { name: "uuid", namespace: None }, aliases: None, doc: None, size: 16, default: None, attributes: {} })): 550e8400-e29b-41d4-a716-446655440000. Cause: Expected bytes but got a string. Did you mean to use `Schema::Uuid(UuidSchema::String)` or `utils::serde_set_human_readable(false)`?"#
    );

    Ok(())
}
