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
fn avro_rs_53_uuid_with_fixed() -> TestResult {
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
                            "type" : "fixed",
                            "size" : 16,
                            "logicalType" : "uuid",
                            "name": "FixedUUID"
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

    // serialize the Uuid as Fixed
    assert!(!apache_avro::util::set_serde_human_readable(false));
    let bytes = SpecificSingleObjectWriter::<Comment>::with_capacity(64)?
        .write_ref(&payload, &mut buffer)?;
    assert_eq!(bytes, 26);

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

    assert!(!apache_avro::util::set_serde_human_readable(false));
    let mut writer = SpecificSingleObjectWriter::with_capacity(64)?;
    assert_eq!(
        writer.write(uuid, &mut buffer).unwrap_err().to_string(),
        "Failed to serialize value of type bytes using schema Uuid(String): 55e840e29b41d4a7164466554400. Cause: Expected a string, but got 16 bytes. Did you mean to use `Schema::Uuid(UuidSchema::Fixed)` or `utils::serde_set_human_readable(true)`?"
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

    assert!(!apache_avro::util::set_serde_human_readable(false));
    let mut writer = SpecificSingleObjectWriter::with_capacity(64)?;
    writer.write(uuid, &mut buffer)?;

    assert_eq!(
        buffer.as_slice(),
        &[
            195, 1, 46, 208, 56, 148, 57, 0, 104, 249, 32, 85, 14, 132, 0, 226, 155, 65, 212, 167,
            22, 68, 102, 85, 68, 0, 0
        ][..]
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

    assert!(!apache_avro::util::set_serde_human_readable(false));
    let mut writer = SpecificSingleObjectWriter::with_capacity(64)?;
    writer.write(uuid, &mut buffer)?;

    assert_eq!(
        buffer.as_slice(),
        &[
            195, 1, 22, 19, 155, 41, 216, 175, 73, 144, 85, 14, 132, 0, 226, 155, 65, 212, 167, 22,
            68, 102, 85, 68, 0, 0
        ][..]
    );

    Ok(())
}
