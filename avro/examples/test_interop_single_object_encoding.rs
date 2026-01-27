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

use apache_avro::{serde::AvroSchema, types::Value};
use std::error::Error;

struct InteropMessage;

impl AvroSchema for InteropMessage {
    fn get_schema() -> apache_avro::Schema {
        let interop_root_folder: String = std::env::var("INTEROP_ROOT_FOLDER")
            .expect("INTEROP_ROOT_FOLDER env var should be set");
        let resource_folder = format!("{interop_root_folder}/share/test/data/messageV1");

        let schema = std::fs::read_to_string(format!("{resource_folder}/test_schema.avsc"))
            .expect("File should exist with schema inside");
        apache_avro::Schema::parse_str(schema.as_str())
            .expect("File should exist with schema inside")
    }
}

impl From<InteropMessage> for Value {
    fn from(_: InteropMessage) -> Value {
        Value::Record(vec![
            ("id".into(), 42i64.into()),
            ("name".into(), "Bill".into()),
            (
                "tags".into(),
                Value::Array(
                    vec!["dog_lover", "cat_hater"]
                        .into_iter()
                        .map(|s| s.into())
                        .collect(),
                ),
            ),
        ])
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let interop_root_folder: String = std::env::var("INTEROP_ROOT_FOLDER")?;
    let resource_folder = format!("{interop_root_folder}/share/test/data/messageV1");

    let single_object = std::fs::read(format!("{resource_folder}/test_message.bin"))
        .expect("File with single object not found or error occurred while reading it.");
    test_write(&single_object);
    test_read(single_object);

    Ok(())
}

fn test_write(expected: &[u8]) {
    let mut encoded: Vec<u8> = Vec::new();
    apache_avro::SpecificSingleObjectWriter::<InteropMessage>::new()
        .expect("Resolving failed")
        .write_value(InteropMessage, &mut encoded)
        .expect("Encoding failed");
    assert_eq!(expected, &encoded)
}

fn test_read(encoded: Vec<u8>) {
    let mut encoded = &encoded[..];
    let read_message = apache_avro::GenericSingleObjectReader::new(InteropMessage::get_schema())
        .expect("Resolving failed")
        .read_value(&mut encoded)
        .expect("Decoding failed");
    let expected_value: Value = InteropMessage.into();
    assert_eq!(expected_value, read_message)
}
