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

use apache_avro_test_helper::TestResult;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct ExampleByteArray {
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    data_bytes: Option<Vec<u8>>,
    description: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct ExampleByteArrayFiltered {
    description: Option<String>,
}

#[test]
fn avro_rs_285_bytes_deserialization_round_trip() -> TestResult {
    // define schema
    let raw_schema = r#"
    {
        "type": "record",
        "name": "SimpleRecord",
        "fields": [
            {"name": "data_bytes", "type": ["null", "bytes"], "default": null},
            {"name": "description", "type": ["null", "string"], "default": null}
        ]
    }
    "#;

    let schema = apache_avro::Schema::parse_str(raw_schema)?;

    let records = vec![
        ExampleByteArray {
            data_bytes: Some(vec![1, 2, 3, 4, 5]),
            description: Some("First record".to_string()),
        },
        ExampleByteArray {
            data_bytes: None,
            description: Some("Second record".to_string()),
        },
        ExampleByteArray {
            data_bytes: Some(vec![10, 20, 30]),
            description: None,
        },
    ];

    // serialize records to Avro binary format with schema
    let mut writer = apache_avro::Writer::new(&schema, Vec::new())?;
    for record in &records {
        writer.append_ser(record)?;
    }

    let avro_data = writer.into_inner()?;

    // deserialize Avro binary data back into ExampleByteArray structs
    let reader = apache_avro::Reader::new(&avro_data[..])?;
    let deserialized_records: Vec<ExampleByteArray> = reader
        .map(|value| apache_avro::from_value::<ExampleByteArray>(&value.unwrap()).unwrap())
        .collect();

    assert_eq!(records, deserialized_records);
    Ok(())
}

#[test]
fn avro_rs_285_bytes_deserialization_filtered_round_trip() -> TestResult {
    let raw_schema = r#"
    {
        "type": "record",
        "name": "SimpleRecord",
        "fields": [
            {"name": "data_bytes", "type": ["null", "bytes"], "default": null},
            {"name": "description", "type": ["null", "string"], "default": null}
        ]
    }
    "#;

    let schema = apache_avro::Schema::parse_str(raw_schema)?;

    let records = vec![
        ExampleByteArray {
            data_bytes: Some(vec![1, 2, 3, 4, 5]),
            description: Some("First record".to_string()),
        },
        ExampleByteArray {
            data_bytes: None,
            description: Some("Second record".to_string()),
        },
        ExampleByteArray {
            data_bytes: Some(vec![10, 20, 30]),
            description: None,
        },
    ];

    // serialize records to Avro binary format with schema
    let mut writer = apache_avro::Writer::new(&schema, Vec::new())?;
    for record in &records {
        writer.append_ser(record)?;
    }

    let avro_data = writer.into_inner()?;

    // deserialize Avro binary data back into ExampleByteArrayFiltered structs
    let reader = apache_avro::Reader::new(&avro_data[..])?;
    let deserialized_records: Vec<ExampleByteArrayFiltered> = reader
        .map(|value| apache_avro::from_value::<ExampleByteArrayFiltered>(&value.unwrap()).unwrap())
        .collect();

    assert_eq!(records.len(), deserialized_records.len());

    Ok(())
}
