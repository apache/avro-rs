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

use apache_avro::{AvroSchema, Schema, Writer, from_value};
use apache_avro_test_helper::TestResult;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt::Debug;

fn ser_deser<T>(schema: &Schema, record: T) -> TestResult
where
    T: Serialize + DeserializeOwned + Debug + PartialEq + Clone,
{
    let record2 = record.clone();
    let mut writer = Writer::new(schema, vec![]);
    writer.append_ser(record)?;
    let bytes_written = writer.into_inner()?;

    let reader = apache_avro::Reader::new(&bytes_written[..])?;
    for value in reader {
        let value = value?;
        let deserialized = from_value::<T>(&value)?;
        assert_eq!(deserialized, record2);
    }

    Ok(())
}

#[test]
fn avro_rs_226_index_out_of_bounds_with_serde_skip_serializing_skip_middle_field() -> TestResult {
    #[derive(AvroSchema, Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct T {
        x: Option<i8>,
        #[serde(skip_serializing_if = "Option::is_none")]
        y: Option<String>,
        z: Option<i8>,
    }

    ser_deser::<T>(
        &T::get_schema(),
        T {
            x: None,
            y: None,
            z: Some(1),
        },
    )
}

#[test]
fn avro_rs_226_index_out_of_bounds_with_serde_skip_serializing_skip_first_field() -> TestResult {
    #[derive(AvroSchema, Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct T {
        #[serde(skip_serializing_if = "Option::is_none")]
        x: Option<i8>,
        y: Option<String>,
        z: Option<i8>,
    }

    ser_deser::<T>(
        &T::get_schema(),
        T {
            x: None,
            y: None,
            z: Some(1),
        },
    )
}

#[test]
fn avro_rs_226_index_out_of_bounds_with_serde_skip_serializing_skip_last_field() -> TestResult {
    #[derive(AvroSchema, Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct T {
        x: Option<i8>,
        y: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        z: Option<i8>,
    }

    ser_deser::<T>(
        &T::get_schema(),
        T {
            x: Some(0),
            y: None,
            z: None,
        },
    )
}

#[test]
#[ignore = "This test should be re-enabled once the serde-driven deserialization is implemented! PR #227"]
fn avro_rs_226_index_out_of_bounds_with_serde_skip_multiple_fields() -> TestResult {
    #[derive(AvroSchema, Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct T {
        no_skip1: Option<i8>,
        #[serde(skip_serializing)]
        skip_serializing: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        skip_serializing_if: Option<i8>,
        #[serde(skip_deserializing)]
        skip_deserializing: Option<String>,
        #[serde(skip)]
        skip: Option<String>,
        no_skip2: Option<i8>,
    }

    ser_deser::<T>(
        &T::get_schema(),
        T {
            no_skip1: Some(1),
            skip_serializing: None,
            skip_serializing_if: None,
            skip_deserializing: None,
            skip: None,
            no_skip2: Some(2),
        },
    )
}
