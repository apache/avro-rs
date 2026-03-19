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

use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::writer::datum::GenericDatumWriter;
use apache_avro::{Codec, Reader, Schema, Writer, types::Value};
use apache_avro_test_helper::{TestResult, init};

static SCHEMA_A_STR: &str = r#"{
        "name": "A",
        "type": "record",
        "fields": [
            {"name": "field_a", "type": "float"}
        ]
    }"#;

static SCHEMA_B_STR: &str = r#"{
        "name": "B",
        "type": "record",
        "fields": [
            {"name": "field_b", "type": "A"}
        ]
    }"#;

#[test]
fn test_avro_3683_multiple_schemata_to_from_avro_datum() -> TestResult {
    init();

    let record: Value = Value::Record(vec![(
        String::from("field_b"),
        Value::Record(vec![(String::from("field_a"), Value::Float(1.0))]),
    )]);

    let schema_a = Schema::parse_str(SCHEMA_A_STR)?;
    let schema_b = Schema::parse_str(SCHEMA_B_STR)?;

    let expected: Vec<u8> = vec![0, 0, 128, 63];
    let actual = GenericDatumWriter::builder(&schema_b)
        .schemata(vec![&schema_a])?
        .build()?
        .write_value_to_vec(record.clone())?;
    assert_eq!(actual, expected);

    let value = GenericDatumReader::builder(&schema_b)
        .writer_schemata(vec![&schema_a])?
        .build()?
        .read_value(&mut actual.as_slice())?;
    assert_eq!(value, record);

    Ok(())
}

#[test]
fn avro_rs_106_test_multiple_schemata_to_from_avro_datum_with_resolution() -> TestResult {
    init();

    let record: Value = Value::Record(vec![(
        String::from("field_b"),
        Value::Record(vec![(String::from("field_a"), Value::Float(1.0))]),
    )]);

    let schema_a = Schema::parse_str(SCHEMA_A_STR)?;
    let schema_b = Schema::parse_str(SCHEMA_B_STR)?;

    let expected: Vec<u8> = vec![0, 0, 128, 63];
    let actual = GenericDatumWriter::builder(&schema_b)
        .schemata(vec![&schema_a])?
        .build()?
        .write_value_to_vec(record.clone())?;
    assert_eq!(actual, expected);

    let value = GenericDatumReader::builder(&schema_b)
        .writer_schemata(vec![&schema_a])?
        .reader_schema_with_schemata(&schema_b,vec![&schema_a])?
        .build()?
        .read_value(&mut actual.as_slice())?;
    assert_eq!(value, record);

    Ok(())
}

#[test]
fn test_avro_3683_multiple_schemata_writer_reader() -> TestResult {
    init();

    let record: Value = Value::Record(vec![(
        String::from("field_b"),
        Value::Record(vec![(String::from("field_a"), Value::Float(1.0))]),
    )]);

    let schema_a = Schema::parse_str(SCHEMA_A_STR)?;
    let schema_b = Schema::parse_str(SCHEMA_B_STR)?;

    let mut output: Vec<u8> = Vec::new();

    let mut writer = Writer::with_schemata(&schema_b, vec![&schema_a], &mut output, Codec::Null)?;
    writer.append_value(record.clone())?;
    writer.flush()?;
    drop(writer); //drop the writer so that `output` is no more referenced mutably

    let reader = Reader::builder(output.as_slice())
        .reader_schema(&schema_b)
        .schemata(vec![&schema_a])
        .build()?;
    let value = reader.into_iter().next().unwrap()?;
    assert_eq!(value, record);

    Ok(())
}
