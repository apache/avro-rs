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

use apache_avro::writer::datum::GenericDatumWriter;
use apache_avro::{Schema, reader::datum::GenericDatumReader, types};
use apache_avro_test_helper::TestResult;

#[test]
fn float_under_double_schema_roundtrip() -> TestResult {
    let schema = Schema::Double;
    let datum = GenericDatumWriter::builder(&schema)
        .validate(true)
        .build()?
        .write_value_to_vec(types::Value::Float(0.1_f32))?;

    let mut remaining = datum.as_slice();
    let reader = GenericDatumReader::builder(&schema).build()?;
    let read = reader.read_value(&mut remaining)?;
    assert_eq!(read, types::Value::Double(f64::from(0.1_f32)));
    assert!(remaining.is_empty());

    Ok(())
}
