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

use apache_avro::{AvroSchemaComponent, Schema};
use std::collections::HashMap;

#[test]
fn avro_rs_394_avro_schema_component_without_derive_feature() {
    let schema = i32::get_schema_in_ctxt(&mut HashMap::default(), &None);
    assert!(matches!(schema, Schema::Int));
}

#[test]
#[should_panic(expected = "Option<T> must produce a valid (non-nested) union")]
fn avro_rs_394_avro_schema_component_nested_options() {
    type VeryOptional = Option<Option<i32>>;

    let _schema = VeryOptional::get_schema_in_ctxt(&mut HashMap::default(), &None);
}
