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

use apache_avro::{
    Schema,
    serde::{AvroSchemaComponent, get_record_fields_in_ctxt},
};
use std::collections::HashMap;

use apache_avro_test_helper::TestResult;

#[test]
fn avro_rs_448_default_get_record_fields_no_recursion() -> TestResult {
    #[derive(apache_avro_derive::AvroSchema)]
    struct Foo {
        _a: i32,
        _b: String,
    }

    let mut named_schemas = HashMap::new();
    let fields =
        get_record_fields_in_ctxt(0, &mut named_schemas, &None, Foo::get_schema_in_ctxt).unwrap();

    assert_eq!(fields.len(), 2);
    assert!(
        named_schemas.is_empty(),
        "Name shouldn't have been added: {named_schemas:?}"
    );

    // Insert Foo into named_schemas
    match Foo::get_schema_in_ctxt(&mut named_schemas, &None) {
        Schema::Record(_) => {}
        schema => panic!("Expected a record got {schema:?}"),
    }
    assert_eq!(
        named_schemas.len(),
        1,
        "Name should have been added: {named_schemas:?}"
    );

    let fields =
        get_record_fields_in_ctxt(0, &mut named_schemas, &None, Foo::get_schema_in_ctxt).unwrap();
    assert_eq!(fields.len(), 2);
    assert_eq!(
        named_schemas.len(),
        1,
        "Name shouldn't have been removed: {named_schemas:?}"
    );

    Ok(())
}

#[test]
fn avro_rs_448_default_get_record_fields_recursion() -> TestResult {
    #[derive(apache_avro_derive::AvroSchema)]
    struct Foo {
        _a: i32,
        _b: Option<Box<Foo>>,
    }

    let mut named_schemas = HashMap::new();
    let fields =
        get_record_fields_in_ctxt(0, &mut named_schemas, &None, Foo::get_schema_in_ctxt).unwrap();

    assert_eq!(fields.len(), 2);
    assert_eq!(
        named_schemas.len(),
        1,
        "Name shouldn't have been removed: {named_schemas:?}"
    );

    // Insert Foo into named_schemas
    match Foo::get_schema_in_ctxt(&mut named_schemas, &None) {
        Schema::Ref { name: _ } => {}
        schema => panic!("Expected a ref got {schema:?}"),
    }
    assert_eq!(named_schemas.len(), 1);

    let fields =
        get_record_fields_in_ctxt(0, &mut named_schemas, &None, Foo::get_schema_in_ctxt).unwrap();
    assert_eq!(fields.len(), 2);
    assert_eq!(
        named_schemas.len(),
        1,
        "Name shouldn't have been removed: {named_schemas:?}"
    );

    Ok(())
}

#[test]
fn avro_rs_448_default_get_record_fields_position() -> TestResult {
    #[derive(apache_avro_derive::AvroSchema)]
    struct Foo {
        _a: i32,
        _b: String,
    }

    let mut named_schemas = HashMap::new();
    let fields =
        get_record_fields_in_ctxt(10, &mut named_schemas, &None, Foo::get_schema_in_ctxt).unwrap();

    assert_eq!(fields.len(), 2);
    assert!(
        named_schemas.is_empty(),
        "Name shouldn't have been added: {named_schemas:?}"
    );
    let positions = fields.into_iter().map(|f| f.position).collect::<Vec<_>>();
    assert_eq!(positions.as_slice(), &[10, 11][..]);

    // Insert Foo into named_schemas
    match Foo::get_schema_in_ctxt(&mut named_schemas, &None) {
        Schema::Record(_) => {}
        schema => panic!("Expected a record got {schema:?}"),
    }
    assert_eq!(
        named_schemas.len(),
        1,
        "Name should have been added: {named_schemas:?}"
    );

    let fields =
        get_record_fields_in_ctxt(5043, &mut named_schemas, &None, Foo::get_schema_in_ctxt)
            .unwrap();
    assert_eq!(fields.len(), 2);
    assert_eq!(
        named_schemas.len(),
        1,
        "Name shouldn't have been removed: {named_schemas:?}"
    );
    let positions = fields.into_iter().map(|f| f.position).collect::<Vec<_>>();
    assert_eq!(positions.as_slice(), &[5043, 5044][..]);

    Ok(())
}
