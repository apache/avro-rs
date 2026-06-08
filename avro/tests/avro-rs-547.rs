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

//! Regression: parsing a schema whose name / alias / type-reference is not a
//! valid Avro identifier must return `Err`, never panic.
//!
//! `Schema::parse_str` is a `Result`-returning, public API; callers cannot
//! guard against an internal `unwrap()`, so a panic on attacker-controlled
//! schema JSON is a denial-of-service. `Name::parse` was fixed previously,
//! but the sibling sites in `schema::parser` (`get_already_seen_schema`,
//! `fix_aliases_namespace`, `get_schema_type_name`) still called
//! `Name::new(..).unwrap()` / `Alias::new(..).unwrap()`.

use apache_avro::Schema;

/// Each of these declares a name, alias, or type reference that is not a valid
/// Avro identifier. Before the fix at least one site (`fix_aliases_namespace`)
/// panicked with `called 'Result::unwrap()' on an 'Err' value: Invalid schema
/// name ...`.
const INVALID_NAME_SCHEMAS: &[&str] = &[
    // invalid `aliases` entry on a record / fixed / enum
    r#"{"type": "record", "name": "R", "aliases": [":"], "fields": []}"#,
    r#"{"type": "fixed", "name": "F", "aliases": ["a.b:c"], "size": 4}"#,
    r#"{"type": "enum", "name": "E", "aliases": [""], "symbols": ["A"]}"#,
    // invalid nested type name
    r#"{"type": "record", "name": "R", "fields": [
        {"name": "f", "type": {"type": "record", "name": ":", "fields": []}}
       ]}"#,
    // invalid string type reference
    r#"{"type": "record", "name": "R", "fields": [{"name": "f", "type": ":"}]}"#,
    // invalid top-level name
    r#"{"type": "record", "name": ":", "fields": []}"#,
];

#[test]
fn invalid_schema_names_return_err_not_panic() {
    for schema in INVALID_NAME_SCHEMAS {
        let result = Schema::parse_str(schema);
        assert!(
            result.is_err(),
            "expected Err (not a panic) for invalid-name schema:\n{schema}"
        );
    }
}

#[test]
fn valid_schema_with_aliases_still_parses() {
    // Guards against the fix over-rejecting: legitimate aliases must still work.
    let schema = r#"{
        "type": "record",
        "name": "R",
        "namespace": "ns",
        "aliases": ["Old", "other.Older"],
        "fields": [{"name": "f", "type": "int"}]
    }"#;
    assert!(Schema::parse_str(schema).is_ok());
}
