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

use apache_avro::Schema;
use apache_avro::types::Value;
use apache_avro::util::max_decode_recursion_depth;
use apache_avro_test_helper::TestResult;

// This is an IT test because it sets the default recursion depth limit (OnceLock).

#[test]
fn avro_rs_642_resolve_recursion_depth_is_bounded() -> TestResult {
    let schema = Schema::parse_str(
        r#"{
                "type": "record",
                "name": "Node",
                "fields": [
                    {"name": "next", "type": ["null", "Node"]}
                ]
            }"#,
    )?;

    let recursion_depth_trigger = max_decode_recursion_depth(16) + 1;

    // Build a value that nests deeper than the recursion limit but is
    // still shallow enough for the test itself to drop safely.
    let mut value = Value::Record(vec![(
        "next".into(),
        Value::Union(0, Box::new(Value::Null)),
    )]);
    for _ in 0..recursion_depth_trigger {
        value = Value::Record(vec![("next".into(), Value::Union(1, Box::new(value)))]);
    }

    let result = value.resolve(&schema);
    assert!(result.is_err(), "unbounded resolution must be rejected");

    Ok(())
}
