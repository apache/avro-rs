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

use serde::Deserialize;

#[test]
fn avro_rs_219_failing_deserialization_due_to_bigdecimal_dependency() {
    #[derive(Deserialize, PartialEq, Debug)]
    struct S3 {
        f1: Option<f64>,

        #[serde(flatten)]
        inner: Inner,
    }

    #[derive(Deserialize, PartialEq, Debug)]
    struct Inner {
        f2: f64,
    }

    let test = r#"{
      "f1": 0.3,
      "f2": 3.76
    }"#;

    let result = serde_json::from_str::<S3>(test);
    println!("result : {result:#?}");
    result.unwrap();
}
