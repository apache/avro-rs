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

// By including the (unexpanded) modules, we can also test that the
// generated code is valid.
mod expanded;

/// These tests only run on nightly as the output can change per compiler version.
/// Use `MACROTEST=overwrite cargo +nightly test expand` to re-generate them
#[rustversion::attr(not(nightly), ignore)]
#[test]
fn expand() {
    macrotest::expand("tests/expanded/avro_*.rs");
}
