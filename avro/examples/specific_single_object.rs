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

use apache_avro::{AvroSchema, SpecificSingleObjectReader, SpecificSingleObjectWriter};
use serde::{Deserialize, Serialize};
use std::iter::repeat;

#[derive(Debug, Clone, Serialize, Deserialize, AvroSchema, PartialEq)]
struct Test {
    a: i64,
    b: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer: Vec<u8> = Vec::new();
    let test = Test {
        a: 27,
        b: "foo".to_string(),
    };

    let mut writer = SpecificSingleObjectWriter::<Test>::with_capacity(1024)?;
    let reader = SpecificSingleObjectReader::<Test>::new()?;

    for test in repeat(test).take(2) {
        buffer.clear();

        match writer.write(test.clone(), &mut buffer) {
            Ok(bytes_written) => {
                assert_eq!(bytes_written, 15);
                assert_eq!(
                    buffer,
                    vec![
                        195, 1, 166, 59, 243, 49, 82, 230, 8, 161, 54, 6, 102, 111, 111
                    ]
                );
            }
            Err(err) => {
                panic!("Error during serialization: {err:?}");
            }
        }

        let read = reader.read(&mut buffer.as_slice())?;
        assert_eq!(test, read);
    }

    Ok(())
}
