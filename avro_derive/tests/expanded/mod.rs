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

#![expect(dead_code, reason = "Only code generation is tested")]

// Do not include the `.expanded` modules here, as that code won't compile

mod avro_3687_basic_enum_with_default;
mod avro_3709_record_field_attributes;
mod avro_rs_207_rename_all_attribute;
mod avro_rs_207_rename_attr_over_rename_all_attribute;
mod avro_rs_501_basic;
mod avro_rs_501_namespace;
mod avro_rs_501_reference;
mod avro_rs_501_struct_with_optional;
