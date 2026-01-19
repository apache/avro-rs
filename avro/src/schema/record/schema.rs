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

use crate::schema::{Aliases, Documentation, Name, RecordField};
use serde_json::Value;
use std::collections::BTreeMap;

/// A description of a Record schema.
#[derive(bon::Builder, Debug, Clone)]
pub struct RecordSchema {
    /// The name of the schema
    pub name: Name,
    /// The aliases of the schema
    #[builder(default)]
    pub aliases: Aliases,
    /// The documentation of the schema
    #[builder(default)]
    pub doc: Documentation,
    /// The set of fields of the schema
    pub fields: Vec<RecordField>,
    /// The `lookup` table maps field names to their position in the `Vec`
    /// of `fields`.
    pub lookup: BTreeMap<String, usize>,
    /// The custom attributes of the schema
    #[builder(default = BTreeMap::new())]
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Default)]
pub(crate) enum RecordSchemaParseLocation {
    /// When the parse is happening at root level
    #[default]
    Root,

    /// When the parse is happening inside a record field
    FromField,
}
