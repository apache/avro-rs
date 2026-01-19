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

use crate::AvroResult;
use crate::error::Details;
use crate::schema::{Name, Namespace, ResolvedSchema, Schema, SchemaKind};
use crate::types;
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;

/// A description of a Union schema
#[derive(Debug, Clone)]
pub struct UnionSchema {
    /// The schemas that make up this union
    pub(crate) schemas: Vec<Schema>,
    // Used to ensure uniqueness of schema inputs, and provide constant time finding of the
    // schema index given a value.
    // **NOTE** that this approach does not work for named types, and will have to be modified
    // to support that. A simple solution is to also keep a mapping of the names used.
    variant_index: BTreeMap<SchemaKind, usize>,
}

impl UnionSchema {
    /// Creates a new UnionSchema from a vector of schemas.
    ///
    /// # Errors
    /// Will return an error if `schemas` has duplicate unnamed schemas or if `schemas`
    /// contains a union.
    pub fn new(schemas: Vec<Schema>) -> AvroResult<Self> {
        let mut vindex = BTreeMap::new();
        for (i, schema) in schemas.iter().enumerate() {
            if let Schema::Union(_) = schema {
                return Err(Details::GetNestedUnion.into());
            }
            if !schema.is_named() && vindex.insert(SchemaKind::from(schema), i).is_some() {
                return Err(Details::GetUnionDuplicate.into());
            }
        }
        Ok(UnionSchema {
            schemas,
            variant_index: vindex,
        })
    }

    /// Returns a slice to all variants of this schema.
    pub fn variants(&self) -> &[Schema] {
        &self.schemas
    }

    /// Returns true if the any of the variants of this `UnionSchema` is `Null`.
    pub fn is_nullable(&self) -> bool {
        self.schemas.iter().any(|x| matches!(x, Schema::Null))
    }

    /// Optionally returns a reference to the schema matched by this value, as well as its position
    /// within this union.
    ///
    /// Extra arguments:
    /// - `known_schemata` - mapping between `Name` and `Schema` - if passed, additional external schemas would be used to resolve references.
    pub fn find_schema_with_known_schemata<S: Borrow<Schema> + Debug>(
        &self,
        value: &types::Value,
        known_schemata: Option<&HashMap<Name, S>>,
        enclosing_namespace: &Namespace,
    ) -> Option<(usize, &Schema)> {
        let schema_kind = SchemaKind::from(value);
        if let Some(&i) = self.variant_index.get(&schema_kind) {
            // fast path
            Some((i, &self.schemas[i]))
        } else {
            // slow path (required for matching logical or named types)

            // first collect what schemas we already know
            let mut collected_names: HashMap<Name, &Schema> = known_schemata
                .map(|names| {
                    names
                        .iter()
                        .map(|(name, schema)| (name.clone(), schema.borrow()))
                        .collect()
                })
                .unwrap_or_default();

            self.schemas.iter().enumerate().find(|(_, schema)| {
                let resolved_schema = ResolvedSchema::new_with_known_schemata(
                    vec![*schema],
                    enclosing_namespace,
                    &collected_names,
                )
                .expect("Schema didn't successfully parse");
                let resolved_names = resolved_schema.names_ref;

                // extend known schemas with just resolved names
                collected_names.extend(resolved_names);
                let namespace = &schema.namespace().or_else(|| enclosing_namespace.clone());

                value
                    .clone()
                    .resolve_internal(schema, &collected_names, namespace, &None)
                    .is_ok()
            })
        }
    }
}

// No need to compare variant_index, it is derivative of schemas.
impl PartialEq for UnionSchema {
    fn eq(&self, other: &UnionSchema) -> bool {
        self.schemas.eq(&other.schemas)
    }
}
