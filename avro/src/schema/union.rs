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

use crate::error::Details;
use crate::schema::{
    DecimalSchema, InnerDecimalSchema, Name, Namespace, Schema, SchemaKind, UuidSchema,
};
use crate::types;
use crate::{AvroResult, Error};
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use strum::IntoDiscriminant;

/// A description of a Union schema
#[derive(Clone)]
pub struct UnionSchema {
    /// The schemas that make up this union
    pub(crate) schemas: Vec<Schema>,
    /// The indexes of unnamed types.
    ///
    /// Logical types have been reduced to their inner type.
    /// Used to provide constant time finding of the
    /// schema index given an unnamed type. Must only contain unnamed types.
    variant_index: BTreeMap<SchemaKind, usize>,
    /// The indexes of named types.
    ///
    /// The names self aren't saved as they aren't used.
    named_index: Vec<usize>,
}

impl Debug for UnionSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Doesn't include `variant_index` as it's a derivative of `schemas`
        f.debug_struct("UnionSchema")
            .field("schemas", &self.schemas)
            .finish()
    }
}

impl UnionSchema {
    /// Creates a new `UnionSchema` from a vector of schemas.
    ///
    /// # Errors
    /// Will return an error if `schemas` has duplicate unnamed schemas or if `schemas`
    /// contains a union.
    pub fn new(schemas: Vec<Schema>) -> AvroResult<Self> {
        let mut builder = Self::builder();
        for schema in schemas {
            builder.variant(schema)?;
        }
        Ok(builder.build())
    }

    /// Build a `UnionSchema` piece-by-piece.
    pub fn builder() -> UnionSchemaBuilder {
        UnionSchemaBuilder::new()
    }

    /// Returns a slice to all variants of this schema.
    pub fn variants(&self) -> &[Schema] {
        &self.schemas
    }

    /// Returns true if the any of the variants of this `UnionSchema` is `Null`.
    pub fn is_nullable(&self) -> bool {
        self.variant_index.contains_key(&SchemaKind::Null)
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
        let ValueSchemaKind { unnamed, named } = Self::value_to_base_schemakind(value);
        // Unnamed schema types can be looked up directly using the variant_index
        let unnamed = unnamed
            .and_then(|kind| self.variant_index.get(&kind).copied())
            .map(|index| (index, &self.schemas[index]))
            .and_then(|(index, schema)| {
                let kind = schema.discriminant();
                // Maps and arrays need to be checked if they actually match the value
                if kind == SchemaKind::Map || kind == SchemaKind::Array {
                    let known_schemata_if_none = HashMap::new();
                    let known_schemata = known_schemata.unwrap_or(&known_schemata_if_none);
                    let namespace = if schema.namespace().is_some() {
                        &schema.namespace()
                    } else {
                        enclosing_namespace
                    };

                    // TODO: Do this without the clone
                    value
                        .clone()
                        .resolve_internal(schema, known_schemata, namespace, &None)
                        .ok()
                        .map(|_| (index, schema))
                } else {
                    Some((index, schema))
                }
            });
        let named = named.and_then(|kind| {
            // Every named type needs to be checked against a value until one matches

            let known_schemata_if_none = HashMap::new();
            let known_schemata = known_schemata.unwrap_or(&known_schemata_if_none);

            self.named_index
                .iter()
                .copied()
                .map(|i| (i, &self.schemas[i]))
                .filter(|(_i, s)| s.discriminant() == kind || s.discriminant() == SchemaKind::Ref)
                .find(|(_i, schema)| {
                    let namespace = if schema.namespace().is_some() {
                        &schema.namespace()
                    } else {
                        enclosing_namespace
                    };

                    // TODO: Do this without the clone
                    value
                        .clone()
                        .resolve_internal(schema, known_schemata, namespace, &None)
                        .is_ok()
                })
        });

        match (unnamed, named) {
            (Some((u_i, _)), Some((n_i, _))) if u_i < n_i => unnamed,
            (Some(_), Some(_)) => named,
            (Some(_), None) => unnamed,
            (None, Some(_)) => named,
            (None, None) => None,
        }
    }

    /// Convert a value to a [`SchemaKind`] stripping logical types to their base type.
    fn value_to_base_schemakind(value: &types::Value) -> ValueSchemaKind {
        let schemakind = SchemaKind::from(value);
        match schemakind {
            SchemaKind::Decimal => ValueSchemaKind {
                unnamed: Some(SchemaKind::Bytes),
                named: Some(SchemaKind::Fixed),
            },
            SchemaKind::BigDecimal => ValueSchemaKind {
                unnamed: Some(SchemaKind::Bytes),
                named: None,
            },
            SchemaKind::Uuid => ValueSchemaKind {
                unnamed: Some(SchemaKind::String),
                named: Some(SchemaKind::Fixed),
            },
            SchemaKind::Date | SchemaKind::TimeMillis => ValueSchemaKind {
                unnamed: Some(SchemaKind::Int),
                named: None,
            },
            SchemaKind::TimeMicros
            | SchemaKind::TimestampMillis
            | SchemaKind::TimestampMicros
            | SchemaKind::TimestampNanos
            | SchemaKind::LocalTimestampMillis
            | SchemaKind::LocalTimestampMicros
            | SchemaKind::LocalTimestampNanos => ValueSchemaKind {
                unnamed: Some(SchemaKind::Long),
                named: None,
            },
            SchemaKind::Duration => ValueSchemaKind {
                unnamed: None,
                named: Some(SchemaKind::Fixed),
            },
            SchemaKind::Record | SchemaKind::Enum | SchemaKind::Fixed => ValueSchemaKind {
                unnamed: None,
                named: Some(schemakind),
            },
            // When a `serde_json::Value` is converted to a `types::Value` a object will always become a map
            // so a `types::Value::Map` can also be a record.
            SchemaKind::Map => ValueSchemaKind {
                unnamed: Some(SchemaKind::Map),
                named: Some(SchemaKind::Record),
            },
            _ => ValueSchemaKind {
                unnamed: Some(schemakind),
                named: None,
            },
        }
    }
}

/// The schema kinds matching a specific value.
struct ValueSchemaKind {
    unnamed: Option<SchemaKind>,
    named: Option<SchemaKind>,
}

// No need to compare variant_index, it is derivative of schemas.
impl PartialEq for UnionSchema {
    fn eq(&self, other: &UnionSchema) -> bool {
        self.schemas.eq(&other.schemas)
    }
}

/// A builder for [`UnionSchema`]
#[derive(Default, Debug)]
pub struct UnionSchemaBuilder {
    schemas: Vec<Schema>,
    names: BTreeMap<Name, usize>,
    variant_index: BTreeMap<SchemaKind, usize>,
}

impl UnionSchemaBuilder {
    /// Create a builder.
    ///
    /// See also [`UnionSchema::builder`].
    pub fn new() -> Self {
        Self::default()
    }

    #[doc(hidden)]
    /// This is not a public API, it should only be used by `avro_derive`
    ///
    /// Add a variant to this union, if it already exists ignore it.
    ///
    /// # Errors
    /// Will return a [`Details::GetUnionDuplicateMap`] or [`Details::GetUnionDuplicateArray`] if
    /// duplicate maps or arrays are encountered with different subtypes.
    pub fn variant_ignore_duplicates(&mut self, schema: Schema) -> Result<&mut Self, Error> {
        if let Some(name) = schema.name() {
            if let Some(current) = self.names.get(name).copied() {
                if self.schemas[current] != schema {
                    return Err(Details::GetUnionDuplicateNamedSchemas(name.to_string()).into());
                }
            } else {
                self.names.insert(name.clone(), self.schemas.len());
                self.schemas.push(schema);
            }
        } else if let Schema::Map(_) = &schema {
            if let Some(index) = self.variant_index.get(&SchemaKind::Map).copied() {
                if self.schemas[index] != schema {
                    return Err(
                        Details::GetUnionDuplicateMap(self.schemas.remove(index), schema).into(),
                    );
                }
            } else {
                self.variant_index
                    .insert(SchemaKind::Map, self.schemas.len());
                self.schemas.push(schema);
            }
        } else if let Schema::Array(_) = &schema {
            if let Some(index) = self.variant_index.get(&SchemaKind::Array).copied() {
                if self.schemas[index] != schema {
                    return Err(
                        Details::GetUnionDuplicateMap(self.schemas.remove(index), schema).into(),
                    );
                }
            } else {
                self.variant_index
                    .insert(SchemaKind::Array, self.schemas.len());
                self.schemas.push(schema);
            }
        } else {
            let discriminant = Self::schema_to_base_schemakind(&schema);
            if discriminant == SchemaKind::Union {
                return Err(Details::GetNestedUnion.into());
            }
            if !self.variant_index.contains_key(&discriminant) {
                self.variant_index.insert(discriminant, self.schemas.len());
                self.schemas.push(schema);
            }
        }
        Ok(self)
    }

    /// Add a variant to this union.
    ///
    /// # Errors
    /// Will return a [`Details::GetUnionDuplicateNamedSchemas`] or [`Details::GetUnionDuplicate`] if
    /// duplicate names or schema kinds are found.
    pub fn variant(&mut self, schema: Schema) -> Result<&mut Self, Error> {
        if let Some(name) = schema.name() {
            if self.names.contains_key(name) {
                return Err(Details::GetUnionDuplicateNamedSchemas(name.to_string()).into());
            } else {
                self.names.insert(name.clone(), self.schemas.len());
                self.schemas.push(schema);
            }
        } else {
            let discriminant = Self::schema_to_base_schemakind(&schema);
            if discriminant == SchemaKind::Union {
                return Err(Details::GetNestedUnion.into());
            }
            if self.variant_index.contains_key(&discriminant) {
                return Err(Details::GetUnionDuplicate(discriminant).into());
            } else {
                self.variant_index.insert(discriminant, self.schemas.len());
                self.schemas.push(schema);
            }
        }
        Ok(self)
    }

    /// Check if a schema already exists in this union.
    pub fn contains(&self, schema: &Schema) -> bool {
        if let Some(name) = schema.name() {
            if let Some(current) = self.names.get(name).copied() {
                &self.schemas[current] == schema
            } else {
                false
            }
        } else {
            let discriminant = Self::schema_to_base_schemakind(schema);
            if let Some(index) = self.variant_index.get(&discriminant).copied() {
                &self.schemas[index] == schema
            } else {
                false
            }
        }
    }

    /// Create the `UnionSchema`.
    pub fn build(mut self) -> UnionSchema {
        self.schemas.shrink_to_fit();
        UnionSchema {
            variant_index: self.variant_index,
            named_index: self.names.into_values().collect(),
            schemas: self.schemas,
        }
    }

    /// Get the [`SchemaKind`] of a [`Schema`] converting logical types to their base type.
    fn schema_to_base_schemakind(schema: &Schema) -> SchemaKind {
        let kind = schema.discriminant();
        match kind {
            SchemaKind::Date | SchemaKind::TimeMillis => SchemaKind::Int,
            SchemaKind::TimeMicros
            | SchemaKind::TimestampMillis
            | SchemaKind::TimestampMicros
            | SchemaKind::TimestampNanos
            | SchemaKind::LocalTimestampMillis
            | SchemaKind::LocalTimestampMicros
            | SchemaKind::LocalTimestampNanos => SchemaKind::Long,
            SchemaKind::Uuid => match schema {
                Schema::Uuid(UuidSchema::Bytes) => SchemaKind::Bytes,
                Schema::Uuid(UuidSchema::String) => SchemaKind::String,
                Schema::Uuid(UuidSchema::Fixed(_)) => SchemaKind::Fixed,
                _ => unreachable!(),
            },
            SchemaKind::Decimal => match schema {
                Schema::Decimal(DecimalSchema {
                    inner: InnerDecimalSchema::Bytes,
                    ..
                }) => SchemaKind::Bytes,
                Schema::Decimal(DecimalSchema {
                    inner: InnerDecimalSchema::Fixed(_),
                    ..
                }) => SchemaKind::Fixed,
                _ => unreachable!(),
            },
            SchemaKind::Duration => SchemaKind::Fixed,
            _ => kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Details, Error};
    use crate::schema::RecordSchema;
    use crate::types::Value;
    use apache_avro_test_helper::TestResult;

    #[test]
    fn avro_rs_402_new_union_schema() -> TestResult {
        let schema1 = Schema::Int;
        let schema2 = Schema::String;
        let union_schema = UnionSchema::new(vec![schema1.clone(), schema2.clone()])?;

        assert_eq!(union_schema.variants(), &[schema1, schema2]);

        Ok(())
    }

    #[test]
    fn avro_rs_402_new_union_schema_duplicate_names() -> TestResult {
        let res = UnionSchema::new(vec![
            Schema::Record(RecordSchema::builder().try_name("Same_name")?.build()),
            Schema::Record(RecordSchema::builder().try_name("Same_name")?.build()),
        ])
        .map_err(Error::into_details);

        match res {
            Err(Details::GetUnionDuplicateNamedSchemas(name)) => {
                assert_eq!(name, Name::new("Same_name")?.to_string());
            }
            err => panic!("Expected GetUnionDuplicateNamedSchemas error, got: {err:?}"),
        }

        Ok(())
    }

    #[test]
    fn avro_rs_489_union_schema_builder_primitive_type() -> TestResult {
        let mut builder = UnionSchema::builder();
        builder.variant(Schema::Null)?;
        assert!(builder.variant(Schema::Null).is_err());
        builder.variant_ignore_duplicates(Schema::Null)?;
        builder.variant(Schema::Int)?;
        assert!(builder.variant(Schema::Int).is_err());
        builder.variant_ignore_duplicates(Schema::Int)?;
        builder.variant(Schema::Long)?;
        assert!(builder.variant(Schema::Long).is_err());
        builder.variant_ignore_duplicates(Schema::Long)?;

        let union = builder.build();
        assert_eq!(union.schemas, &[Schema::Null, Schema::Int, Schema::Long]);

        Ok(())
    }

    #[test]
    fn avro_rs_489_union_schema_builder_complex_types() -> TestResult {
        let enum_abc = Schema::parse_str(
            r#"{
            "type": "enum",
            "name": "ABC",
            "symbols": ["A", "B", "C"]
        }"#,
        )?;
        let enum_abc_with_extra_symbol = Schema::parse_str(
            r#"{
            "type": "enum",
            "name": "ABC",
            "symbols": ["A", "B", "C", "D"]
        }"#,
        )?;
        let enum_def = Schema::parse_str(
            r#"{
            "type": "enum",
            "name": "DEF",
            "symbols": ["D", "E", "F"]
        }"#,
        )?;
        let fixed_abc = Schema::parse_str(
            r#"{
            "type": "fixed",
            "name": "ABC",
            "size": 1
        }"#,
        )?;
        let fixed_foo = Schema::parse_str(
            r#"{
            "type": "fixed",
            "name": "Foo",
            "size": 1
        }"#,
        )?;

        let mut builder = UnionSchema::builder();
        builder.variant(enum_abc.clone())?;
        assert!(builder.variant(enum_abc.clone()).is_err());
        builder.variant_ignore_duplicates(enum_abc.clone())?;
        // Name is the same but different schemas, so should always fail
        assert!(builder.variant(fixed_abc.clone()).is_err());
        assert!(
            builder
                .variant_ignore_duplicates(fixed_abc.clone())
                .is_err()
        );
        // Name and schema type are the same but symbols are different
        assert!(builder.variant(enum_abc_with_extra_symbol.clone()).is_err());
        assert!(
            builder
                .variant_ignore_duplicates(enum_abc_with_extra_symbol.clone())
                .is_err()
        );
        builder.variant(enum_def.clone())?;
        assert!(builder.variant(enum_def.clone()).is_err());
        builder.variant_ignore_duplicates(enum_def.clone())?;
        builder.variant(fixed_foo.clone())?;
        assert!(builder.variant(fixed_foo.clone()).is_err());
        builder.variant_ignore_duplicates(fixed_foo.clone())?;

        let union = builder.build();
        assert_eq!(union.variants(), &[enum_abc, enum_def, fixed_foo]);

        Ok(())
    }

    #[test]
    fn avro_rs_489_union_schema_builder_logical_types() -> TestResult {
        let fixed_uuid = Schema::parse_str(
            r#"{
            "type": "fixed",
            "name": "Uuid",
            "size": 16
        }"#,
        )?;
        let uuid = Schema::parse_str(
            r#"{
            "type": "fixed",
            "logicalType": "uuid",
            "name": "Uuid",
            "size": 16
        }"#,
        )?;

        let mut builder = UnionSchema::builder();

        builder.variant(Schema::Date)?;
        assert!(builder.variant(Schema::Date).is_err());
        builder.variant_ignore_duplicates(Schema::Date)?;
        assert!(builder.variant(Schema::Int).is_err());
        builder.variant_ignore_duplicates(Schema::Int)?;
        builder.variant(uuid.clone())?;
        assert!(builder.variant(uuid.clone()).is_err());
        builder.variant_ignore_duplicates(uuid.clone())?;
        assert!(builder.variant(fixed_uuid.clone()).is_err());
        assert!(
            builder
                .variant_ignore_duplicates(fixed_uuid.clone())
                .is_err()
        );

        let union = builder.build();
        assert_eq!(union.schemas, &[Schema::Date, uuid]);

        Ok(())
    }

    #[test]
    fn avro_rs_489_find_schema_with_known_schemata_wrong_map() -> TestResult {
        let union = UnionSchema::new(vec![Schema::map(Schema::Int).build(), Schema::Null])?;
        let value = Value::Map(
            [("key".to_string(), Value::String("value".to_string()))]
                .into_iter()
                .collect(),
        );

        assert!(
            union
                .find_schema_with_known_schemata(&value, None::<&HashMap<Name, Schema>>, &None)
                .is_none()
        );

        Ok(())
    }
}
