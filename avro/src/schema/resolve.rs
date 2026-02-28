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
    DecimalSchema, EnumSchema, FixedSchema, InnerDecimalSchema, NamesRef, NamespaceRef,
    RecordSchema, UnionSchema, UuidSchema,
};
use crate::{AvroResult, Error, Schema};
use std::collections::HashMap;

#[derive(Debug)]
pub struct ResolvedSchema<'s> {
    pub(super) names_ref: NamesRef<'s>,
    schemata: Vec<&'s Schema>,
}

impl<'s> ResolvedSchema<'s> {
    pub fn get_schemata(&self) -> &[&'s Schema] {
        &self.schemata
    }

    pub fn get_names(&self) -> &NamesRef<'s> {
        &self.names_ref
    }

    /// Resolve all references in this schema.
    ///
    /// If some references are to other schemas, see [`ResolvedSchema::new_with_schemata`].
    pub fn new(schema: &'s Schema) -> AvroResult<Self> {
        Self::new_with_schemata(vec![schema])
    }

    /// Resolve all references in these schemas.
    ///
    // TODO: Support this
    /// These schemas will be resolved in order, so references to schemas later in the
    /// list is not supported.
    pub fn new_with_schemata(schemata: Vec<&'s Schema>) -> AvroResult<Self> {
        Self::new_with_known_schemata(schemata, None, &HashMap::new())
    }

    /// Creates `ResolvedSchema` with some already known schemas.
    ///
    /// Those schemata would be used to resolve references if needed.
    pub fn new_with_known_schemata<'n>(
        schemata_to_resolve: Vec<&'s Schema>,
        enclosing_namespace: NamespaceRef,
        known_schemata: &'n NamesRef<'n>,
    ) -> AvroResult<Self> {
        let mut names = HashMap::new();
        resolve_names_with_schemata(
            schemata_to_resolve.iter().copied(),
            &mut names,
            enclosing_namespace,
            known_schemata,
        )?;
        Ok(ResolvedSchema {
            names_ref: names,
            schemata: schemata_to_resolve,
        })
    }
}

impl<'s> TryFrom<&'s Schema> for ResolvedSchema<'s> {
    type Error = Error;

    fn try_from(schema: &'s Schema) -> AvroResult<Self> {
        Self::new(schema)
    }
}

impl<'s> TryFrom<Vec<&'s Schema>> for ResolvedSchema<'s> {
    type Error = Error;

    fn try_from(schemata: Vec<&'s Schema>) -> AvroResult<Self> {
        Self::new_with_schemata(schemata)
    }
}

/// Implementation detail of [`ResolvedOwnedSchema`]
///
/// This struct is self-referencing. The references in `names` point to `root_schema`.
/// This allows resolving an owned schema without having to clone all the named schemas.
#[ouroboros::self_referencing]
struct InnerResolvedOwnedSchema {
    root_schema: Schema,
    #[borrows(root_schema)]
    #[covariant]
    names: NamesRef<'this>,
}

/// A variant of [`ResolvedSchema`] that owns the schema
pub struct ResolvedOwnedSchema {
    inner: InnerResolvedOwnedSchema,
}

impl ResolvedOwnedSchema {
    pub fn new(root_schema: Schema) -> AvroResult<Self> {
        Ok(Self {
            inner: InnerResolvedOwnedSchemaTryBuilder {
                root_schema,
                names_builder: |schema: &Schema| {
                    let mut names = HashMap::new();
                    resolve_names(schema, &mut names, None, &HashMap::new())?;
                    Ok::<_, Error>(names)
                },
            }
            .try_build()?,
        })
    }

    pub fn get_root_schema(&self) -> &Schema {
        self.inner.borrow_root_schema()
    }
    pub fn get_names(&self) -> &NamesRef<'_> {
        self.inner.borrow_names()
    }
}

impl TryFrom<Schema> for ResolvedOwnedSchema {
    type Error = Error;

    fn try_from(schema: Schema) -> AvroResult<Self> {
        Self::new(schema)
    }
}

/// Resolve all references in the schema, saving any named type found in `names`
///
/// `known_schemata` will be used to resolve references but they won't be added to `names`.
pub fn resolve_names<'s, 'n>(
    schema: &'s Schema,
    names: &mut NamesRef<'s>,
    enclosing_namespace: NamespaceRef,
    known_schemata: &NamesRef<'n>,
) -> AvroResult<()> {
    match schema {
        Schema::Array(schema) => {
            resolve_names(&schema.items, names, enclosing_namespace, known_schemata)
        }
        Schema::Map(schema) => {
            resolve_names(&schema.types, names, enclosing_namespace, known_schemata)
        }
        Schema::Union(UnionSchema { schemas, .. }) => {
            for schema in schemas {
                resolve_names(schema, names, enclosing_namespace, known_schemata)?
            }
            Ok(())
        }
        Schema::Enum(EnumSchema { name, .. })
        | Schema::Fixed(FixedSchema { name, .. })
        | Schema::Uuid(UuidSchema::Fixed(FixedSchema { name, .. }))
        | Schema::Decimal(DecimalSchema {
            inner: InnerDecimalSchema::Fixed(FixedSchema { name, .. }),
            ..
        })
        | Schema::Duration(FixedSchema { name, .. }) => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace).into_owned();
            if names.contains_key(&fully_qualified_name)
                || known_schemata.contains_key(&fully_qualified_name)
            {
                Err(Details::AmbiguousSchemaDefinition(fully_qualified_name).into())
            } else {
                names.insert(fully_qualified_name, schema);
                Ok(())
            }
        }
        Schema::Record(RecordSchema { name, fields, .. }) => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace).into_owned();
            if names.contains_key(&fully_qualified_name)
                || known_schemata.contains_key(&fully_qualified_name)
            {
                Err(Details::AmbiguousSchemaDefinition(fully_qualified_name).into())
            } else {
                let record_namespace = fully_qualified_name.namespace().map(ToString::to_string);
                names.insert(fully_qualified_name, schema);
                for field in fields {
                    resolve_names(
                        &field.schema,
                        names,
                        record_namespace.as_deref(),
                        known_schemata,
                    )?
                }
                Ok(())
            }
        }
        Schema::Ref { name } => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            if names.contains_key(&fully_qualified_name)
                || known_schemata.contains_key(&fully_qualified_name)
            {
                Ok(())
            } else {
                Err(Details::SchemaResolutionError(fully_qualified_name.into_owned()).into())
            }
        }
        _ => Ok(()),
    }
}

pub fn resolve_names_with_schemata<'s, 'n>(
    schemata: impl IntoIterator<Item = &'s Schema>,
    names: &mut NamesRef<'s>,
    enclosing_namespace: NamespaceRef,
    known_schemata: &NamesRef<'n>,
) -> AvroResult<()> {
    for schema in schemata {
        resolve_names(schema, names, enclosing_namespace, known_schemata)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ResolvedOwnedSchema, ResolvedSchema};
    use crate::{
        Schema,
        schema::{Name, NamesRef},
    };
    use apache_avro_test_helper::TestResult;
    use std::collections::HashMap;

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"inner_record_name",
                            "fields":[
                                {
                                    "name":"inner_field_1",
                                    "type":"double"
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_qualified_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"inner_record_name",
                            "fields":[
                                {
                                    "name":"inner_field_1",
                                    "type":"double"
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_enum_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_qualified_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_enum_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_fixed_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_qualified_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_fixed_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"inner_record_name",
                            "namespace":"inner_space",
                            "fields":[
                                {
                                    "name":"inner_field_1",
                                    "type":"double"
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "namespace": "inner_space",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_enum_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "namespace": "inner_space",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_fixed_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_outer_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"middle_record_name",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 3);
        for s in [
            "space.record_name",
            "space.middle_record_name",
            "space.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_middle_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "middle_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 3);
        for s in [
            "space.record_name",
            "middle_namespace.middle_record_name",
            "middle_namespace.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "namespace":"inner_namespace",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 3);
        for s in [
            "space.record_name",
            "middle_namespace.middle_record_name",
            "inner_namespace.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_in_array_resolution_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": {
                  "type":"array",
                  "items":{
                      "type":"record",
                      "name":"in_array_record",
                      "fields": [
                          {
                              "name":"array_record_field",
                              "type":"string"
                          }
                      ]
                  }
              }
            },
            {
                "name":"outer_field_2",
                "type":"in_array_record"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.in_array_record"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_in_map_resolution_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": {
                  "type":"map",
                  "values":{
                      "type":"record",
                      "name":"in_map_record",
                      "fields": [
                          {
                              "name":"map_record_field",
                              "type":"string"
                          }
                      ]
                  }
              }
            },
            {
                "name":"outer_field_2",
                "type":"in_map_record"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.in_map_record"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3466_test_to_json_inner_enum_inner_namespace() -> TestResult {
        let schema = r#"
        {
        "name": "record_name",
        "namespace": "space",
        "type": "record",
        "fields": [
            {
            "name": "outer_field_1",
            "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "namespace": "inner_space",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_enum_name"
            }
        ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;

        // confirm we have expected 2 full-names
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        // convert Schema back to JSON string
        let schema_str = serde_json::to_string(&schema)?;
        let _schema = Schema::parse_str(&schema_str)?;
        assert_eq!(schema, _schema);

        Ok(())
    }

    #[test]
    fn avro_3466_test_to_json_inner_fixed_inner_namespace() -> TestResult {
        let schema = r#"
        {
        "name": "record_name",
        "namespace": "space",
        "type": "record",
        "fields": [
            {
            "name": "outer_field_1",
            "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "namespace": "inner_space",
                            "size":54
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_fixed_name"
            }
        ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let rs = ResolvedSchema::new(&schema)?;

        // confirm we have expected 2 full-names
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        // convert Schema back to JSON string
        let schema_str = serde_json::to_string(&schema)?;
        let _schema = Schema::parse_str(&schema_str)?;
        assert_eq!(schema, _schema);

        Ok(())
    }

    #[test]
    fn avro_rs_339_schema_ref_uuid() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "name": "foo",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "uuid",
                        "name": "bar"
                    }
                },
                {
                    "name": "b",
                    "type": "bar"
                }
            ]
        }"#,
        )?;
        let _resolved = ResolvedSchema::new(&schema)?;
        let _resolved_owned = ResolvedOwnedSchema::try_from(schema)?;

        Ok(())
    }

    #[test]
    fn avro_rs_339_schema_ref_decimal() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "name": "foo",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "decimal",
                        "precision": 4,
                        "scale": 2,
                        "name": "bar"
                    }
                },
                {
                    "name": "b",
                    "type": "bar"
                }
            ]
        }"#,
        )?;
        let _resolved = ResolvedSchema::new(&schema)?;
        let _resolved_owned = ResolvedOwnedSchema::try_from(schema)?;

        Ok(())
    }

    #[test]
    fn avro_rs_444_do_not_allow_duplicate_names_in_known_schemata() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "name": "foo",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "decimal",
                        "precision": 4,
                        "scale": 2,
                        "name": "bar"
                    }
                },
                {
                    "name": "b",
                    "type": "bar"
                },
                {
                    "name": "c",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "uuid",
                        "name": "duplicated_name"
                    }
                }
            ]
        }"#,
        )?;

        let mut known_schemata: NamesRef = HashMap::default();
        known_schemata.insert("duplicated_name".try_into()?, &Schema::Boolean);

        let result = ResolvedSchema::new_with_known_schemata(vec![&schema], None, &known_schemata)
            .unwrap_err();

        assert_eq!(
            result.to_string(),
            "Two named schema defined for same fullname: duplicated_name."
        );

        Ok(())
    }
}
