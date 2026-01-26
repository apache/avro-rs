use crate::error::Details;
use crate::schema::{
    DecimalSchema, EnumSchema, FixedSchema, InnerDecimalSchema, Names, NamesRef, Namespace,
    RecordSchema, UnionSchema, UuidSchema,
};
use crate::{AvroResult, Error, Schema};
use std::borrow::Borrow;
use std::collections::HashMap;

#[derive(Debug)]
pub struct ResolvedSchema<'s> {
    pub(super) names_ref: NamesRef<'s>,
    schemata: Vec<&'s Schema>,
}

impl<'s> TryFrom<&'s Schema> for ResolvedSchema<'s> {
    type Error = Error;

    fn try_from(schema: &'s Schema) -> AvroResult<Self> {
        let names = HashMap::new();
        let mut rs = ResolvedSchema {
            names_ref: names,
            schemata: vec![schema],
        };
        rs.resolve(rs.get_schemata(), &None, None)?;
        Ok(rs)
    }
}

impl<'s> TryFrom<Vec<&'s Schema>> for ResolvedSchema<'s> {
    type Error = Error;

    fn try_from(schemata: Vec<&'s Schema>) -> AvroResult<Self> {
        let names = HashMap::new();
        let mut rs = ResolvedSchema {
            names_ref: names,
            schemata,
        };
        rs.resolve(rs.get_schemata(), &None, None)?;
        Ok(rs)
    }
}

impl<'s> ResolvedSchema<'s> {
    pub fn get_schemata(&self) -> Vec<&'s Schema> {
        self.schemata.clone()
    }

    pub fn get_names(&self) -> &NamesRef<'s> {
        &self.names_ref
    }

    /// Creates `ResolvedSchema` with some already known schemas.
    ///
    /// Those schemata would be used to resolve references if needed.
    pub fn new_with_known_schemata<'n>(
        schemata_to_resolve: Vec<&'s Schema>,
        enclosing_namespace: &Namespace,
        known_schemata: &'n NamesRef<'n>,
    ) -> AvroResult<Self> {
        let names = HashMap::new();
        let mut rs = ResolvedSchema {
            names_ref: names,
            schemata: schemata_to_resolve,
        };
        rs.resolve(rs.get_schemata(), enclosing_namespace, Some(known_schemata))?;
        Ok(rs)
    }

    fn resolve<'n>(
        &mut self,
        schemata: Vec<&'s Schema>,
        enclosing_namespace: &Namespace,
        known_schemata: Option<&'n NamesRef<'n>>,
    ) -> AvroResult<()> {
        for schema in schemata {
            match schema {
                Schema::Array(schema) => {
                    self.resolve(vec![&schema.items], enclosing_namespace, known_schemata)?
                }
                Schema::Map(schema) => {
                    self.resolve(vec![&schema.types], enclosing_namespace, known_schemata)?
                }
                Schema::Union(UnionSchema { schemas, .. }) => {
                    for schema in schemas {
                        self.resolve(vec![schema], enclosing_namespace, known_schemata)?
                    }
                }
                Schema::Enum(EnumSchema { name, .. })
                | Schema::Fixed(FixedSchema { name, .. })
                | Schema::Uuid(UuidSchema::Fixed(FixedSchema { name, .. }))
                | Schema::Decimal(DecimalSchema {
                    inner: InnerDecimalSchema::Fixed(FixedSchema { name, .. }),
                    ..
                })
                | Schema::Duration(FixedSchema { name, .. }) => {
                    let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
                    if self
                        .names_ref
                        .insert(fully_qualified_name.clone(), schema)
                        .is_some()
                    {
                        return Err(Details::AmbiguousSchemaDefinition(fully_qualified_name).into());
                    }
                }
                Schema::Record(RecordSchema { name, fields, .. }) => {
                    let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
                    if self
                        .names_ref
                        .insert(fully_qualified_name.clone(), schema)
                        .is_some()
                    {
                        return Err(Details::AmbiguousSchemaDefinition(fully_qualified_name).into());
                    } else {
                        let record_namespace = fully_qualified_name.namespace;
                        for field in fields {
                            self.resolve(vec![&field.schema], &record_namespace, known_schemata)?
                        }
                    }
                }
                Schema::Ref { name } => {
                    let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
                    // first search for reference in current schemata, then look into external references.
                    if !self.names_ref.contains_key(&fully_qualified_name) {
                        let is_resolved_with_known_schemas = known_schemata
                            .as_ref()
                            .map(|names| names.contains_key(&fully_qualified_name))
                            .unwrap_or(false);
                        if !is_resolved_with_known_schemas {
                            return Err(Details::SchemaResolutionError(fully_qualified_name).into());
                        }
                    }
                }
                _ => (),
            }
        }
        Ok(())
    }
}

pub struct ResolvedOwnedSchema {
    names: Names,
    root_schema: Schema,
}

impl TryFrom<Schema> for ResolvedOwnedSchema {
    type Error = Error;

    fn try_from(schema: Schema) -> AvroResult<Self> {
        let names = HashMap::new();
        let mut rs = ResolvedOwnedSchema {
            names,
            root_schema: schema,
        };
        resolve_names(&rs.root_schema, &mut rs.names, &None)?;
        Ok(rs)
    }
}

impl ResolvedOwnedSchema {
    pub fn get_root_schema(&self) -> &Schema {
        &self.root_schema
    }
    pub fn get_names(&self) -> &Names {
        &self.names
    }
}

pub fn resolve_names(
    schema: &Schema,
    names: &mut Names,
    enclosing_namespace: &Namespace,
) -> AvroResult<()> {
    match schema {
        Schema::Array(schema) => resolve_names(&schema.items, names, enclosing_namespace),
        Schema::Map(schema) => resolve_names(&schema.types, names, enclosing_namespace),
        Schema::Union(UnionSchema { schemas, .. }) => {
            for schema in schemas {
                resolve_names(schema, names, enclosing_namespace)?
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
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            if names
                .insert(fully_qualified_name.clone(), schema.clone())
                .is_some()
            {
                Err(Details::AmbiguousSchemaDefinition(fully_qualified_name).into())
            } else {
                Ok(())
            }
        }
        Schema::Record(RecordSchema { name, fields, .. }) => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            if names
                .insert(fully_qualified_name.clone(), schema.clone())
                .is_some()
            {
                Err(Details::AmbiguousSchemaDefinition(fully_qualified_name).into())
            } else {
                let record_namespace = fully_qualified_name.namespace;
                for field in fields {
                    resolve_names(&field.schema, names, &record_namespace)?
                }
                Ok(())
            }
        }
        Schema::Ref { name } => {
            let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
            names
                .get(&fully_qualified_name)
                .map(|_| ())
                .ok_or_else(|| Details::SchemaResolutionError(fully_qualified_name).into())
        }
        _ => Ok(()),
    }
}

pub fn resolve_names_with_schemata(
    schemata: impl IntoIterator<Item = impl Borrow<Schema>>,
    names: &mut Names,
    enclosing_namespace: &Namespace,
) -> AvroResult<()> {
    for schema in schemata {
        resolve_names(schema.borrow(), names, enclosing_namespace)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::Schema;
    use crate::schema::Name;
    use crate::schema::resolve::{ResolvedOwnedSchema, ResolvedSchema};
    use apache_avro_test_helper::TestResult;

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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_record_name"] {
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_record_name"] {
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_enum_name"] {
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_enum_name"] {
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_fixed_name"] {
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.inner_fixed_name"] {
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "inner_space.inner_record_name"] {
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "inner_space.inner_enum_name"] {
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "inner_space.inner_fixed_name"] {
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 3);
        for s in &[
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 3);
        for s in &[
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 3);
        for s in &[
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.in_array_record"] {
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "space.in_map_record"] {
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");

        // confirm we have expected 2 full-names
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "inner_space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        // convert Schema back to JSON string
        let schema_str = serde_json::to_string(&schema).expect("test failed");
        let _schema = Schema::parse_str(&schema_str).expect("test failed");
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
        let rs = ResolvedSchema::try_from(&schema).expect("Schema didn't successfully parse");

        // confirm we have expected 2 full-names
        assert_eq!(rs.get_names().len(), 2);
        for s in &["space.record_name", "inner_space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        // convert Schema back to JSON string
        let schema_str = serde_json::to_string(&schema).expect("test failed");
        let _schema = Schema::parse_str(&schema_str).expect("test failed");
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
        let _resolved = ResolvedSchema::try_from(&schema)?;
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
        let _resolved = ResolvedSchema::try_from(&schema)?;
        let _resolved_owned = ResolvedOwnedSchema::try_from(schema)?;

        Ok(())
    }
}
