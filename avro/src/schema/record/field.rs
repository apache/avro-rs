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
use crate::schema::{Documentation, Name, Names, Parser, Schema, SchemaKind};
use crate::types;
use crate::util::MapHelper;
use crate::validator::validate_record_field_name;
use log::warn;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};

/// Represents a `field` in a `record` Avro schema.
#[derive(bon::Builder, Clone, PartialEq)]
pub struct RecordField {
    /// Name of the field.
    #[builder(into)]
    pub name: String,
    /// Documentation of the field.
    #[builder(default)]
    pub doc: Documentation,
    /// Aliases of the field's name. They have no namespace.
    #[builder(default)]
    pub aliases: Vec<String>,
    /// Default value of the field.
    /// This value will be used when reading Avro datum if schema resolution
    /// is enabled.
    pub default: Option<Value>,
    /// Schema of the field.
    pub schema: Schema,
    /// A collection of all unknown fields in the record field.
    #[builder(default = BTreeMap::new())]
    pub custom_attributes: BTreeMap<String, Value>,
}

impl Debug for RecordField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("RecordField");
        debug.field("name", &self.name);
        if let Some(doc) = &self.doc {
            debug.field("doc", &doc);
        }
        if !self.aliases.is_empty() {
            debug.field("aliases", &self.aliases);
        }
        if let Some(default) = &self.default {
            debug.field("default", &default);
        }
        debug.field("schema", &self.schema);
        if !self.custom_attributes.is_empty() {
            debug.field("custom_attributes", &self.custom_attributes);
        }
        if self.doc.is_none()
            || !self.aliases.is_empty()
            || self.default.is_none()
            || self.custom_attributes.is_empty()
        {
            debug.finish_non_exhaustive()
        } else {
            debug.finish()
        }
    }
}

impl RecordField {
    /// Parse a `serde_json::Value` into a `RecordField`.
    pub(crate) fn parse(
        field: &Map<String, Value>,
        parser: &mut Parser,
        enclosing_record: &Name,
    ) -> AvroResult<Self> {
        let name = field.name().ok_or(Details::GetNameFieldFromRecord)?;

        validate_record_field_name(&name)?;

        let ty = field.get("type").ok_or(Details::GetRecordFieldTypeField)?;
        let schema = parser.parse(ty, &enclosing_record.namespace)?;

        if let Some(logical_type) = field.get("logicalType") {
            warn!(
                "Ignored the {enclosing_record}.logicalType property (`{logical_type}`). It should probably be nested inside the `type` for the field"
            );
        }

        let default = field.get("default").cloned();
        Self::resolve_default_value(
            &schema,
            &name,
            &enclosing_record.fullname(None),
            parser.get_parsed_schemas(),
            &default,
        )?;

        let aliases = field
            .get("aliases")
            .and_then(|aliases| {
                aliases.as_array().map(|aliases| {
                    aliases
                        .iter()
                        .flat_map(|alias| alias.as_str())
                        .map(|alias| alias.to_string())
                        .collect::<Vec<String>>()
                })
            })
            .unwrap_or_default();

        Ok(RecordField {
            name,
            doc: field.doc(),
            default,
            aliases,
            custom_attributes: RecordField::get_field_custom_attributes(field),
            schema,
        })
    }

    fn resolve_default_value(
        field_schema: &Schema,
        field_name: &str,
        record_name: &str,
        names: &Names,
        default: &Option<Value>,
    ) -> AvroResult<()> {
        if let Some(value) = default {
            let avro_value = types::Value::try_from(value.clone())?;
            match field_schema {
                Schema::Union(union_schema) => {
                    let schemas = &union_schema.schemas;
                    let resolved = schemas.iter().any(|schema| {
                        avro_value
                            .to_owned()
                            .resolve_internal(schema, names, &schema.namespace(), &None)
                            .is_ok()
                    });

                    if !resolved {
                        let schema: Option<&Schema> = schemas.first();
                        return match schema {
                            Some(first_schema) => Err(Details::GetDefaultUnion(
                                SchemaKind::from(first_schema),
                                types::ValueKind::from(avro_value),
                            )
                            .into()),
                            None => Err(Details::EmptyUnion.into()),
                        };
                    }
                }
                _ => {
                    let resolved = avro_value
                        .resolve_internal(field_schema, names, &field_schema.namespace(), &None)
                        .is_ok();

                    if !resolved {
                        let schemata = names.values().cloned().collect::<Vec<_>>();
                        return Err(Details::GetDefaultRecordField(
                            field_name.to_string(),
                            record_name.to_string(),
                            field_schema
                                .independent_canonical_form(&schemata)
                                .unwrap_or_else(|_| field_schema.canonical_form()),
                            value.clone(),
                        )
                        .into());
                    }
                }
            };
        }

        Ok(())
    }

    fn get_field_custom_attributes(field: &Map<String, Value>) -> BTreeMap<String, Value> {
        let mut custom_attributes: BTreeMap<String, Value> = BTreeMap::new();
        for (key, value) in field {
            match key.as_str() {
                "type" | "name" | "doc" | "default" | "aliases" => continue,
                _ => custom_attributes.insert(key.clone(), value.clone()),
            };
        }
        custom_attributes
    }

    /// Returns true if this `RecordField` is nullable, meaning the schema is a `UnionSchema` where the first variant is `Null`.
    pub fn is_nullable(&self) -> bool {
        match self.schema {
            Schema::Union(ref inner) => inner.is_nullable(),
            _ => false,
        }
    }
}

impl Serialize for RecordField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("type", &self.schema)?;

        if let Some(default) = &self.default {
            map.serialize_entry("default", default)?;
        }

        if let Some(doc) = &self.doc {
            map.serialize_entry("doc", doc)?;
        }

        if !self.aliases.is_empty() {
            map.serialize_entry("aliases", &self.aliases)?;
        }

        for attr in &self.custom_attributes {
            map.serialize_entry(attr.0, attr.1)?;
        }

        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Name, Schema, UnionSchema};
    use apache_avro_test_helper::TestResult;
    use serde_json::json;

    #[test]
    fn test_avro_3621_nullable_record_field() -> TestResult {
        let nullable_record_field = RecordField::builder()
            .name("next".to_string())
            .schema(Schema::Union(UnionSchema::new(vec![
                Schema::Null,
                Schema::Ref {
                    name: Name {
                        name: "LongList".to_owned(),
                        namespace: None,
                    },
                },
            ])?))
            .build();

        assert!(nullable_record_field.is_nullable());

        let non_nullable_record_field = RecordField::builder()
            .name("next".to_string())
            .default(json!(2))
            .schema(Schema::Long)
            .build();

        assert!(!non_nullable_record_field.is_nullable());
        Ok(())
    }

    #[test]
    fn avro_rs_419_name_into() -> TestResult {
        let field = RecordField::builder()
            .name("str_slice")
            .schema(Schema::Boolean)
            .build();
        assert_eq!(field.name, "str_slice");

        let field = RecordField::builder()
            .name("String".to_string())
            .schema(Schema::Boolean)
            .build();
        assert_eq!(field.name, "String");

        Ok(())
    }
}
