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

use crate::schema::{
    Alias, ArraySchema, EnumSchema, FixedSchema, MapSchema, Name, RecordField, RecordSchema,
    UnionSchema,
};
use crate::types::Value;
use crate::{AvroResult, Schema};
use bon::bon;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};

#[bon]
impl Schema {
    /// Returns a `Schema::Map` with the given types and optional default
    /// and custom attributes.
    #[builder(finish_fn = build)]
    pub fn map(
        #[builder(start_fn)] types: Schema,
        default: Option<HashMap<String, Value>>,
        attributes: Option<BTreeMap<String, JsonValue>>,
    ) -> Self {
        let attributes = attributes.unwrap_or_default();
        Schema::Map(MapSchema {
            types: Box::new(types),
            default,
            attributes,
        })
    }

    /// Returns a `Schema::Array` with the given items and optional default
    /// and custom attributes.
    #[builder(finish_fn = build)]
    pub fn array(
        #[builder(start_fn)] items: Schema,
        default: Option<Vec<Value>>,
        attributes: Option<BTreeMap<String, JsonValue>>,
    ) -> Self {
        let attributes = attributes.unwrap_or_default();
        Schema::Array(ArraySchema {
            items: Box::new(items),
            default,
            attributes,
        })
    }

    /// Returns a `Schema::Enum` with the given name, symbols and optional
    /// aliases, doc, default and custom attributes.
    #[builder(finish_fn = build)]
    pub fn r#enum(
        #[builder(start_fn)] name: Name,
        #[builder(start_fn)] symbols: Vec<impl Into<String>>,
        aliases: Option<Vec<Alias>>,
        doc: Option<String>,
        default: Option<String>,
        attributes: Option<BTreeMap<String, JsonValue>>,
    ) -> Self {
        let attributes = attributes.unwrap_or_default();
        let symbols = symbols.into_iter().map(Into::into).collect();
        Schema::Enum(EnumSchema {
            name,
            symbols,
            aliases,
            doc,
            default,
            attributes,
        })
    }

    /// Returns a `Schema::Fixed` with the given name, size and optional
    /// aliases, doc and custom attributes.
    #[builder(finish_fn = build)]
    pub fn fixed(
        #[builder(start_fn)] name: Name,
        #[builder(start_fn)] size: usize,
        aliases: Option<Vec<Alias>>,
        doc: Option<String>,
        attributes: Option<BTreeMap<String, JsonValue>>,
    ) -> Self {
        let attributes = attributes.unwrap_or_default();
        Schema::Fixed(FixedSchema {
            name,
            size,
            aliases,
            doc,
            attributes,
        })
    }

    /// Returns a `Schema::Record` with the given name, size and optional
    /// aliases, doc and custom attributes.
    #[builder(finish_fn = build)]
    pub fn record(
        #[builder(start_fn)] name: Name,
        #[builder(default)] fields: Vec<RecordField>,
        aliases: Option<Vec<Alias>>,
        doc: Option<String>,
        #[builder(default)] attributes: BTreeMap<String, JsonValue>,
    ) -> Self {
        let record_schema = RecordSchema::builder()
            .name(name)
            .fields(fields)
            .aliases(aliases)
            .doc(doc)
            .attributes(attributes)
            .build();
        Schema::Record(record_schema)
    }

    /// Returns a [`Schema::Union`] with the given variants.
    ///
    /// # Errors
    /// Will return an error if `schemas` has duplicate unnamed schemas or if `schemas`
    /// contains a union.
    pub fn union(schemas: Vec<Schema>) -> AvroResult<Schema> {
        UnionSchema::new(schemas).map(Schema::Union)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro_test_helper::TestResult;

    #[test]
    fn avro_rs_472_enum_builder_only_mandatory() -> TestResult {
        let name = Name::new("enum_builder")?;
        let symbols = vec!["A", "B", "C", "D", "E"];

        let schema = Schema::r#enum(name.clone(), symbols.clone()).build();

        if let Schema::Enum(enum_schema) = schema {
            assert_eq!(enum_schema.name, name);
            assert_eq!(enum_schema.symbols, symbols);
            assert_eq!(enum_schema.aliases, None);
            assert_eq!(enum_schema.doc, None);
            assert_eq!(enum_schema.default, None);
            assert_eq!(enum_schema.attributes, Default::default());
        } else {
            panic!("Expected a Schema::Enum, got: {schema}");
        }

        Ok(())
    }

    #[test]
    fn avro_rs_472_enum_builder_with_optionals() -> TestResult {
        let name = Name::new("enum_builder")?;
        let symbols = vec!["A", "B", "C", "D", "E"];
        let aliases = vec![Alias::new("alias")?];
        let doc = "docu";
        let default = "default value";
        let attributes =
            BTreeMap::from_iter([("key".to_string(), JsonValue::String("value".into()))]);

        let schema = Schema::r#enum(name.clone(), symbols.clone())
            .aliases(aliases.clone())
            .doc(doc.into())
            .default(default.into())
            .attributes(attributes.clone())
            .build();

        if let Schema::Enum(enum_schema) = schema {
            assert_eq!(enum_schema.name, name);
            assert_eq!(enum_schema.symbols, symbols);
            assert_eq!(enum_schema.aliases, Some(aliases));
            assert_eq!(enum_schema.doc, Some(doc.into()));
            assert_eq!(enum_schema.default, Some(default.into()));
            assert_eq!(enum_schema.attributes, attributes);
        } else {
            panic!("Expected a Schema::Enum, got: {schema}");
        }

        Ok(())
    }

    #[test]
    fn avro_rs_472_fixed_builder_only_mandatory() -> TestResult {
        let name = Name::new("fixed_builder")?;
        let size = 123;

        let schema = Schema::fixed(name.clone(), size).build();

        if let Schema::Fixed(fixed_schema) = schema {
            assert_eq!(fixed_schema.name, name);
            assert_eq!(fixed_schema.size, size);
            assert_eq!(fixed_schema.aliases, None);
            assert_eq!(fixed_schema.doc, None);
            assert_eq!(fixed_schema.attributes, Default::default());
        } else {
            panic!("Expected a Schema::Fixed, got: {schema}");
        }

        Ok(())
    }

    #[test]
    fn avro_rs_472_fixed_builder_with_optionals() -> TestResult {
        let name = Name::new("fixed_builder")?;
        let size = 234;
        let aliases = vec![Alias::new("alias")?];
        let doc = "docu";
        let attributes =
            BTreeMap::from_iter([("key".to_string(), JsonValue::String("value".into()))]);

        let schema = Schema::fixed(name.clone(), size)
            .aliases(aliases.clone())
            .doc(doc.into())
            .attributes(attributes.clone())
            .build();

        if let Schema::Fixed(fixed_schema) = schema {
            assert_eq!(fixed_schema.name, name);
            assert_eq!(fixed_schema.size, size);
            assert_eq!(fixed_schema.aliases, Some(aliases));
            assert_eq!(fixed_schema.doc, Some(doc.into()));
            assert_eq!(fixed_schema.attributes, attributes);
        } else {
            panic!("Expected a Schema::Fixed, got: {schema}");
        }

        Ok(())
    }

    #[test]
    fn avro_rs_472_record_builder_only_mandatory() -> TestResult {
        let name = Name::new("record_builder")?;

        let schema = Schema::record(name.clone()).build();

        if let Schema::Record(record_schema) = schema {
            assert_eq!(record_schema.name, name);
            assert_eq!(record_schema.fields, vec![]);
            assert_eq!(record_schema.aliases, None);
            assert_eq!(record_schema.doc, None);
            assert_eq!(record_schema.lookup, Default::default());
            assert_eq!(record_schema.attributes, Default::default());
        } else {
            panic!("Expected a Schema::Record, got: {schema}");
        }

        Ok(())
    }

    #[test]
    fn avro_rs_472_record_builder_with_optionals() -> TestResult {
        let name = Name::new("record_builder")?;
        let fields = vec![
            RecordField::builder()
                .name("f1")
                .schema(Schema::Boolean)
                .build(),
            RecordField::builder()
                .name("f2")
                .schema(Schema::Int)
                .build(),
        ];
        let aliases = vec![Alias::new("alias")?];
        let doc = "docu";
        let attributes =
            BTreeMap::from_iter([("key".to_string(), JsonValue::String("value".into()))]);

        let schema = Schema::record(name.clone())
            .fields(fields.clone())
            .aliases(aliases.clone())
            .doc(doc.into())
            .attributes(attributes.clone())
            .build();

        if let Schema::Record(fixed_schema) = schema {
            assert_eq!(fixed_schema.name, name);
            assert_eq!(fixed_schema.fields, fields);
            assert_eq!(fixed_schema.aliases, Some(aliases));
            assert_eq!(fixed_schema.doc, Some(doc.into()));
            assert_eq!(
                fixed_schema.lookup,
                BTreeMap::from_iter([("f1".into(), 0), ("f2".into(), 1)])
            );
            assert_eq!(fixed_schema.attributes, attributes);
        } else {
            panic!("Expected a Schema::Record, got: {schema}");
        }

        Ok(())
    }

    #[test]
    fn avro_rs_472_union_builder() -> TestResult {
        let variants = vec![Schema::Null, Schema::Boolean, Schema::Int];

        let schema = Schema::union(variants.clone())?;

        if let Schema::Union(union_schema) = schema {
            assert_eq!(union_schema.variants(), variants);
        } else {
            panic!("Expected a Schema::Union, got: {schema}");
        }

        Ok(())
    }
}
