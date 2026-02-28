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
use std::fmt::{Debug, Formatter};

/// A description of a Record schema.
#[derive(bon::Builder, Clone)]
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
    #[builder(default)]
    pub fields: Vec<RecordField>,
    /// The `lookup` table maps field names to their position in the `Vec`
    /// of `fields`.
    #[builder(skip = calculate_lookup_table(&fields))]
    pub lookup: BTreeMap<String, usize>,
    /// The custom attributes of the schema
    #[builder(default)]
    pub attributes: BTreeMap<String, Value>,
}

impl Debug for RecordSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("RecordSchema");
        debug.field("name", &self.name);
        if !self.aliases.is_empty() {
            debug.field("default", &self.aliases);
        }
        if let Some(doc) = &self.doc {
            debug.field("doc", doc);
        }
        debug.field("fields", &self.fields);
        if !self.attributes.is_empty() {
            debug.field("attributes", &self.attributes);
        }
        if self.aliases.is_empty() || self.doc.is_none() || self.attributes.is_empty() {
            debug.finish_non_exhaustive()
        } else {
            debug.finish()
        }
    }
}

impl<S: record_schema_builder::State> RecordSchemaBuilder<S> {
    /// Try to set a Name from the given string.
    pub fn try_name<T>(
        self,
        name: T,
    ) -> Result<RecordSchemaBuilder<record_schema_builder::SetName<S>>, <T as TryInto<Name>>::Error>
    where
        <S as record_schema_builder::State>::Name: record_schema_builder::IsUnset,
        T: TryInto<Name>,
    {
        let name = name.try_into()?;
        Ok(self.name(name))
    }
}

/// Calculate the lookup table for the given fields.
fn calculate_lookup_table(fields: &[RecordField]) -> BTreeMap<String, usize> {
    fields
        .iter()
        .enumerate()
        .map(|(i, field)| (field.name.clone(), i))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Schema;
    use apache_avro_test_helper::TestResult;
    use pretty_assertions::assert_eq;

    #[test]
    fn avro_rs_403_record_schema_builder_no_fields() -> TestResult {
        let name = Name::new("TestRecord")?;

        let record_schema = RecordSchema::builder().name(name.clone()).build();

        assert_eq!(record_schema.name, name);
        assert_eq!(record_schema.aliases, Vec::new());
        assert_eq!(record_schema.doc, None);
        assert_eq!(record_schema.fields.len(), 0);
        assert_eq!(record_schema.lookup.len(), 0);
        assert_eq!(record_schema.attributes.len(), 0);

        Ok(())
    }

    #[test]
    fn avro_rs_403_record_schema_builder_no_fields_with_aliases() -> TestResult {
        let name = Name::new("TestRecord")?;

        let record_schema = RecordSchema::builder()
            .name(name.clone())
            .aliases(vec!["alias_1".try_into()?])
            .build();

        assert_eq!(record_schema.name, name);
        assert_eq!(record_schema.aliases, vec!["alias_1".try_into()?]);
        assert_eq!(record_schema.doc, None);
        assert_eq!(record_schema.fields.len(), 0);
        assert_eq!(record_schema.lookup.len(), 0);
        assert_eq!(record_schema.attributes.len(), 0);

        Ok(())
    }

    #[test]
    fn avro_rs_403_record_schema_builder_no_fields_with_doc() -> TestResult {
        let name = Name::new("TestRecord")?;

        let record_schema = RecordSchema::builder()
            .name(name.clone())
            .doc(Some("some_doc".into()))
            .build();

        assert_eq!(record_schema.name, name);
        assert_eq!(record_schema.aliases, Vec::new());
        assert_eq!(record_schema.doc, Some("some_doc".into()));
        assert_eq!(record_schema.fields.len(), 0);
        assert_eq!(record_schema.lookup.len(), 0);
        assert_eq!(record_schema.attributes.len(), 0);

        Ok(())
    }

    #[test]
    fn avro_rs_403_record_schema_builder_no_fields_with_attributes() -> TestResult {
        let name = Name::new("TestRecord")?;
        let attrs: BTreeMap<String, Value> = [
            ("bool_key".into(), Value::Bool(true)),
            ("key_2".into(), Value::String("value_2".into())),
        ]
        .into_iter()
        .collect();

        let record_schema = RecordSchema::builder()
            .name(name.clone())
            .attributes(attrs.clone())
            .build();

        assert_eq!(record_schema.name, name);
        assert_eq!(record_schema.aliases, Vec::new());
        assert_eq!(record_schema.doc, None);
        assert_eq!(record_schema.fields.len(), 0);
        assert_eq!(record_schema.lookup.len(), 0);
        assert_eq!(record_schema.attributes, attrs);

        Ok(())
    }

    #[test]
    fn avro_rs_403_record_schema_builder_with_fields() -> TestResult {
        let name = Name::new("TestRecord")?;
        let fields = vec![
            RecordField::builder()
                .name("field1_null")
                .schema(Schema::Null)
                .build(),
            RecordField::builder()
                .name("field2_bool")
                .schema(Schema::Boolean)
                .build(),
        ];

        let record_schema = RecordSchema::builder()
            .name(name.clone())
            .fields(fields.clone())
            .build();

        let expected_lookup: BTreeMap<String, usize> =
            [("field1_null".into(), 0), ("field2_bool".into(), 1)]
                .iter()
                .cloned()
                .collect();

        assert_eq!(record_schema.name, name);
        assert_eq!(record_schema.aliases, Vec::new());
        assert_eq!(record_schema.doc, None);
        assert_eq!(record_schema.fields, fields);
        assert_eq!(record_schema.lookup, expected_lookup);
        assert_eq!(record_schema.attributes.len(), 0);

        Ok(())
    }

    #[test]
    fn avro_rs_419_name_into() -> TestResult {
        let schema = RecordSchema::builder().try_name("str_slice")?.build();
        assert_eq!(schema.name, "str_slice".try_into()?);

        let schema = RecordSchema::builder()
            .try_name("String".to_string())?
            .build();
        assert_eq!(schema.name, "String".try_into()?);

        Ok(())
    }
}
