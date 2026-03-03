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

//! Check if the reader's schema is compatible with the writer's schema.
//!
//! To allow for schema evolution, Avro supports resolving the writer's schema to the reader's schema.
//! To check if this is possible, [`SchemaCompatibility`] can be used. For the complete rules see
//! [the specification](https://avro.apache.org/docs/++version++/specification/#schema-resolution).
//!
//! There are three levels of compatibility.
//!
//! 1. Fully compatible schemas (`Ok(Compatibility::Full)`)
//!
//! For example, an integer can always be resolved to a long:
//!
//! ```
//! # use apache_avro::{Schema, schema_compatibility::{Compatibility, SchemaCompatibility}};
//! let writers_schema = Schema::array(Schema::Int).build();
//! let readers_schema = Schema::array(Schema::Long).build();
//! assert_eq!(SchemaCompatibility::can_read(&writers_schema, &readers_schema), Ok(Compatibility::Full));
//! ```
//!
//! 2. Incompatible schemas (`Err`)
//!
//! For example, a long can never be resolved to an int:
//!
//! ```
//! # use apache_avro::{Schema, schema_compatibility::SchemaCompatibility};
//! let writers_schema = Schema::array(Schema::Long).build();
//! let readers_schema = Schema::array(Schema::Int).build();
//! assert!(SchemaCompatibility::can_read(&writers_schema, &readers_schema).is_err());
//! ```
//!
//! 3. Partially compatible schemas (`Ok(Compatibility::Partial)`)
//!
//! For example, a union of a string and integer is only compatible with an integer if an integer was written:
//!
//! ```
//! # use apache_avro::{Error, Schema, schema_compatibility::{Compatibility, SchemaCompatibility}};
//! let writers_schema = Schema::union(vec![Schema::Int, Schema::String])?;
//! let readers_schema = Schema::Int;
//! assert_eq!(SchemaCompatibility::can_read(&writers_schema, &readers_schema), Ok(Compatibility::Partial));
//! # Ok::<(), Error>(())
//! ```
//!
use crate::{
    error::CompatibilityError,
    schema::{
        ArraySchema, DecimalSchema, EnumSchema, InnerDecimalSchema, MapSchema, RecordSchema,
        Schema, UuidSchema,
    },
};
use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::Hasher,
    iter::once,
    ops::BitAndAssign,
    ptr,
};

/// Check if two schemas can be resolved.
///
/// See [the module documentation] for more details.
///
/// [the module documentation]: crate::schema_compatibility
pub struct SchemaCompatibility;

impl SchemaCompatibility {
    /// Recursively check if the reader's schema can be resolved to the writer's schema
    pub fn can_read(
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> Result<Compatibility, CompatibilityError> {
        let mut c = Checker::new();
        c.can_read(writers_schema, readers_schema)
    }

    /// Recursively check if both schemas can be resolved to each other
    pub fn mutual_read(
        schema_a: &Schema,
        schema_b: &Schema,
    ) -> Result<Compatibility, CompatibilityError> {
        let mut c = SchemaCompatibility::can_read(schema_a, schema_b)?;
        c &= SchemaCompatibility::can_read(schema_b, schema_a)?;
        Ok(c)
    }
}

/// How compatible two schemas are.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Compatibility {
    /// Full compatibility, resolving will always work.
    Full,
    /// Partial compatibility, resolving may error.
    ///
    /// This can happen if an enum doesn't have all fields, or unions don't entirely overlap.
    Partial,
}

impl BitAndAssign for Compatibility {
    /// Combine two compatibilities.
    ///
    /// # Truth table
    /// |         | Full    | Partial |
    /// | ------- | ------- | ------- |
    /// | Full    | Full    | Partial |
    /// | Partial | Partial | Partial |
    fn bitand_assign(&mut self, rhs: Self) {
        match (*self, rhs) {
            (Self::Full, Self::Full) => *self = Self::Full,
            _ => *self = Self::Partial,
        }
    }
}

struct Checker {
    recursion: HashMap<(u64, u64), Compatibility>,
}

impl Checker {
    /// Create a new checker, with recursion set to an empty set.
    pub(crate) fn new() -> Self {
        Self {
            recursion: HashMap::new(),
        }
    }

    /// Check if the reader schema can be resolved from the writer schema.
    pub(crate) fn full_match_schemas(
        &mut self,
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> Result<Compatibility, CompatibilityError> {
        // Hash both reader and writer based on their pointer value. This is a fast way to see if
        // we get the exact same schemas multiple times (because of recursive types)
        let key = (
            Self::pointer_hash(writers_schema),
            Self::pointer_hash(readers_schema),
        );

        // If we already saw this pairing, return the previous value
        if let Some(c) = self.recursion.get(&key).copied() {
            Ok(c)
        } else {
            let c = self.inner_full_match_schemas(writers_schema, readers_schema)?;
            // Insert the new value
            self.recursion.insert(key, c);
            Ok(c)
        }
    }

    /// Hash a schema based only on its pointer value.
    fn pointer_hash(schema: &Schema) -> u64 {
        let mut hasher = DefaultHasher::new();
        ptr::hash(schema, &mut hasher);
        hasher.finish()
    }

    /// The actual implementation of [`full_match_schemas`] but without the recursion protection.
    ///
    /// This function should never be called directly as it can recurse infinitely on recursive types.
    #[rustfmt::skip]
    fn inner_full_match_schemas(
        &mut self,
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> Result<Compatibility, CompatibilityError> {
        // Compare unqualified names if the schemas have them
        if let Some(w_name) = writers_schema.name()
            && let Some(r_name) = readers_schema.name()
            && w_name.name != r_name.name
        {
            return Err(CompatibilityError::NameMismatch {
                writer_name: w_name.name.clone(),
                reader_name: r_name.name.clone(),
            });
        }

        // Logical types are downgraded to their actual type
        match (writers_schema, readers_schema) {
            (Schema::Ref { name: w_name }, Schema::Ref { name: r_name }) => {
                if r_name == w_name {
                    Ok(Compatibility::Full)
                } else {
                    Err(CompatibilityError::NameMismatch {
                        writer_name: w_name.fullname(None),
                        reader_name: r_name.fullname(None),
                    })
                }
            }
            (Schema::Union(writer), Schema::Union(reader)) => {
                let mut any = false;
                let mut all = true;
                for writer in &writer.schemas {
                    // Try to find a reader variant that is fully compatible with this writer variant.
                    // In case that does not exist, we keep track of any partial compatibility we find.
                    let mut local_any = false;
                    all &= reader.schemas.iter().any(|reader| {
                        match self.full_match_schemas(writer, reader) {
                            Ok(Compatibility::Full) => {
                                local_any = true;
                                true
                            }
                            Ok(Compatibility::Partial) => {
                                local_any = true;
                                false
                            }
                            Err(_) => false,
                        }
                    });
                    // Save any match we found
                    any |= local_any;
                }
                if all {
                    // All writer variants are fully compatible with reader variants
                    Ok(Compatibility::Full)
                } else if any {
                    // At least one writer variant is partially or fully compatible with a reader variant
                    Ok(Compatibility::Partial)
                } else {
                    Err(CompatibilityError::MissingUnionElements)
                }
            }
            (Schema::Union(writer), _) => {
                // Check if all writer variants are fully compatible with the reader schema.
                // We keep track of if we see any (partial) compatibility.
                let mut any = false;
                let mut all = true;
                for writer in &writer.schemas {
                    match self.full_match_schemas(writer, readers_schema) {
                        Ok(Compatibility::Full) => any = true,
                        Ok(Compatibility::Partial) => {
                            any = true;
                            all = false;
                        }
                        Err(_) => {
                            all = false;
                        }
                    }
                }
                if all {
                    // All writer variants are fully compatible with the reader schema
                    Ok(Compatibility::Full)
                } else if any {
                    // At least one writer variant is partially compatible with the reader schema
                    Ok(Compatibility::Partial)
                } else {
                    Err(CompatibilityError::SchemaMismatchAllUnionElements)
                }
            }
            (_, Schema::Union(reader)) => {
                // Try to find a fully compatible reader variant for the writer schema.
                // In case that does not exist, we keep track of any partial compatibility.
                let mut partial = false;
                if reader.schemas.iter().any(|reader| {
                    match self.full_match_schemas(writers_schema, reader) {
                        Ok(Compatibility::Full) => true,
                        Ok(Compatibility::Partial) => {
                            partial = true;
                            false
                        }
                        Err(_) => false,
                    }
                }) {
                    // At least one reader variant is fully compatible with the writer schema
                    Ok(Compatibility::Full)
                } else if partial {
                    // At least one reader variant is partially compatible with the writer schema
                    Ok(Compatibility::Partial)
                } else {
                    Err(CompatibilityError::SchemaMismatchAllUnionElements)
                }
            }
            (Schema::Null, Schema::Null) => Ok(Compatibility::Full),
            (Schema::Boolean, Schema::Boolean) => Ok(Compatibility::Full),
            // int promotes to long, float and double
            (
                Schema::Int | Schema::Date | Schema::TimeMillis,
                Schema::Int | Schema::Long | Schema::Float | Schema::Double | Schema::Date
                | Schema::TimeMillis | Schema::TimeMicros | Schema::TimestampMillis
                | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis
                | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos,
            ) => Ok(Compatibility::Full),
            // long promotes to float and double
            (
                Schema::Long | Schema::TimeMicros | Schema::TimestampMillis
                | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis
                | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos,
                Schema::Long | Schema::Float | Schema::Double | Schema::TimeMicros
                | Schema::TimestampMillis | Schema::TimestampMicros | Schema::TimestampNanos
                | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros
                | Schema::LocalTimestampNanos,
            ) => Ok(Compatibility::Full),
            // float promotes to double
            (Schema::Float, Schema::Float | Schema::Double) => Ok(Compatibility::Full),
            (Schema::Double, Schema::Double) => Ok(Compatibility::Full),
            // This check needs to be above the other Decimal checks, so that if both schemas are
            // Decimal then the precision and scale are checked. The other Decimal checks below will
            // thus only hit if only one of the schemas is a Decimal
            (
                Schema::Decimal(DecimalSchema { precision: w_precision, scale: w_scale, .. }),
                Schema::Decimal(DecimalSchema { precision: r_precision, scale: r_scale, .. }),
            ) => {
                // precision and scale must match
                if r_precision == w_precision && r_scale == w_scale {
                    Ok(Compatibility::Full)
                } else {
                    Err(CompatibilityError::DecimalMismatch {
                        r_precision: *r_precision,
                        r_scale: *r_scale,
                        w_precision: *w_precision,
                        w_scale: *w_scale
                    })
                }
            }
            // bytes and strings are interchangeable
            (
                Schema::Bytes | Schema::String | Schema::BigDecimal
                | Schema::Uuid(UuidSchema::String | UuidSchema::Bytes)
                | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Bytes, .. }),
                Schema::Bytes | Schema::String | Schema::BigDecimal
                | Schema::Uuid(UuidSchema::String | UuidSchema::Bytes)
                | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Bytes, .. }),
            ) => Ok(Compatibility::Full),
            (Schema::Uuid(_), Schema::Uuid(_)) => Ok(Compatibility::Full),
            (
                Schema::Fixed(w_fixed) | Schema::Uuid(UuidSchema::Fixed(w_fixed))
                | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Fixed(w_fixed), .. })
                | Schema::Duration(w_fixed),
                Schema::Fixed(r_fixed) | Schema::Uuid(UuidSchema::Fixed(r_fixed))
                | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Fixed(r_fixed), .. })
                | Schema::Duration(r_fixed),
            ) => {
                // Size must match
                if r_fixed.size == w_fixed.size {
                    Ok(Compatibility::Full)
                } else {
                    Err(CompatibilityError::FixedMismatch)
                }
            }
            (
                Schema::Array(ArraySchema { items: w_items, .. }),
                Schema::Array(ArraySchema { items: r_items, .. }),
            ) => {
                // array schemas must match
                self.full_match_schemas(w_items, r_items)
            }
            (
                Schema::Map(MapSchema { types: w_types, .. }),
                Schema::Map(MapSchema { types: r_types, .. }),
            ) => {
                // type schemas must match
                self.full_match_schemas(w_types, r_types)
            }
            (
                Schema::Enum(EnumSchema { symbols: w_symbols, .. }),
                Schema::Enum(EnumSchema { symbols: r_symbols, default: r_default, .. }),
            ) => {
                // Reader must have a default or all symbols in the writer must also be in the reader
                if r_default.is_some() {
                    // No need to iter over all the fields if there is a default
                    Ok(Compatibility::Full)
                } else {
                    let mut any = false;
                    let mut all = true;
                    w_symbols.iter().for_each(|s| {
                        let res = r_symbols.contains(s);
                        any |= res;
                        all &= res;
                    });
                    if all {
                        // All symbols match
                        Ok(Compatibility::Full)
                    } else if any {
                        // Only some symbols match
                        Ok(Compatibility::Partial)
                    } else {
                        // No symbols match
                        Err(CompatibilityError::MissingSymbols)
                    }
                }
            }
            (
                Schema::Record(RecordSchema { fields: w_fields, .. }),
                Schema::Record(RecordSchema { fields: r_fields, .. }),
            ) => {
                let mut compatibility = Compatibility::Full;
                for r_field in r_fields {
                    // Can't use RecordField.lookup as aliases are also inserted into there and we
                    // are not allowed to match on writer aliases.
                    // Search using field name and *after* that aliases.
                    if let Some(w_field) = once(&r_field.name)
                        .chain(r_field.aliases.iter())
                        .find_map(|ra| w_fields.iter().find(|wf| &wf.name == ra))
                    {
                        // Check that the schemas are compatible
                        match self.full_match_schemas(&w_field.schema, &r_field.schema) {
                            Ok(c) => compatibility &= c,
                            Err(err) => {
                                return Err(CompatibilityError::FieldTypeMismatch(
                                    r_field.name.clone(),
                                    Box::new(err),
                                ));
                            }
                        }
                    } else if r_field.default.is_none() {
                        // No default and no matching field in the writer
                        return Err(CompatibilityError::MissingDefaultValue(
                            r_field.name.clone(),
                        ));
                    }
                }
                Ok(compatibility)
            }
            (_, _) => Err(CompatibilityError::WrongType {
                writer_schema_type: format!("{writers_schema:#?}"),
                reader_schema_type: format!("{readers_schema:#?}"),
            }),
        }
    }

    pub(crate) fn can_read(
        &mut self,
        writers_schema: &Schema,
        readers_schema: &Schema,
    ) -> Result<Compatibility, CompatibilityError> {
        self.full_match_schemas(writers_schema, readers_schema)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        Codec, Decimal, Reader, Writer,
        schema::{FixedSchema, Name, UuidSchema},
        types::{Record, Value},
    };
    use apache_avro_test_helper::TestResult;
    use rstest::*;

    fn int_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"int"}"#).unwrap()
    }

    fn long_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"long"}"#).unwrap()
    }

    fn string_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"string"}"#).unwrap()
    }

    fn int_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"int"}"#).unwrap()
    }

    fn long_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"long"}"#).unwrap()
    }

    fn string_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"string"}"#).unwrap()
    }

    fn enum1_ab_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["A","B"]}"#).unwrap()
    }

    fn enum1_abc_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["A","B","C"]}"#).unwrap()
    }

    fn enum1_bc_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["B","C"]}"#).unwrap()
    }

    fn enum2_ab_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum2", "symbols":["A","B"]}"#).unwrap()
    }

    fn empty_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[]}"#).unwrap()
    }

    fn empty_record2_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record2", "fields": []}"#).unwrap()
    }

    fn a_int_record1_schema() -> Schema {
        Schema::parse_str(
            r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}]}"#,
        )
        .unwrap()
    }

    fn a_long_record1_schema() -> Schema {
        Schema::parse_str(
            r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"long"}]}"#,
        )
        .unwrap()
    }

    fn a_int_b_int_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}, {"name":"b", "type":"int"}]}"#).unwrap()
    }

    fn a_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int", "default":0}]}"#).unwrap()
    }

    fn a_int_b_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}, {"name":"b", "type":"int", "default":0}]}"#).unwrap()
    }

    fn a_dint_b_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int", "default":0}, {"name":"b", "type":"int", "default":0}]}"#).unwrap()
    }

    fn nested_record() -> Schema {
        Schema::parse_str(r#"{"type":"record","name":"parent","fields":[{"name":"attribute","type":{"type":"record","name":"child","fields":[{"name":"id","type":"string"}]}}]}"#).unwrap()
    }

    fn nested_optional_record() -> Schema {
        Schema::parse_str(r#"{"type":"record","name":"parent","fields":[{"name":"attribute","type":["null",{"type":"record","name":"child","fields":[{"name":"id","type":"string"}]}],"default":null}]}"#).unwrap()
    }

    fn int_list_record_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"List", "fields": [{"name": "head", "type": "int"},{"name": "tail", "type": {"type": "array", "items": "int"}}]}"#).unwrap()
    }

    fn long_list_record_schema() -> Schema {
        Schema::parse_str(
            r#"
      {
        "type":"record", "name":"List", "fields": [
          {"name": "head", "type": "long"},
          {"name": "tail", "type": {"type": "array", "items": "long"}}
      ]}
"#,
        )
        .unwrap()
    }

    fn union_schema(schemas: Vec<Schema>) -> Schema {
        let schema_string = schemas
            .iter()
            .map(|s| s.canonical_form())
            .collect::<Vec<String>>()
            .join(",");
        Schema::parse_str(&format!("[{schema_string}]")).unwrap()
    }

    fn empty_union_schema() -> Schema {
        union_schema(vec![])
    }

    // unused
    // fn null_union_schema() -> Schema { union_schema(vec![Schema::Null]) }

    fn int_union_schema() -> Schema {
        union_schema(vec![Schema::Int])
    }

    fn long_union_schema() -> Schema {
        union_schema(vec![Schema::Long])
    }

    fn string_union_schema() -> Schema {
        union_schema(vec![Schema::String])
    }

    fn int_string_union_schema() -> Schema {
        union_schema(vec![Schema::Int, Schema::String])
    }

    fn string_int_union_schema() -> Schema {
        union_schema(vec![Schema::String, Schema::Int])
    }

    #[test]
    fn test_broken() {
        assert_eq!(
            Compatibility::Partial,
            SchemaCompatibility::can_read(&int_string_union_schema(), &int_union_schema()).unwrap(),
            "Only compatible if writer writes an int"
        )
    }

    #[test]
    fn test_incompatible_reader_writer_pairs() {
        let incompatible_schemas = vec![
            // null
            (Schema::Null, Schema::Int),
            (Schema::Null, Schema::Long),
            // boolean
            (Schema::Boolean, Schema::Int),
            // int
            (Schema::Int, Schema::Null),
            (Schema::Int, Schema::Boolean),
            (Schema::Int, Schema::Long),
            (Schema::Int, Schema::Float),
            (Schema::Int, Schema::Double),
            // long
            (Schema::Long, Schema::Float),
            (Schema::Long, Schema::Double),
            // float
            (Schema::Float, Schema::Double),
            // string
            (Schema::String, Schema::Boolean),
            (Schema::String, Schema::Int),
            // bytes
            (Schema::Bytes, Schema::Null),
            (Schema::Bytes, Schema::Int),
            // logical types
            (Schema::TimeMicros, Schema::Int),
            (Schema::TimestampMillis, Schema::Int),
            (Schema::TimestampMicros, Schema::Int),
            (Schema::TimestampNanos, Schema::Int),
            (Schema::LocalTimestampMillis, Schema::Int),
            (Schema::LocalTimestampMicros, Schema::Int),
            (Schema::LocalTimestampNanos, Schema::Int),
            (Schema::Date, Schema::Long),
            (Schema::TimeMillis, Schema::Long),
            // array and maps
            (int_array_schema(), long_array_schema()),
            (int_map_schema(), int_array_schema()),
            (int_array_schema(), int_map_schema()),
            (int_map_schema(), long_map_schema()),
            // enum
            (enum1_ab_schema(), enum1_abc_schema()),
            (enum1_bc_schema(), enum1_abc_schema()),
            (enum1_ab_schema(), enum2_ab_schema()),
            (Schema::Int, enum2_ab_schema()),
            (enum2_ab_schema(), Schema::Int),
            //union
            (int_union_schema(), int_string_union_schema()),
            (string_union_schema(), int_string_union_schema()),
            //record
            (empty_record2_schema(), empty_record1_schema()),
            (a_int_record1_schema(), empty_record1_schema()),
            (a_int_b_dint_record1_schema(), empty_record1_schema()),
            (int_list_record_schema(), long_list_record_schema()),
            (nested_record(), nested_optional_record()),
        ];

        assert!(
            incompatible_schemas
                .iter()
                .any(|(reader, writer)| SchemaCompatibility::can_read(writer, reader).is_err())
        );
    }

    #[rstest]
    // Record type test
    #[case(
        r#"{"type": "record", "name": "record_a", "fields": [{"type": "long", "name": "date"}]}"#,
        r#"{"type": "record", "name": "record_a", "fields": [{"type": "long", "name": "date", "default": 18181}]}"#
    )]
    // Fixed type test
    #[case(
        r#"{"type": "fixed", "name": "EmployeeId", "size": 16}"#,
        r#"{"type": "fixed", "name": "EmployeeId", "size": 16, "default": "u00ffffffffffffx"}"#
    )]
    // Enum type test
    #[case(
        r#"{"type": "enum", "name":"Enum1", "symbols": ["A","B"]}"#,
        r#"{"type": "enum", "name":"Enum1", "symbols": ["A","B", "C"], "default": "C"}"#
    )]
    // Map type test
    #[case(
        r#"{"type": "map", "values": "int"}"#,
        r#"{"type": "map", "values": "long"}"#
    )]
    // Date type
    #[case(r#"{"type": "int"}"#, r#"{"type": "int", "logicalType": "date"}"#)]
    // time-millis type
    #[case(
        r#"{"type": "int"}"#,
        r#"{"type": "int", "logicalType": "time-millis"}"#
    )]
    // time-micros type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "time-micros"}"#
    )]
    // timestamp-nanos type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "timestamp-nanos"}"#
    )]
    // timestamp-millis type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "timestamp-millis"}"#
    )]
    // timestamp-micros type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "timestamp-micros"}"#
    )]
    // local-timestamp-millis type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "local-timestamp-millis"}"#
    )]
    // local-timestamp-micros type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "local-timestamp-micros"}"#
    )]
    // local-timestamp-nanos type
    #[case(
        r#"{"type": "long"}"#,
        r#"{"type": "long", "logicalType": "local-timestamp-nanos"}"#
    )]
    // Array type test
    #[case(
        r#"{"type": "array", "items": "int"}"#,
        r#"{"type": "array", "items": "long"}"#
    )]
    fn test_avro_3950_match_schemas_ok(
        #[case] writer_schema_str: &str,
        #[case] reader_schema_str: &str,
    ) {
        let writer_schema = Schema::parse_str(writer_schema_str).unwrap();
        let reader_schema = Schema::parse_str(reader_schema_str).unwrap();

        assert!(SchemaCompatibility::can_read(&writer_schema, &reader_schema).is_ok());
    }

    #[rstest]
    // Record type test
    #[case(
        r#"{"type": "record", "name":"record_a", "fields": [{"type": "long", "name": "date"}]}"#,
        r#"{"type": "record", "name":"record_b", "fields": [{"type": "long", "name": "date"}]}"#,
        CompatibilityError::NameMismatch{writer_name: String::from("record_a"), reader_name: String::from("record_b")}
    )]
    // Fixed type test
    #[case(
        r#"{"type": "fixed", "name": "EmployeeId", "size": 16}"#,
        r#"{"type": "fixed", "name": "EmployeeId", "size": 20}"#,
        CompatibilityError::FixedMismatch
    )]
    // Enum type test
    #[case(
        r#"{"type": "enum", "name": "Enum1", "symbols": ["A","B"]}"#,
        r#"{"type": "enum", "name": "Enum2", "symbols": ["A","B"]}"#,
        CompatibilityError::NameMismatch{writer_name: String::from("Enum1"), reader_name: String::from("Enum2")}
    )]
    // Map type test
    #[case(
        r#"{"type":"map", "values": "long"}"#,
        r#"{"type":"map", "values": "int"}"#,
        CompatibilityError::WrongType { writer_schema_type: "Long".to_string(), reader_schema_type: "Int".to_string() }
    )]
    // Array type test
    #[case(
        r#"{"type": "array", "items": "long"}"#,
        r#"{"type": "array", "items": "int"}"#,
        CompatibilityError::WrongType { writer_schema_type: "Long".to_string(), reader_schema_type: "Int".to_string() }
    )]
    // Date type test
    #[case(
        r#"{"type": "string"}"#,
        r#"{"type": "int", "logicalType": "date"}"#,
        CompatibilityError::WrongType { writer_schema_type: "String".to_string(), reader_schema_type: "Date".to_string() }
    )]
    // time-millis type
    #[case(
        r#"{"type": "string"}"#,
        r#"{"type": "int", "logicalType": "time-millis"}"#,
        CompatibilityError::WrongType { writer_schema_type: "String".to_string(), reader_schema_type: "TimeMillis".to_string() }
    )]
    // time-millis type
    #[case(
        r#"{"type": "string"}"#,
        r#"{"type": "long", "logicalType": "time-micros"}"#,
        CompatibilityError::WrongType { writer_schema_type: "String".to_string(), reader_schema_type: "TimeMicros".to_string() }
    )]
    // timestamp-nanos type
    #[case(
        r#"{"type": "string"}"#,
        r#"{"type": "long", "logicalType": "timestamp-nanos"}"#,
        CompatibilityError::WrongType { writer_schema_type: "String".to_string(), reader_schema_type: "TimestampNanos".to_string() }
    )]
    // timestamp-millis type
    #[case(
        r#"{"type": "string"}"#,
        r#"{"type": "long", "logicalType": "timestamp-millis"}"#,
        CompatibilityError::WrongType { writer_schema_type: "String".to_string(), reader_schema_type: "TimestampMillis".to_string() }
    )]
    // timestamp-micros type
    #[case(
        r#"{"type": "string"}"#,
        r#"{"type": "long", "logicalType": "timestamp-micros"}"#,
        CompatibilityError::WrongType { writer_schema_type: "String".to_string(), reader_schema_type: "TimestampMicros".to_string() }
    )]
    // local-timestamp-millis type
    #[case(
        r#"{"type": "string"}"#,
        r#"{"type": "long", "logicalType": "local-timestamp-millis"}"#,
        CompatibilityError::WrongType { writer_schema_type: "String".to_string(), reader_schema_type: "LocalTimestampMillis".to_string() }
    )]
    // local-timestamp-micros type
    #[case(
        r#"{"type": "string"}"#,
        r#"{"type": "long", "logicalType": "local-timestamp-micros"}"#,
        CompatibilityError::WrongType { writer_schema_type: "String".to_string(), reader_schema_type: "LocalTimestampMicros".to_string() }
    )]
    // local-timestamp-nanos type
    #[case(
        r#"{"type": "string"}"#,
        r#"{"type": "long", "logicalType": "local-timestamp-nanos"}"#,
        CompatibilityError::WrongType { writer_schema_type: "String".to_string(), reader_schema_type: "LocalTimestampNanos".to_string() }
    )]
    // Names are checked first, so this should not be a WrongType
    #[case(
        r#"{"type": "record", "name":"record_b", "fields": [{"type": "long", "name": "date"}]}"#,
        r#"{"type": "fixed", "name": "EmployeeId", "size": 16}"#,
        CompatibilityError::NameMismatch { writer_name: "record_b".to_string(), reader_name: "EmployeeId".to_string() }
    )]
    fn test_avro_3950_match_schemas_error(
        #[case] writer_schema_str: &str,
        #[case] reader_schema_str: &str,
        #[case] expected_error: CompatibilityError,
    ) {
        let writer_schema = Schema::parse_str(writer_schema_str).unwrap();
        let reader_schema = Schema::parse_str(reader_schema_str).unwrap();

        assert_eq!(
            expected_error,
            SchemaCompatibility::can_read(&writer_schema, &reader_schema).unwrap_err()
        )
    }

    #[test]
    fn test_compatible_reader_writer_pairs() {
        let uuid_fixed = FixedSchema {
            name: Name::new("uuid_fixed").unwrap(),
            aliases: None,
            doc: None,
            size: 16,
            attributes: Default::default(),
        };

        let compatible_schemas = vec![
            (Schema::Null, Schema::Null),
            (Schema::Long, Schema::Int),
            (Schema::Float, Schema::Int),
            (Schema::Float, Schema::Long),
            (Schema::Double, Schema::Long),
            (Schema::Double, Schema::Int),
            (Schema::Double, Schema::Float),
            (Schema::String, Schema::Bytes),
            (Schema::Bytes, Schema::String),
            // logical types
            (
                Schema::Uuid(UuidSchema::String),
                Schema::Uuid(UuidSchema::String),
            ),
            (Schema::Uuid(UuidSchema::String), Schema::String),
            (Schema::String, Schema::Uuid(UuidSchema::String)),
            (
                Schema::Uuid(UuidSchema::Bytes),
                Schema::Uuid(UuidSchema::Bytes),
            ),
            (Schema::Uuid(UuidSchema::Bytes), Schema::Bytes),
            (Schema::Bytes, Schema::Uuid(UuidSchema::Bytes)),
            (
                Schema::Uuid(UuidSchema::Fixed(uuid_fixed.clone())),
                Schema::Uuid(UuidSchema::Fixed(uuid_fixed.clone())),
            ),
            (
                Schema::Uuid(UuidSchema::Fixed(uuid_fixed.clone())),
                Schema::Fixed(uuid_fixed.clone()),
            ),
            (
                Schema::Fixed(uuid_fixed.clone()),
                Schema::Uuid(UuidSchema::Fixed(uuid_fixed.clone())),
            ),
            (Schema::Date, Schema::Int),
            (Schema::TimeMillis, Schema::Int),
            (Schema::TimeMicros, Schema::Long),
            (Schema::TimestampMillis, Schema::Long),
            (Schema::TimestampMicros, Schema::Long),
            (Schema::TimestampNanos, Schema::Long),
            (Schema::LocalTimestampMillis, Schema::Long),
            (Schema::LocalTimestampMicros, Schema::Long),
            (Schema::LocalTimestampNanos, Schema::Long),
            (Schema::Int, Schema::Date),
            (Schema::Int, Schema::TimeMillis),
            (Schema::Long, Schema::TimeMicros),
            (Schema::Long, Schema::TimestampMillis),
            (Schema::Long, Schema::TimestampMicros),
            (Schema::Long, Schema::TimestampNanos),
            (Schema::Long, Schema::LocalTimestampMillis),
            (Schema::Long, Schema::LocalTimestampMicros),
            (Schema::Long, Schema::LocalTimestampNanos),
            (int_array_schema(), int_array_schema()),
            (long_array_schema(), int_array_schema()),
            (int_map_schema(), int_map_schema()),
            (long_map_schema(), int_map_schema()),
            (enum1_ab_schema(), enum1_ab_schema()),
            (enum1_abc_schema(), enum1_ab_schema()),
            (empty_union_schema(), empty_union_schema()),
            (int_union_schema(), int_union_schema()),
            (int_string_union_schema(), string_int_union_schema()),
            (int_union_schema(), empty_union_schema()),
            (long_union_schema(), int_union_schema()),
            (int_union_schema(), Schema::Int),
            (Schema::Int, int_union_schema()),
            (empty_record1_schema(), empty_record1_schema()),
            (empty_record1_schema(), a_int_record1_schema()),
            (a_int_record1_schema(), a_int_record1_schema()),
            (a_dint_record1_schema(), a_int_record1_schema()),
            (a_dint_record1_schema(), a_dint_record1_schema()),
            (a_int_record1_schema(), a_dint_record1_schema()),
            (a_long_record1_schema(), a_int_record1_schema()),
            (a_int_record1_schema(), a_int_b_int_record1_schema()),
            (a_dint_record1_schema(), a_int_b_int_record1_schema()),
            (a_int_b_dint_record1_schema(), a_int_record1_schema()),
            (a_dint_b_dint_record1_schema(), empty_record1_schema()),
            (a_dint_b_dint_record1_schema(), a_int_record1_schema()),
            (a_int_b_int_record1_schema(), a_dint_b_dint_record1_schema()),
            (int_list_record_schema(), int_list_record_schema()),
            (long_list_record_schema(), long_list_record_schema()),
            (long_list_record_schema(), int_list_record_schema()),
            (nested_optional_record(), nested_record()),
        ];

        for (reader, writer) in compatible_schemas {
            SchemaCompatibility::can_read(&writer, &reader).unwrap();
        }
    }

    fn writer_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"},
        {"name":"oldfield2", "type":"string"}
      ]}
"#,
        )
        .unwrap()
    }

    #[test]
    fn test_missing_field() -> TestResult {
        let reader_schema = Schema::parse_str(
            r#"
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"}
      ]}
"#,
        )?;
        assert!(SchemaCompatibility::can_read(&writer_schema(), &reader_schema,).is_ok());
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("oldfield2")),
            SchemaCompatibility::can_read(&reader_schema, &writer_schema()).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_missing_second_field() -> TestResult {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield2", "type":"string"}
        ]}
"#,
        )?;
        assert!(SchemaCompatibility::can_read(&writer_schema(), &reader_schema).is_ok());
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("oldfield1")),
            SchemaCompatibility::can_read(&reader_schema, &writer_schema()).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_all_fields() -> TestResult {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"oldfield2", "type":"string"}
        ]}
"#,
        )?;
        assert!(SchemaCompatibility::can_read(&writer_schema(), &reader_schema).is_ok());
        assert!(SchemaCompatibility::can_read(&reader_schema, &writer_schema()).is_ok());

        Ok(())
    }

    #[test]
    fn test_new_field_with_default() -> TestResult {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"newfield1", "type":"int", "default":42}
        ]}
"#,
        )?;
        assert!(SchemaCompatibility::can_read(&writer_schema(), &reader_schema).is_ok());
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("oldfield2")),
            SchemaCompatibility::can_read(&reader_schema, &writer_schema()).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_new_field() -> TestResult {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"newfield1", "type":"int"}
        ]}
"#,
        )?;
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("newfield1")),
            SchemaCompatibility::can_read(&writer_schema(), &reader_schema).unwrap_err()
        );
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("oldfield2")),
            SchemaCompatibility::can_read(&reader_schema, &writer_schema()).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_array_writer_schema() {
        let valid_reader = string_array_schema();
        let invalid_reader = string_map_schema();

        assert_eq!(
            Compatibility::Full,
            SchemaCompatibility::can_read(&string_array_schema(), &valid_reader).unwrap()
        );
        assert!(matches!(
            SchemaCompatibility::can_read(&string_array_schema(), &invalid_reader),
            Err(CompatibilityError::WrongType { .. }),
        ));
    }

    #[test]
    fn test_primitive_writer_schema() {
        let valid_reader = Schema::String;
        assert!(SchemaCompatibility::can_read(&Schema::String, &valid_reader).is_ok());
        assert_eq!(
            CompatibilityError::WrongType {
                writer_schema_type: "Int".to_string(),
                reader_schema_type: "String".to_string()
            },
            SchemaCompatibility::can_read(&Schema::Int, &Schema::String).unwrap_err()
        );
    }

    #[test]
    fn test_union_reader_writer_subset_incompatibility() {
        // reader union schema must contain all writer union branches
        let union_writer = union_schema(vec![Schema::Int, Schema::String]);
        let union_reader = union_schema(vec![Schema::String]);

        assert_eq!(
            Compatibility::Partial,
            SchemaCompatibility::can_read(&union_writer, &union_reader).unwrap()
        );
        assert_eq!(
            Compatibility::Full,
            SchemaCompatibility::can_read(&union_reader, &union_writer).unwrap()
        );
    }

    #[test]
    fn test_incompatible_record_field() -> TestResult {
        let string_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"MyRecord", "namespace":"ns", "fields": [
            {"name":"field1", "type":"string"}
        ]}
        "#,
        )?;

        let int_schema = Schema::parse_str(
            r#"
              {"type":"record", "name":"MyRecord", "namespace":"ns", "fields": [
                {"name":"field1", "type":"int"}
              ]}
        "#,
        )?;

        assert_eq!(
            CompatibilityError::FieldTypeMismatch(
                "field1".to_owned(),
                Box::new(CompatibilityError::WrongType {
                    writer_schema_type: "String".to_string(),
                    reader_schema_type: "Int".to_string()
                })
            ),
            SchemaCompatibility::can_read(&string_schema, &int_schema).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_enum_symbols() -> TestResult {
        let enum_schema1 = Schema::parse_str(
            r#"
      {"type":"enum", "name":"MyEnum", "symbols":["A","B"]}
"#,
        )?;
        let enum_schema2 =
            Schema::parse_str(r#"{"type":"enum", "name":"MyEnum", "symbols":["A","B","C"]}"#)?;
        assert_eq!(
            Compatibility::Partial,
            SchemaCompatibility::can_read(&enum_schema2, &enum_schema1)?
        );
        assert_eq!(
            Compatibility::Full,
            SchemaCompatibility::can_read(&enum_schema1, &enum_schema2)?
        );

        Ok(())
    }

    fn point_2d_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point2D", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"}
      ]}
    "#,
        )
        .unwrap()
    }

    fn point_2d_fullname_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point", "namespace":"written", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"}
      ]}
    "#,
        )
        .unwrap()
    }

    fn point_3d_no_default_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"},
        {"name":"z", "type":"double"}
      ]}
    "#,
        )
        .unwrap()
    }

    fn point_3d_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point3D", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"},
        {"name":"z", "type":"double", "default": 0.0}
      ]}
    "#,
        )
        .unwrap()
    }

    fn point_3d_match_name_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"},
        {"name":"z", "type":"double", "default": 0.0}
      ]}
    "#,
        )
        .unwrap()
    }

    #[test]
    fn test_union_resolution_no_structure_match() {
        // short name match, but no structure match
        let read_schema = union_schema(vec![Schema::Null, point_3d_no_default_schema()]);
        assert_eq!(
            CompatibilityError::SchemaMismatchAllUnionElements,
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema).unwrap_err()
        );
    }

    #[test]
    fn test_union_resolution_first_structure_match_2d() {
        // multiple structure matches with no name matches
        let read_schema = union_schema(vec![
            Schema::Null,
            point_3d_no_default_schema(),
            point_2d_schema(),
            point_3d_schema(),
        ]);
        assert_eq!(
            CompatibilityError::SchemaMismatchAllUnionElements,
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema).unwrap_err()
        );
    }

    #[test]
    fn test_union_resolution_first_structure_match_3d() {
        // multiple structure matches with no name matches
        let read_schema = union_schema(vec![
            Schema::Null,
            point_3d_no_default_schema(),
            point_3d_schema(),
            point_2d_schema(),
        ]);
        assert_eq!(
            CompatibilityError::SchemaMismatchAllUnionElements,
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema).unwrap_err()
        );
    }

    #[test]
    fn test_union_resolution_named_structure_match() {
        // multiple structure matches with a short name match
        let read_schema = union_schema(vec![
            Schema::Null,
            point_2d_schema(),
            point_3d_match_name_schema(),
            point_3d_schema(),
        ]);
        assert_eq!(
            CompatibilityError::SchemaMismatchAllUnionElements,
            SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema).unwrap_err()
        );
    }

    #[test]
    fn test_union_resolution_full_name_match() {
        // there is a full name match that should be chosen
        let read_schema = union_schema(vec![
            Schema::Null,
            point_2d_schema(),
            point_3d_match_name_schema(),
            point_3d_schema(),
            point_2d_fullname_schema(),
        ]);
        assert!(SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema).is_ok());
    }

    #[test]
    fn test_avro_3772_enum_default() -> TestResult {
        let writer_raw_schema = r#"
        {
          "type": "record",
          "name": "test",
          "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"},
            {
              "name": "c",
              "type": {
                "type": "enum",
                "name": "suit",
                "symbols": ["diamonds", "spades", "clubs", "hearts"],
                "default": "spades"
              }
            }
          ]
        }
        "#;

        let reader_raw_schema = r#"
        {
          "type": "record",
          "name": "test",
          "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"},
            {
              "name": "c",
              "type": {
                 "type": "enum",
                 "name": "suit",
                 "symbols": ["diamonds", "spades", "ninja", "hearts"],
                 "default": "spades"
              }
            }
          ]
        }
      "#;
        let writer_schema = Schema::parse_str(writer_raw_schema)?;
        let reader_schema = Schema::parse_str(reader_raw_schema)?;
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null)?;
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "clubs");
        writer.append_value(record).unwrap();
        let input = writer.into_inner()?;
        let mut reader = Reader::builder(&input[..])
            .reader_schema(&reader_schema)
            .build()?;
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(1, "spades".to_string())),
            ])
        );
        assert!(reader.next().is_none());

        Ok(())
    }

    #[test]
    fn test_avro_3772_enum_default_less_symbols() -> TestResult {
        let writer_raw_schema = r#"
        {
          "type": "record",
          "name": "test",
          "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"},
            {
              "name": "c",
              "type": {
                "type": "enum",
                "name": "suit",
                "symbols": ["diamonds", "spades", "clubs", "hearts"],
                "default": "spades"
              }
            }
          ]
        }
        "#;

        let reader_raw_schema = r#"
        {
          "type": "record",
          "name": "test",
          "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"},
            {
              "name": "c",
              "type": {
                 "type": "enum",
                  "name": "suit",
                  "symbols": ["hearts", "spades"],
                  "default": "spades"
              }
            }
          ]
        }
      "#;
        let writer_schema = Schema::parse_str(writer_raw_schema)?;
        let reader_schema = Schema::parse_str(reader_raw_schema)?;
        let mut writer = Writer::with_codec(&writer_schema, Vec::new(), Codec::Null)?;
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "hearts");
        writer.append_value(record).unwrap();
        let input = writer.into_inner()?;
        let mut reader = Reader::builder(&input[..])
            .reader_schema(&reader_schema)
            .build()?;
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::Record(vec![
                ("a".to_string(), Value::Long(27)),
                ("b".to_string(), Value::String("foo".to_string())),
                ("c".to_string(), Value::Enum(0, "hearts".to_string())),
            ])
        );
        assert!(reader.next().is_none());

        Ok(())
    }

    #[test]
    fn avro_3894_take_aliases_into_account_when_serializing_for_schema_compatibility() -> TestResult
    {
        let schema_v1 = Schema::parse_str(
            r#"
        {
            "type": "record",
            "name": "Conference",
            "namespace": "advdaba",
            "fields": [
                {"type": "string", "name": "name"},
                {"type": "long", "name": "date"}
            ]
        }"#,
        )?;

        let schema_v2 = Schema::parse_str(
            r#"
        {
            "type": "record",
            "name": "Conference",
            "namespace": "advdaba",
            "fields": [
                {"type": "string", "name": "name"},
                {"type": "long", "name": "date", "aliases" : [ "time" ]}
            ]
        }"#,
        )?;

        assert!(SchemaCompatibility::mutual_read(&schema_v1, &schema_v2).is_ok());

        Ok(())
    }

    #[test]
    fn avro_3917_take_aliases_into_account_for_schema_compatibility() -> TestResult {
        let schema_v1 = Schema::parse_str(
            r#"
        {
            "type": "record",
            "name": "Conference",
            "namespace": "advdaba",
            "fields": [
                {"type": "string", "name": "name"},
                {"type": "long", "name": "date", "aliases" : [ "time" ]}
            ]
        }"#,
        )?;

        let schema_v2 = Schema::parse_str(
            r#"
        {
            "type": "record",
            "name": "Conference",
            "namespace": "advdaba",
            "fields": [
                {"type": "string", "name": "name"},
                {"type": "long", "name": "time"}
            ]
        }"#,
        )?;

        assert_eq!(
            Compatibility::Full,
            SchemaCompatibility::can_read(&schema_v2, &schema_v1)?
        );
        assert_eq!(
            CompatibilityError::MissingDefaultValue(String::from("time")),
            SchemaCompatibility::can_read(&schema_v1, &schema_v2).unwrap_err()
        );

        Ok(())
    }

    #[test]
    fn test_avro_3898_record_schemas_match_by_unqualified_name() -> TestResult {
        let schemas = [
            // Record schemas
            (
                Schema::parse_str(
                    r#"{
              "type": "record",
              "name": "Statistics",
              "fields": [
                { "name": "success", "type": "int" },
                { "name": "fail", "type": "int" },
                { "name": "time", "type": "string" },
                { "name": "max", "type": "int", "default": 0 }
              ]
            }"#,
                )?,
                Schema::parse_str(
                    r#"{
              "type": "record",
              "name": "Statistics",
              "namespace": "my.namespace",
              "fields": [
                { "name": "success", "type": "int" },
                { "name": "fail", "type": "int" },
                { "name": "time", "type": "string" },
                { "name": "average", "type": "int", "default": 0}
              ]
            }"#,
                )?,
            ),
            // Enum schemas
            (
                Schema::parse_str(
                    r#"{
                    "type": "enum",
                    "name": "Suit",
                    "symbols": ["diamonds", "spades", "clubs"]
                }"#,
                )?,
                Schema::parse_str(
                    r#"{
                    "type": "enum",
                    "name": "Suit",
                    "namespace": "my.namespace",
                    "symbols": ["diamonds", "spades", "clubs", "hearts"]
                }"#,
                )?,
            ),
            // Fixed schemas
            (
                Schema::parse_str(
                    r#"{
                    "type": "fixed",
                    "name": "EmployeeId",
                    "size": 16
                }"#,
                )?,
                Schema::parse_str(
                    r#"{
                    "type": "fixed",
                    "name": "EmployeeId",
                    "namespace": "my.namespace",
                    "size": 16
                }"#,
                )?,
            ),
        ];

        for (schema_1, schema_2) in schemas {
            assert!(SchemaCompatibility::can_read(&schema_1, &schema_2).is_ok());
        }

        Ok(())
    }

    #[test]
    fn test_can_read_compatibility_errors() -> TestResult {
        let schemas = [
            (
                Schema::parse_str(
                    r#"{
                    "type": "record",
                    "name": "StatisticsMap",
                    "fields": [
                        {"name": "average", "type": "int", "default": 0},
                        {"name": "success", "type": {"type": "map", "values": "int"}}
                    ]
                }"#,
                )?,
                Schema::parse_str(
                    r#"{
                    "type": "record",
                    "name": "StatisticsMap",
                    "fields": [
                        {"name": "average", "type": "int", "default": 0},
                        {"name": "success", "type": ["null", {"type": "map", "values": "int"}], "default": null}
                    ]
                }"#,
                )?,
            ),
            (
                Schema::parse_str(
                    r#"{
                        "type": "record",
                        "name": "StatisticsArray",
                        "fields": [
                            {"name": "max_values", "type": {"type": "array", "items": "int"}}
                        ]
                    }"#,
                )?,
                Schema::parse_str(
                    r#"{
                        "type": "record",
                        "name": "StatisticsArray",
                        "fields": [
                            {"name": "max_values", "type": ["null", {"type": "array", "items": "int"}], "default": null}
                        ]
                    }"#,
                )?,
            ),
        ];

        for (schema_1, schema_2) in schemas {
            assert_eq!(
                Compatibility::Full,
                SchemaCompatibility::can_read(&schema_1, &schema_2).unwrap()
            );
            assert_eq!(
                Compatibility::Partial,
                SchemaCompatibility::can_read(&schema_2, &schema_1).unwrap()
            );
        }

        Ok(())
    }

    #[test]
    fn avro_3974_can_read_schema_references() -> TestResult {
        let schema_strs = vec![
            r#"{
          "type": "record",
          "name": "Child",
          "namespace": "avro",
          "fields": [
            {
              "name": "val",
              "type": "int"
            }
          ]
        }
        "#,
            r#"{
          "type": "record",
          "name": "Parent",
          "namespace": "avro",
          "fields": [
            {
              "name": "child",
              "type": "avro.Child"
            }
          ]
        }
        "#,
        ];

        let schemas = Schema::parse_list(schema_strs).unwrap();
        SchemaCompatibility::can_read(&schemas[1], &schemas[1])?;

        Ok(())
    }

    #[test]
    fn duration_and_fixed_of_different_size() -> TestResult {
        let schema_strs = vec![
            r#"{
          "type": "fixed",
          "name": "Fixed25",
          "size": 25
        }
        "#,
            r#"{
          "type": "fixed",
          "logicalType": "duration",
          "name": "Duration",
          "size": 12
        }
        "#,
        ];

        let schemas = Schema::parse_list(schema_strs).unwrap();
        assert!(SchemaCompatibility::can_read(&schemas[0], &schemas[1]).is_err());
        assert!(SchemaCompatibility::can_read(&schemas[1], &schemas[0]).is_err());
        SchemaCompatibility::can_read(&schemas[1], &schemas[1])?;
        SchemaCompatibility::can_read(&schemas[0], &schemas[0])?;

        Ok(())
    }

    #[test]
    fn avro_rs_342_decimal_fixed_and_bytes() -> TestResult {
        let bytes = Schema::Decimal(DecimalSchema {
            precision: 20,
            scale: 0,
            inner: InnerDecimalSchema::Bytes,
        });
        let fixed = Schema::Decimal(DecimalSchema {
            precision: 20,
            scale: 0,
            inner: InnerDecimalSchema::Fixed(FixedSchema {
                name: Name::new("DecimalFixed")?,
                aliases: None,
                doc: None,
                size: 20,
                attributes: BTreeMap::default(),
            }),
        });

        assert_eq!(
            Compatibility::Full,
            SchemaCompatibility::mutual_read(&bytes, &fixed)?
        );

        let value = Value::Decimal(Decimal::from(vec![1; 10]));
        let fixed_value = value.clone().resolve(&fixed)?;
        let bytes_value = value.resolve(&bytes)?;

        assert_eq!(fixed_value, bytes_value);

        Ok(())
    }
}
