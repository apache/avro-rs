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

use crate::schema::{InnerDecimalSchema, UuidSchema};
use crate::{
    Schema,
    schema::{
        ArraySchema, DecimalSchema, EnumSchema, FixedSchema, MapSchema, RecordField, RecordSchema,
        UnionSchema,
    },
};
use log::debug;
use std::{fmt::Debug, sync::OnceLock};

/// A trait that compares two schemata for equality.
/// To register a custom one use [set_schemata_equality_comparator].
pub trait SchemataEq: Debug + Send + Sync {
    /// Compares two schemata for equality.
    fn compare(&self, schema_one: &Schema, schema_two: &Schema) -> bool;
}

/// Compares two schemas according to the Avro specification by using
/// their canonical forms.
/// See <https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas>
#[derive(Debug)]
pub struct SpecificationEq;
impl SchemataEq for SpecificationEq {
    fn compare(&self, schema_one: &Schema, schema_two: &Schema) -> bool {
        schema_one.canonical_form() == schema_two.canonical_form()
    }
}

/// Compares two schemas for equality field by field, using only the fields that
/// are used to construct their canonical forms.
/// See <https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas>
#[derive(Debug)]
pub struct StructFieldEq {
    /// Whether to include custom attributes in the comparison.
    /// The custom attributes are not used to construct the canonical form of the schema!
    pub include_attributes: bool,
}

impl SchemataEq for StructFieldEq {
    #[rustfmt::skip]
    fn compare(&self, schema_one: &Schema, schema_two: &Schema) -> bool {
        if schema_one.name() != schema_two.name() {
            return false;
        }

        if self.include_attributes
            && schema_one.custom_attributes() != schema_two.custom_attributes()
        {
            return false;
        }

        match (schema_one, schema_two) {
            (Schema::Null, Schema::Null) => true,
            (Schema::Null, _) => false,
            (Schema::Boolean, Schema::Boolean) => true,
            (Schema::Boolean, _) => false,
            (Schema::Int, Schema::Int) => true,
            (Schema::Int, _) => false,
            (Schema::Long, Schema::Long) => true,
            (Schema::Long, _) => false,
            (Schema::Float, Schema::Float) => true,
            (Schema::Float, _) => false,
            (Schema::Double, Schema::Double) => true,
            (Schema::Double, _) => false,
            (Schema::Bytes, Schema::Bytes) => true,
            (Schema::Bytes, _) => false,
            (Schema::String, Schema::String) => true,
            (Schema::String, _) => false,
            (Schema::BigDecimal, Schema::BigDecimal) => true,
            (Schema::BigDecimal, _) => false,
            (Schema::Date, Schema::Date) => true,
            (Schema::Date, _) => false,
            (Schema::TimeMicros, Schema::TimeMicros) => true,
            (Schema::TimeMicros, _) => false,
            (Schema::TimeMillis, Schema::TimeMillis) => true,
            (Schema::TimeMillis, _) => false,
            (Schema::TimestampMicros, Schema::TimestampMicros) => true,
            (Schema::TimestampMicros, _) => false,
            (Schema::TimestampMillis, Schema::TimestampMillis) => true,
            (Schema::TimestampMillis, _) => false,
            (Schema::TimestampNanos, Schema::TimestampNanos) => true,
            (Schema::TimestampNanos, _) => false,
            (Schema::LocalTimestampMicros, Schema::LocalTimestampMicros) => true,
            (Schema::LocalTimestampMicros, _) => false,
            (Schema::LocalTimestampMillis, Schema::LocalTimestampMillis) => true,
            (Schema::LocalTimestampMillis, _) => false,
            (Schema::LocalTimestampNanos, Schema::LocalTimestampNanos) => true,
            (Schema::LocalTimestampNanos, _) => false,
            (
                Schema::Record(RecordSchema { fields: fields_one, ..}),
                Schema::Record(RecordSchema { fields: fields_two, ..})
            ) => {
                self.compare_fields(fields_one, fields_two)
            }
            (Schema::Record(_), _) => false,
            (
                Schema::Enum(EnumSchema { symbols: symbols_one, ..}),
                Schema::Enum(EnumSchema { symbols: symbols_two, .. })
            ) => {
                symbols_one == symbols_two
            }
            (Schema::Enum(_), _) => false,
            (
                Schema::Fixed(FixedSchema { size: size_one, ..}),
                Schema::Fixed(FixedSchema { size: size_two, .. })
            ) => {
                size_one == size_two
            }
            (Schema::Fixed(_), _) => false,
            (
                Schema::Union(UnionSchema { schemas: schemas_one, ..}),
                Schema::Union(UnionSchema { schemas: schemas_two, .. })
            ) => {
                schemas_one.len() == schemas_two.len()
                    && schemas_one
                    .iter()
                    .zip(schemas_two.iter())
                    .all(|(s1, s2)| self.compare(s1, s2))
            }
            (Schema::Union(_), _) => false,
            (
                Schema::Decimal(DecimalSchema { precision: precision_one, scale: scale_one, inner: inner_one }),
                Schema::Decimal(DecimalSchema { precision: precision_two, scale: scale_two, inner: inner_two })
            ) => {
                precision_one == precision_two && scale_one == scale_two && match (inner_one, inner_two) {
                    (InnerDecimalSchema::Bytes, InnerDecimalSchema::Bytes) => true,
                    (InnerDecimalSchema::Fixed(FixedSchema { size: size_one, .. }), InnerDecimalSchema::Fixed(FixedSchema { size: size_two, ..})) => {
                        size_one == size_two
                    }
                    _ => false,
                }
            }
            (Schema::Decimal(_), _) => false,
            (Schema::Uuid(UuidSchema::Bytes), Schema::Uuid(UuidSchema::Bytes)) => true,
            (Schema::Uuid(UuidSchema::Bytes), _) => false,
            (Schema::Uuid(UuidSchema::String), Schema::Uuid(UuidSchema::String)) => true,
            (Schema::Uuid(UuidSchema::String), _) => false,
            (Schema::Uuid(UuidSchema::Fixed(FixedSchema { size: size_one, ..})), Schema::Uuid(UuidSchema::Fixed(FixedSchema { size: size_two, ..}))) => {
                size_one == size_two
            },
            (Schema::Uuid(UuidSchema::Fixed(_)), _) => false,
            (
                Schema::Array(ArraySchema { items: items_one, ..}),
                Schema::Array(ArraySchema { items: items_two, ..})
            ) => {
                self.compare(items_one, items_two)
            }
            (Schema::Duration(FixedSchema { size: size_one, ..}), Schema::Duration(FixedSchema { size: size_two, ..})) => size_one == size_two,
            (Schema::Duration(_), _) => false,
            (Schema::Array(_), _) => false,
            (
                Schema::Map(MapSchema { types: types_one, ..}),
                Schema::Map(MapSchema { types: types_two, ..})
            ) => {
                self.compare(types_one, types_two)
            }
            (Schema::Map(_), _) => false,
            (
                Schema::Ref { name: name_one },
                Schema::Ref { name: name_two }
            ) => {
                name_one == name_two
            }
            (Schema::Ref { .. }, _) => false,
        }
    }
}

impl StructFieldEq {
    fn compare_fields(&self, fields_one: &[RecordField], fields_two: &[RecordField]) -> bool {
        fields_one.len() == fields_two.len()
            && fields_one
                .iter()
                .zip(fields_two.iter())
                .all(|(f1, f2)| f1.name == f2.name && self.compare(&f1.schema, &f2.schema))
    }
}

static SCHEMATA_COMPARATOR_ONCE: OnceLock<Box<dyn SchemataEq>> = OnceLock::new();

/// Sets a custom schemata equality comparator.
///
/// Returns a unit if the registration was successful or the already
/// registered comparator if the registration failed.
///
/// **Note**: This function must be called before parsing any schema because this will
/// register the default comparator and the registration is one time only!
pub fn set_schemata_equality_comparator(
    comparator: Box<dyn SchemataEq>,
) -> Result<(), Box<dyn SchemataEq>> {
    debug!("Setting a custom schemata equality comparator: {comparator:?}.");
    SCHEMATA_COMPARATOR_ONCE.set(comparator)
}

pub(crate) fn compare_schemata(schema_one: &Schema, schema_two: &Schema) -> bool {
    SCHEMATA_COMPARATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default schemata equality comparator: StructFieldEq.",);
            Box::new(StructFieldEq {
                include_attributes: false,
            })
        })
        .compare(schema_one, schema_two)
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use crate::schema::{InnerDecimalSchema, Name, RecordFieldOrder};
    use apache_avro_test_helper::TestResult;
    use serde_json::Value;
    use std::collections::BTreeMap;

    const SPECIFICATION_EQ: SpecificationEq = SpecificationEq;
    const STRUCT_FIELD_EQ: StructFieldEq = StructFieldEq {
        include_attributes: false,
    };
    const STRUCT_FIELD_EQ_WITH_ATTRS: StructFieldEq = StructFieldEq {
        include_attributes: true,
    };

    macro_rules! test_primitives {
        ($primitive:ident) => {
            paste::item! {
                #[test]
                fn [<test_avro_3939_compare_schemata_$primitive>]() {
                    let specification_eq_res = SPECIFICATION_EQ.compare(&Schema::$primitive, &Schema::$primitive);
                    let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&Schema::$primitive, &Schema::$primitive);
                    assert_eq!(specification_eq_res, struct_field_eq_res)
                }
            }
        };
    }

    test_primitives!(Null);
    test_primitives!(Boolean);
    test_primitives!(Int);
    test_primitives!(Long);
    test_primitives!(Float);
    test_primitives!(Double);
    test_primitives!(Bytes);
    test_primitives!(String);
    test_primitives!(BigDecimal);
    test_primitives!(Date);
    test_primitives!(TimeMicros);
    test_primitives!(TimeMillis);
    test_primitives!(TimestampMicros);
    test_primitives!(TimestampMillis);
    test_primitives!(TimestampNanos);
    test_primitives!(LocalTimestampMicros);
    test_primitives!(LocalTimestampMillis);
    test_primitives!(LocalTimestampNanos);

    #[test]
    fn avro_rs_382_compare_schemata_duration_equal() {
        let schema_one = Schema::Duration(FixedSchema {
            name: Name::from("name1"),
            size: 12,
            aliases: None,
            doc: None,
            default: None,
            attributes: BTreeMap::new(),
        });
        let schema_two = Schema::Duration(FixedSchema {
            name: Name::from("name1"),
            size: 12,
            aliases: None,
            doc: None,
            default: None,
            attributes: BTreeMap::new(),
        });
        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert_eq!(specification_eq_res, struct_field_eq_res)
    }

    #[test]
    fn avro_rs_382_compare_schemata_duration_different_names() {
        let schema_one = Schema::Duration(FixedSchema {
            name: Name::from("name1"),
            size: 12,
            aliases: None,
            doc: None,
            default: None,
            attributes: BTreeMap::new(),
        });
        let schema_two = Schema::Duration(FixedSchema {
            name: Name::from("name2"),
            size: 12,
            aliases: None,
            doc: None,
            default: None,
            attributes: BTreeMap::new(),
        });
        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        assert!(!specification_eq_res);

        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(!struct_field_eq_res)
    }

    #[test]
    fn avro_rs_382_compare_schemata_duration_different_attributes() {
        let schema_one = Schema::Duration(FixedSchema {
            name: Name::from("name1"),
            size: 12,
            aliases: None,
            doc: None,
            default: None,
            attributes: vec![(String::from("attr1"), serde_json::Value::Bool(true))]
                .into_iter()
                .collect(),
        });
        let schema_two = Schema::Duration(FixedSchema {
            name: Name::from("name1"),
            size: 12,
            aliases: None,
            doc: None,
            default: None,
            attributes: BTreeMap::new(),
        });
        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        assert!(specification_eq_res);

        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(struct_field_eq_res);

        let struct_field_eq_with_attrs_res =
            STRUCT_FIELD_EQ_WITH_ATTRS.compare(&schema_one, &schema_two);
        assert!(!struct_field_eq_with_attrs_res);
    }

    #[test]
    fn avro_rs_382_compare_schemata_duration_different_sizes() {
        let schema_one = Schema::Duration(FixedSchema {
            name: Name::from("name1"),
            size: 8,
            aliases: None,
            doc: None,
            default: None,
            attributes: BTreeMap::new(),
        });
        let schema_two = Schema::Duration(FixedSchema {
            name: Name::from("name1"),
            size: 12,
            aliases: None,
            doc: None,
            default: None,
            attributes: BTreeMap::new(),
        });
        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        assert!(!specification_eq_res);

        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(!struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_named_schemata_with_different_names() {
        let schema_one = Schema::Ref {
            name: Name::from("name1"),
        };

        let schema_two = Schema::Ref {
            name: Name::from("name2"),
        };

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        assert!(!specification_eq_res);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(!struct_field_eq_res);

        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_schemata_not_including_attributes() {
        let schema_one = Schema::map_with_attributes(
            Schema::Boolean,
            BTreeMap::from_iter([("key1".to_string(), Value::Bool(true))]),
        );
        let schema_two = Schema::map_with_attributes(
            Schema::Boolean,
            BTreeMap::from_iter([("key2".to_string(), Value::Bool(true))]),
        );
        // STRUCT_FIELD_EQ does not include attributes !
        assert!(STRUCT_FIELD_EQ.compare(&schema_one, &schema_two));
    }

    #[test]
    fn test_avro_3939_compare_schemata_including_attributes() {
        let struct_field_eq = StructFieldEq {
            include_attributes: true,
        };
        let schema_one = Schema::map_with_attributes(
            Schema::Boolean,
            BTreeMap::from_iter([("key1".to_string(), Value::Bool(true))]),
        );
        let schema_two = Schema::map_with_attributes(
            Schema::Boolean,
            BTreeMap::from_iter([("key2".to_string(), Value::Bool(true))]),
        );
        assert!(!struct_field_eq.compare(&schema_one, &schema_two));
    }

    #[test]
    fn test_avro_3939_compare_map_schemata() {
        let schema_one = Schema::map(Schema::Boolean);
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::map(Schema::Boolean);

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Map failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Map failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_array_schemata() {
        let schema_one = Schema::array(Schema::Boolean);
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::array(Schema::Boolean);

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Array failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Array failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_decimal_schemata() {
        let schema_one = Schema::Decimal(DecimalSchema {
            precision: 10,
            scale: 2,
            inner: InnerDecimalSchema::Bytes,
        });
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Decimal(DecimalSchema {
            precision: 10,
            scale: 2,
            inner: InnerDecimalSchema::Bytes,
        });

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Decimal failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Decimal failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_fixed_schemata() {
        let schema_one = Schema::Fixed(FixedSchema {
            name: Name::from("fixed"),
            doc: None,
            size: 10,
            default: None,
            aliases: None,
            attributes: BTreeMap::new(),
        });
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Fixed(FixedSchema {
            name: Name::from("fixed"),
            doc: None,
            size: 10,
            default: None,
            aliases: None,
            attributes: BTreeMap::new(),
        });

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Fixed failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Fixed failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_enum_schemata() {
        let schema_one = Schema::Enum(EnumSchema {
            name: Name::from("enum"),
            doc: None,
            symbols: vec!["A".to_string(), "B".to_string()],
            default: None,
            aliases: None,
            attributes: BTreeMap::new(),
        });
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Enum(EnumSchema {
            name: Name::from("enum"),
            doc: None,
            symbols: vec!["A".to_string(), "B".to_string()],
            default: None,
            aliases: None,
            attributes: BTreeMap::new(),
        });

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Enum failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Enum failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_ref_schemata() {
        let schema_one = Schema::Ref {
            name: Name::from("ref"),
        };
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Ref {
            name: Name::from("ref"),
        };

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Ref failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Ref failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_record_schemata() {
        let schema_one = Schema::Record(RecordSchema {
            name: Name::from("record"),
            doc: None,
            fields: vec![RecordField {
                name: "field".to_string(),
                doc: None,
                default: None,
                schema: Schema::Boolean,
                order: RecordFieldOrder::Ignore,
                aliases: None,
                custom_attributes: BTreeMap::new(),
                position: 0,
            }],
            aliases: None,
            attributes: BTreeMap::new(),
            lookup: Default::default(),
        });
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Record(RecordSchema {
            name: Name::from("record"),
            doc: None,
            fields: vec![RecordField {
                name: "field".to_string(),
                doc: None,
                default: None,
                schema: Schema::Boolean,
                order: RecordFieldOrder::Ignore,
                aliases: None,
                custom_attributes: BTreeMap::new(),
                position: 0,
            }],
            aliases: None,
            attributes: BTreeMap::new(),
            lookup: Default::default(),
        });

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Record failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Record failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
    }

    #[test]
    fn test_avro_3939_compare_union_schemata() -> TestResult {
        let schema_one = Schema::Union(UnionSchema::new(vec![Schema::Boolean, Schema::Int])?);
        assert!(!SPECIFICATION_EQ.compare(&schema_one, &Schema::Boolean));
        assert!(!STRUCT_FIELD_EQ.compare(&schema_one, &Schema::Boolean));

        let schema_two = Schema::Union(UnionSchema::new(vec![Schema::Boolean, Schema::Int])?);

        let specification_eq_res = SPECIFICATION_EQ.compare(&schema_one, &schema_two);
        let struct_field_eq_res = STRUCT_FIELD_EQ.compare(&schema_one, &schema_two);
        assert!(
            specification_eq_res,
            "SpecificationEq: Equality of two Schema::Union failed!"
        );
        assert!(
            struct_field_eq_res,
            "StructFieldEq: Equality of two Schema::Union failed!"
        );
        assert_eq!(specification_eq_res, struct_field_eq_res);
        Ok(())
    }

    #[test]
    fn test_uuid_compare_uuid() -> TestResult {
        let string = Schema::Uuid(UuidSchema::String);
        let bytes = Schema::Uuid(UuidSchema::Bytes);
        let mut fixed_schema = FixedSchema {
            name: Name {
                name: "some_name".to_string(),
                namespace: None,
            },
            aliases: None,
            doc: None,
            size: 16,
            default: None,
            attributes: Default::default(),
        };
        let fixed = Schema::Uuid(UuidSchema::Fixed(fixed_schema.clone()));
        fixed_schema
            .attributes
            .insert("Something".to_string(), Value::Null);
        let fixed_different = Schema::Uuid(UuidSchema::Fixed(fixed_schema));

        assert!(SPECIFICATION_EQ.compare(&string, &string));
        assert!(STRUCT_FIELD_EQ.compare(&string, &string));
        assert!(SPECIFICATION_EQ.compare(&bytes, &bytes));
        assert!(STRUCT_FIELD_EQ.compare(&bytes, &bytes));
        assert!(SPECIFICATION_EQ.compare(&fixed, &fixed));
        assert!(STRUCT_FIELD_EQ.compare(&fixed, &fixed));

        assert!(!SPECIFICATION_EQ.compare(&string, &bytes));
        assert!(!STRUCT_FIELD_EQ.compare(&string, &bytes));
        assert!(!SPECIFICATION_EQ.compare(&bytes, &string));
        assert!(!STRUCT_FIELD_EQ.compare(&bytes, &string));
        assert!(!SPECIFICATION_EQ.compare(&string, &fixed));
        assert!(!STRUCT_FIELD_EQ.compare(&string, &fixed));
        assert!(!SPECIFICATION_EQ.compare(&fixed, &string));
        assert!(!STRUCT_FIELD_EQ.compare(&fixed, &string));
        assert!(!SPECIFICATION_EQ.compare(&bytes, &fixed));
        assert!(!STRUCT_FIELD_EQ.compare(&bytes, &fixed));
        assert!(!SPECIFICATION_EQ.compare(&fixed, &bytes));
        assert!(!STRUCT_FIELD_EQ.compare(&fixed, &bytes));

        assert!(SPECIFICATION_EQ.compare(&fixed, &fixed_different));
        assert!(STRUCT_FIELD_EQ.compare(&fixed, &fixed_different));
        assert!(SPECIFICATION_EQ.compare(&fixed_different, &fixed));
        assert!(STRUCT_FIELD_EQ.compare(&fixed_different, &fixed));

        let strict = StructFieldEq {
            include_attributes: true,
        };

        assert!(!strict.compare(&fixed, &fixed_different));
        assert!(!strict.compare(&fixed_different, &fixed));

        Ok(())
    }
}
