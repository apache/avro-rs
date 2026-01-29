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

use crate::Schema;
use crate::schema::{
    FixedSchema, Name, Names, Namespace, RecordField, RecordSchema, UnionSchema, UuidSchema,
};
use std::borrow::Cow;
use std::collections::HashMap;

/// Trait for types that serve as an Avro data model. Derive implementation available
/// through `derive` feature. Do not implement directly!
/// Implement [`AvroSchemaComponent`] to get this trait
/// through a blanket implementation.
pub trait AvroSchema {
    fn get_schema() -> Schema;
}

/// Trait for types that serve as fully defined components inside an Avro data model. Derive
/// implementation available through `derive` feature. This is what is implemented by
/// the `derive(AvroSchema)` macro.
///
/// # Implementation guide
///
/// ### Simple implementation
/// To construct a non named simple schema, it is possible to ignore the input argument making the
/// general form implementation look like
/// ```ignore
/// impl AvroSchemaComponent for AType {
///     fn get_schema_in_ctxt(_: &mut Names, _: &Namespace) -> Schema {
///        Schema::?
///    }
///}
/// ```
///
/// ### Passthrough implementation
///
/// To construct a schema for a Type that acts as in "inner" type, such as for smart pointers, simply
/// pass through the arguments to the inner type
/// ```ignore
/// impl AvroSchemaComponent for PassthroughType {
///     fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
///        InnerType::get_schema_in_ctxt(named_schemas, enclosing_namespace)
///    }
///}
/// ```
///
/// ### Complex implementation
///
/// To implement this for Named schema there is a general form needed to avoid creating invalid
/// schemas or infinite loops.
/// ```ignore
/// impl AvroSchemaComponent for ComplexType {
///     fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
///         // Create the fully qualified name for your type given the enclosing namespace
///         let name =  apache_avro::schema::Name::new("MyName")
///             .expect("Unable to parse schema name")
///             .fully_qualified_name(enclosing_namespace);
///         let enclosing_namespace = &name.namespace;
///         // Check, if your name is already defined, and if so, return a ref to that name
///         if named_schemas.contains_key(&name) {
///             apache_avro::schema::Schema::Ref{name: name.clone()}
///         } else {
///             named_schemas.insert(name.clone(), apache_avro::schema::Schema::Ref{name: name.clone()});
///             // YOUR SCHEMA DEFINITION HERE with the name equivalent to "MyName".
///             // For non-simple sub types delegate to their implementation of AvroSchemaComponent
///         }
///    }
///}
/// ```
pub trait AvroSchemaComponent {
    /// Get the schema for this component
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema;

    /// Get the fields of this schema if it is a record.
    ///
    /// This returns `None` if the schema is not a record.
    ///
    /// The default implementation has to do a lot of extra work, so it is strongly recommended to
    /// implement this function when manually implementing this trait.
    fn get_record_fields_in_ctxt(
        named_schemas: &mut Names,
        enclosing_namespace: &Namespace,
    ) -> Option<Vec<RecordField>> {
        get_record_fields_in_ctxt(named_schemas, enclosing_namespace, Self::get_schema_in_ctxt)
    }
}

/// Get the record fields from `schema_fn` without polluting `named_schemas` or causing duplicate names
///
/// This is public so the derive macro can use it for `#[avro(with = ||)]` and `#[avro(with = path)]`
pub fn get_record_fields_in_ctxt(
    named_schemas: &mut Names,
    enclosing_namespace: &Namespace,
    schema_fn: fn(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema,
) -> Option<Vec<RecordField>> {
    let mut record = match schema_fn(named_schemas, enclosing_namespace) {
        Schema::Record(record) => record,
        Schema::Ref { name } => {
            // This schema already exists in `named_schemas` so temporarily remove it so we can
            // get the actual schema.
            let temp = named_schemas
                .remove(&name)
                .expect("Name should exist in `named_schemas` otherwise Ref is invalid");
            // Get the schema
            let schema = schema_fn(named_schemas, enclosing_namespace);
            // Reinsert the old value
            named_schemas.insert(name, temp);

            // Now check if we actually got a record and return the fields if that is the case
            let Schema::Record(record) = schema else {
                return None;
            };
            return Some(record.fields);
        }
        _ => return None,
    };
    // This schema did not yet exist in `named_schemas`, so we need to remove it if and only if
    // it isn't used somewhere in the schema (recursive type).

    // Find the first Schema::Ref that has the target name
    fn find_first_ref<'a>(schema: &'a mut Schema, target: &Name) -> Option<&'a mut Schema> {
        match schema {
            Schema::Ref { name } if name == target => Some(schema),
            Schema::Array(array) => find_first_ref(&mut array.items, target),
            Schema::Map(map) => find_first_ref(&mut map.types, target),
            Schema::Union(union) => {
                for schema in &mut union.schemas {
                    if let Some(schema) = find_first_ref(schema, target) {
                        return Some(schema);
                    }
                }
                None
            }
            Schema::Record(record) => {
                assert_ne!(
                    &record.name, target,
                    "Only expecting a Ref named {target:?}"
                );
                for field in &mut record.fields {
                    if let Some(schema) = find_first_ref(&mut field.schema, target) {
                        return Some(schema);
                    }
                }
                None
            }
            _ => None,
        }
    }

    // Prepare the fields for the new record. All named types will become references.
    let new_fields = record
        .fields
        .iter()
        .map(|field| RecordField {
            name: field.name.clone(),
            doc: field.doc.clone(),
            aliases: field.aliases.clone(),
            default: field.default.clone(),
            schema: if field.schema.is_named() {
                Schema::Ref {
                    name: field.schema.name().expect("Schema is named").clone(),
                }
            } else {
                field.schema.clone()
            },
            order: field.order.clone(),
            position: field.position,
            custom_attributes: field.custom_attributes.clone(),
        })
        .collect();

    // Remove the name in case it is not used
    named_schemas.remove(&record.name);

    // Find the first reference to this schema so we can replace it with the actual schema
    for field in &mut record.fields {
        if let Some(schema) = find_first_ref(&mut field.schema, &record.name) {
            let new_schema = RecordSchema {
                name: record.name,
                aliases: record.aliases,
                doc: record.doc,
                fields: new_fields,
                lookup: record.lookup,
                attributes: record.attributes,
            };

            let Schema::Ref { name } = std::mem::replace(schema, Schema::Record(new_schema)) else {
                panic!("Expected only Refs from find_first_ref");
            };

            // The schema is used, so reinsert it
            named_schemas.insert(name.clone(), Schema::Ref { name });

            break;
        }
    }

    Some(record.fields)
}

impl<T> AvroSchema for T
where
    T: AvroSchemaComponent + ?Sized,
{
    fn get_schema() -> Schema {
        T::get_schema_in_ctxt(&mut HashMap::default(), &None)
    }
}

macro_rules! impl_schema (
    ($type:ty, $variant_constructor:expr) => (
        impl AvroSchemaComponent for $type {
            fn get_schema_in_ctxt(_: &mut Names, _: &Namespace) -> Schema {
                $variant_constructor
            }

            fn get_record_fields_in_ctxt(_: &mut Names, _: &Namespace) -> Option<Vec<RecordField>> {
                None
            }
        }
    );
);

impl_schema!(bool, Schema::Boolean);
impl_schema!(i8, Schema::Int);
impl_schema!(i16, Schema::Int);
impl_schema!(i32, Schema::Int);
impl_schema!(i64, Schema::Long);
impl_schema!(u8, Schema::Int);
impl_schema!(u16, Schema::Int);
impl_schema!(u32, Schema::Long);
impl_schema!(f32, Schema::Float);
impl_schema!(f64, Schema::Double);
impl_schema!(String, Schema::String);
impl_schema!(str, Schema::String);
impl_schema!(char, Schema::String);

macro_rules! impl_passthrough_schema (
    ($type:ty where T: AvroSchemaComponent + ?Sized $(+ $bound:tt)*) => (
        impl<T: AvroSchemaComponent $(+ $bound)* + ?Sized> AvroSchemaComponent for $type {
            fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
                T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
            }

            fn get_record_fields_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Option<Vec<RecordField>> {
                T::get_record_fields_in_ctxt(named_schemas, enclosing_namespace)
            }
        }
    );
);

impl_passthrough_schema!(&T where T: AvroSchemaComponent + ?Sized);
impl_passthrough_schema!(&mut T where T: AvroSchemaComponent + ?Sized);
impl_passthrough_schema!(Box<T> where T: AvroSchemaComponent + ?Sized);
impl_passthrough_schema!(Cow<'_, T> where T: AvroSchemaComponent + ?Sized + ToOwned);
impl_passthrough_schema!(std::sync::Mutex<T> where T: AvroSchemaComponent + ?Sized);

macro_rules! impl_array_schema (
    ($type:ty where T: AvroSchemaComponent) => (
        impl<T: AvroSchemaComponent> AvroSchemaComponent for $type {
            fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
                Schema::array(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
            }

            fn get_record_fields_in_ctxt(_: &mut Names, _: &Namespace) -> Option<Vec<RecordField>> {
                None
            }
        }
    );
);

impl_array_schema!([T] where T: AvroSchemaComponent);
impl_array_schema!(Vec<T> where T: AvroSchemaComponent);
// This doesn't work as the macro doesn't allow specifying the N parameter
// impl_array_schema!([T; N] where T: AvroSchemaComponent);

impl<const N: usize, T> AvroSchemaComponent for [T; N]
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        Schema::array(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
    }

    fn get_record_fields_in_ctxt(_: &mut Names, _: &Namespace) -> Option<Vec<RecordField>> {
        None
    }
}

impl<T> AvroSchemaComponent for HashMap<String, T>
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        Schema::map(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
    }

    fn get_record_fields_in_ctxt(_: &mut Names, _: &Namespace) -> Option<Vec<RecordField>> {
        None
    }
}

impl<T> AvroSchemaComponent for Option<T>
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        let variants = vec![
            Schema::Null,
            T::get_schema_in_ctxt(named_schemas, enclosing_namespace),
        ];

        Schema::Union(
            UnionSchema::new(variants).expect("Option<T> must produce a valid (non-nested) union"),
        )
    }

    fn get_record_fields_in_ctxt(_: &mut Names, _: &Namespace) -> Option<Vec<RecordField>> {
        None
    }
}

impl AvroSchemaComponent for core::time::Duration {
    /// The schema is [`Schema::Duration`] with the name `duration`.
    ///
    /// This is a lossy conversion as this Avro type does not store the amount of nanoseconds.
    #[expect(clippy::map_entry, reason = "We don't use the value from the map")]
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        let name = Name::new("duration")
            .expect("Name is valid")
            .fully_qualified_name(enclosing_namespace);
        if named_schemas.contains_key(&name) {
            Schema::Ref { name }
        } else {
            let schema = Schema::Duration(FixedSchema {
                name: name.clone(),
                aliases: None,
                doc: None,
                size: 12,
                default: None,
                attributes: Default::default(),
            });
            named_schemas.insert(name, schema.clone());
            schema
        }
    }

    fn get_record_fields_in_ctxt(_: &mut Names, _: &Namespace) -> Option<Vec<RecordField>> {
        None
    }
}

impl AvroSchemaComponent for uuid::Uuid {
    /// The schema is [`Schema::Uuid`] with the name `uuid`.
    ///
    /// The underlying schema is [`Schema::Fixed`] with a size of 16.
    #[expect(clippy::map_entry, reason = "We don't use the value from the map")]
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        let name = Name::new("uuid")
            .expect("Name is valid")
            .fully_qualified_name(enclosing_namespace);
        if named_schemas.contains_key(&name) {
            Schema::Ref { name }
        } else {
            let schema = Schema::Uuid(UuidSchema::Fixed(FixedSchema {
                name: name.clone(),
                aliases: None,
                doc: None,
                size: 16,
                default: None,
                attributes: Default::default(),
            }));
            named_schemas.insert(name, schema.clone());
            schema
        }
    }

    fn get_record_fields_in_ctxt(_: &mut Names, _: &Namespace) -> Option<Vec<RecordField>> {
        None
    }
}

impl AvroSchemaComponent for u64 {
    /// The schema is [`Schema::Fixed`] of size 8 with the name `u64`.
    #[expect(clippy::map_entry, reason = "We don't use the value from the map")]
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        let name = Name::new("u64")
            .expect("Name is valid")
            .fully_qualified_name(enclosing_namespace);
        if named_schemas.contains_key(&name) {
            Schema::Ref { name }
        } else {
            let schema = Schema::Fixed(FixedSchema {
                name: name.clone(),
                aliases: None,
                doc: None,
                size: 8,
                default: None,
                attributes: Default::default(),
            });
            named_schemas.insert(name, schema.clone());
            schema
        }
    }

    fn get_record_fields_in_ctxt(_: &mut Names, _: &Namespace) -> Option<Vec<RecordField>> {
        None
    }
}

impl AvroSchemaComponent for u128 {
    /// The schema is [`Schema::Fixed`] of size 16 with the name `u128`.
    #[expect(clippy::map_entry, reason = "We don't use the value from the map")]
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        let name = Name::new("u128")
            .expect("Name is valid")
            .fully_qualified_name(enclosing_namespace);
        if named_schemas.contains_key(&name) {
            Schema::Ref { name }
        } else {
            let schema = Schema::Fixed(FixedSchema {
                name: name.clone(),
                aliases: None,
                doc: None,
                size: 16,
                default: None,
                attributes: Default::default(),
            });
            named_schemas.insert(name, schema.clone());
            schema
        }
    }

    fn get_record_fields_in_ctxt(_: &mut Names, _: &Namespace) -> Option<Vec<RecordField>> {
        None
    }
}

impl AvroSchemaComponent for i128 {
    /// The schema is [`Schema::Fixed`] of size 16 with the name `i128`.
    #[expect(clippy::map_entry, reason = "We don't use the value from the map")]
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        let name = Name::new("i128")
            .expect("Name is valid")
            .fully_qualified_name(enclosing_namespace);
        if named_schemas.contains_key(&name) {
            Schema::Ref { name }
        } else {
            let schema = Schema::Fixed(FixedSchema {
                name: name.clone(),
                aliases: None,
                doc: None,
                size: 16,
                default: None,
                attributes: Default::default(),
            });
            named_schemas.insert(name, schema.clone());
            schema
        }
    }

    fn get_record_fields_in_ctxt(_: &mut Names, _: &Namespace) -> Option<Vec<RecordField>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::{FixedSchema, Name};
    use crate::{AvroSchema, Schema};
    use apache_avro_test_helper::TestResult;

    #[test]
    fn avro_rs_401_str() -> TestResult {
        let schema = str::get_schema();
        assert_eq!(schema, Schema::String);

        Ok(())
    }

    #[test]
    fn avro_rs_401_references() -> TestResult {
        let schema_ref = <&str>::get_schema();
        let schema_ref_mut = <&mut str>::get_schema();

        assert_eq!(schema_ref, Schema::String);
        assert_eq!(schema_ref_mut, Schema::String);

        Ok(())
    }

    #[test]
    fn avro_rs_401_slice() -> TestResult {
        let schema = <[u8]>::get_schema();
        assert_eq!(schema, Schema::array(Schema::Int));

        Ok(())
    }

    #[test]
    fn avro_rs_401_array() -> TestResult {
        let schema = <[u8; 55]>::get_schema();
        assert_eq!(schema, Schema::array(Schema::Int));

        Ok(())
    }

    #[test]
    fn avro_rs_401_option_ref_slice_array() -> TestResult {
        let schema = <Option<&[[u8; 55]]>>::get_schema();
        assert_eq!(
            schema,
            Schema::union(vec![
                Schema::Null,
                Schema::array(Schema::array(Schema::Int))
            ])?
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_char() -> TestResult {
        let schema = char::get_schema();
        assert_eq!(schema, Schema::String);

        Ok(())
    }

    #[test]
    fn avro_rs_414_u64() -> TestResult {
        let schema = u64::get_schema();
        assert_eq!(
            schema,
            Schema::Fixed(FixedSchema {
                name: Name::new("u64")?,
                aliases: None,
                doc: None,
                size: 8,
                default: None,
                attributes: Default::default(),
            })
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_i128() -> TestResult {
        let schema = i128::get_schema();
        assert_eq!(
            schema,
            Schema::Fixed(FixedSchema {
                name: Name::new("i128")?,
                aliases: None,
                doc: None,
                size: 16,
                default: None,
                attributes: Default::default(),
            })
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_u128() -> TestResult {
        let schema = u128::get_schema();
        assert_eq!(
            schema,
            Schema::Fixed(FixedSchema {
                name: Name::new("u128")?,
                aliases: None,
                doc: None,
                size: 16,
                default: None,
                attributes: Default::default(),
            })
        );

        Ok(())
    }
}
