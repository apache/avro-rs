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
use crate::schema::{FixedSchema, Name, Names, Namespace, RecordField, UnionSchema, UuidSchema};
use serde_json::Map;
use std::borrow::Cow;
use std::collections::HashMap;

/// Trait for types that serve as an Avro data model.
///
/// Do not implement directly! Either derive it or implement [`AvroSchemaComponent`] to get this trait
/// through a blanket implementation.
///
/// ## Deriving `AvroSchema`
///
/// Using the custom derive requires that you enable the `"derive"` cargo
/// feature in your `Cargo.toml`:
///
/// ```toml
/// [dependencies]
/// apache-avro = { version = "..", features = ["derive"] }
/// ```
///
/// Then, you add the `#[derive(AvroSchema)]` annotation to your `struct` and
/// `enum` type definition:
///
/// ```
/// # use serde::{Serialize, Deserialize};
/// # use apache_avro::AvroSchema;
/// #[derive(AvroSchema, Serialize, Deserialize)]
/// pub struct Foo {
///     bar: Vec<Bar>,
/// }
///
/// #[derive(AvroSchema, Serialize, Deserialize)]
/// pub enum Bar {
///     Spam,
///     Maps
/// }
/// ```
///
/// This will implement [`AvroSchemaComponent`] for the type, and `AvroSchema`
/// through the blanket implementation for `T: AvroSchemaComponent`.
///
/// When deriving `struct`s, every member must also implement `AvroSchemaComponent`.
///
/// ## Changing the generated schema
///
/// The derive macro will read both the `avro` and `serde` attributes to modify the generated schema.
/// It will also check for compatibility between the various attributes.
///
/// #### Container attributes
///
///  - `#[serde(rename = "name")]`
///
// TODO: Should we check if `name` contains any dots? As that would imply a namespace
///    Set the `name` of the schema to the given string. Defaults to the name of the type.
///
///  - `#[avro(namespace = "some.name.space")]`
///
///    Set the `namespace` of the schema. This will be the relative namespace if the schema is included
///    in another schema.
///
///  - `#[avro(doc = "Some documentation")]`
///
///    Set the `doc` attribute of the schema. Defaults to the documentation of the type.
///
///  - `#[avro(alias = "name")]`
///
///    Set the `alias` attribute of the schema. Can be specified multiple times.
///
///  - `#[serde(rename_all = "camelCase")]`
///
///    Rename all the fields or variants in the schema to follow the given case convention. The possible values
///    are `"lowercase"`, `"UPPERCASE"`, `"PascalCase"`, `"camelCase"`, `"snake_case"`, `"kebab-case"`,
///    `"SCREAMING_SNAKE_CASE"`, `"SCREAMING-KEBAB-CASE"`.
///
///  - `#[serde(transparent)]`
///
///    Use the schema of the inner field directly. Is only allowed on structs with only unskipped field.
///
///
/// #### Variant attributes
///
///  - `#[serde(rename = "name")]`
///
///    Rename the variant to the given name.
///
///
/// #### Field attributes
///
///  - `#[serde(rename = "name")]`
///
///    Rename the field name to the given name.
///
///  - `#[avro(doc = "Some documentation")]`
///
///    Set the `doc` attribute of the field. Defaults to the documentation of the field.
///
///  - `#[avro(default = "null")]`
///
///    Set the `default` attribute of the field.
///
///    _Note:_ This is a JSON value not a Rust value, as this is put in the schema itself.
///
///  - `#[serde(alias = "name")]`
///
///    Set the `alias` attribute of the field. Can be specified multiple times.
///
///  - `#[serde(flatten)]`
///
///    Flatten the content of this field into the container it is defined in.
///
///  - `#[serde(skip)]`
///
///    Do not include this field in the schema.
///
///  - `#[serde(skip_serializing)]`
///
///    When combined with `#[serde(skip_deserializing)]`, don't include this field in the schema.
///    Otherwise, it will be included in the schema and the `#[avro(default)]` attribute **must** be
///    set. That value will be used for serializing.
///
///  - `#[serde(skip_serializing_if)]`
///
///    Conditionally use the value of the field or the value provided by `#[avro(default)]`. The
///    `#[avro(default)]` attribute **must** be set.
///
///  - `#[avro(with)]` and `#[serde(with = "module")]`
///
///    Override the schema used for this field. See [Working with foreign types](#working-with-foreign-types).
///
/// #### Incompatible Serde attributes
///
/// The derive macro is compatible with most Serde attributes, but it is incompatible with
/// the following attributes:
///
/// - Container attributes
///     - `tag`
///     - `content`
///     - `untagged`
///     - `variant_identifier`
///     - `field_identifier`
///     - `remote`
///     - `rename_all(serialize = "..", deserialize = "..")` where `serialize` != `deserialize`
/// - Variant attributes
///     - `other`
///     - `untagged`
/// - Field attributes
///     - `getter`
///
/// ## Working with foreign types
///
/// Most foreign types won't have a [`AvroSchema`] implementation. This crate implements it only
/// for built-in types and [`uuid::Uuid`].
///
/// To still be able to derive schemas for fields of foreign types, the `#[avro(with)`]
/// attribute can be used to get the schema for those fields. It can be used in two ways:
///
/// 1. In combination with `#[serde(with = "path::to::module)]`
///
///    To get the schema, it will call the functions `fn get_schema_in_ctxt(&mut Names, &Namespace) -> Schema`
///    and `fn get_record_fields_in_ctxt(&mut Names, &Namespace) -> Schema` in the module provided
///    to the Serde attribute.
///
/// 2. By providing a function directly, `#[avro(with = some_fn)]`.
///
///    To get the schema, it will call the function provided. It must have the signature
///    `fn(&mut Names, &Namespace) -> Schema`. When this is used for a `transparent` struct, the
///    default implementation of [`AvroSchemaComponent::get_record_fields_in_ctxt`] will be used
///    which is implemented with a lot of backtracking and cloning.
///
pub trait AvroSchema {
    /// Construct the full schema that represents this type.
    ///
    /// The returned schema is fully independent and contains only `Schema::Ref` to named types defined
    /// earlier in the schema.
    fn get_schema() -> Schema;
}

/// Trait for types that serve as fully defined components inside an Avro data model.
///
/// This trait can be derived with [`#[derive(AvroSchema)]`](AvroSchema) when the `derive` feature is enabled.
///
/// # Implementation guide
///
/// ### Implementation for returning primitive types
/// When the schema you want to return is a primitive type (a type without a name), the function
/// arguments can be ignored.
///
/// For example, you have a custom integer type:
/// ```
/// # use apache_avro::{Schema, serde::{AvroSchemaComponent}, schema::{Names, Namespace, RecordField}};
/// // Make sure to implement `Serialize` and `Deserialize` to use the right serialization methods
/// pub struct U24([u8; 3]);
/// impl AvroSchemaComponent for U24 {
///     fn get_schema_in_ctxt(_: &mut Names, _: &Namespace) -> Schema {
///         Schema::Int
///     }
///
///     fn get_record_fields_in_ctxt(_: &mut Names, _: &Namespace) -> Option<Vec<RecordField>> {
///         None // A Schema::Int is not a Schema::Record so there are no fields to return
///     }
///}
/// ```
///
/// ### Passthrough implementation
///
/// To construct a schema for a type is "transparent", such as for smart pointers, simply
/// pass through the arguments to the inner type:
/// ```
/// # use apache_avro::{Schema, serde::{AvroSchemaComponent}, schema::{Names, Namespace, RecordField}};
/// # use serde::{Serialize, Deserialize};
/// #[derive(Serialize, Deserialize)]
/// #[serde(transparent)] // This attribute is important for all passthrough implementations!
/// pub struct Transparent<T>(T);
/// impl<T: AvroSchemaComponent> AvroSchemaComponent for Transparent<T> {
///     fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
///         T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
///     }
///
///     fn get_record_fields_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Option<Vec<RecordField>> {
///         T::get_record_fields_in_ctxt(named_schemas, enclosing_namespace)
///     }
///}
/// ```
///
/// ### Implementation for complex types
/// When the schema you want to return is a complex type (a type with a name), special care has to
/// be taken to avoid duplicate type definitions and getting the correct namespace.
///
/// Things to keep in mind:
///  - If the fully qualified name already exists, return a [`Schema::Ref`]
///  - Use the `AvroSchemaComponent` implementations to get the schemas for the subtypes
///  - The ordering of fields in the schema **must** match with the ordering in Serde
///  - Implement `get_record_fields_in_ctxt` as the default implementation has to be implemented
///    with backtracking and a lot of cloning.
///      - Even if your schema is not a record, still implement the function and just return `None`
///
/// ```rust
/// # use apache_avro::{Schema, serde::{AvroSchemaComponent}, schema::{Name, Names, Namespace, RecordField, RecordSchema}};
/// # use serde::{Serialize, Deserialize};
/// # use std::time::Duration;
/// pub struct Foo {
///     one: String,
///     two: i32,
///     three: Option<Duration>
/// }
///
/// impl AvroSchemaComponent for Foo {
///     fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
///         // Create the fully qualified name for your type given the enclosing namespace
///         let name = Name::new("Foo").unwrap().fully_qualified_name(enclosing_namespace);
///         if named_schemas.contains_key(&name) {
///             Schema::Ref { name }
///         } else {
///             let enclosing_namespace = &name.namespace;
///             // This is needed because otherwise recursive types will recurse forever and cause a stack overflow
///             named_schemas.insert(name.clone(), Schema::Ref { name: name.clone() });
///             let schema = Schema::Record(RecordSchema::builder()
///                 .name(name.clone())
///                 .fields(Self::get_record_fields_in_ctxt(named_schemas, enclosing_namespace).expect("Impossible!"))
///                 .build()
///             );
///             named_schemas.insert(name, schema.clone());
///             schema
///         }
///     }
///
///     fn get_record_fields_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Option<Vec<RecordField>> {
///         Some(vec![
///             RecordField::builder()
///                 .name("one")
///                 .schema(String::get_schema_in_ctxt(named_schemas, enclosing_namespace))
///                 .build(),
///             RecordField::builder()
///                 .name("two")
///                 .schema(i32::get_schema_in_ctxt(named_schemas, enclosing_namespace))
///                 .build(),
///             RecordField::builder()
///                 .name("three")
///                 .schema(<Option<Duration>>::get_schema_in_ctxt(named_schemas, enclosing_namespace))
///                 .build(),
///         ])
///     }
///}
/// ```
pub trait AvroSchemaComponent {
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema;
    fn get_record_fields_in_ctxt(
        named_schemas: &mut Names,
        enclosing_namespace: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }
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

impl<T> AvroSchemaComponent for &T
where
    T: AvroSchemaComponent + ?Sized,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
    }
}

impl<T> AvroSchemaComponent for &mut T
where
    T: AvroSchemaComponent + ?Sized,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
    }
}

impl<T> AvroSchemaComponent for [T]
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        Schema::array(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
    }
}

impl<const N: usize, T> AvroSchemaComponent for [T; N]
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        Schema::array(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
    }
}

impl<T> AvroSchemaComponent for Vec<T>
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        Schema::array(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
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
}

impl<T> AvroSchemaComponent for Map<String, T>
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        Schema::map(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
    }
}

impl<T> AvroSchemaComponent for HashMap<String, T>
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        Schema::map(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
    }
}

impl<T> AvroSchemaComponent for Box<T>
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
    }
}

impl<T> AvroSchemaComponent for std::sync::Mutex<T>
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
    }
}

impl<T> AvroSchemaComponent for Cow<'_, T>
where
    T: AvroSchemaComponent + Clone,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
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
