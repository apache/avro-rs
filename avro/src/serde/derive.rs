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
    FixedSchema, Name, Namespace, RecordField, RecordSchema, UnionSchema, UuidSchema,
};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

/// Trait for types that serve as an Avro data model.
///
/// **Do not implement directly!** Either derive it or implement [`AvroSchemaComponent`] to get this trait
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
///  - `#[avro(default = r#"{"field": 42, "other": "Spam"}"#)]`
///
///    Provide the default value for this type when it is used in a field.
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
///    Use the schema of the inner field directly. Is only allowed on structs with only one unskipped field.
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
///  - `#[avro(default = ..)]`
///
///    Control the `default` attribute of the field. When not used, it will use [`AvroSchemaComponent::field_default`]
///    to get the default value for a type. To remove the `default` attribute for a field, set `default` to `false`: `#[avro(default = false)]`.
///
///    To override or set a default value, provide a JSON string:
///
///      - Null: `#[avro(default = "null")]`
///      - Boolean: `#[avro(default = "true")]`.
///      - Number: `#[avro(default = "42")]` or `#[avro(default = "42.5")]`
///      - String: `#[avro(default = r#""String needs extra quotes""#)]`.
///      - Array: `#[avro(default = r#"["One", "Two", "Three"]"#)]`.
///      - Object: `#[avro(default = r#"{"One": 1}"#)]`.
///
///    See [the specification](https://avro.apache.org/docs/++version++/specification/#schema-record)
///    for details on how to map a type to a JSON value.
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
///    To get the schema, it will call the functions `fn get_schema_in_ctxt(&mut HashSet<Name>, &Namespace) -> Schema`
///    and `fn get_record_fields_in_ctxt(usize, &mut HashSet<Name>, &Namespace) -> Option<Vec<RecordField>>` in the module provided
///    to the Serde attribute. See [`AvroSchemaComponent`] for details on how to implement those
///    functions.
///
/// 2. By providing a function directly, `#[avro(with = some_fn)]`.
///
///    To get the schema, it will call the function provided. It must have the signature
///    `fn(&mut HashSet<Name>, &Namespace) -> Schema`. When this is used for a `transparent` struct, the
///    default implementation of [`AvroSchemaComponent::get_record_fields_in_ctxt`] will be used.
///    This is only recommended for primitive types, as the default implementation cannot be efficiently
///    implemented for complex types.
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
/// # use apache_avro::{Schema, serde::{AvroSchemaComponent}, schema::{Name, Namespace, RecordField}};
/// # use std::collections::HashSet;
/// // Make sure to implement `Serialize` and `Deserialize` to use the right serialization methods
/// pub struct U24([u8; 3]);
/// impl AvroSchemaComponent for U24 {
///     fn get_schema_in_ctxt(_: &mut HashSet<Name>, _: &Namespace) -> Schema {
///         Schema::Int
///     }
///
///     fn get_record_fields_in_ctxt(_: usize, _: &mut HashSet<Name>, _: &Namespace) -> Option<Vec<RecordField>> {
///         None // A Schema::Int is not a Schema::Record so there are no fields to return
///     }
///
///     fn field_default() -> Option<serde_json::Value> {
///         // Zero as default value. Can also be None if you don't want to provide a default value
///         Some(0u8.into())
///     }
///}
/// ```
///
/// ### Passthrough implementation
///
/// To construct a schema for a type is "transparent", such as for smart pointers, simply
/// pass through the arguments to the inner type:
/// ```
/// # use apache_avro::{Schema, serde::{AvroSchemaComponent}, schema::{Name, Namespace, RecordField}};
/// # use serde::{Serialize, Deserialize};
/// # use std::collections::HashSet;
/// #[derive(Serialize, Deserialize)]
/// #[serde(transparent)] // This attribute is important for all passthrough implementations!
/// pub struct Transparent<T>(T);
/// impl<T: AvroSchemaComponent> AvroSchemaComponent for Transparent<T> {
///     fn get_schema_in_ctxt(named_schemas: &mut HashSet<Name>, enclosing_namespace: &Namespace) -> Schema {
///         T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
///     }
///
///     fn get_record_fields_in_ctxt(first_field_position: usize, named_schemas: &mut HashSet<Name>, enclosing_namespace: &Namespace) -> Option<Vec<RecordField>> {
///         T::get_record_fields_in_ctxt(first_field_position, named_schemas, enclosing_namespace)
///     }
///
///     fn field_default() -> Option<serde_json::Value> {
///         T::field_default()
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
///  - Implement `field_default()` if you want to use `#[serde(skip_serializing{,_if})]`.
///
/// ```
/// # use apache_avro::{Schema, serde::{AvroSchemaComponent}, schema::{Name, Namespace, RecordField, RecordSchema}};
/// # use serde::{Serialize, Deserialize};
/// # use std::{time::Duration, collections::HashSet};
/// pub struct Foo {
///     one: String,
///     two: i32,
///     three: Option<Duration>
/// }
///
/// impl AvroSchemaComponent for Foo {
///     fn get_schema_in_ctxt(named_schemas: &mut HashSet<Name>, enclosing_namespace: &Namespace) -> Schema {
///         // Create the fully qualified name for your type given the enclosing namespace
///         let name = Name::new("Foo").unwrap().fully_qualified_name(enclosing_namespace);
///         if named_schemas.contains(&name) {
///             Schema::Ref { name }
///         } else {
///             let enclosing_namespace = &name.namespace;
///             // Do this before you start creating the schema, as otherwise recursive types will cause infinite recursion.
///             named_schemas.insert(name.clone());
///             let schema = Schema::Record(RecordSchema::builder()
///                 .name(name.clone())
///                 .fields(Self::get_record_fields_in_ctxt(0, named_schemas, enclosing_namespace).expect("Impossible!"))
///                 .build()
///             );
///             schema
///         }
///     }
///
///     fn get_record_fields_in_ctxt(first_field_position: usize, named_schemas: &mut HashSet<Name>, enclosing_namespace: &Namespace) -> Option<Vec<RecordField>> {
///         Some(vec![
///             RecordField::builder()
///                 .name("one")
///                 .schema(String::get_schema_in_ctxt(named_schemas, enclosing_namespace))
///                 .position(first_field_position)
///                 .build(),
///             RecordField::builder()
///                 .name("two")
///                 .schema(i32::get_schema_in_ctxt(named_schemas, enclosing_namespace))
///                 .position(first_field_position+1)
///                 .build(),
///             RecordField::builder()
///                 .name("three")
///                 .schema(<Option<Duration>>::get_schema_in_ctxt(named_schemas, enclosing_namespace))
///                 .position(first_field_position+2)
///                 .build(),
///         ])
///     }
///
///     fn field_default() -> Option<serde_json::Value> {
///         // This type does not provide a default value
///         None
///     }
///}
/// ```
pub trait AvroSchemaComponent {
    /// Get the schema for this component
    fn get_schema_in_ctxt(
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: &Namespace,
    ) -> Schema;

    /// Get the fields of this schema if it is a record.
    ///
    /// This returns `None` if the schema is not a record.
    ///
    /// The default implementation has to do a lot of extra work, so it is strongly recommended to
    /// implement this function when manually implementing this trait.
    fn get_record_fields_in_ctxt(
        first_field_position: usize,
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: &Namespace,
    ) -> Option<Vec<RecordField>> {
        get_record_fields_in_ctxt(
            first_field_position,
            named_schemas,
            enclosing_namespace,
            Self::get_schema_in_ctxt,
        )
    }

    /// The default value of this type when used for a record field.
    ///
    /// `None` means no default value, which is also the default implementation.
    ///
    /// Implementations of this trait provided by this crate return `None` except for `Option<T>`
    /// which returns `Some(serde_json::Value::Null)`.
    fn field_default() -> Option<serde_json::Value> {
        None
    }
}

/// Get the record fields from `schema_fn` without polluting `named_schemas` or causing duplicate names
///
/// This is public so the derive macro can use it for `#[avro(with = ||)]` and `#[avro(with = path)]`
pub fn get_record_fields_in_ctxt(
    first_field_position: usize,
    named_schemas: &mut HashSet<Name>,
    enclosing_namespace: &Namespace,
    schema_fn: fn(named_schemas: &mut HashSet<Name>, enclosing_namespace: &Namespace) -> Schema,
) -> Option<Vec<RecordField>> {
    let mut record = match schema_fn(named_schemas, enclosing_namespace) {
        Schema::Record(record) => record,
        Schema::Ref { name } => {
            // This schema already exists in `named_schemas` so temporarily remove it so we can
            // get the actual schema.
            assert!(
                named_schemas.remove(&name),
                "Name '{name}' should exist in `named_schemas` otherwise Ref is invalid: {named_schemas:?}"
            );
            // Get the schema
            let schema = schema_fn(named_schemas, enclosing_namespace);
            // Reinsert the old value
            named_schemas.insert(name);

            // Now check if we actually got a record and return the fields if that is the case
            let Schema::Record(record) = schema else {
                return None;
            };
            let fields = record
                .fields
                .into_iter()
                .map(|mut f| {
                    f.position += first_field_position;
                    f
                })
                .collect();
            return Some(fields);
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

            let name = match std::mem::replace(schema, Schema::Record(new_schema)) {
                Schema::Ref { name } => name,
                schema => {
                    panic!("Only expected `Schema::Ref` from `find_first_ref`, got: {schema:?}")
                }
            };

            // The schema is used, so reinsert it
            named_schemas.insert(name.clone());

            break;
        }
    }

    let fields = record
        .fields
        .into_iter()
        .map(|mut f| {
            f.position += first_field_position;
            f
        })
        .collect();
    Some(fields)
}

impl<T> AvroSchema for T
where
    T: AvroSchemaComponent + ?Sized,
{
    fn get_schema() -> Schema {
        T::get_schema_in_ctxt(&mut HashSet::default(), &None)
    }
}

macro_rules! impl_schema (
    ($type:ty, $variant_constructor:expr) => (
        impl AvroSchemaComponent for $type {
            fn get_schema_in_ctxt(_: &mut HashSet<Name>, _: &Namespace) -> Schema {
                $variant_constructor
            }

            fn get_record_fields_in_ctxt(_: usize, _: &mut HashSet<Name>, _: &Namespace) -> Option<Vec<RecordField>> {
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
impl_schema!((), Schema::Null);

macro_rules! impl_passthrough_schema (
    ($type:ty where T: AvroSchemaComponent + ?Sized $(+ $bound:tt)*) => (
        impl<T: AvroSchemaComponent $(+ $bound)* + ?Sized> AvroSchemaComponent for $type {
            fn get_schema_in_ctxt(named_schemas: &mut HashSet<Name>, enclosing_namespace: &Namespace) -> Schema {
                T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
            }

            fn get_record_fields_in_ctxt(first_field_position: usize, named_schemas: &mut HashSet<Name>, enclosing_namespace: &Namespace) -> Option<Vec<RecordField>> {
                T::get_record_fields_in_ctxt(first_field_position, named_schemas, enclosing_namespace)
            }

            fn field_default() -> Option<serde_json::Value> {
                T::field_default()
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
            fn get_schema_in_ctxt(named_schemas: &mut HashSet<Name>, enclosing_namespace: &Namespace) -> Schema {
                Schema::array(T::get_schema_in_ctxt(named_schemas, enclosing_namespace)).build()
            }

            fn get_record_fields_in_ctxt(_: usize, _: &mut HashSet<Name>, _: &Namespace) -> Option<Vec<RecordField>> {
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
    fn get_schema_in_ctxt(
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: &Namespace,
    ) -> Schema {
        Schema::array(T::get_schema_in_ctxt(named_schemas, enclosing_namespace)).build()
    }

    fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut HashSet<Name>,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }
}

impl<T> AvroSchemaComponent for HashMap<String, T>
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: &Namespace,
    ) -> Schema {
        Schema::map(T::get_schema_in_ctxt(named_schemas, enclosing_namespace)).build()
    }

    fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut HashSet<Name>,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }
}

impl<T> AvroSchemaComponent for Option<T>
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: &Namespace,
    ) -> Schema {
        let variants = vec![
            Schema::Null,
            T::get_schema_in_ctxt(named_schemas, enclosing_namespace),
        ];

        Schema::Union(
            UnionSchema::new(variants).expect("Option<T> must produce a valid (non-nested) union"),
        )
    }

    fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut HashSet<Name>,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }

    fn field_default() -> Option<serde_json::Value> {
        Some(serde_json::Value::Null)
    }
}

impl AvroSchemaComponent for core::time::Duration {
    /// The schema is [`Schema::Duration`] with the name `duration`.
    ///
    /// This is a lossy conversion as this Avro type does not store the amount of nanoseconds.
    fn get_schema_in_ctxt(
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: &Namespace,
    ) -> Schema {
        let name = Name::new("duration")
            .expect("Name is valid")
            .fully_qualified_name(enclosing_namespace);
        if named_schemas.contains(&name) {
            Schema::Ref { name }
        } else {
            let schema = Schema::Duration(FixedSchema {
                name: name.clone(),
                aliases: None,
                doc: None,
                size: 12,
                attributes: Default::default(),
            });
            named_schemas.insert(name);
            schema
        }
    }

    fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut HashSet<Name>,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }
}

impl AvroSchemaComponent for uuid::Uuid {
    /// The schema is [`Schema::Uuid`] with the name `uuid`.
    ///
    /// The underlying schema is [`Schema::Fixed`] with a size of 16.
    fn get_schema_in_ctxt(
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: &Namespace,
    ) -> Schema {
        let name = Name::new("uuid")
            .expect("Name is valid")
            .fully_qualified_name(enclosing_namespace);
        if named_schemas.contains(&name) {
            Schema::Ref { name }
        } else {
            let schema = Schema::Uuid(UuidSchema::Fixed(FixedSchema {
                name: name.clone(),
                aliases: None,
                doc: None,
                size: 16,
                attributes: Default::default(),
            }));
            named_schemas.insert(name);
            schema
        }
    }

    fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut HashSet<Name>,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }
}

impl AvroSchemaComponent for u64 {
    /// The schema is [`Schema::Fixed`] of size 8 with the name `u64`.
    fn get_schema_in_ctxt(
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: &Namespace,
    ) -> Schema {
        let name = Name::new("u64")
            .expect("Name is valid")
            .fully_qualified_name(enclosing_namespace);
        if named_schemas.contains(&name) {
            Schema::Ref { name }
        } else {
            let schema = Schema::Fixed(FixedSchema {
                name: name.clone(),
                aliases: None,
                doc: None,
                size: 8,
                attributes: Default::default(),
            });
            named_schemas.insert(name);
            schema
        }
    }

    fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut HashSet<Name>,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }
}

impl AvroSchemaComponent for u128 {
    /// The schema is [`Schema::Fixed`] of size 16 with the name `u128`.
    fn get_schema_in_ctxt(
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: &Namespace,
    ) -> Schema {
        let name = Name::new("u128")
            .expect("Name is valid")
            .fully_qualified_name(enclosing_namespace);
        if named_schemas.contains(&name) {
            Schema::Ref { name }
        } else {
            let schema = Schema::Fixed(FixedSchema {
                name: name.clone(),
                aliases: None,
                doc: None,
                size: 16,
                attributes: Default::default(),
            });
            named_schemas.insert(name);
            schema
        }
    }

    fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut HashSet<Name>,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }
}

impl AvroSchemaComponent for i128 {
    /// The schema is [`Schema::Fixed`] of size 16 with the name `i128`.
    fn get_schema_in_ctxt(
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: &Namespace,
    ) -> Schema {
        let name = Name::new("i128")
            .expect("Name is valid")
            .fully_qualified_name(enclosing_namespace);
        if named_schemas.contains(&name) {
            Schema::Ref { name }
        } else {
            let schema = Schema::Fixed(FixedSchema {
                name: name.clone(),
                aliases: None,
                doc: None,
                size: 16,
                attributes: Default::default(),
            });
            named_schemas.insert(name);
            schema
        }
    }

    fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut HashSet<Name>,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        AvroSchema, Schema,
        schema::{FixedSchema, Name},
    };
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
        assert_eq!(schema, Schema::array(Schema::Int).build());

        Ok(())
    }

    #[test]
    fn avro_rs_401_array() -> TestResult {
        let schema = <[u8; 55]>::get_schema();
        assert_eq!(schema, Schema::array(Schema::Int).build());

        Ok(())
    }

    #[test]
    fn avro_rs_401_option_ref_slice_array() -> TestResult {
        let schema = <Option<&[[u8; 55]]>>::get_schema();
        assert_eq!(
            schema,
            Schema::union(vec![
                Schema::Null,
                Schema::array(Schema::array(Schema::Int).build()).build()
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
                attributes: Default::default(),
            })
        );

        Ok(())
    }

    #[test]
    fn avro_rs_486_unit() -> TestResult {
        let schema = <()>::get_schema();
        assert_eq!(schema, Schema::Null);

        Ok(())
    }

    #[test]
    #[should_panic(
        expected = "Option<T> must produce a valid (non-nested) union: Error { details: Unions cannot contain duplicate types, found at least two Null }"
    )]
    fn avro_rs_489_some_unit() {
        <Option<()>>::get_schema();
    }

    #[test]
    #[should_panic(
        expected = "Option<T> must produce a valid (non-nested) union: Error { details: Unions may not directly contain a union }"
    )]
    fn avro_rs_489_option_option() {
        <Option<Option<i32>>>::get_schema();
    }
}
