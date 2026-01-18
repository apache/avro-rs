use crate::Schema;
use crate::schema::{FixedSchema, Name, Names, Namespace, UnionSchema, UuidSchema};
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
/// Every member of the `struct` and `enum` must also implement `AvroSchemaComponent`.
///
/// ## Changing the generated schema
///
/// The derive macro will read both the `avro` and `serde` attributes to modify the generated schema.
/// It will also check for compatibility between the various attributes.
///
/// ### Container attributes
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
/// ### Variant attributes
///
///  - `#[serde(rename = "name")]`
///
///    Rename the variant to the given name.
///
///
/// ### Field attributes
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
/// ### Incompatible Serde attributes
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
/// for built-in types, [`serde_json::Map`] and [`uuid::Uuid`]. Notable exceptions are [`char`] and
/// [`u64`] types, as there is no equivalent for char in Avro and the largest integer type in Avro
/// is `long` (equal to an [`i64`]).
///
/// To still be able to derive schemas for fields of foreign types, the `#[avro(with)`]
/// attribute can be used to get the schema for those fields. It can be used in two ways:
///
/// 1. In combination with `#[serde(with = "path::to::module)]`
///
///    To get the schema, it will call the function `fn get_schema_in_ctxt(&mut Names, &Namespace) -> Schema`
///    in the module provided to the Serde attribute.
///
/// 2. By providing a function directly, `#[avro(with = some_fn)]`.
///
///    To get the schema, it will call the function provided. It must have the signature
///    `fn(&mut Names, &Namespace) -> Schema`
pub trait AvroSchema {
    fn get_schema() -> Schema;
}

/// Trait for types that serve as fully defined components inside an Avro data model. Derive
/// implementation available through `derive` feature. This is what is implemented by
/// the `derive(AvroSchema)` macro.
///
/// Note: This trait is **not** implemented for `char` and `u64`. `char` is a 32-bit value
/// that does not have a logical mapping to an Avro schema. `u64` is too large to fit in a
/// Avro `long`.
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
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema;
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
