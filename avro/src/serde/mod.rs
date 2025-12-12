//! Everything needed to use this crate with Serde.
//!
//! # Using `apache-avro` for `serde`
//!
//! Avro is a schema-based format, this means it requires a few extra steps to use compared to
//! a data format like JSON.
//!
//! ## Schemas
//! It's strongly recommended to derive the schemas for your types using the [`AvroSchema`] derive macro.
//! The macro uses the Serde attributes to generate a matching schema and checks that no attributes are
//! used that are incompatible with the Serde implementation in this crate. See the trait documenation for
//! details on how to change the generated schema.
//!
//! Alternatively, you can write your own schema. If you go down this path, it is recommended you start with
//! the schema derived by [`AvroSchema`] and then modify it to fit your needs.
//!
//! ### Using existing schemas
//! If you have schemas that are already being used in other parts of your software stack, generating types
//! from the schema can be very useful. There is a **third-party** crate [`rsgen-avro`] that implements this.
//!
//! ## Reading and writing data
//!
//! ```
//! # use std::io::Cursor;
//! # use serde::{Serialize, Deserialize};
//! # use apache_avro::{AvroSchema, Error, Reader, Writer, serde::{from_value, to_value}};
//! #
//! # #[derive(PartialEq, Debug)]
//! #[derive(Serialize, Deserialize, AvroSchema)]
//! struct Foo {
//!     a: i64,
//!     b: String,
//! }
//!
//! # fn main() -> Result<(), Error> {
//! let schema = Foo::get_schema();
//! // A writer needs the schema of the type that is going to be written
//! let mut writer = Writer::new(&schema, Vec::new())?;
//!
//! let foo = Foo {
//!     a: 42,
//!     b: "Hello".to_string(),
//! };
//!
//! // There are two ways to serialize data.
//! // 1: Serialize directly to the writer:
//! writer.append_ser(&foo)?;
//! // 2: First serialize to an Avro `Value` then write that:
//! let foo_value = to_value(&foo)?;
//! writer.append(foo_value)?;
//!
//! // Always flush or consume the writer
//! let data = writer.into_inner()?;
//!
//! // The reader does not need a schema as it's included in the data
//! let reader = Reader::new(Cursor::new(data))?;
//! // The reader is an iterator
//! for result in reader {
//!     let value = result?;
//!     let new_foo: Foo = from_value(&value)?;
//!     assert_eq!(new_foo, foo);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! [`rsgen-avro`]: https://docs.rs/rsgen-avro/latest/rsgen_avro/

use crate::{
    Schema,
    schema::{Names, Namespace, SchemaKind, UnionSchema},
};

mod de;
mod ser;
mod ser_schema;
mod util;

pub mod bytes;

pub use de::from_value;
pub use ser::to_value;
pub(crate) use ser_schema::SchemaAwareWriteSerializer;

/// Trait for types that serve as an Avro data model.
///
/// Do not implement directly! Implement [`AvroSchemaComponent`] to get this trait through
/// a blanket implementation or use the derive macro.
///
/// # Deriving `AvroSchema`
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
/// # Compatibility with Serde attributes
///
/// The derive macro is compatible with most Serde attributes. It is incompatible with
/// the following attributes:
///
/// - Container attributes
///     - `tag`
///     - `content`
///     - `untagged`
///     - `variant_identifier`
///     - `field_identifier`
///     - `remote`
///     - `transparent`
///     - `rename_all(serialize = "..", deserialize = "..")` where `serialize` != `deserialize`
/// - Variant attributes
///     - `other`
///     - `untagged`
/// - Field attributes
///     - `getter`
///
/// The Serde attributes `skip_serializing` and `skip_serializing_if` require the `#[avro(default = "..")]`
/// attribute because Avro does not support skipping fields.
pub trait AvroSchema {
    fn get_schema() -> Schema;
}

/// Trait for types that serve as fully defined components inside an Avro data model.
///
/// This can be derived via the [`AvroSchema`] derive.
///
/// # Implementation guide
///
/// ### Simple implementation
/// To construct a unnamed simple schema, it is possible to ignore the input argument making the
/// general form implementation look like
/// ```
/// # use apache_avro::{Schema, schema::{Names, Namespace}, serde::AvroSchemaComponent};
/// # struct AType;
/// impl AvroSchemaComponent for AType {
///     fn get_schema_in_ctxt(_names: &mut Names, _enclosing_namespace: &Namespace) -> Schema {
///        Schema::Int
///    }
/// }
/// ```
/// ### Passthrough implementation
/// To construct a schema for a Type that acts as in "inner" type, such as for smart pointers, simply
/// pass through the arguments to the inner type
/// ```
/// # use apache_avro::{Schema, schema::{Names, Namespace}, serde::AvroSchemaComponent};
/// # struct InnerType;
/// # impl AvroSchemaComponent for InnerType {
/// #    fn get_schema_in_ctxt(_names: &mut Names, _enclosing_namespace: &Namespace) -> Schema {
/// #       Schema::Int
/// #    }
/// # }
/// # struct PassthroughType(InnerType);
/// impl AvroSchemaComponent for PassthroughType {
///     fn get_schema_in_ctxt(names: &mut Names, enclosing_namespace: &Namespace) -> Schema {
///        InnerType::get_schema_in_ctxt(names, enclosing_namespace)
///    }
/// }
/// ```
/// ### Complex implementation
/// To implement this for Named schema there is a general form needed to avoid creating invalid
/// schemas or infinite loops.
/// ```ignore
/// # use apache_avro::{Schema, schema::{Name, Names, Namespace}, serde::AvroSchemaComponent};
/// impl AvroSchemaComponent for ComplexType {
///     fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
///         // Create the fully qualified name for your type given the enclosing namespace
///         let name =  Name::new("MyName").unwrap().fully_qualified_name(enclosing_namespace);
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
    T: AvroSchemaComponent,
{
    fn get_schema() -> Schema {
        T::get_schema_in_ctxt(&mut std::collections::HashMap::default(), &None)
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
impl_schema!(uuid::Uuid, Schema::Uuid(crate::schema::UuidSchema::String));
impl_schema!(core::time::Duration, Schema::Duration);

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
        let inner_schema = T::get_schema_in_ctxt(named_schemas, enclosing_namespace);
        Schema::Union(UnionSchema {
            schemas: vec![Schema::Null, inner_schema.clone()],
            variant_index: [Schema::Null, inner_schema]
                .iter()
                .enumerate()
                .map(|(idx, s)| (SchemaKind::from(s), idx))
                .collect(),
        })
    }
}

impl<T> AvroSchemaComponent for serde_json::Map<String, T>
where
    T: AvroSchemaComponent,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        Schema::map(T::get_schema_in_ctxt(named_schemas, enclosing_namespace))
    }
}

impl<T> AvroSchemaComponent for std::collections::HashMap<String, T>
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

impl<T> AvroSchemaComponent for std::borrow::Cow<'_, T>
where
    T: AvroSchemaComponent + Clone,
{
    fn get_schema_in_ctxt(named_schemas: &mut Names, enclosing_namespace: &Namespace) -> Schema {
        T::get_schema_in_ctxt(named_schemas, enclosing_namespace)
    }
}
