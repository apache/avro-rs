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

//! Logic for parsing and interacting with schemas in Avro format.

mod builders;
mod name;
mod parser;
mod record;
mod resolve;
mod union;

pub(crate) use crate::schema::resolve::{
    ResolvedOwnedSchema, resolve_names, resolve_names_with_schemata,
};
pub use crate::schema::{
    name::{Alias, Aliases, Name, Names, NamesRef, Namespace},
    record::{
        RecordField, RecordFieldBuilder, RecordFieldOrder, RecordSchema, RecordSchemaBuilder,
    },
    resolve::ResolvedSchema,
    union::UnionSchema,
};
use crate::{
    AvroResult,
    error::{Details, Error},
    schema::parser::Parser,
    schema_equality,
    types::{self, Value},
};
use digest::Digest;
use serde::{
    Serialize, Serializer,
    ser::{Error as _, SerializeMap, SerializeSeq},
};
use serde_json::{Map, Value as JsonValue};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    fmt::Debug,
    hash::Hash,
    io::Read,
};
use strum::{Display, EnumDiscriminants};

/// Represents documentation for complex Avro schemas.
pub type Documentation = Option<String>;

/// Represents an Avro schema fingerprint.
///
/// More information about Avro schema fingerprints can be found in the
/// [Avro Schema Fingerprint documentation](https://avro.apache.org/docs/++version++/specification/#schema-fingerprints)
pub struct SchemaFingerprint {
    pub bytes: Vec<u8>,
}

impl fmt::Display for SchemaFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            self.bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<Vec<String>>()
                .join("")
        )
    }
}

/// Represents any valid Avro schema
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/++version++/specification/#schema-declaration)
#[derive(Clone, Debug, EnumDiscriminants, Display)]
#[strum_discriminants(name(SchemaKind), derive(Hash, Ord, PartialOrd))]
pub enum Schema {
    /// A `null` Avro schema.
    Null,
    /// A `boolean` Avro schema.
    Boolean,
    /// An `int` Avro schema.
    Int,
    /// A `long` Avro schema.
    Long,
    /// A `float` Avro schema.
    Float,
    /// A `double` Avro schema.
    Double,
    /// A `bytes` Avro schema.
    ///
    /// `Bytes` represents a sequence of 8-bit unsigned bytes.
    Bytes,
    /// A `string` Avro schema.
    ///
    /// `String` represents a unicode character sequence.
    String,
    /// An `array` Avro schema.
    ///
    /// All items will have the same schema.
    Array(ArraySchema),
    /// A `map` Avro schema.
    ///
    /// Keys are always a `Schema::String` and all values will have the same schema.
    Map(MapSchema),
    /// A `union` Avro schema.
    Union(UnionSchema),
    /// A `record` Avro schema.
    Record(RecordSchema),
    /// An `enum` Avro schema.
    Enum(EnumSchema),
    /// A `fixed` Avro schema.
    Fixed(FixedSchema),
    /// Logical type which represents `Decimal` values.
    ///
    /// The underlying type is serialized and deserialized as `Schema::Bytes` or `Schema::Fixed`.
    Decimal(DecimalSchema),
    /// Logical type which represents `Decimal` values without predefined scale.
    ///
    /// The underlying type is serialized and deserialized as `Schema::Bytes`
    BigDecimal,
    /// A universally unique identifier, annotating a string, bytes or fixed.
    Uuid(UuidSchema),
    /// Logical type which represents the number of days since the unix epoch.
    ///
    /// Serialization format is `Schema::Int`.
    Date,
    /// The time of day in number of milliseconds after midnight.
    ///
    /// This type has no reference to any calendar, time zone or date in particular.
    TimeMillis,
    /// The time of day in number of microseconds after midnight.
    ///
    /// This type has no reference to any calendar, time zone or date in particular.
    TimeMicros,
    /// An instant in time represented as the number of milliseconds after the UNIX epoch.
    TimestampMillis,
    /// An instant in time represented as the number of microseconds after the UNIX epoch.
    TimestampMicros,
    /// An instant in time represented as the number of nanoseconds after the UNIX epoch.
    TimestampNanos,
    /// An instant in localtime represented as the number of milliseconds after the UNIX epoch.
    LocalTimestampMillis,
    /// An instant in local time represented as the number of microseconds after the UNIX epoch.
    LocalTimestampMicros,
    /// An instant in local time represented as the number of nanoseconds after the UNIX epoch.
    LocalTimestampNanos,
    /// An amount of time defined by a number of months, days and milliseconds.
    Duration(FixedSchema),
    /// A reference to another schema.
    Ref { name: Name },
}

#[derive(Clone, Debug, PartialEq)]
pub struct MapSchema {
    pub types: Box<Schema>,
    pub default: Option<HashMap<String, Value>>,
    pub attributes: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ArraySchema {
    pub items: Box<Schema>,
    pub default: Option<Vec<Value>>,
    pub attributes: BTreeMap<String, JsonValue>,
}

impl PartialEq for Schema {
    /// Assess equality of two `Schema` based on [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/1.11.1/specification/#parsing-canonical-form-for-schemas
    fn eq(&self, other: &Self) -> bool {
        schema_equality::compare_schemata(self, other)
    }
}

impl SchemaKind {
    pub fn is_primitive(self) -> bool {
        matches!(
            self,
            SchemaKind::Null
                | SchemaKind::Boolean
                | SchemaKind::Int
                | SchemaKind::Long
                | SchemaKind::Double
                | SchemaKind::Float
                | SchemaKind::Bytes
                | SchemaKind::String,
        )
    }

    #[deprecated(since = "0.22.0", note = "Use Schema::is_named instead")]
    pub fn is_named(self) -> bool {
        matches!(
            self,
            SchemaKind::Record
                | SchemaKind::Enum
                | SchemaKind::Fixed
                | SchemaKind::Ref
                | SchemaKind::Duration
        )
    }
}

impl From<&types::Value> for SchemaKind {
    fn from(value: &types::Value) -> Self {
        use crate::types::Value;
        match value {
            Value::Null => Self::Null,
            Value::Boolean(_) => Self::Boolean,
            Value::Int(_) => Self::Int,
            Value::Long(_) => Self::Long,
            Value::Float(_) => Self::Float,
            Value::Double(_) => Self::Double,
            Value::Bytes(_) => Self::Bytes,
            Value::String(_) => Self::String,
            Value::Array(_) => Self::Array,
            Value::Map(_) => Self::Map,
            Value::Union(_, _) => Self::Union,
            Value::Record(_) => Self::Record,
            Value::Enum(_, _) => Self::Enum,
            Value::Fixed(_, _) => Self::Fixed,
            Value::Decimal { .. } => Self::Decimal,
            Value::BigDecimal(_) => Self::BigDecimal,
            Value::Uuid(_) => Self::Uuid,
            Value::Date(_) => Self::Date,
            Value::TimeMillis(_) => Self::TimeMillis,
            Value::TimeMicros(_) => Self::TimeMicros,
            Value::TimestampMillis(_) => Self::TimestampMillis,
            Value::TimestampMicros(_) => Self::TimestampMicros,
            Value::TimestampNanos(_) => Self::TimestampNanos,
            Value::LocalTimestampMillis(_) => Self::LocalTimestampMillis,
            Value::LocalTimestampMicros(_) => Self::LocalTimestampMicros,
            Value::LocalTimestampNanos(_) => Self::LocalTimestampNanos,
            Value::Duration { .. } => Self::Duration,
        }
    }
}

/// A description of an Enum schema.
#[derive(bon::Builder, Debug, Clone)]
pub struct EnumSchema {
    /// The name of the schema
    pub name: Name,
    /// The aliases of the schema
    #[builder(default)]
    pub aliases: Aliases,
    /// The documentation of the schema
    #[builder(default)]
    pub doc: Documentation,
    /// The set of symbols of the schema
    pub symbols: Vec<String>,
    /// An optional default symbol used for compatibility
    pub default: Option<String>,
    /// The custom attributes of the schema
    #[builder(default = BTreeMap::new())]
    pub attributes: BTreeMap<String, JsonValue>,
}

/// A description of a Fixed schema.
#[derive(bon::Builder, Debug, Clone)]
pub struct FixedSchema {
    /// The name of the schema
    pub name: Name,
    /// The aliases of the schema
    #[builder(default)]
    pub aliases: Aliases,
    /// The documentation of the schema
    #[builder(default)]
    pub doc: Documentation,
    /// The size of the fixed schema
    pub size: usize,
    /// The custom attributes of the schema
    #[builder(default = BTreeMap::new())]
    pub attributes: BTreeMap<String, JsonValue>,
}

impl FixedSchema {
    fn serialize_to_map<S>(&self, mut map: S::SerializeMap) -> Result<S::SerializeMap, S::Error>
    where
        S: Serializer,
    {
        map.serialize_entry("type", "fixed")?;
        if let Some(n) = self.name.namespace.as_ref() {
            map.serialize_entry("namespace", n)?;
        }
        map.serialize_entry("name", &self.name.name)?;
        if let Some(docstr) = self.doc.as_ref() {
            map.serialize_entry("doc", docstr)?;
        }
        map.serialize_entry("size", &self.size)?;

        if let Some(aliases) = self.aliases.as_ref() {
            map.serialize_entry("aliases", aliases)?;
        }

        for attr in &self.attributes {
            map.serialize_entry(attr.0, attr.1)?;
        }

        Ok(map)
    }

    /// Create a new `FixedSchema` copying only the size.
    ///
    /// All other fields are `None` or empty.
    pub(crate) fn copy_only_size(&self) -> Self {
        Self {
            name: Name {
                name: String::new(),
                namespace: None,
            },
            aliases: None,
            doc: None,
            size: self.size,
            attributes: Default::default(),
        }
    }
}

/// A description of a Decimal schema.
///
/// `scale` defaults to 0 and is an integer greater than or equal to 0 and `precision` is an
/// integer greater than 0.
#[derive(Debug, Clone)]
pub struct DecimalSchema {
    /// The number of digits in the unscaled value
    pub precision: DecimalMetadata,
    /// The number of digits to the right of the decimal point
    pub scale: DecimalMetadata,
    /// The inner schema of the decimal (fixed or bytes)
    pub inner: InnerDecimalSchema,
}

/// The inner schema of the Decimal type.
#[derive(Debug, Clone)]
pub enum InnerDecimalSchema {
    Bytes,
    Fixed(FixedSchema),
}

impl TryFrom<Schema> for InnerDecimalSchema {
    type Error = Error;

    fn try_from(value: Schema) -> Result<Self, Self::Error> {
        match value {
            Schema::Bytes => Ok(InnerDecimalSchema::Bytes),
            Schema::Fixed(fixed) => Ok(InnerDecimalSchema::Fixed(fixed)),
            _ => Err(Details::ResolveDecimalSchema(value.into()).into()),
        }
    }
}

/// The inner schema of the Uuid type.
#[derive(Debug, Clone)]
pub enum UuidSchema {
    /// [`Schema::Bytes`] with size of 16.
    ///
    /// This is not according to specification, but was what happened in `0.21.0` and earlier when
    /// a schema with logical type `uuid` and inner type `fixed` was used.
    Bytes,
    /// [`Schema::String`].
    String,
    /// [`Schema::Fixed`] with size of 16.
    Fixed(FixedSchema),
}

type DecimalMetadata = usize;
pub(crate) type Precision = DecimalMetadata;
pub(crate) type Scale = DecimalMetadata;

impl Schema {
    /// Converts `self` into its [Parsing Canonical Form].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/++version++/specification/#parsing-canonical-form-for-schemas
    pub fn canonical_form(&self) -> String {
        let json = serde_json::to_value(self)
            .unwrap_or_else(|e| panic!("Cannot parse Schema from JSON: {e}"));
        let mut defined_names = HashSet::new();
        parsing_canonical_form(&json, &mut defined_names)
    }

    /// Returns the [Parsing Canonical Form] of `self` that is self contained (not dependent on
    /// any definitions in `schemata`)
    ///
    /// If you require a self contained schema including `default` and `doc` attributes, see [`denormalize`][Schema::denormalize].
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/++version++/specification/#parsing-canonical-form-for-schemas
    pub fn independent_canonical_form(&self, schemata: &[Schema]) -> Result<String, Error> {
        let mut this = self.clone();
        this.denormalize(schemata)?;
        Ok(this.canonical_form())
    }

    /// Generate the [fingerprint] of the schema's [Parsing Canonical Form].
    ///
    /// # Example
    /// ```
    /// use apache_avro::rabin::Rabin;
    /// use apache_avro::{Schema, Error};
    /// use md5::Md5;
    /// use sha2::Sha256;
    ///
    /// fn main() -> Result<(), Error> {
    ///     let raw_schema = r#"
    ///         {
    ///             "type": "record",
    ///             "name": "test",
    ///             "fields": [
    ///                 {"name": "a", "type": "long", "default": 42},
    ///                 {"name": "b", "type": "string"}
    ///             ]
    ///         }
    ///     "#;
    ///     let schema = Schema::parse_str(raw_schema)?;
    ///     println!("{}", schema.fingerprint::<Sha256>());
    ///     println!("{}", schema.fingerprint::<Md5>());
    ///     println!("{}", schema.fingerprint::<Rabin>());
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/++version++/specification/#parsing-canonical-form-for-schemas
    /// [fingerprint]:
    /// https://avro.apache.org/docs/++version++/specification/#schema-fingerprints
    pub fn fingerprint<D: Digest>(&self) -> SchemaFingerprint {
        let mut d = D::new();
        d.update(self.canonical_form());
        SchemaFingerprint {
            bytes: d.finalize().to_vec(),
        }
    }

    /// Create a `Schema` from a string representing a JSON Avro schema.
    pub fn parse_str(input: &str) -> Result<Schema, Error> {
        let mut parser = Parser::default();
        parser.parse_str(input)
    }

    /// Create an array of `Schema`'s from a list of named JSON Avro schemas (Record, Enum, and
    /// Fixed).
    ///
    /// It is allowed that the schemas have cross-dependencies; these will be resolved
    /// during parsing.
    ///
    /// If two of the input schemas have the same fullname, an Error will be returned.
    pub fn parse_list(input: impl IntoIterator<Item = impl AsRef<str>>) -> AvroResult<Vec<Schema>> {
        let input = input.into_iter();
        let input_len = input.size_hint().0;
        let mut input_schemas: HashMap<Name, JsonValue> = HashMap::with_capacity(input_len);
        let mut input_order: Vec<Name> = Vec::with_capacity(input_len);
        for json in input {
            let json = json.as_ref();
            let schema: JsonValue = serde_json::from_str(json).map_err(Details::ParseSchemaJson)?;
            if let JsonValue::Object(inner) = &schema {
                let name = Name::parse(inner, &None)?;
                let previous_value = input_schemas.insert(name.clone(), schema);
                if previous_value.is_some() {
                    return Err(Details::NameCollision(name.fullname(None)).into());
                }
                input_order.push(name);
            } else {
                return Err(Details::GetNameField.into());
            }
        }
        let mut parser = Parser::new(
            input_schemas,
            input_order,
            HashMap::with_capacity(input_len),
        );
        parser.parse_list()
    }

    /// Create a `Schema` from a string representing a JSON Avro schema,
    /// along with an array of `Schema`'s from a list of named JSON Avro schemas (Record, Enum, and
    /// Fixed).
    ///
    /// It is allowed that the schemas have cross-dependencies; these will be resolved
    /// during parsing.
    ///
    /// If two of the named input schemas have the same fullname, an Error will be returned.
    ///
    /// # Arguments
    /// * `schema` - the JSON string of the schema to parse
    /// * `schemata` - a slice of additional schemas that is used to resolve cross-references
    pub fn parse_str_with_list(
        schema: &str,
        schemata: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> AvroResult<(Schema, Vec<Schema>)> {
        let schemata = schemata.into_iter();
        let schemata_len = schemata.size_hint().0;
        let mut input_schemas: HashMap<Name, JsonValue> = HashMap::with_capacity(schemata_len);
        let mut input_order: Vec<Name> = Vec::with_capacity(schemata_len);
        for json in schemata {
            let json = json.as_ref();
            let schema: JsonValue = serde_json::from_str(json).map_err(Details::ParseSchemaJson)?;
            if let JsonValue::Object(inner) = &schema {
                let name = Name::parse(inner, &None)?;
                if let Some(_previous) = input_schemas.insert(name.clone(), schema) {
                    return Err(Details::NameCollision(name.fullname(None)).into());
                }
                input_order.push(name);
            } else {
                return Err(Details::GetNameField.into());
            }
        }
        let mut parser = Parser::new(
            input_schemas,
            input_order,
            HashMap::with_capacity(schemata_len),
        );
        parser.parse_input_schemas()?;

        let value = serde_json::from_str(schema).map_err(Details::ParseSchemaJson)?;
        let schema = parser.parse(&value, &None)?;
        let schemata = parser.parse_list()?;
        Ok((schema, schemata))
    }

    /// Create a `Schema` from a reader which implements [`Read`].
    pub fn parse_reader(reader: &mut (impl Read + ?Sized)) -> AvroResult<Schema> {
        let mut buf = String::new();
        match reader.read_to_string(&mut buf) {
            Ok(_) => Self::parse_str(&buf),
            Err(e) => Err(Details::ReadSchemaFromReader(e).into()),
        }
    }

    /// Parses an Avro schema from JSON.
    pub fn parse(value: &JsonValue) -> AvroResult<Schema> {
        let mut parser = Parser::default();
        parser.parse(value, &None)
    }

    /// Parses an Avro schema from JSON.
    /// Any `Schema::Ref`s must be known in the `names` map.
    pub(crate) fn parse_with_names(value: &JsonValue, names: Names) -> AvroResult<Schema> {
        let mut parser = Parser::new(HashMap::with_capacity(1), Vec::with_capacity(1), names);
        parser.parse(value, &None)
    }

    /// Returns the custom attributes (metadata) if the schema supports them.
    pub fn custom_attributes(&self) -> Option<&BTreeMap<String, JsonValue>> {
        match self {
            Schema::Record(RecordSchema { attributes, .. })
            | Schema::Enum(EnumSchema { attributes, .. })
            | Schema::Fixed(FixedSchema { attributes, .. })
            | Schema::Array(ArraySchema { attributes, .. })
            | Schema::Map(MapSchema { attributes, .. })
            | Schema::Decimal(DecimalSchema {
                inner: InnerDecimalSchema::Fixed(FixedSchema { attributes, .. }),
                ..
            })
            | Schema::Uuid(UuidSchema::Fixed(FixedSchema { attributes, .. })) => Some(attributes),
            Schema::Duration(FixedSchema { attributes, .. }) => Some(attributes),
            _ => None,
        }
    }

    /// Returns whether the schema represents a named type according to the avro specification
    pub fn is_named(&self) -> bool {
        matches!(
            self,
            Schema::Ref { .. }
                | Schema::Record(_)
                | Schema::Enum(_)
                | Schema::Fixed(_)
                | Schema::Decimal(DecimalSchema {
                    inner: InnerDecimalSchema::Fixed(_),
                    ..
                })
                | Schema::Uuid(UuidSchema::Fixed(_))
                | Schema::Duration(_)
        )
    }

    /// Returns the name of the schema if it has one.
    pub fn name(&self) -> Option<&Name> {
        match self {
            Schema::Ref { name, .. }
            | Schema::Record(RecordSchema { name, .. })
            | Schema::Enum(EnumSchema { name, .. })
            | Schema::Fixed(FixedSchema { name, .. })
            | Schema::Decimal(DecimalSchema {
                inner: InnerDecimalSchema::Fixed(FixedSchema { name, .. }),
                ..
            })
            | Schema::Uuid(UuidSchema::Fixed(FixedSchema { name, .. }))
            | Schema::Duration(FixedSchema { name, .. }) => Some(name),
            _ => None,
        }
    }

    /// Returns the namespace of the schema if it has one.
    pub fn namespace(&self) -> Namespace {
        self.name().and_then(|n| n.namespace.clone())
    }

    /// Returns the aliases of the schema if it has ones.
    pub fn aliases(&self) -> Option<&Vec<Alias>> {
        match self {
            Schema::Record(RecordSchema { aliases, .. })
            | Schema::Enum(EnumSchema { aliases, .. })
            | Schema::Fixed(FixedSchema { aliases, .. })
            | Schema::Decimal(DecimalSchema {
                inner: InnerDecimalSchema::Fixed(FixedSchema { aliases, .. }),
                ..
            })
            | Schema::Uuid(UuidSchema::Fixed(FixedSchema { aliases, .. })) => aliases.as_ref(),
            Schema::Duration(FixedSchema { aliases, .. }) => aliases.as_ref(),
            _ => None,
        }
    }

    /// Returns the doc of the schema if it has one.
    pub fn doc(&self) -> Option<&String> {
        match self {
            Schema::Record(RecordSchema { doc, .. })
            | Schema::Enum(EnumSchema { doc, .. })
            | Schema::Fixed(FixedSchema { doc, .. })
            | Schema::Decimal(DecimalSchema {
                inner: InnerDecimalSchema::Fixed(FixedSchema { doc, .. }),
                ..
            })
            | Schema::Uuid(UuidSchema::Fixed(FixedSchema { doc, .. })) => doc.as_ref(),
            Schema::Duration(FixedSchema { doc, .. }) => doc.as_ref(),
            _ => None,
        }
    }

    /// Remove all external references from the schema.
    ///
    /// `schemata` must contain all externally referenced schemas.
    ///
    /// # Errors
    /// Will return a [`Details::SchemaResolutionError`] if it fails to find
    /// a referenced schema. This will put the schema in a partly denormalized state.
    pub fn denormalize(&mut self, schemata: &[Schema]) -> AvroResult<()> {
        self.denormalize_inner(schemata, &mut HashSet::new())
    }

    fn denormalize_inner(
        &mut self,
        schemata: &[Schema],
        defined_names: &mut HashSet<Name>,
    ) -> AvroResult<()> {
        // If this name already exists in this schema we can reference it.
        // This makes the denormalized form as small as possible and prevent infinite loops for recursive types.
        if let Some(name) = self.name()
            && defined_names.contains(name)
        {
            *self = Schema::Ref { name: name.clone() };
            return Ok(());
        }
        match self {
            Schema::Ref { name } => {
                let replacement_schema = schemata
                    .iter()
                    .find(|s| s.name().map(|n| *n == *name).unwrap_or(false));
                if let Some(schema) = replacement_schema {
                    let mut denorm = schema.clone();
                    denorm.denormalize_inner(schemata, defined_names)?;
                    *self = denorm;
                } else {
                    return Err(Details::SchemaResolutionError(name.clone()).into());
                }
            }
            Schema::Record(record_schema) => {
                defined_names.insert(record_schema.name.clone());
                for field in &mut record_schema.fields {
                    field.schema.denormalize_inner(schemata, defined_names)?;
                }
            }
            Schema::Array(array_schema) => {
                array_schema
                    .items
                    .denormalize_inner(schemata, defined_names)?;
            }
            Schema::Map(map_schema) => {
                map_schema
                    .types
                    .denormalize_inner(schemata, defined_names)?;
            }
            Schema::Union(union_schema) => {
                for schema in &mut union_schema.schemas {
                    schema.denormalize_inner(schemata, defined_names)?;
                }
            }
            schema if schema.is_named() => {
                defined_names.insert(schema.name().expect("Schema is named").clone());
            }
            _ => (),
        }
        Ok(())
    }
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self {
            Schema::Ref { name } => serializer.serialize_str(&name.fullname(None)),
            Schema::Null => serializer.serialize_str("null"),
            Schema::Boolean => serializer.serialize_str("boolean"),
            Schema::Int => serializer.serialize_str("int"),
            Schema::Long => serializer.serialize_str("long"),
            Schema::Float => serializer.serialize_str("float"),
            Schema::Double => serializer.serialize_str("double"),
            Schema::Bytes => serializer.serialize_str("bytes"),
            Schema::String => serializer.serialize_str("string"),
            Schema::Array(ArraySchema {
                items,
                default,
                attributes,
            }) => {
                let mut map = serializer.serialize_map(Some(
                    2 + attributes.len() + if default.is_some() { 1 } else { 0 },
                ))?;
                map.serialize_entry("type", "array")?;
                map.serialize_entry("items", items)?;
                if let Some(default) = default {
                    let value = JsonValue::try_from(Value::Array(default.clone()))
                        .map_err(S::Error::custom)?;
                    map.serialize_entry("default", &value)?;
                }
                for (key, value) in attributes {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
            Schema::Map(MapSchema {
                types,
                default,
                attributes,
            }) => {
                let mut map = serializer.serialize_map(Some(
                    2 + attributes.len() + if default.is_some() { 1 } else { 0 },
                ))?;
                map.serialize_entry("type", "map")?;
                map.serialize_entry("values", types)?;
                if let Some(default) = default {
                    let value = JsonValue::try_from(Value::Map(default.clone()))
                        .map_err(S::Error::custom)?;
                    map.serialize_entry("default", &value)?;
                }
                for (key, value) in attributes {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
            Schema::Union(inner) => {
                let variants = inner.variants();
                let mut seq = serializer.serialize_seq(Some(variants.len()))?;
                for v in variants {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Schema::Record(RecordSchema {
                name,
                aliases,
                doc,
                fields,
                attributes,
                lookup: _lookup,
            }) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "record")?;
                if let Some(ref n) = name.namespace {
                    map.serialize_entry("namespace", n)?;
                }
                map.serialize_entry("name", &name.name)?;
                if let Some(docstr) = doc {
                    map.serialize_entry("doc", docstr)?;
                }
                if let Some(aliases) = aliases {
                    map.serialize_entry("aliases", aliases)?;
                }
                map.serialize_entry("fields", fields)?;
                for attr in attributes {
                    map.serialize_entry(attr.0, attr.1)?;
                }
                map.end()
            }
            Schema::Enum(EnumSchema {
                name,
                symbols,
                aliases,
                attributes,
                default,
                doc,
            }) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "enum")?;
                if let Some(ref n) = name.namespace {
                    map.serialize_entry("namespace", n)?;
                }
                map.serialize_entry("name", &name.name)?;
                map.serialize_entry("symbols", symbols)?;

                if let Some(aliases) = aliases {
                    map.serialize_entry("aliases", aliases)?;
                }
                if let Some(default) = default {
                    map.serialize_entry("default", default)?;
                }
                if let Some(doc) = doc {
                    map.serialize_entry("doc", doc)?;
                }
                for attr in attributes {
                    map.serialize_entry(attr.0, attr.1)?;
                }
                map.end()
            }
            Schema::Fixed(fixed_schema) => {
                let mut map = serializer.serialize_map(None)?;
                map = fixed_schema.serialize_to_map::<S>(map)?;
                map.end()
            }
            Schema::Decimal(DecimalSchema {
                scale,
                precision,
                inner,
            }) => {
                let mut map = serializer.serialize_map(None)?;
                match inner {
                    InnerDecimalSchema::Fixed(fixed_schema) => {
                        map = fixed_schema.serialize_to_map::<S>(map)?;
                    }
                    InnerDecimalSchema::Bytes => {
                        map.serialize_entry("type", "bytes")?;
                    }
                }
                map.serialize_entry("logicalType", "decimal")?;
                map.serialize_entry("scale", scale)?;
                map.serialize_entry("precision", precision)?;
                map.end()
            }

            Schema::BigDecimal => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "bytes")?;
                map.serialize_entry("logicalType", "big-decimal")?;
                map.end()
            }
            Schema::Uuid(inner) => {
                let mut map = serializer.serialize_map(None)?;
                match inner {
                    UuidSchema::Bytes => {
                        map.serialize_entry("type", "bytes")?;
                    }
                    UuidSchema::String => {
                        map.serialize_entry("type", "string")?;
                    }
                    UuidSchema::Fixed(fixed_schema) => {
                        map = fixed_schema.serialize_to_map::<S>(map)?;
                    }
                }
                map.serialize_entry("logicalType", "uuid")?;
                map.end()
            }
            Schema::Date => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "int")?;
                map.serialize_entry("logicalType", "date")?;
                map.end()
            }
            Schema::TimeMillis => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "int")?;
                map.serialize_entry("logicalType", "time-millis")?;
                map.end()
            }
            Schema::TimeMicros => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "time-micros")?;
                map.end()
            }
            Schema::TimestampMillis => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "timestamp-millis")?;
                map.end()
            }
            Schema::TimestampMicros => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "timestamp-micros")?;
                map.end()
            }
            Schema::TimestampNanos => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "timestamp-nanos")?;
                map.end()
            }
            Schema::LocalTimestampMillis => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "local-timestamp-millis")?;
                map.end()
            }
            Schema::LocalTimestampMicros => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "local-timestamp-micros")?;
                map.end()
            }
            Schema::LocalTimestampNanos => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", "long")?;
                map.serialize_entry("logicalType", "local-timestamp-nanos")?;
                map.end()
            }
            Schema::Duration(fixed) => {
                let map = serializer.serialize_map(None)?;

                let mut map = fixed.serialize_to_map::<S>(map)?;
                map.serialize_entry("logicalType", "duration")?;
                map.end()
            }
        }
    }
}

/// Parses a valid Avro schema into [the Parsing Canonical Form].
///
/// [the Parsing Canonical Form](https://avro.apache.org/docs/++version++/specification/#parsing-canonical-form-for-schemas)
fn parsing_canonical_form(schema: &JsonValue, defined_names: &mut HashSet<String>) -> String {
    match schema {
        JsonValue::Object(map) => pcf_map(map, defined_names),
        JsonValue::String(s) => pcf_string(s),
        JsonValue::Array(v) => pcf_array(v, defined_names),
        json => panic!("got invalid JSON value for canonical form of schema: {json}"),
    }
}

fn pcf_map(schema: &Map<String, JsonValue>, defined_names: &mut HashSet<String>) -> String {
    let typ = schema.get("type").and_then(|v| v.as_str());
    let name = if is_named_type(typ) {
        let ns = schema.get("namespace").and_then(|v| v.as_str());
        let raw_name = schema.get("name").and_then(|v| v.as_str());
        Some(format!(
            "{}{}",
            ns.map_or("".to_string(), |n| { format!("{n}.") }),
            raw_name.unwrap_or_default()
        ))
    } else {
        None
    };

    //if this is already a defined type, early return
    if let Some(ref n) = name {
        if defined_names.contains(n) {
            return pcf_string(n);
        } else {
            defined_names.insert(n.clone());
        }
    }

    let mut fields = Vec::new();
    for (k, v) in schema {
        // Reduce primitive types to their simple form. ([PRIMITIVE] rule)
        if schema.len() == 1 && k == "type" {
            // Invariant: function is only callable from a valid schema, so this is acceptable.
            if let JsonValue::String(s) = v {
                return pcf_string(s);
            }
        }

        // Strip out unused fields ([STRIP] rule)
        if field_ordering_position(k).is_none()
            || k == "default"
            || k == "doc"
            || k == "aliases"
            || k == "logicalType"
        {
            continue;
        }

        // Fully qualify the name, if it isn't already ([FULLNAMES] rule).
        if k == "name"
            && let Some(ref n) = name
        {
            fields.push(("name", format!("{}:{}", pcf_string(k), pcf_string(n))));
            continue;
        }

        // Strip off quotes surrounding "size" type, if they exist ([INTEGERS] rule).
        if k == "size" || k == "precision" || k == "scale" {
            let i = match v.as_str() {
                Some(s) => s.parse::<i64>().expect("Only valid schemas are accepted!"),
                None => v.as_i64().unwrap(),
            };
            fields.push((k, format!("{}:{}", pcf_string(k), i)));
            continue;
        }

        // For anything else, recursively process the result.
        fields.push((
            k,
            format!(
                "{}:{}",
                pcf_string(k),
                parsing_canonical_form(v, defined_names)
            ),
        ));
    }

    // Sort the fields by their canonical ordering ([ORDER] rule).
    fields.sort_unstable_by_key(|(k, _)| field_ordering_position(k).unwrap());
    let inter = fields
        .into_iter()
        .map(|(_, v)| v)
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{inter}}}")
}

fn is_named_type(typ: Option<&str>) -> bool {
    matches!(
        typ,
        Some("record") | Some("enum") | Some("fixed") | Some("ref")
    )
}

fn pcf_array(arr: &[JsonValue], defined_names: &mut HashSet<String>) -> String {
    let inter = arr
        .iter()
        .map(|a| parsing_canonical_form(a, defined_names))
        .collect::<Vec<String>>()
        .join(",");
    format!("[{inter}]")
}

fn pcf_string(s: &str) -> String {
    format!(r#""{s}""#)
}

const RESERVED_FIELDS: &[&str] = &[
    "name",
    "type",
    "fields",
    "symbols",
    "items",
    "values",
    "size",
    "logicalType",
    "order",
    "doc",
    "aliases",
    "default",
    "precision",
    "scale",
];

// Used to define the ordering and inclusion of fields.
fn field_ordering_position(field: &str) -> Option<usize> {
    RESERVED_FIELDS
        .iter()
        .position(|&f| f == field)
        .map(|pos| pos + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{error::Details, rabin::Rabin};
    use apache_avro_test_helper::{
        TestResult,
        logger::{assert_logged, assert_not_logged},
    };
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[test]
    fn test_invalid_schema() {
        assert!(Schema::parse_str("invalid").is_err());
    }

    #[test]
    fn test_primitive_schema() -> TestResult {
        assert_eq!(Schema::Null, Schema::parse_str(r#""null""#)?);
        assert_eq!(Schema::Int, Schema::parse_str(r#""int""#)?);
        assert_eq!(Schema::Double, Schema::parse_str(r#""double""#)?);
        Ok(())
    }

    #[test]
    fn test_array_schema() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "array", "items": "string"}"#)?;
        assert_eq!(Schema::array(Schema::String).build(), schema);
        Ok(())
    }

    #[test]
    fn test_map_schema() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "map", "values": "double"}"#)?;
        assert_eq!(Schema::map(Schema::Double).build(), schema);
        Ok(())
    }

    #[test]
    fn test_union_schema() -> TestResult {
        let schema = Schema::parse_str(r#"["null", "int"]"#)?;
        assert_eq!(
            Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::Int])?),
            schema
        );
        Ok(())
    }

    #[test]
    fn test_union_unsupported_schema() {
        let schema = Schema::parse_str(r#"["null", ["null", "int"], "string"]"#);
        assert!(schema.is_err());
    }

    #[test]
    fn test_multi_union_schema() -> TestResult {
        let schema = Schema::parse_str(r#"["null", "int", "float", "string", "bytes"]"#);
        assert!(schema.is_ok());
        let schema = schema?;
        assert_eq!(SchemaKind::from(&schema), SchemaKind::Union);
        let union_schema = match schema {
            Schema::Union(u) => u,
            _ => unreachable!(),
        };
        assert_eq!(union_schema.variants().len(), 5);
        let mut variants = union_schema.variants().iter();
        assert_eq!(SchemaKind::from(variants.next().unwrap()), SchemaKind::Null);
        assert_eq!(SchemaKind::from(variants.next().unwrap()), SchemaKind::Int);
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::Float
        );
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::String
        );
        assert_eq!(
            SchemaKind::from(variants.next().unwrap()),
            SchemaKind::Bytes
        );
        assert_eq!(variants.next(), None);

        Ok(())
    }

    // AVRO-3248
    #[test]
    fn test_union_of_records() -> TestResult {
        // A and B are the same except the name.
        let schema_str_a = r#"{
            "name": "A",
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        let schema_str_b = r#"{
            "name": "B",
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        // we get Details::GetNameField if we put ["A", "B"] directly here.
        let schema_str_c = r#"{
            "name": "C",
            "type": "record",
            "fields": [
                {"name": "field_one",  "type": ["A", "B"]}
            ]
        }"#;

        let schema_c = Schema::parse_list([schema_str_a, schema_str_b, schema_str_c])?
            .last()
            .unwrap()
            .clone();

        let schema_c_expected = Schema::Record(
            RecordSchema::builder()
                .try_name("C")?
                .fields(vec![
                    RecordField::builder()
                        .name("field_one".to_string())
                        .schema(Schema::Union(UnionSchema::new(vec![
                            Schema::Ref {
                                name: Name::new("A")?,
                            },
                            Schema::Ref {
                                name: Name::new("B")?,
                            },
                        ])?))
                        .build(),
                ])
                .build(),
        );

        assert_eq!(schema_c, schema_c_expected);
        Ok(())
    }

    #[test]
    fn avro_rs_104_test_root_union_of_records() -> TestResult {
        // A and B are the same except the name.
        let schema_str_a = r#"{
            "name": "A",
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        let schema_str_b = r#"{
            "name": "B",
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        let schema_str_c = r#"["A", "B"]"#;

        let (schema_c, schemata) =
            Schema::parse_str_with_list(schema_str_c, [schema_str_a, schema_str_b])?;

        let schema_a_expected = Schema::Record(RecordSchema {
            name: Name::new("A")?,
            aliases: None,
            doc: None,
            fields: vec![RecordField {
                name: "field_one".to_string(),
                doc: None,
                default: None,
                aliases: None,
                schema: Schema::Float,
                order: RecordFieldOrder::Ignore,
                position: 0,
                custom_attributes: Default::default(),
            }],
            lookup: BTreeMap::from_iter(vec![("field_one".to_string(), 0)]),
            attributes: Default::default(),
        });

        let schema_b_expected = Schema::Record(RecordSchema {
            name: Name::new("B")?,
            aliases: None,
            doc: None,
            fields: vec![RecordField {
                name: "field_one".to_string(),
                doc: None,
                default: None,
                aliases: None,
                schema: Schema::Float,
                order: RecordFieldOrder::Ignore,
                position: 0,
                custom_attributes: Default::default(),
            }],
            lookup: BTreeMap::from_iter(vec![("field_one".to_string(), 0)]),
            attributes: Default::default(),
        });

        let schema_c_expected = Schema::Union(UnionSchema::new(vec![
            Schema::Ref {
                name: Name::new("A")?,
            },
            Schema::Ref {
                name: Name::new("B")?,
            },
        ])?);

        assert_eq!(schema_c, schema_c_expected);
        assert_eq!(schemata[0], schema_a_expected);
        assert_eq!(schemata[1], schema_b_expected);

        Ok(())
    }

    #[test]
    fn avro_rs_104_test_root_union_of_records_name_collision() -> TestResult {
        // A and B are exactly the same.
        let schema_str_a1 = r#"{
            "name": "A",
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        let schema_str_a2 = r#"{
            "name": "A",
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        let schema_str_c = r#"["A", "A"]"#;

        match Schema::parse_str_with_list(schema_str_c, [schema_str_a1, schema_str_a2]) {
            Ok(_) => unreachable!("Expected an error that the name is already defined"),
            Err(e) => assert_eq!(
                e.to_string(),
                r#"Two schemas with the same fullname were given: "A""#
            ),
        }

        Ok(())
    }

    #[test]
    fn avro_rs_104_test_root_union_of_records_no_name() -> TestResult {
        let schema_str_a = r#"{
            "name": "A",
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        // B has no name field.
        let schema_str_b = r#"{
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        let schema_str_c = r#"["A", "A"]"#;

        match Schema::parse_str_with_list(schema_str_c, [schema_str_a, schema_str_b]) {
            Ok(_) => unreachable!("Expected an error that schema_str_b is missing a name field"),
            Err(e) => assert_eq!(e.to_string(), "No `name` field"),
        }

        Ok(())
    }

    #[test]
    fn avro_3584_test_recursion_records() -> TestResult {
        // A and B are the same except the name.
        let schema_str_a = r#"{
            "name": "A",
            "type": "record",
            "fields": [ {"name": "field_one", "type": "B"} ]
        }"#;

        let schema_str_b = r#"{
            "name": "B",
            "type": "record",
            "fields": [ {"name": "field_one", "type": "A"} ]
        }"#;

        let list = Schema::parse_list([schema_str_a, schema_str_b])?;

        let schema_a = list.first().unwrap().clone();

        match schema_a {
            Schema::Record(RecordSchema { fields, .. }) => {
                let f1 = fields.first();

                let ref_schema = Schema::Ref {
                    name: Name::new("B")?,
                };
                assert_eq!(ref_schema, f1.unwrap().schema);
            }
            _ => panic!("Expected a record schema!"),
        }

        Ok(())
    }

    #[test]
    fn test_avro_3248_nullable_record() -> TestResult {
        use std::iter::FromIterator;

        let schema_str_a = r#"{
            "name": "A",
            "type": "record",
            "fields": [
                {"name": "field_one", "type": "float"}
            ]
        }"#;

        // we get Details::GetNameField if we put ["null", "B"] directly here.
        let schema_str_option_a = r#"{
            "name": "OptionA",
            "type": "record",
            "fields": [
                {"name": "field_one",  "type": ["null", "A"], "default": null}
            ]
        }"#;

        let schema_option_a = Schema::parse_list([schema_str_a, schema_str_option_a])?
            .last()
            .unwrap()
            .clone();

        let schema_option_a_expected = Schema::Record(RecordSchema {
            name: Name::new("OptionA")?,
            aliases: None,
            doc: None,
            fields: vec![RecordField {
                name: "field_one".to_string(),
                doc: None,
                default: Some(JsonValue::Null),
                aliases: None,
                schema: Schema::Union(UnionSchema::new(vec![
                    Schema::Null,
                    Schema::Ref {
                        name: Name::new("A")?,
                    },
                ])?),
                order: RecordFieldOrder::Ignore,
                position: 0,
                custom_attributes: Default::default(),
            }],
            lookup: BTreeMap::from_iter(vec![("field_one".to_string(), 0)]),
            attributes: Default::default(),
        });

        assert_eq!(schema_option_a, schema_option_a_expected);

        Ok(())
    }

    #[test]
    fn test_record_schema() -> TestResult {
        let parsed = Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#,
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("a".to_owned(), 0);
        lookup.insert("b".to_owned(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name::new("test")?,
            aliases: None,
            doc: None,
            fields: vec![
                RecordField {
                    name: "a".to_string(),
                    doc: None,
                    default: Some(JsonValue::Number(42i64.into())),
                    aliases: None,
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "b".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::String,
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    custom_attributes: Default::default(),
                },
            ],
            lookup,
            attributes: Default::default(),
        });

        assert_eq!(parsed, expected);

        Ok(())
    }

    #[test]
    fn test_avro_3302_record_schema_with_currently_parsing_schema() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [{
                    "name": "recordField",
                    "type": {
                        "type": "record",
                        "name": "Node",
                        "fields": [
                            {"name": "label", "type": "string"},
                            {"name": "children", "type": {"type": "array", "items": "Node"}}
                        ]
                    }
                }]
            }
        "#,
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("recordField".to_owned(), 0);

        let mut node_lookup = BTreeMap::new();
        node_lookup.insert("children".to_owned(), 1);
        node_lookup.insert("label".to_owned(), 0);

        let expected = Schema::Record(RecordSchema {
            name: Name::new("test")?,
            aliases: None,
            doc: None,
            fields: vec![RecordField {
                name: "recordField".to_string(),
                doc: None,
                default: None,
                aliases: None,
                schema: Schema::Record(RecordSchema {
                    name: Name::new("Node")?,
                    aliases: None,
                    doc: None,
                    fields: vec![
                        RecordField {
                            name: "label".to_string(),
                            doc: None,
                            default: None,
                            aliases: None,
                            schema: Schema::String,
                            order: RecordFieldOrder::Ascending,
                            position: 0,
                            custom_attributes: Default::default(),
                        },
                        RecordField {
                            name: "children".to_string(),
                            doc: None,
                            default: None,
                            aliases: None,
                            schema: Schema::array(Schema::Ref {
                                name: Name::new("Node")?,
                            })
                            .build(),
                            order: RecordFieldOrder::Ascending,
                            position: 1,
                            custom_attributes: Default::default(),
                        },
                    ],
                    lookup: node_lookup,
                    attributes: Default::default(),
                }),
                order: RecordFieldOrder::Ascending,
                position: 0,
                custom_attributes: Default::default(),
            }],
            lookup,
            attributes: Default::default(),
        });
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"test","type":"record","fields":[{"name":"recordField","type":{"name":"Node","type":"record","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}"#;
        assert_eq!(canonical_form, &expected);

        Ok(())
    }

    // https://github.com/flavray/avro-rs/pull/99#issuecomment-1016948451
    #[test]
    fn test_parsing_of_recursive_type_enum() -> TestResult {
        let schema = r#"
    {
        "type": "record",
        "name": "User",
        "namespace": "office",
        "fields": [
            {
              "name": "details",
              "type": [
                {
                  "type": "record",
                  "name": "Employee",
                  "fields": [
                    {
                      "name": "gender",
                      "type": {
                        "type": "enum",
                        "name": "Gender",
                        "symbols": [
                          "male",
                          "female"
                        ]
                      },
                      "default": "female"
                    }
                  ]
                },
                {
                  "type": "record",
                  "name": "Manager",
                  "fields": [
                    {
                      "name": "gender",
                      "type": "Gender"
                    }
                  ]
                }
              ]
            }
          ]
        }
        "#;

        let schema = Schema::parse_str(schema)?;
        let schema_str = schema.canonical_form();
        let expected = r#"{"name":"office.User","type":"record","fields":[{"name":"details","type":[{"name":"office.Employee","type":"record","fields":[{"name":"gender","type":{"name":"office.Gender","type":"enum","symbols":["male","female"]}}]},{"name":"office.Manager","type":"record","fields":[{"name":"gender","type":"office.Gender"}]}]}]}"#;
        assert_eq!(schema_str, expected);

        Ok(())
    }

    #[test]
    fn test_parsing_of_recursive_type_fixed() -> TestResult {
        let schema = r#"
    {
        "type": "record",
        "name": "User",
        "namespace": "office",
        "fields": [
            {
              "name": "details",
              "type": [
                {
                  "type": "record",
                  "name": "Employee",
                  "fields": [
                    {
                      "name": "id",
                      "type": {
                        "type": "fixed",
                        "name": "EmployeeId",
                        "size": 16
                      },
                      "default": "female"
                    }
                  ]
                },
                {
                  "type": "record",
                  "name": "Manager",
                  "fields": [
                    {
                      "name": "id",
                      "type": "EmployeeId"
                    }
                  ]
                }
              ]
            }
          ]
        }
        "#;

        let schema = Schema::parse_str(schema)?;
        let schema_str = schema.canonical_form();
        let expected = r#"{"name":"office.User","type":"record","fields":[{"name":"details","type":[{"name":"office.Employee","type":"record","fields":[{"name":"id","type":{"name":"office.EmployeeId","type":"fixed","size":16}}]},{"name":"office.Manager","type":"record","fields":[{"name":"id","type":"office.EmployeeId"}]}]}]}"#;
        assert_eq!(schema_str, expected);

        Ok(())
    }

    #[test]
    fn test_avro_3302_record_schema_with_currently_parsing_schema_aliases() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "LongList",
              "aliases": ["LinkedLongs"],
              "fields" : [
                {"name": "value", "type": "long"},
                {"name": "next", "type": ["null", "LinkedLongs"]}
              ]
            }
        "#,
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("value".to_owned(), 0);
        lookup.insert("next".to_owned(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name {
                name: "LongList".to_owned(),
                namespace: None,
            },
            aliases: Some(vec![Alias::new("LinkedLongs").unwrap()]),
            doc: None,
            fields: vec![
                RecordField {
                    name: "value".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "next".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Union(UnionSchema::new(vec![
                        Schema::Null,
                        Schema::Ref {
                            name: Name {
                                name: "LongList".to_owned(),
                                namespace: None,
                            },
                        },
                    ])?),
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    custom_attributes: Default::default(),
                },
            ],
            lookup,
            attributes: Default::default(),
        });
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"LongList","type":"record","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","LongList"]}]}"#;
        assert_eq!(canonical_form, &expected);

        Ok(())
    }

    #[test]
    fn test_avro_3370_record_schema_with_currently_parsing_schema_named_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type" : "record",
              "name" : "record",
              "fields" : [
                 { "name" : "value", "type" : "long" },
                 { "name" : "next", "type" : "record" }
             ]
            }
        "#,
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("value".to_owned(), 0);
        lookup.insert("next".to_owned(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name {
                name: "record".to_owned(),
                namespace: None,
            },
            aliases: None,
            doc: None,
            fields: vec![
                RecordField {
                    name: "value".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "next".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Ref {
                        name: Name {
                            name: "record".to_owned(),
                            namespace: None,
                        },
                    },
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    custom_attributes: Default::default(),
                },
            ],
            lookup,
            attributes: Default::default(),
        });
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"record","type":"record","fields":[{"name":"value","type":"long"},{"name":"next","type":"record"}]}"#;
        assert_eq!(canonical_form, &expected);

        Ok(())
    }

    #[test]
    fn test_avro_3370_record_schema_with_currently_parsing_schema_named_enum() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type" : "record",
              "name" : "record",
              "fields" : [
                 {
                    "name" : "enum",
                    "type": {
                        "name" : "enum",
                        "type" : "enum",
                        "symbols": ["one", "two", "three"]
                    }
                 },
                 { "name" : "next", "type" : "enum" }
             ]
            }
        "#,
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("enum".to_owned(), 0);
        lookup.insert("next".to_owned(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name {
                name: "record".to_owned(),
                namespace: None,
            },
            aliases: None,
            doc: None,
            fields: vec![
                RecordField {
                    name: "enum".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Enum(
                        EnumSchema::builder()
                            .name(Name::new("enum")?)
                            .symbols(vec![
                                "one".to_string(),
                                "two".to_string(),
                                "three".to_string(),
                            ])
                            .build(),
                    ),
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "next".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Ref {
                        name: Name::new("enum")?,
                    },
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    custom_attributes: Default::default(),
                },
            ],
            lookup,
            attributes: Default::default(),
        });
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"record","type":"record","fields":[{"name":"enum","type":{"name":"enum","type":"enum","symbols":["one","two","three"]}},{"name":"next","type":"enum"}]}"#;
        assert_eq!(canonical_form, &expected);

        Ok(())
    }

    #[test]
    fn test_avro_3370_record_schema_with_currently_parsing_schema_named_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type" : "record",
              "name" : "record",
              "fields" : [
                 {
                    "name": "fixed",
                    "type": {
                        "type" : "fixed",
                        "name" : "fixed",
                        "size": 456
                    }
                 },
                 { "name" : "next", "type" : "fixed" }
             ]
            }
        "#,
        )?;

        let mut lookup = BTreeMap::new();
        lookup.insert("fixed".to_owned(), 0);
        lookup.insert("next".to_owned(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name {
                name: "record".to_owned(),
                namespace: None,
            },
            aliases: None,
            doc: None,
            fields: vec![
                RecordField {
                    name: "fixed".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Fixed(FixedSchema {
                        name: Name {
                            name: "fixed".to_owned(),
                            namespace: None,
                        },
                        aliases: None,
                        doc: None,
                        size: 456,
                        attributes: Default::default(),
                    }),
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: Default::default(),
                },
                RecordField {
                    name: "next".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Ref {
                        name: Name::new("fixed")?,
                    },
                    order: RecordFieldOrder::Ascending,
                    position: 1,
                    custom_attributes: Default::default(),
                },
            ],
            lookup,
            attributes: Default::default(),
        });
        assert_eq!(schema, expected);

        let canonical_form = &schema.canonical_form();
        let expected = r#"{"name":"record","type":"record","fields":[{"name":"fixed","type":{"name":"fixed","type":"fixed","size":456}},{"name":"next","type":"fixed"}]}"#;
        assert_eq!(canonical_form, &expected);

        Ok(())
    }

    #[test]
    fn test_enum_schema() -> TestResult {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Suit", "symbols": ["diamonds", "spades", "clubs", "hearts"]}"#,
        )?;

        let expected = Schema::Enum(EnumSchema {
            name: Name::new("Suit")?,
            aliases: None,
            doc: None,
            symbols: vec![
                "diamonds".to_owned(),
                "spades".to_owned(),
                "clubs".to_owned(),
                "hearts".to_owned(),
            ],
            default: None,
            attributes: Default::default(),
        });

        assert_eq!(expected, schema);

        Ok(())
    }

    #[test]
    fn test_enum_schema_duplicate() -> TestResult {
        // Duplicate "diamonds"
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Suit", "symbols": ["diamonds", "spades", "clubs", "diamonds"]}"#,
        );
        assert!(schema.is_err());

        Ok(())
    }

    #[test]
    fn test_enum_schema_name() -> TestResult {
        // Invalid name "0000" does not match [A-Za-z_][A-Za-z0-9_]*
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Enum", "symbols": ["0000", "variant"]}"#,
        );
        assert!(schema.is_err());

        Ok(())
    }

    #[test]
    fn test_fixed_schema() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "fixed", "name": "test", "size": 16}"#)?;

        let expected = Schema::Fixed(FixedSchema {
            name: Name::new("test")?,
            aliases: None,
            doc: None,
            size: 16_usize,
            attributes: Default::default(),
        });

        assert_eq!(expected, schema);

        Ok(())
    }

    #[test]
    fn test_fixed_schema_with_documentation() -> TestResult {
        let schema = Schema::parse_str(
            r#"{"type": "fixed", "name": "test", "size": 16, "doc": "FixedSchema documentation"}"#,
        )?;

        let expected = Schema::Fixed(FixedSchema {
            name: Name::new("test")?,
            aliases: None,
            doc: Some(String::from("FixedSchema documentation")),
            size: 16_usize,
            attributes: Default::default(),
        });

        assert_eq!(expected, schema);

        Ok(())
    }

    #[test]
    fn test_no_documentation() -> TestResult {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Coin", "symbols": ["heads", "tails"]}"#,
        )?;

        let doc = match schema {
            Schema::Enum(EnumSchema { doc, .. }) => doc,
            _ => unreachable!(),
        };

        assert!(doc.is_none());

        Ok(())
    }

    #[test]
    fn test_documentation() -> TestResult {
        let schema = Schema::parse_str(
            r#"{"type": "enum", "name": "Coin", "doc": "Some documentation", "symbols": ["heads", "tails"]}"#,
        )?;

        let doc = match schema {
            Schema::Enum(EnumSchema { doc, .. }) => doc,
            _ => None,
        };

        assert_eq!("Some documentation".to_owned(), doc.unwrap());

        Ok(())
    }

    // Tests to ensure Schema is Send + Sync. These tests don't need to _do_ anything, if they can
    // compile, they pass.
    #[test]
    fn test_schema_is_send() {
        fn send<S: Send>(_s: S) {}

        let schema = Schema::Null;
        send(schema);
    }

    #[test]
    fn test_schema_is_sync() {
        fn sync<S: Sync>(_s: S) {}

        let schema = Schema::Null;
        sync(&schema);
        sync(schema);
    }

    #[test]
    fn test_schema_fingerprint() -> TestResult {
        use crate::rabin::Rabin;
        use md5::Md5;
        use sha2::Sha256;

        let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"},
            {"name": "c", "type": {"type": "long", "logicalType": "timestamp-micros"}}
        ]
    }
"#;

        let schema = Schema::parse_str(raw_schema)?;
        assert_eq!(
            "7eb3b28d73dfc99bdd9af1848298b40804a2f8ad5d2642be2ecc2ad34842b987",
            format!("{}", schema.fingerprint::<Sha256>())
        );

        assert_eq!(
            "cb11615e412ee5d872620d8df78ff6ae",
            format!("{}", schema.fingerprint::<Md5>())
        );
        assert_eq!(
            "92f2ccef718c6754",
            format!("{}", schema.fingerprint::<Rabin>())
        );

        Ok(())
    }

    #[test]
    fn test_logical_types() -> TestResult {
        let schema = Schema::parse_str(r#"{"type": "int", "logicalType": "date"}"#)?;
        assert_eq!(schema, Schema::Date);

        let schema = Schema::parse_str(r#"{"type": "long", "logicalType": "timestamp-micros"}"#)?;
        assert_eq!(schema, Schema::TimestampMicros);

        Ok(())
    }

    #[test]
    fn test_nullable_logical_type() -> TestResult {
        let schema = Schema::parse_str(
            r#"{"type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]}"#,
        )?;
        assert_eq!(
            schema,
            Schema::Union(UnionSchema::new(vec![
                Schema::Null,
                Schema::TimestampMicros,
            ])?)
        );

        Ok(())
    }

    #[test]
    fn record_field_order_from_str() -> TestResult {
        use std::str::FromStr;

        assert_eq!(
            RecordFieldOrder::from_str("ascending").unwrap(),
            RecordFieldOrder::Ascending
        );
        assert_eq!(
            RecordFieldOrder::from_str("descending").unwrap(),
            RecordFieldOrder::Descending
        );
        assert_eq!(
            RecordFieldOrder::from_str("ignore").unwrap(),
            RecordFieldOrder::Ignore
        );
        assert!(RecordFieldOrder::from_str("not an ordering").is_err());

        Ok(())
    }

    #[test]
    fn test_avro_3374_preserve_namespace_for_primitive() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type" : "record",
              "name" : "ns.int",
              "fields" : [
                {"name" : "value", "type" : "int"},
                {"name" : "next", "type" : [ "null", "ns.int" ]}
              ]
            }
            "#,
        )?;

        let json = schema.canonical_form();
        assert_eq!(
            json,
            r#"{"name":"ns.int","type":"record","fields":[{"name":"value","type":"int"},{"name":"next","type":["null","ns.int"]}]}"#
        );

        Ok(())
    }

    #[test]
    fn test_avro_3433_preserve_schema_refs_in_json() -> TestResult {
        let schema = r#"
    {
      "name": "test.test",
      "type": "record",
      "fields": [
        {
          "name": "bar",
          "type": { "name": "test.foo", "type": "record", "fields": [{ "name": "id", "type": "long" }] }
        },
        { "name": "baz", "type": "test.foo" }
      ]
    }
    "#;

        let schema = Schema::parse_str(schema)?;

        let expected = r#"{"name":"test.test","type":"record","fields":[{"name":"bar","type":{"name":"test.foo","type":"record","fields":[{"name":"id","type":"long"}]}},{"name":"baz","type":"test.foo"}]}"#;
        assert_eq!(schema.canonical_form(), expected);

        Ok(())
    }

    #[test]
    fn test_read_namespace_from_name() -> TestResult {
        let schema = r#"
    {
      "name": "space.name",
      "type": "record",
      "fields": [
        {
          "name": "num",
          "type": "int"
        }
      ]
    }
    "#;

        let schema = Schema::parse_str(schema)?;
        if let Schema::Record(RecordSchema { name, .. }) = schema {
            assert_eq!(name.name, "name");
            assert_eq!(name.namespace, Some("space".to_string()));
        } else {
            panic!("Expected a record schema!");
        }

        Ok(())
    }

    #[test]
    fn test_namespace_from_name_has_priority_over_from_field() -> TestResult {
        let schema = r#"
    {
      "name": "space1.name",
      "namespace": "space2",
      "type": "record",
      "fields": [
        {
          "name": "num",
          "type": "int"
        }
      ]
    }
    "#;

        let schema = Schema::parse_str(schema)?;
        if let Schema::Record(RecordSchema { name, .. }) = schema {
            assert_eq!(name.namespace, Some("space1".to_string()));
        } else {
            panic!("Expected a record schema!");
        }

        Ok(())
    }

    #[test]
    fn test_namespace_from_field() -> TestResult {
        let schema = r#"
    {
      "name": "name",
      "namespace": "space2",
      "type": "record",
      "fields": [
        {
          "name": "num",
          "type": "int"
        }
      ]
    }
    "#;

        let schema = Schema::parse_str(schema)?;
        if let Schema::Record(RecordSchema { name, .. }) = schema {
            assert_eq!(name.namespace, Some("space2".to_string()));
        } else {
            panic!("Expected a record schema!");
        }

        Ok(())
    }

    fn assert_avro_3512_aliases(aliases: &Aliases) {
        match aliases {
            Some(aliases) => {
                assert_eq!(aliases.len(), 3);
                assert_eq!(aliases[0], Alias::new("space.b").unwrap());
                assert_eq!(aliases[1], Alias::new("x.y").unwrap());
                assert_eq!(aliases[2], Alias::new(".c").unwrap());
            }
            None => {
                panic!("'aliases' must be Some");
            }
        }
    }

    #[test]
    fn avro_3512_alias_with_null_namespace_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "fields" : [
                {"name": "time", "type": "long"}
              ]
            }
        "#,
        )?;

        if let Schema::Record(RecordSchema { ref aliases, .. }) = schema {
            assert_avro_3512_aliases(aliases);
        } else {
            panic!("The Schema should be a record: {schema:?}");
        }

        Ok(())
    }

    #[test]
    fn avro_3512_alias_with_null_namespace_enum() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "enum",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "symbols" : [
                "symbol1", "symbol2"
              ]
            }
        "#,
        )?;

        if let Schema::Enum(EnumSchema { ref aliases, .. }) = schema {
            assert_avro_3512_aliases(aliases);
        } else {
            panic!("The Schema should be an enum: {schema:?}");
        }

        Ok(())
    }

    #[test]
    fn avro_3512_alias_with_null_namespace_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "fixed",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "size" : 12
            }
        "#,
        )?;

        if let Schema::Fixed(FixedSchema { ref aliases, .. }) = schema {
            assert_avro_3512_aliases(aliases);
        } else {
            panic!("The Schema should be a fixed: {schema:?}");
        }

        Ok(())
    }

    #[test]
    fn avro_3518_serialize_aliases_record() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "fields" : [
                {
                    "name": "time",
                    "type": "long",
                    "doc": "The documentation is not serialized",
                    "default": 123,
                    "aliases": ["time1", "ns.time2"]
                }
              ]
            }
        "#,
        )?;

        let value = serde_json::to_value(&schema)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"aliases":["space.b","x.y","c"],"fields":[{"aliases":["time1","ns.time2"],"default":123,"name":"time","type":"long"}],"name":"a","namespace":"space","type":"record"}"#,
            &serialized
        );
        assert_eq!(schema, Schema::parse_str(&serialized)?);

        Ok(())
    }

    #[test]
    fn avro_3518_serialize_aliases_enum() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "enum",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "symbols" : [
                "symbol1", "symbol2"
              ]
            }
        "#,
        )?;

        let value = serde_json::to_value(&schema)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"aliases":["space.b","x.y","c"],"name":"a","namespace":"space","symbols":["symbol1","symbol2"],"type":"enum"}"#,
            &serialized
        );
        assert_eq!(schema, Schema::parse_str(&serialized)?);

        Ok(())
    }

    #[test]
    fn avro_3518_serialize_aliases_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"
            {
              "type": "fixed",
              "name": "a",
              "namespace": "space",
              "aliases": ["b", "x.y", ".c"],
              "size" : 12
            }
        "#,
        )?;

        let value = serde_json::to_value(&schema)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"aliases":["space.b","x.y","c"],"name":"a","namespace":"space","size":12,"type":"fixed"}"#,
            &serialized
        );
        assert_eq!(schema, Schema::parse_str(&serialized)?);

        Ok(())
    }

    #[test]
    fn avro_3130_parse_anonymous_union_type() -> TestResult {
        let schema_str = r#"
        {
            "type": "record",
            "name": "AccountEvent",
            "fields": [
                {"type":
                  ["null",
                   { "name": "accountList",
                      "type": {
                        "type": "array",
                        "items": "long"
                      }
                  }
                  ],
                 "name":"NullableLongArray"
               }
            ]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;

        if let Schema::Record(RecordSchema { name, fields, .. }) = schema {
            assert_eq!(name, Name::new("AccountEvent")?);

            let field = &fields[0];
            assert_eq!(&field.name, "NullableLongArray");

            if let Schema::Union(ref union) = field.schema {
                assert_eq!(union.schemas[0], Schema::Null);

                if let Schema::Array(ref array_schema) = union.schemas[1] {
                    if let Schema::Long = *array_schema.items {
                        // OK
                    } else {
                        panic!("Expected a Schema::Array of type Long");
                    }
                } else {
                    panic!("Expected Schema::Array");
                }
            } else {
                panic!("Expected Schema::Union");
            }
        } else {
            panic!("Expected Schema::Record");
        }

        Ok(())
    }

    #[test]
    fn avro_custom_attributes_schema_without_attributes() -> TestResult {
        let schemata_str = [
            r#"
            {
                "type": "record",
                "name": "Rec",
                "doc": "A Record schema without custom attributes",
                "fields": []
            }
            "#,
            r#"
            {
                "type": "enum",
                "name": "Enum",
                "doc": "An Enum schema without custom attributes",
                "symbols": []
            }
            "#,
            r#"
            {
                "type": "fixed",
                "name": "Fixed",
                "doc": "A Fixed schema without custom attributes",
                "size": 0
            }
            "#,
        ];
        for schema_str in schemata_str.iter() {
            let schema = Schema::parse_str(schema_str)?;
            assert_eq!(schema.custom_attributes(), Some(&Default::default()));
        }

        Ok(())
    }

    const CUSTOM_ATTRS_SUFFIX: &str = r#"
            "string_key": "value",
            "number_key": 1.23,
            "null_key": null,
            "array_key": [1, 2, 3],
            "object_key": {
                "key": "value"
            }
        "#;

    #[test]
    fn avro_3609_custom_attributes_schema_with_attributes() -> TestResult {
        let schemata_str = [
            r#"
            {
                "type": "record",
                "name": "Rec",
                "namespace": "ns",
                "doc": "A Record schema with custom attributes",
                "fields": [],
                {{{}}}
            }
            "#,
            r#"
            {
                "type": "enum",
                "name": "Enum",
                "namespace": "ns",
                "doc": "An Enum schema with custom attributes",
                "symbols": [],
                {{{}}}
            }
            "#,
            r#"
            {
                "type": "fixed",
                "name": "Fixed",
                "namespace": "ns",
                "doc": "A Fixed schema with custom attributes",
                "size": 2,
                {{{}}}
            }
            "#,
        ];

        for schema_str in schemata_str.iter() {
            let schema = Schema::parse_str(
                schema_str
                    .to_owned()
                    .replace("{{{}}}", CUSTOM_ATTRS_SUFFIX)
                    .as_str(),
            )?;

            assert_eq!(
                schema.custom_attributes(),
                Some(&expected_custom_attributes())
            );
        }

        Ok(())
    }

    fn expected_custom_attributes() -> BTreeMap<String, JsonValue> {
        let mut expected_attributes: BTreeMap<String, JsonValue> = Default::default();
        expected_attributes.insert(
            "string_key".to_string(),
            JsonValue::String("value".to_string()),
        );
        expected_attributes.insert("number_key".to_string(), json!(1.23));
        expected_attributes.insert("null_key".to_string(), JsonValue::Null);
        expected_attributes.insert(
            "array_key".to_string(),
            JsonValue::Array(vec![json!(1), json!(2), json!(3)]),
        );
        let mut object_value: HashMap<String, JsonValue> = HashMap::new();
        object_value.insert("key".to_string(), JsonValue::String("value".to_string()));
        expected_attributes.insert("object_key".to_string(), json!(object_value));
        expected_attributes
    }

    #[test]
    fn avro_3609_custom_attributes_record_field_without_attributes() -> TestResult {
        let schema_str = String::from(
            r#"
            {
                "type": "record",
                "name": "Rec",
                "doc": "A Record schema without custom attributes",
                "fields": [
                    {
                        "name": "field_one",
                        "type": "float",
                        {{{}}}
                    }
                ]
            }
        "#,
        );

        let schema = Schema::parse_str(schema_str.replace("{{{}}}", CUSTOM_ATTRS_SUFFIX).as_str())?;

        match schema {
            Schema::Record(RecordSchema { name, fields, .. }) => {
                assert_eq!(name, Name::new("Rec")?);
                assert_eq!(fields.len(), 1);
                let field = &fields[0];
                assert_eq!(&field.name, "field_one");
                assert_eq!(field.custom_attributes, expected_custom_attributes());
            }
            _ => panic!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3625_null_is_first() -> TestResult {
        let schema_str = String::from(
            r#"
            {
                "type": "record",
                "name": "union_schema_test",
                "fields": [
                    {"name": "a", "type": ["null", "long"], "default": null}
                ]
            }
        "#,
        );

        let schema = Schema::parse_str(&schema_str)?;

        match schema {
            Schema::Record(RecordSchema { name, fields, .. }) => {
                assert_eq!(name, Name::new("union_schema_test")?);
                assert_eq!(fields.len(), 1);
                let field = &fields[0];
                assert_eq!(&field.name, "a");
                assert_eq!(&field.default, &Some(JsonValue::Null));
                match &field.schema {
                    Schema::Union(union) => {
                        assert_eq!(union.variants().len(), 2);
                        assert!(union.is_nullable());
                        assert_eq!(union.variants()[0], Schema::Null);
                        assert_eq!(union.variants()[1], Schema::Long);
                    }
                    _ => panic!("Expected Schema::Union"),
                }
            }
            _ => panic!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3625_null_is_last() -> TestResult {
        let schema_str = String::from(
            r#"
            {
                "type": "record",
                "name": "union_schema_test",
                "fields": [
                    {"name": "a", "type": ["long","null"], "default": 123}
                ]
            }
        "#,
        );

        let schema = Schema::parse_str(&schema_str)?;

        match schema {
            Schema::Record(RecordSchema { name, fields, .. }) => {
                assert_eq!(name, Name::new("union_schema_test")?);
                assert_eq!(fields.len(), 1);
                let field = &fields[0];
                assert_eq!(&field.name, "a");
                assert_eq!(&field.default, &Some(json!(123)));
                match &field.schema {
                    Schema::Union(union) => {
                        assert_eq!(union.variants().len(), 2);
                        assert_eq!(union.variants()[0], Schema::Long);
                        assert_eq!(union.variants()[1], Schema::Null);
                    }
                    _ => panic!("Expected Schema::Union"),
                }
            }
            _ => panic!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3625_null_is_the_middle() -> TestResult {
        let schema_str = String::from(
            r#"
            {
                "type": "record",
                "name": "union_schema_test",
                "fields": [
                    {"name": "a", "type": ["long","null","int"], "default": 123}
                ]
            }
        "#,
        );

        let schema = Schema::parse_str(&schema_str)?;

        match schema {
            Schema::Record(RecordSchema { name, fields, .. }) => {
                assert_eq!(name, Name::new("union_schema_test")?);
                assert_eq!(fields.len(), 1);
                let field = &fields[0];
                assert_eq!(&field.name, "a");
                assert_eq!(&field.default, &Some(json!(123)));
                match &field.schema {
                    Schema::Union(union) => {
                        assert_eq!(union.variants().len(), 3);
                        assert_eq!(union.variants()[0], Schema::Long);
                        assert_eq!(union.variants()[1], Schema::Null);
                        assert_eq!(union.variants()[2], Schema::Int);
                    }
                    _ => panic!("Expected Schema::Union"),
                }
            }
            _ => panic!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3649_default_notintfirst() -> TestResult {
        let schema_str = String::from(
            r#"
            {
                "type": "record",
                "name": "union_schema_test",
                "fields": [
                    {"name": "a", "type": ["string", "int"], "default": 123}
                ]
            }
        "#,
        );

        let schema = Schema::parse_str(&schema_str)?;

        match schema {
            Schema::Record(RecordSchema { name, fields, .. }) => {
                assert_eq!(name, Name::new("union_schema_test")?);
                assert_eq!(fields.len(), 1);
                let field = &fields[0];
                assert_eq!(&field.name, "a");
                assert_eq!(&field.default, &Some(json!(123)));
                match &field.schema {
                    Schema::Union(union) => {
                        assert_eq!(union.variants().len(), 2);
                        assert_eq!(union.variants()[0], Schema::String);
                        assert_eq!(union.variants()[1], Schema::Int);
                    }
                    _ => panic!("Expected Schema::Union"),
                }
            }
            _ => panic!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3709_parsing_of_record_field_aliases() -> TestResult {
        let schema = r#"
        {
          "name": "rec",
          "type": "record",
          "fields": [
            {
              "name": "num",
              "type": "int",
              "aliases": ["num1", "num2"]
            }
          ]
        }
        "#;

        let schema = Schema::parse_str(schema)?;
        if let Schema::Record(RecordSchema { fields, .. }) = schema {
            let num_field = &fields[0];
            assert_eq!(num_field.name, "num");
            assert_eq!(num_field.aliases, Some(vec!("num1".into(), "num2".into())));
        } else {
            panic!("Expected a record schema!");
        }

        Ok(())
    }

    #[test]
    fn avro_3735_parse_enum_namespace() -> TestResult {
        let schema = r#"
        {
            "type": "record",
            "name": "Foo",
            "namespace": "name.space",
            "fields":
            [
                {
                    "name": "barInit",
                    "type":
                    {
                        "type": "enum",
                        "name": "Bar",
                        "symbols":
                        [
                            "bar0",
                            "bar1"
                        ]
                    }
                },
                {
                    "name": "barUse",
                    "type": "Bar"
                }
            ]
        }
        "#;

        #[derive(
            Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, serde::Deserialize, serde::Serialize,
        )]
        pub enum Bar {
            #[serde(rename = "bar0")]
            Bar0,
            #[serde(rename = "bar1")]
            Bar1,
        }

        #[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
        pub struct Foo {
            #[serde(rename = "barInit")]
            pub bar_init: Bar,
            #[serde(rename = "barUse")]
            pub bar_use: Bar,
        }

        let schema = Schema::parse_str(schema)?;

        let foo = Foo {
            bar_init: Bar::Bar0,
            bar_use: Bar::Bar1,
        };

        let avro_value = crate::to_value(foo)?;
        assert!(avro_value.validate(&schema));

        let mut writer = crate::Writer::new(&schema, Vec::new())?;

        // schema validation happens here
        writer.append_value(avro_value)?;

        Ok(())
    }

    #[test]
    fn avro_3755_deserialize() -> TestResult {
        #[derive(
            Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, serde::Deserialize, serde::Serialize,
        )]
        pub enum Bar {
            #[serde(rename = "bar0")]
            Bar0,
            #[serde(rename = "bar1")]
            Bar1,
            #[serde(rename = "bar2")]
            Bar2,
        }

        #[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
        pub struct Foo {
            #[serde(rename = "barInit")]
            pub bar_init: Bar,
            #[serde(rename = "barUse")]
            pub bar_use: Bar,
        }

        let writer_schema = r#"{
            "type": "record",
            "name": "Foo",
            "fields":
            [
                {
                    "name": "barInit",
                    "type":
                    {
                        "type": "enum",
                        "name": "Bar",
                        "symbols":
                        [
                            "bar0",
                            "bar1"
                        ]
                    }
                },
                {
                    "name": "barUse",
                    "type": "Bar"
                }
            ]
            }"#;

        let reader_schema = r#"{
            "type": "record",
            "name": "Foo",
            "namespace": "name.space",
            "fields":
            [
                {
                    "name": "barInit",
                    "type":
                    {
                        "type": "enum",
                        "name": "Bar",
                        "symbols":
                        [
                            "bar0",
                            "bar1",
                            "bar2"
                        ]
                    }
                },
                {
                    "name": "barUse",
                    "type": "Bar"
                }
            ]
            }"#;

        let writer_schema = Schema::parse_str(writer_schema)?;
        let foo = Foo {
            bar_init: Bar::Bar0,
            bar_use: Bar::Bar1,
        };
        let avro_value = crate::to_value(foo)?;
        assert!(
            avro_value.validate(&writer_schema),
            "value is valid for schema",
        );
        let datum = crate::to_avro_datum(&writer_schema, avro_value)?;
        let mut x = &datum[..];
        let reader_schema = Schema::parse_str(reader_schema)?;
        let deser_value = crate::from_avro_datum(&writer_schema, &mut x, Some(&reader_schema))?;
        match deser_value {
            types::Value::Record(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "barInit");
                assert_eq!(fields[0].1, types::Value::Enum(0, "bar0".to_string()));
                assert_eq!(fields[1].0, "barUse");
                assert_eq!(fields[1].1, types::Value::Enum(1, "bar1".to_string()));
            }
            _ => panic!("Expected Value::Record"),
        }

        Ok(())
    }

    #[test]
    fn test_avro_3780_decimal_schema_type_with_fixed() -> TestResult {
        let schema = json!(
        {
          "type": "record",
          "name": "recordWithDecimal",
          "fields": [
            {
                "name": "decimal",
                "type": {
                    "type": "fixed",
                    "name": "nestedFixed",
                    "size": 8,
                    "logicalType": "decimal",
                    "precision": 4
                }
            }
          ]
        });

        let parse_result = Schema::parse(&schema);
        assert!(
            parse_result.is_ok(),
            "parse result must be ok, got: {parse_result:?}"
        );

        Ok(())
    }

    #[test]
    fn test_avro_3772_enum_default_wrong_type() -> TestResult {
        let schema = r#"
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
                "default": 123
              }
            }
          ]
        }
        "#;

        match Schema::parse_str(schema) {
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    "Default value for an enum must be a string! Got: 123"
                );
            }
            _ => panic!("Expected an error"),
        }
        Ok(())
    }

    #[test]
    fn test_avro_3812_handle_null_namespace_properly() -> TestResult {
        let schema_str = r#"
        {
          "namespace": "",
          "type": "record",
          "name": "my_schema",
          "fields": [
            {
              "name": "a",
              "type": {
                "type": "enum",
                "name": "my_enum",
                "namespace": "",
                "symbols": ["a", "b"]
              }
            },  {
              "name": "b",
              "type": {
                "type": "fixed",
                "name": "my_fixed",
                "namespace": "",
                "size": 10
              }
            }
          ]
         }
         "#;

        let expected = r#"{"name":"my_schema","type":"record","fields":[{"name":"a","type":{"name":"my_enum","type":"enum","symbols":["a","b"]}},{"name":"b","type":{"name":"my_fixed","type":"fixed","size":10}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        let name = Name::new("my_name")?;
        let fullname = name.fullname(Some("".to_string()));
        assert_eq!(fullname, "my_name");
        let qname = name.fully_qualified_name(&Some("".to_string())).to_string();
        assert_eq!(qname, "my_name");

        Ok(())
    }

    #[test]
    fn test_avro_3818_inherit_enclosing_namespace() -> TestResult {
        // Enclosing namespace is specified but inner namespaces are not.
        let schema_str = r#"
        {
          "namespace": "my_ns",
          "type": "record",
          "name": "my_schema",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "enum1",
                "type": "enum",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": "fixed1",
                "type": "fixed",
                "size": 1
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"my_ns.my_schema","type":"record","fields":[{"name":"f1","type":{"name":"my_ns.enum1","type":"enum","symbols":["a"]}},{"name":"f2","type":{"name":"my_ns.fixed1","type":"fixed","size":1}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Enclosing namespace and inner namespaces are specified
        // but inner namespaces are ""
        let schema_str = r#"
        {
          "namespace": "my_ns",
          "type": "record",
          "name": "my_schema",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "enum1",
                "type": "enum",
                "namespace": "",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": "fixed1",
                "type": "fixed",
                "namespace": "",
                "size": 1
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"my_ns.my_schema","type":"record","fields":[{"name":"f1","type":{"name":"enum1","type":"enum","symbols":["a"]}},{"name":"f2","type":{"name":"fixed1","type":"fixed","size":1}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Enclosing namespace is "" and inner non-empty namespaces are specified.
        let schema_str = r#"
        {
          "namespace": "",
          "type": "record",
          "name": "my_schema",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "enum1",
                "type": "enum",
                "namespace": "f1.ns",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": "f2.ns.fixed1",
                "type": "fixed",
                "size": 1
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"my_schema","type":"record","fields":[{"name":"f1","type":{"name":"f1.ns.enum1","type":"enum","symbols":["a"]}},{"name":"f2","type":{"name":"f2.ns.fixed1","type":"fixed","size":1}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Nested complex types with non-empty enclosing namespace.
        let schema_str = r#"
        {
          "type": "record",
          "name": "my_ns.my_schema",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "inner_record1",
                "type": "record",
                "fields": [
                  {
                    "name": "f1_1",
                    "type": {
                      "name": "enum1",
                      "type": "enum",
                      "symbols": ["a"]
                    }
                  }
                ]
              }
            },  {
              "name": "f2",
                "type": {
                "name": "inner_record2",
                "type": "record",
                "namespace": "inner_ns",
                "fields": [
                  {
                    "name": "f2_1",
                    "type": {
                      "name": "enum2",
                      "type": "enum",
                      "symbols": ["a"]
                    }
                  }
                ]
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"my_ns.my_schema","type":"record","fields":[{"name":"f1","type":{"name":"my_ns.inner_record1","type":"record","fields":[{"name":"f1_1","type":{"name":"my_ns.enum1","type":"enum","symbols":["a"]}}]}},{"name":"f2","type":{"name":"inner_ns.inner_record2","type":"record","fields":[{"name":"f2_1","type":{"name":"inner_ns.enum2","type":"enum","symbols":["a"]}}]}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        Ok(())
    }

    #[test]
    fn test_avro_3779_bigdecimal_schema() -> TestResult {
        let schema = json!(
            {
                "name": "decimal",
                "type": "bytes",
                "logicalType": "big-decimal"
            }
        );

        let parse_result = Schema::parse(&schema);
        assert!(
            parse_result.is_ok(),
            "parse result must be ok, got: {parse_result:?}"
        );
        match parse_result? {
            Schema::BigDecimal => (),
            other => panic!("Expected Schema::BigDecimal but got: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_avro_3820_deny_invalid_field_names() -> TestResult {
        let schema_str = r#"
        {
          "name": "my_record",
          "type": "record",
          "fields": [
            {
              "name": "f1.x",
              "type": {
                "name": "my_enum",
                "type": "enum",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": "my_fixed",
                "type": "fixed",
                "size": 1
              }
            }
          ]
        }
        "#;

        match Schema::parse_str(schema_str).map_err(Error::into_details) {
            Err(Details::FieldName(x)) if x == "f1.x" => Ok(()),
            other => Err(format!("Expected Details::FieldName, got {other:?}").into()),
        }
    }

    #[test]
    fn test_avro_3827_disallow_duplicate_field_names() -> TestResult {
        let schema_str = r#"
        {
          "name": "my_schema",
          "type": "record",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "a",
                "type": "record",
                "fields": []
              }
            },  {
              "name": "f1",
              "type": {
                "name": "b",
                "type": "record",
                "fields": []
              }
            }
          ]
        }
        "#;

        match Schema::parse_str(schema_str).map_err(Error::into_details) {
            Err(Details::FieldNameDuplicate(_)) => (),
            other => {
                return Err(format!("Expected Details::FieldNameDuplicate, got {other:?}").into());
            }
        };

        let schema_str = r#"
        {
          "name": "my_schema",
          "type": "record",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "a",
                "type": "record",
                "fields": [
                  {
                    "name": "f1",
                    "type": {
                      "name": "b",
                      "type": "record",
                      "fields": []
                    }
                  }
                ]
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"my_schema","type":"record","fields":[{"name":"f1","type":{"name":"a","type":"record","fields":[{"name":"f1","type":{"name":"b","type":"record","fields":[]}}]}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        Ok(())
    }

    #[test]
    fn test_avro_3830_null_namespace_in_fully_qualified_names() -> TestResult {
        // Check whether all the named types don't refer to the namespace field
        // if their name starts with a dot.
        let schema_str = r#"
        {
          "name": ".record1",
          "namespace": "ns1",
          "type": "record",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": ".enum1",
                "namespace": "ns2",
                "type": "enum",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": ".fxed1",
                "namespace": "ns3",
                "type": "fixed",
                "size": 1
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"record1","type":"record","fields":[{"name":"f1","type":{"name":"enum1","type":"enum","symbols":["a"]}},{"name":"f2","type":{"name":"fxed1","type":"fixed","size":1}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Check whether inner types don't inherit ns1.
        let schema_str = r#"
        {
          "name": ".record1",
          "namespace": "ns1",
          "type": "record",
          "fields": [
            {
              "name": "f1",
              "type": {
                "name": "enum1",
                "type": "enum",
                "symbols": ["a"]
              }
            },  {
              "name": "f2",
              "type": {
                "name": "fxed1",
                "type": "fixed",
                "size": 1
              }
            }
          ]
        }
        "#;

        let expected = r#"{"name":"record1","type":"record","fields":[{"name":"f1","type":{"name":"enum1","type":"enum","symbols":["a"]}},{"name":"f2","type":{"name":"fxed1","type":"fixed","size":1}}]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        let name = Name::new(".my_name")?;
        let fullname = name.fullname(None);
        assert_eq!(fullname, "my_name");
        let qname = name.fully_qualified_name(&None).to_string();
        assert_eq!(qname, "my_name");

        Ok(())
    }

    #[test]
    fn test_avro_3814_schema_resolution_failure() -> TestResult {
        // Define a reader schema: a nested record with an optional field.
        let reader_schema = json!(
            {
                "type": "record",
                "name": "MyOuterRecord",
                "fields": [
                    {
                        "name": "inner_record",
                        "type": [
                            "null",
                            {
                                "type": "record",
                                "name": "MyRecord",
                                "fields": [
                                    {"name": "a", "type": "string"}
                                ]
                            }
                        ],
                        "default": null
                    }
                ]
            }
        );

        // Define a writer schema: a nested record with an optional field, which
        // may optionally contain an enum.
        let writer_schema = json!(
            {
                "type": "record",
                "name": "MyOuterRecord",
                "fields": [
                    {
                        "name": "inner_record",
                        "type": [
                            "null",
                            {
                                "type": "record",
                                "name": "MyRecord",
                                "fields": [
                                    {"name": "a", "type": "string"},
                                    {
                                        "name": "b",
                                        "type": [
                                            "null",
                                            {
                                                "type": "enum",
                                                "name": "MyEnum",
                                                "symbols": ["A", "B", "C"],
                                                "default": "C"
                                            }
                                        ],
                                        "default": null
                                    },
                                ]
                            }
                        ]
                    }
                ],
                "default": null
            }
        );

        // Use different structs to represent the "Reader" and the "Writer"
        // to mimic two different versions of a producer & consumer application.
        #[derive(Serialize, Deserialize, Debug)]
        struct MyInnerRecordReader {
            a: String,
        }

        #[derive(Serialize, Deserialize, Debug)]
        struct MyRecordReader {
            inner_record: Option<MyInnerRecordReader>,
        }

        #[derive(Serialize, Deserialize, Debug)]
        enum MyEnum {
            A,
            B,
            C,
        }

        #[derive(Serialize, Deserialize, Debug)]
        struct MyInnerRecordWriter {
            a: String,
            b: Option<MyEnum>,
        }

        #[derive(Serialize, Deserialize, Debug)]
        struct MyRecordWriter {
            inner_record: Option<MyInnerRecordWriter>,
        }

        let s = MyRecordWriter {
            inner_record: Some(MyInnerRecordWriter {
                a: "foo".to_string(),
                b: None,
            }),
        };

        // Serialize using the writer schema.
        let writer_schema = Schema::parse(&writer_schema)?;
        let avro_value = crate::to_value(s)?;
        assert!(
            avro_value.validate(&writer_schema),
            "value is valid for schema",
        );
        let datum = crate::to_avro_datum(&writer_schema, avro_value)?;

        // Now, attempt to deserialize using the reader schema.
        let reader_schema = Schema::parse(&reader_schema)?;
        let mut x = &datum[..];

        // Deserialization should succeed and we should be able to resolve the schema.
        let deser_value = crate::from_avro_datum(&writer_schema, &mut x, Some(&reader_schema))?;
        assert!(deser_value.validate(&reader_schema));

        // Verify that we can read a field from the record.
        let d: MyRecordReader = crate::from_value(&deser_value)?;
        assert_eq!(d.inner_record.unwrap().a, "foo".to_string());
        Ok(())
    }

    #[test]
    fn test_avro_3837_disallow_invalid_namespace() -> TestResult {
        // Valid namespace #1 (Single name portion)
        let schema_str = r#"
        {
          "name": "record1",
          "namespace": "ns1",
          "type": "record",
          "fields": []
        }
        "#;

        let expected = r#"{"name":"ns1.record1","type":"record","fields":[]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Valid namespace #2 (multiple name portions).
        let schema_str = r#"
        {
          "name": "enum1",
          "namespace": "ns1.foo.bar",
          "type": "enum",
          "symbols": ["a"]
        }
        "#;

        let expected = r#"{"name":"ns1.foo.bar.enum1","type":"enum","symbols":["a"]}"#;
        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        assert_eq!(canonical_form, expected);

        // Invalid namespace #1 (a name portion starts with dot)
        let schema_str = r#"
        {
          "name": "fixed1",
          "namespace": ".ns1.a.b",
          "type": "fixed",
          "size": 1
        }
        "#;

        match Schema::parse_str(schema_str).map_err(Error::into_details) {
            Err(Details::InvalidNamespace(_, _)) => (),
            other => {
                return Err(format!("Expected Details::InvalidNamespace, got {other:?}").into());
            }
        };

        // Invalid namespace #2 (invalid character in a name portion)
        let schema_str = r#"
        {
          "name": "record1",
          "namespace": "ns1.a*b.c",
          "type": "record",
          "fields": []
        }
        "#;

        match Schema::parse_str(schema_str).map_err(Error::into_details) {
            Err(Details::InvalidNamespace(_, _)) => (),
            other => {
                return Err(format!("Expected Details::InvalidNamespace, got {other:?}").into());
            }
        };

        // Invalid namespace #3 (a name portion starts with a digit)
        let schema_str = r#"
        {
          "name": "fixed1",
          "namespace": "ns1.1a.b",
          "type": "fixed",
          "size": 1
        }
        "#;

        match Schema::parse_str(schema_str).map_err(Error::into_details) {
            Err(Details::InvalidNamespace(_, _)) => (),
            other => {
                return Err(format!("Expected Details::InvalidNamespace, got {other:?}").into());
            }
        };

        // Invalid namespace #4 (a name portion is missing - two dots in a row)
        let schema_str = r#"
        {
          "name": "fixed1",
          "namespace": "ns1..a",
          "type": "fixed",
          "size": 1
        }
        "#;

        match Schema::parse_str(schema_str).map_err(Error::into_details) {
            Err(Details::InvalidNamespace(_, _)) => (),
            other => {
                return Err(format!("Expected Details::InvalidNamespace, got {other:?}").into());
            }
        };

        // Invalid namespace #5 (a name portion is missing - ends with a dot)
        let schema_str = r#"
        {
          "name": "fixed1",
          "namespace": "ns1.a.",
          "type": "fixed",
          "size": 1
        }
        "#;

        match Schema::parse_str(schema_str).map_err(Error::into_details) {
            Err(Details::InvalidNamespace(_, _)) => (),
            other => {
                return Err(format!("Expected Details::InvalidNamespace, got {other:?}").into());
            }
        };

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_simple_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": "int",
                    "default": "invalid"
                }
            ]
        }
        "#;
        assert_eq!(
            Schema::parse_str(schema_str).unwrap_err().to_string(),
            r#"`default`'s value type of field `f1` in `ns.record1` must be a `"int"`. Got: String("invalid")"#
        );

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_nested_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "name": "record2",
                        "type": "record",
                        "fields": [
                            {
                                "name": "f1_1",
                                "type": "int"
                            }
                        ]
                    },
                    "default": "invalid"
                }
            ]
        }
        "#;
        assert_eq!(
            Schema::parse_str(schema_str).unwrap_err().to_string(),
            r#"`default`'s value type of field `f1` in `ns.record1` must be a `{"name":"ns.record2","type":"record","fields":[{"name":"f1_1","type":"int"}]}`. Got: String("invalid")"#
        );

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_enum_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "name": "enum1",
                        "type": "enum",
                        "symbols": ["a", "b", "c"]
                    },
                    "default": "invalid"
                }
            ]
        }
        "#;
        assert_eq!(
            Schema::parse_str(schema_str).unwrap_err().to_string(),
            r#"`default`'s value type of field `f1` in `ns.record1` must be a `{"name":"ns.enum1","type":"enum","symbols":["a","b","c"]}`. Got: String("invalid")"#
        );

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_fixed_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "name": "fixed1",
                        "type": "fixed",
                        "size": 3
                    },
                    "default": 100
                }
            ]
        }
        "#;
        assert_eq!(
            Schema::parse_str(schema_str).unwrap_err().to_string(),
            r#"`default`'s value type of field `f1` in `ns.record1` must be a `{"name":"ns.fixed1","type":"fixed","size":3}`. Got: Number(100)"#
        );

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_array_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "type": "array",
                        "items": "int"
                    },
                    "default": "invalid"
                }
            ]
        }
        "#;

        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(
            r#"`default`'s value type of field `f1` in `ns.record1` must be a `{"type":"array","items":"int"}`. Got: String("invalid")"#,
            err
        );

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_map_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "type": "map",
                        "values": "string"
                    },
                    "default": "invalid"
                }
            ]
        }
        "#;

        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(
            r#"`default`'s value type of field `f1` in `ns.record1` must be a `{"type":"map","values":"string"}`. Got: String("invalid")"#,
            err
        );

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_ref_record_field() -> TestResult {
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": {
                        "name": "record2",
                        "type": "record",
                        "fields": [
                            {
                                "name": "f1_1",
                                "type": "int"
                            }
                        ]
                    }
                },  {
                    "name": "f2",
                    "type": "ns.record2",
                    "default": { "f1_1": true }
                }
            ]
        }
        "#;
        assert_eq!(
            Schema::parse_str(schema_str).unwrap_err().to_string(),
            r#"`default`'s value type of field `f2` in `ns.record1` must be a `{"name":"ns.record2","type":"record","fields":[{"name":"f1_1","type":"int"}]}`. Got: Object {"f1_1": Bool(true)}"#
        );

        Ok(())
    }

    #[test]
    fn test_avro_3851_validate_default_value_of_enum() -> TestResult {
        let schema_str = r#"
        {
            "name": "enum1",
            "namespace": "ns",
            "type": "enum",
            "symbols": ["a", "b", "c"],
            "default": 100
        }
        "#;
        let expected = Details::EnumDefaultWrongType(100.into()).to_string();
        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(expected, err);

        let schema_str = r#"
        {
            "name": "enum1",
            "namespace": "ns",
            "type": "enum",
            "symbols": ["a", "b", "c"],
            "default": "d"
        }
        "#;
        let expected = Details::GetEnumDefault {
            symbol: "d".to_string(),
            symbols: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        }
        .to_string();
        let result = Schema::parse_str(schema_str);
        assert!(result.is_err());
        let err = result
            .map_err(|e| e.to_string())
            .err()
            .unwrap_or_else(|| "unexpected".to_string());
        assert_eq!(expected, err);

        Ok(())
    }

    #[test]
    fn test_avro_3862_get_aliases() -> TestResult {
        // Test for Record
        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns1",
            "type": "record",
            "aliases": ["r1", "ns2.r2"],
            "fields": [
                { "name": "f1", "type": "int" },
                { "name": "f2", "type": "string" }
            ]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = vec![Alias::new("ns1.r1")?, Alias::new("ns2.r2")?];
        match schema.aliases() {
            Some(aliases) => assert_eq!(aliases, &expected),
            None => panic!("Expected Some({expected:?}), got None"),
        }

        let schema_str = r#"
        {
            "name": "record1",
            "namespace": "ns1",
            "type": "record",
            "fields": [
                { "name": "f1", "type": "int" },
                { "name": "f2", "type": "string" }
            ]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.aliases() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for Enum
        let schema_str = r#"
        {
            "name": "enum1",
            "namespace": "ns1",
            "type": "enum",
            "aliases": ["en1", "ns2.en2"],
            "symbols": ["a", "b", "c"]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = vec![Alias::new("ns1.en1")?, Alias::new("ns2.en2")?];
        match schema.aliases() {
            Some(aliases) => assert_eq!(aliases, &expected),
            None => panic!("Expected Some({expected:?}), got None"),
        }

        let schema_str = r#"
        {
            "name": "enum1",
            "namespace": "ns1",
            "type": "enum",
            "symbols": ["a", "b", "c"]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.aliases() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for Fixed
        let schema_str = r#"
        {
            "name": "fixed1",
            "namespace": "ns1",
            "type": "fixed",
            "aliases": ["fx1", "ns2.fx2"],
            "size": 10
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = vec![Alias::new("ns1.fx1")?, Alias::new("ns2.fx2")?];
        match schema.aliases() {
            Some(aliases) => assert_eq!(aliases, &expected),
            None => panic!("Expected Some({expected:?}), got None"),
        }

        let schema_str = r#"
        {
            "name": "fixed1",
            "namespace": "ns1",
            "type": "fixed",
            "size": 10
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.aliases() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for non-named type
        let schema = Schema::Int;
        match schema.aliases() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_avro_3862_get_doc() -> TestResult {
        // Test for Record
        let schema_str = r#"
        {
            "name": "record1",
            "type": "record",
            "doc": "Record Document",
            "fields": [
                { "name": "f1", "type": "int" },
                { "name": "f2", "type": "string" }
            ]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = "Record Document";
        match schema.doc() {
            Some(doc) => assert_eq!(doc, expected),
            None => panic!("Expected Some({expected:?}), got None"),
        }

        let schema_str = r#"
        {
            "name": "record1",
            "type": "record",
            "fields": [
                { "name": "f1", "type": "int" },
                { "name": "f2", "type": "string" }
            ]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.doc() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for Enum
        let schema_str = r#"
        {
            "name": "enum1",
            "type": "enum",
            "doc": "Enum Document",
            "symbols": ["a", "b", "c"]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = "Enum Document";
        match schema.doc() {
            Some(doc) => assert_eq!(doc, expected),
            None => panic!("Expected Some({expected:?}), got None"),
        }

        let schema_str = r#"
        {
            "name": "enum1",
            "type": "enum",
            "symbols": ["a", "b", "c"]
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.doc() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for Fixed
        let schema_str = r#"
        {
            "name": "fixed1",
            "type": "fixed",
            "doc": "Fixed Document",
            "size": 10
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        let expected = "Fixed Document";
        match schema.doc() {
            Some(doc) => assert_eq!(doc, expected),
            None => panic!("Expected Some({expected:?}), got None"),
        }

        let schema_str = r#"
        {
            "name": "fixed1",
            "type": "fixed",
            "size": 10
        }
        "#;
        let schema = Schema::parse_str(schema_str)?;
        match schema.doc() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        // Test for non-named type
        let schema = Schema::Int;
        match schema.doc() {
            None => (),
            some => panic!("Expected None, got {some:?}"),
        }

        Ok(())
    }

    #[test]
    fn avro_3886_serialize_attributes() -> TestResult {
        let attributes = BTreeMap::from([
            ("string_key".into(), "value".into()),
            ("number_key".into(), 1.23.into()),
            ("null_key".into(), JsonValue::Null),
            (
                "array_key".into(),
                JsonValue::Array(vec![1.into(), 2.into(), 3.into()]),
            ),
            ("object_key".into(), JsonValue::Object(Map::default())),
        ]);

        // Test serialize enum attributes
        let schema = Schema::Enum(EnumSchema {
            name: Name::new("a")?,
            aliases: None,
            doc: None,
            symbols: vec![],
            default: None,
            attributes: attributes.clone(),
        });
        let serialized = serde_json::to_string(&schema)?;
        assert_eq!(
            r#"{"type":"enum","name":"a","symbols":[],"array_key":[1,2,3],"null_key":null,"number_key":1.23,"object_key":{},"string_key":"value"}"#,
            &serialized
        );

        // Test serialize fixed custom_attributes
        let schema = Schema::Fixed(FixedSchema {
            name: Name::new("a")?,
            aliases: None,
            doc: None,
            size: 1,
            attributes: attributes.clone(),
        });
        let serialized = serde_json::to_string(&schema)?;
        assert_eq!(
            r#"{"type":"fixed","name":"a","size":1,"array_key":[1,2,3],"null_key":null,"number_key":1.23,"object_key":{},"string_key":"value"}"#,
            &serialized
        );

        // Test serialize record custom_attributes
        let schema = Schema::Record(RecordSchema {
            name: Name::new("a")?,
            aliases: None,
            doc: None,
            fields: vec![],
            lookup: BTreeMap::new(),
            attributes,
        });
        let serialized = serde_json::to_string(&schema)?;
        assert_eq!(
            r#"{"type":"record","name":"a","fields":[],"array_key":[1,2,3],"null_key":null,"number_key":1.23,"object_key":{},"string_key":"value"}"#,
            &serialized
        );

        Ok(())
    }

    #[test]
    fn test_avro_3896_decimal_schema() -> TestResult {
        // bytes decimal, represented as native logical type.
        let schema = json!(
        {
          "type": "bytes",
          "name": "BytesDecimal",
          "logicalType": "decimal",
          "size": 38,
          "precision": 9,
          "scale": 2
        });
        let parse_result = Schema::parse(&schema)?;
        assert!(matches!(
            parse_result,
            Schema::Decimal(DecimalSchema {
                precision: 9,
                scale: 2,
                ..
            })
        ));

        // long decimal, represents as native complex type.
        let schema = json!(
        {
          "type": "long",
          "name": "LongDecimal",
          "logicalType": "decimal"
        });
        let parse_result = Schema::parse(&schema)?;
        // assert!(matches!(parse_result, Schema::Long));
        assert_eq!(parse_result, Schema::Long);

        Ok(())
    }

    #[test]
    fn avro_3896_uuid_schema_for_string() -> TestResult {
        // string uuid, represents as native logical type.
        let schema = json!(
        {
          "type": "string",
          "name": "StringUUID",
          "logicalType": "uuid"
        });
        let parse_result = Schema::parse(&schema)?;
        assert_eq!(parse_result, Schema::Uuid(UuidSchema::String));

        Ok(())
    }

    #[test]
    fn avro_3926_uuid_schema_for_fixed_with_size_16() -> TestResult {
        let schema = json!(
        {
            "type": "fixed",
            "name": "FixedUUID",
            "size": 16,
            "logicalType": "uuid"
        });
        let parse_result = Schema::parse(&schema)?;
        assert_eq!(
            parse_result,
            Schema::Uuid(UuidSchema::Fixed(FixedSchema {
                name: Name::new("FixedUUID")?,
                aliases: None,
                doc: None,
                size: 16,
                attributes: Default::default(),
            }))
        );
        assert_not_logged(
            r#"Ignoring uuid logical type for a Fixed schema because its size (6) is not 16! Schema: Fixed(FixedSchema { name: Name { name: "FixedUUID", namespace: None }, aliases: None, doc: None, size: 6, attributes: {"logicalType": String("uuid")} })"#,
        );

        Ok(())
    }

    #[test]
    fn uuid_schema_bytes() -> TestResult {
        let schema = json!(
        {
          "type": "bytes",
          "name": "BytesUUID",
          "logicalType": "uuid"
        });
        let parse_result = Schema::parse(&schema)?;
        assert_eq!(parse_result, Schema::Uuid(UuidSchema::Bytes));

        Ok(())
    }

    #[test]
    fn avro_3926_uuid_schema_for_fixed_with_size_different_than_16() -> TestResult {
        let schema = json!(
        {
            "type": "fixed",
            "name": "FixedUUID",
            "size": 6,
            "logicalType": "uuid"
        });
        let parse_result = Schema::parse(&schema)?;

        assert_eq!(
            parse_result,
            Schema::Fixed(FixedSchema {
                name: Name::new("FixedUUID")?,
                aliases: None,
                doc: None,
                size: 6,
                attributes: BTreeMap::new(),
            })
        );
        assert_logged(
            r#"Ignoring uuid logical type for a Fixed schema because its size (6) is not 16! Schema: Fixed(FixedSchema { name: Name { name: "FixedUUID", namespace: None }, aliases: None, doc: None, size: 6, attributes: {} })"#,
        );

        Ok(())
    }

    #[test]
    fn test_avro_3896_timestamp_millis_schema() -> TestResult {
        // long timestamp-millis, represents as native logical type.
        let schema = json!(
        {
          "type": "long",
          "name": "LongTimestampMillis",
          "logicalType": "timestamp-millis"
        });
        let parse_result = Schema::parse(&schema)?;
        assert_eq!(parse_result, Schema::TimestampMillis);

        // int timestamp-millis, represents as native complex type.
        let schema = json!(
        {
            "type": "int",
            "name": "IntTimestampMillis",
            "logicalType": "timestamp-millis"
        });
        let parse_result = Schema::parse(&schema)?;
        assert_eq!(parse_result, Schema::Int);

        Ok(())
    }

    #[test]
    fn test_avro_3896_custom_bytes_schema() -> TestResult {
        // log type, represents as complex type.
        let schema = json!(
        {
            "type": "bytes",
            "name": "BytesLog",
            "logicalType": "custom"
        });
        let parse_result = Schema::parse(&schema)?;
        assert_eq!(parse_result, Schema::Bytes);
        assert_eq!(parse_result.custom_attributes(), None);

        Ok(())
    }

    #[test]
    fn test_avro_3899_parse_decimal_type() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
             "name": "InvalidDecimal",
             "type": "fixed",
             "size": 16,
             "logicalType": "decimal",
             "precision": 2,
             "scale": 3
         }"#,
        )?;
        match schema {
            Schema::Fixed(fixed_schema) => {
                let attrs = fixed_schema.attributes;
                let precision = attrs
                    .get("precision")
                    .expect("The 'precision' attribute is missing");
                let scale = attrs
                    .get("scale")
                    .expect("The 'scale' attribute is missing");
                assert_logged(&format!(
                    "Ignoring invalid decimal logical type: The decimal precision ({precision}) must be bigger or equal to the scale ({scale})"
                ));
            }
            _ => unreachable!("Expected Schema::Fixed, got {:?}", schema),
        }

        let schema = Schema::parse_str(
            r#"{
            "name": "ValidDecimal",
             "type": "bytes",
             "logicalType": "decimal",
             "precision": 3,
             "scale": 2
         }"#,
        )?;
        match schema {
            Schema::Decimal(_) => {
                assert_not_logged(
                    "Ignoring invalid decimal logical type: The decimal precision (2) must be bigger or equal to the scale (3)",
                );
            }
            _ => unreachable!("Expected Schema::Decimal, got {:?}", schema),
        }

        Ok(())
    }

    #[test]
    fn avro_3920_serialize_record_with_custom_attributes() -> TestResult {
        let expected = {
            let mut lookup = BTreeMap::new();
            lookup.insert("value".to_owned(), 0);
            Schema::Record(RecordSchema {
                name: Name {
                    name: "LongList".to_owned(),
                    namespace: None,
                },
                aliases: Some(vec![Alias::new("LinkedLongs").unwrap()]),
                doc: None,
                fields: vec![RecordField {
                    name: "value".to_string(),
                    doc: None,
                    default: None,
                    aliases: None,
                    schema: Schema::Long,
                    order: RecordFieldOrder::Ascending,
                    position: 0,
                    custom_attributes: BTreeMap::from([("field-id".to_string(), 1.into())]),
                }],
                lookup,
                attributes: BTreeMap::from([("custom-attribute".to_string(), "value".into())]),
            })
        };

        let value = serde_json::to_value(&expected)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"aliases":["LinkedLongs"],"custom-attribute":"value","fields":[{"field-id":1,"name":"value","type":"long"}],"name":"LongList","type":"record"}"#,
            &serialized
        );
        assert_eq!(expected, Schema::parse_str(&serialized)?);

        Ok(())
    }

    #[test]
    fn test_avro_3925_serialize_decimal_inner_fixed() -> TestResult {
        let schema = Schema::Decimal(DecimalSchema {
            precision: 36,
            scale: 10,
            inner: InnerDecimalSchema::Fixed(FixedSchema {
                name: Name::new("decimal_36_10").unwrap(),
                aliases: None,
                doc: None,
                size: 16,
                attributes: Default::default(),
            }),
        });

        let serialized_json = serde_json::to_string_pretty(&schema)?;

        let expected_json = r#"{
  "type": "fixed",
  "name": "decimal_36_10",
  "size": 16,
  "logicalType": "decimal",
  "scale": 10,
  "precision": 36
}"#;

        assert_eq!(serialized_json, expected_json);

        Ok(())
    }

    #[test]
    fn test_avro_3925_serialize_decimal_inner_bytes() -> TestResult {
        let schema = Schema::Decimal(DecimalSchema {
            precision: 36,
            scale: 10,
            inner: InnerDecimalSchema::Bytes,
        });

        let serialized_json = serde_json::to_string_pretty(&schema)?;

        let expected_json = r#"{
  "type": "bytes",
  "logicalType": "decimal",
  "scale": 10,
  "precision": 36
}"#;

        assert_eq!(serialized_json, expected_json);

        Ok(())
    }

    #[test]
    fn test_avro_3927_serialize_array_with_custom_attributes() -> TestResult {
        let expected = Schema::array(Schema::Long)
            .attributes(BTreeMap::from([("field-id".to_string(), "1".into())]))
            .build();

        let value = serde_json::to_value(&expected)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"field-id":"1","items":"long","type":"array"}"#,
            &serialized
        );
        let actual_schema = Schema::parse_str(&serialized)?;
        assert_eq!(expected, actual_schema);
        assert_eq!(
            expected.custom_attributes(),
            actual_schema.custom_attributes()
        );

        Ok(())
    }

    #[test]
    fn test_avro_3927_serialize_map_with_custom_attributes() -> TestResult {
        let expected = Schema::map(Schema::Long)
            .attributes(BTreeMap::from([("field-id".to_string(), "1".into())]))
            .build();

        let value = serde_json::to_value(&expected)?;
        let serialized = serde_json::to_string(&value)?;
        assert_eq!(
            r#"{"field-id":"1","type":"map","values":"long"}"#,
            &serialized
        );
        let actual_schema = Schema::parse_str(&serialized)?;
        assert_eq!(expected, actual_schema);
        assert_eq!(
            expected.custom_attributes(),
            actual_schema.custom_attributes()
        );

        Ok(())
    }

    #[test]
    fn avro_3928_parse_int_based_schema_with_default() -> TestResult {
        let schema = r#"
        {
          "type": "record",
          "name": "DateLogicalType",
          "fields": [ {
            "name": "birthday",
            "type": {"type": "int", "logicalType": "date"},
            "default": 1681601653
          } ]
        }"#;

        match Schema::parse_str(schema)? {
            Schema::Record(record_schema) => {
                assert_eq!(record_schema.fields.len(), 1);
                let field = record_schema.fields.first().unwrap();
                assert_eq!(field.name, "birthday");
                assert_eq!(field.schema, Schema::Date);
                assert_eq!(
                    types::Value::try_from(field.default.clone().unwrap())?,
                    types::Value::Int(1681601653)
                );
            }
            _ => unreachable!("Expected Schema::Record"),
        }

        Ok(())
    }

    #[test]
    fn avro_3946_union_with_single_type() -> TestResult {
        let schema = r#"
        {
          "type": "record",
          "name": "Issue",
          "namespace": "invalid.example",
          "fields": [
            {
              "name": "myField",
              "type": ["long"]
            }
          ]
        }"#;

        let _ = Schema::parse_str(schema)?;

        assert_logged(
            "Union schema with just one member! Consider dropping the union! \
                    Please enable debug logging to find out which Record schema \
                    declares the union with 'RUST_LOG=apache_avro::schema=debug'.",
        );

        Ok(())
    }

    #[test]
    fn avro_3946_union_without_any_types() -> TestResult {
        let schema = r#"
        {
          "type": "record",
          "name": "Issue",
          "namespace": "invalid.example",
          "fields": [
            {
              "name": "myField",
              "type": []
            }
          ]
        }"#;

        let _ = Schema::parse_str(schema)?;

        assert_logged(
            "Union schemas should have at least two members! \
                    Please enable debug logging to find out which Record schema \
                    declares the union with 'RUST_LOG=apache_avro::schema=debug'.",
        );

        Ok(())
    }

    #[test]
    fn avro_4004_canonical_form_strip_logical_types() -> TestResult {
        let schema_str = r#"
      {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42, "doc": "The field a"},
            {"name": "b", "type": "string", "namespace": "test.a"},
            {"name": "c", "type": {"type": "long", "logicalType": "timestamp-micros"}}
        ]
    }"#;

        let schema = Schema::parse_str(schema_str)?;
        let canonical_form = schema.canonical_form();
        let fp_rabin = schema.fingerprint::<Rabin>();
        assert_eq!(
            r#"{"name":"test","type":"record","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"},{"name":"c","type":{"type":"long"}}]}"#,
            canonical_form
        );
        assert_eq!("92f2ccef718c6754", fp_rabin.to_string());
        Ok(())
    }

    #[test]
    fn avro_4055_should_fail_to_parse_invalid_schema() -> TestResult {
        // This is invalid because the record type should be inside the type field.
        let invalid_schema_str = r#"
        {
        "type": "record",
        "name": "SampleSchema",
        "fields": [
            {
            "name": "order",
            "type": "record",
            "fields": [
                {
                "name": "order_number",
                "type": ["null", "string"],
                "default": null
                },
                { "name": "order_date", "type": "string" }
            ]
            }
        ]
        }"#;

        let schema = Schema::parse_str(invalid_schema_str);
        assert!(schema.is_err());
        assert_eq!(
            schema.unwrap_err().to_string(),
            "Invalid schema: There is no type called 'record', if you meant to define a non-primitive schema, it should be defined inside `type` attribute."
        );

        let valid_schema = r#"
        {
            "type": "record",
            "name": "SampleSchema",
            "fields": [
                {
                "name": "order",
                "type": {
                    "type": "record",
                    "name": "Order",
                    "fields": [
                    {
                        "name": "order_number",
                        "type": ["null", "string"],
                        "default": null
                    },
                    { "name": "order_date", "type": "string" }
                    ]
                }
                }
            ]
        }"#;
        let schema = Schema::parse_str(valid_schema);
        assert!(schema.is_ok());

        Ok(())
    }

    #[test]
    fn avro_rs_292_array_items_should_be_ignored_in_custom_attributes() -> TestResult {
        let raw_schema = r#"{
                    "type": "array",
                    "items": {
                        "name": "foo",
                        "type": "record",
                        "fields": [
                            {
                                "name": "bar",
                                "type": {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "baz",
                                        "fields": [
                                            {
                                                "name": "quux",
                                                "type": "int"
                                            }
                                        ]
                                    }
                                }
                            }
                        ]
                    }
                }"#;

        let schema1 = Schema::parse_str(raw_schema)?;
        match &schema1 {
            Schema::Array(ArraySchema {
                items,
                default: _,
                attributes,
            }) => {
                assert!(attributes.is_empty());

                match **items {
                    Schema::Record(RecordSchema {
                        ref name,
                        aliases: _,
                        doc: _,
                        ref fields,
                        lookup: _,
                        ref attributes,
                    }) => {
                        assert_eq!(name.to_string(), "foo");
                        assert_eq!(fields.len(), 1);
                        assert!(attributes.is_empty());

                        match &fields[0].schema {
                            Schema::Array(ArraySchema {
                                items: _,
                                default: _,
                                attributes,
                            }) => {
                                assert!(attributes.is_empty());
                            }
                            _ => panic!("Expected ArraySchema. got: {}", &fields[0].schema),
                        }
                    }
                    _ => panic!("Expected RecordSchema. got: {}", &items),
                }
            }
            _ => panic!("Expected ArraySchema. got: {}", &schema1),
        }
        let canonical_form1 = schema1.canonical_form();
        let schema2 = Schema::parse_str(&canonical_form1)?;
        let canonical_form2 = schema2.canonical_form();

        assert_eq!(canonical_form1, canonical_form2);

        Ok(())
    }

    #[test]
    fn avro_rs_292_map_values_should_be_ignored_in_custom_attributes() -> TestResult {
        let raw_schema = r#"{
                    "type": "array",
                    "items": {
                        "name": "foo",
                        "type": "record",
                        "fields": [
                            {
                                "name": "bar",
                                "type": {
                                    "type": "map",
                                    "values": {
                                        "type": "record",
                                        "name": "baz",
                                        "fields": [
                                            {
                                                "name": "quux",
                                                "type": "int"
                                            }
                                        ]
                                    }
                                }
                            }
                        ]
                    }
                }"#;

        let schema1 = Schema::parse_str(raw_schema)?;
        match &schema1 {
            Schema::Array(ArraySchema {
                items,
                default: _,
                attributes,
            }) => {
                assert!(attributes.is_empty());

                match **items {
                    Schema::Record(RecordSchema {
                        ref name,
                        aliases: _,
                        doc: _,
                        ref fields,
                        lookup: _,
                        ref attributes,
                    }) => {
                        assert_eq!(name.to_string(), "foo");
                        assert_eq!(fields.len(), 1);
                        assert!(attributes.is_empty());

                        match &fields[0].schema {
                            Schema::Map(MapSchema {
                                types: _,
                                default: _,
                                attributes,
                            }) => {
                                assert!(attributes.is_empty());
                            }
                            _ => panic!("Expected MapSchema. got: {}", &fields[0].schema),
                        }
                    }
                    _ => panic!("Expected RecordSchema. got: {}", &items),
                }
            }
            _ => panic!("Expected ArraySchema. got: {}", &schema1),
        }
        let canonical_form1 = schema1.canonical_form();
        println!("Canonical Form 1: {}", &canonical_form1);
        let schema2 = Schema::parse_str(&canonical_form1)?;
        let canonical_form2 = schema2.canonical_form();

        assert_eq!(canonical_form1, canonical_form2);

        Ok(())
    }

    #[test]
    fn avro_rs_382_serialize_duration_schema() -> TestResult {
        let schema = Schema::Duration(FixedSchema {
            name: Name::try_from("Duration")?,
            aliases: None,
            doc: None,
            size: 12,
            attributes: BTreeMap::new(),
        });

        let expected_schema_json = json!({
            "type": "fixed",
            "logicalType": "duration",
            "name": "Duration",
            "size": 12
        });

        let schema_json = serde_json::to_value(&schema)?;

        assert_eq!(&schema_json, &expected_schema_json);

        Ok(())
    }

    #[test]
    fn avro_rs_395_logical_type_written_once_for_duration() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "fixed",
            "logicalType": "duration",
            "name": "Duration",
            "size": 12
        }"#,
        )?;

        let schema_json_str = serde_json::to_string(&schema)?;

        assert_eq!(
            schema_json_str.matches("logicalType").count(),
            1,
            "Expected serialized schema to contain only one logicalType key: {schema_json_str}"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_395_logical_type_written_once_for_uuid_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "fixed",
            "logicalType": "uuid",
            "name": "UUID",
            "size": 16
        }"#,
        )?;

        let schema_json_str = serde_json::to_string(&schema)?;

        assert_eq!(
            schema_json_str.matches("logicalType").count(),
            1,
            "Expected serialized schema to contain only one logicalType key: {schema_json_str}"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_395_logical_type_written_once_for_decimal_fixed() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "fixed",
            "logicalType": "decimal",
            "scale": 4,
            "precision": 8,
            "name": "FixedDecimal16",
            "size": 16
        }"#,
        )?;

        let schema_json_str = serde_json::to_string(&schema)?;

        assert_eq!(
            schema_json_str.matches("logicalType").count(),
            1,
            "Expected serialized schema to contain only one logicalType key: {schema_json_str}"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_420_independent_canonical_form() -> TestResult {
        let (record, schemata) = Schema::parse_str_with_list(
            r#"{
            "name": "root",
            "type": "record",
            "fields": [{
                "name": "node",
                "type": "node"
            }]
        }"#,
            [r#"{
            "name": "node",
            "type": "record",
            "fields": [{
                "name": "children",
                "type": ["null", "node"]
            }]
        }"#],
        )?;
        let icf = record.independent_canonical_form(&schemata)?;
        assert_eq!(
            icf,
            r#"{"name":"root","type":"record","fields":[{"name":"node","type":{"name":"node","type":"record","fields":[{"name":"children","type":["null","node"]}]}}]}"#
        );
        Ok(())
    }

    #[test]
    fn avro_rs_456_bool_instead_of_boolean() -> TestResult {
        let error = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "defaults",
            "fields": [
                {"name": "boolean", "type": "bool", "default": true}
            ]
        }"#,
        )
        .unwrap_err()
        .into_details()
        .to_string();
        assert_eq!(
            error,
            Details::ParsePrimitiveSimilar("bool".to_string(), "boolean").to_string()
        );

        Ok(())
    }

    #[test]
    fn avro_rs_460_fixed_default_in_custom_attributes() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "name": "fixed_with_default",
            "type": "fixed",
            "size": 1,
            "default": "\u0000",
            "doc": "a docstring"
        }"#,
        )?;

        assert_eq!(schema.custom_attributes().unwrap().len(), 1);

        let json = serde_json::to_string(&schema)?;
        let schema2 = Schema::parse_str(&json)?;

        assert_eq!(schema2.custom_attributes().unwrap().len(), 1);

        Ok(())
    }

    #[test]
    fn avro_rs_460_enum_default_not_in_custom_attributes() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "name": "enum_with_default",
            "type": "enum",
            "symbols": ["A", "B", "C"],
            "default": "A",
            "doc": "a docstring"
        }"#,
        )?;

        assert_eq!(schema.custom_attributes().unwrap(), &BTreeMap::new());

        let json = serde_json::to_string(&schema)?;
        let schema2 = Schema::parse_str(&json)?;

        let Schema::Enum(enum_schema) = schema2 else {
            panic!("Expected Schema::Enum, got {schema2:?}");
        };
        assert!(enum_schema.default.is_some());
        assert!(enum_schema.doc.is_some());

        Ok(())
    }

    #[test]
    fn avro_rs_467_array_default() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "array",
            "items": "string",
            "default": []
        }"#,
        )?;

        let Schema::Array(array) = schema else {
            panic!("Expected Schema::Array, got {schema:?}");
        };

        assert_eq!(array.attributes, BTreeMap::new());
        assert_eq!(array.default, Some(Vec::new()));

        let json = serde_json::to_string(&Schema::Array(array))?;
        assert!(json.contains(r#""default":[]"#));

        Ok(())
    }

    #[test]
    fn avro_rs_467_array_default_with_actual_values() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "array",
            "items": "string",
            "default": ["foo", "bar"]
        }"#,
        )?;

        let Schema::Array(array) = schema else {
            panic!("Expected Schema::Array, got {schema:?}");
        };

        assert_eq!(array.attributes, BTreeMap::new());
        assert_eq!(
            array.default,
            Some(vec![
                Value::String("foo".into()),
                Value::String("bar".into())
            ])
        );

        let json = serde_json::to_string(&Schema::Array(array))?;
        assert!(json.contains(r#""default":["foo","bar"]"#));

        Ok(())
    }

    #[test]
    fn avro_rs_467_array_default_with_invalid_values() -> TestResult {
        let err = Schema::parse_str(
            r#"{
            "type": "array",
            "items": "string",
            "default": [false, true]
        }"#,
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Default value for an array must be an array of String! Found: Boolean(false)"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_467_array_default_with_mixed_values() -> TestResult {
        let err = Schema::parse_str(
            r#"{
            "type": "array",
            "items": "string",
            "default": ["foo", true]
        }"#,
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Default value for an array must be an array of String! Found: Boolean(true)"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_467_array_default_with_reference() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "Something",
            "fields": [
                {
                    "name": "one",
                    "type": {
                        "type": "enum",
                        "name": "ABC",
                        "symbols": ["A", "B", "C"]
                    }
                },
                {
                    "name": "two",
                    "type": {
                        "type": "array",
                        "items": "ABC",
                        "default": ["A", "B", "C"]
                    }
                }
            ]
        }"#,
        )?;

        let Schema::Record(record) = schema else {
            panic!("Expected Schema::Record, got {schema:?}");
        };
        let Schema::Array(array) = &record.fields[1].schema else {
            panic!("Expected Schema::Array, got {:?}", record.fields[1].schema);
        };

        assert_eq!(array.attributes, BTreeMap::new());
        assert_eq!(
            array.default,
            Some(vec![
                Value::String("A".into()),
                Value::String("B".into()),
                Value::String("C".into())
            ])
        );

        Ok(())
    }

    #[test]
    fn avro_rs_467_map_default() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "map",
            "values": "string",
            "default": {}
        }"#,
        )?;

        let Schema::Map(map) = schema else {
            panic!("Expected Schema::Map, got {schema:?}");
        };

        assert_eq!(map.attributes, BTreeMap::new());
        assert_eq!(map.default, Some(HashMap::new()));

        let json = serde_json::to_string(&Schema::Map(map))?;
        assert!(json.contains(r#""default":{}"#));

        Ok(())
    }

    #[test]
    fn avro_rs_467_map_default_with_actual_values() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "map",
            "values": "string",
            "default": {"foo": "bar"}
        }"#,
        )?;

        let Schema::Map(map) = schema else {
            panic!("Expected Schema::Map, got {schema:?}");
        };

        let mut hashmap = HashMap::new();
        hashmap.insert("foo".to_string(), Value::String("bar".into()));
        assert_eq!(map.attributes, BTreeMap::new());
        assert_eq!(map.default, Some(hashmap));

        let json = serde_json::to_string(&Schema::Map(map))?;
        assert!(json.contains(r#""default":{"foo":"bar"}"#));

        Ok(())
    }

    #[test]
    fn avro_rs_467_map_default_with_invalid_values() -> TestResult {
        let err = Schema::parse_str(
            r#"{
            "type": "map",
            "values": "string",
            "default": {"foo": true}
        }"#,
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Default value for a map must be an object with (String, String)! Found: (String, Boolean(true))"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_467_map_default_with_mixed_values() -> TestResult {
        let err = Schema::parse_str(
            r#"{
            "type": "map",
            "values": "string",
            "default": {"foo": "bar", "spam": true}
        }"#,
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Default value for a map must be an object with (String, String)! Found: (String, Boolean(true))"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_467_map_default_with_reference() -> TestResult {
        let schema = Schema::parse_str(
            r#"{
            "type": "record",
            "name": "Something",
            "fields": [
                {
                    "name": "one",
                    "type": {
                        "type": "enum",
                        "name": "ABC",
                        "symbols": ["A", "B", "C"]
                    }
                },
                {
                    "name": "two",
                    "type": {
                        "type": "map",
                        "values": "ABC",
                        "default": {"foo": "A"}
                    }
                }
            ]
        }"#,
        )?;

        let Schema::Record(record) = schema else {
            panic!("Expected Schema::Record, got {schema:?}");
        };
        let Schema::Map(map) = &record.fields[1].schema else {
            panic!("Expected Schema::Map, got {:?}", record.fields[1].schema);
        };

        let mut hashmap = HashMap::new();
        hashmap.insert("foo".to_string(), Value::String("A".into()));
        assert_eq!(map.attributes, BTreeMap::new());
        assert_eq!(map.default, Some(hashmap));

        Ok(())
    }

    #[test]
    fn avro_rs_476_enum_cannot_be_directly_in_field() -> TestResult {
        let schema_str = r#"{
            "type": "record",
            "name": "ExampleEnum",
            "namespace": "com.schema",
            "fields": [
                {
                "name": "wrong_enum",
                "type": "enum",
                "symbols": ["INSERT", "UPDATE"]
                }
            ]
        }"#;
        let result = Schema::parse_str(schema_str).unwrap_err();
        assert_eq!(
            result.to_string(),
            "Invalid schema: There is no type called 'enum', if you meant to define a non-primitive schema, it should be defined inside `type` attribute."
        );
        Ok(())
    }
}
