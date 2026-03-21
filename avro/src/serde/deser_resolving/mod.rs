mod any;
mod record;

use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::Read;
use serde::de::Visitor;
use serde::Deserialize;
use crate::{Error, Schema};
use crate::error::Details;
use crate::schema::{DecimalSchema, InnerDecimalSchema, Name, SchemaKind, UnionSchema, UuidSchema};
use crate::serde::deser_resolving::any::AnyVisitor;
use crate::serde::deser_schema::{SchemaAwareDeserializer, DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS};
use crate::util::zag_i32;

#[derive(Debug)]
pub struct Config<'s, S: Borrow<Schema>> {
    /// All names that can be referenced in the writer schema.
    pub writer_names: &'s HashMap<Name, S>,
    /// All names that can be referenced in the reader schema.
    pub reader_names: &'s HashMap<Name, S>,
    /// Should `Deserialize` implementations pick a human-readable format.
    ///
    /// This should match the setting used for serialisation.
    pub human_readable: bool,
}

// This needs to be implemented manually as the derive puts a bound on `S`
// which is not necessary as a reference is always Copy.
impl<'s, S: Borrow<Schema>> Copy for Config<'s, S> {}
impl<'s, S: Borrow<Schema>> Clone for Config<'s, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'s, S: Borrow<Schema>> From<Config<'s, S>> for super::deser_schema::Config<'s, S> {
    fn from(value: Config<'s, S>) -> Self {
        Self {
            names: value.writer_names,
            human_readable: value.human_readable,
        }
    }
}

pub struct ResolvingDeserializer<'s, 'r, R: Read, S: Borrow<Schema>> {
    reader: &'r mut R,
    writer_schema: &'s Schema,
    reader_schema: &'s Schema,
    config: Config<'s, S>,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> ResolvingDeserializer<'s, 'r, R, S> {
    pub fn new(
        reader: &'r mut R,
        writer_schema: &'s Schema,
        reader_schema: &'s Schema,
        config: Config<'s, S>,
    ) -> Result<Self, Error> {
        if let Schema::Ref { name } = writer_schema {
            let writer_schema = config.writer_names.get(name).ok_or_else(|| Details::SchemaResolutionError(name.clone()))?.borrow();
            Self::new(reader, writer_schema, reader_schema, config)
        } else if let Schema::Ref { name } = reader_schema {
            let reader_schema = config.reader_names.get(name).ok_or_else(|| Details::SchemaResolutionError(name.clone()))?.borrow();
            Self::new(reader, writer_schema, reader_schema, config)
        } else {
            Ok(Self {
                reader,
                writer_schema,
                reader_schema,
                config,
            })
        }
    }

    fn error(&self, ty: &'static str, error: impl Into<String>) -> Error {
        Error::new(Details::DeserializeValueWithResolvingSchema {
            value_type: ty,
            value: error.into(),
            writer_schema: self.writer_schema.clone(),
            reader_schema: self.reader_schema.clone(),
        })
    }

    /// Create a new deserializer with the existing reader and config.
    ///
    /// This will resolve the schema if it is a reference.
    fn with_different_schema(mut self, writer_schema: &'s Schema, reader_schema: &'s Schema) -> Result<Self, Error> {
        if let Schema::Ref { name } = writer_schema {
            let writer_schema = self.config.writer_names.get(name).ok_or_else(|| Details::SchemaResolutionError(name.clone()))?.borrow();
            self.with_different_schema(writer_schema, reader_schema)
        } else if let Schema::Ref { name } = reader_schema {
            let reader_schema = self.config.reader_names.get(name).ok_or_else(|| Details::SchemaResolutionError(name.clone()))?.borrow();
            self.with_different_schema(writer_schema, reader_schema)
        } else {
            self.writer_schema = writer_schema;
            self.reader_schema = reader_schema;
            Ok(self)
        }
    }

    /// Read the union and create a new deserializer with the existing reader and config.
    ///
    /// This will resolve the read schema if it is a reference.
    fn with_reader_union(self, reader_schema: &'s UnionSchema) -> Result<Self, Error> {
        if let Schema::Union(writer_schema) = self.writer_schema {
            let index = zag_i32(self.reader)?;
            let writer_schema =
                writer_schema
                    .variants()
                    .get(index as usize)
                    .ok_or_else(|| Details::GetUnionVariant {
                        index: index as i64,
                        num_variants: writer_schema.variants().len(),
                    })?;
            let Some(index) = reader_schema.find_compatible_variant(writer_schema, self.config.reader_names, self.config.writer_names) else {
                panic!("writer variant is not in reader variant")
            };
            let reader_schema = &reader_schema.variants()[index];
            self.with_different_schema(writer_schema, reader_schema)
        } else if let Some(index) = reader_schema.find_compatible_variant(self.writer_schema, self.config.reader_names, self.config.writer_names) {
            let reader_schema = &reader_schema.variants()[index];
            let writer_schema = self.writer_schema;
            self.with_different_schema(writer_schema, reader_schema)
        } else {
            panic!("No match found")
        }
    }

    /// Read the union and create a new deserializer with the existing reader and config.
    ///
    /// This will resolve the read schema if it is a reference.
    fn with_writer_union(self, writer_schema: &'s UnionSchema) -> Result<Self, Error> {
        if let Schema::Union(_) = self.reader_schema {
            unreachable!("This should only be called if only the writer schema is a union")
        }
        let index = zag_i32(self.reader)?;
        let writer_schema =
            writer_schema
                .variants()
                .get(index as usize)
                .ok_or_else(|| Details::GetUnionVariant {
                    index: index as i64,
                    num_variants: writer_schema.variants().len(),
                })?;
        let reader_schema = self.reader_schema;
        self.with_different_schema(writer_schema, reader_schema)
    }

    fn read_int(&mut self, original_ty: &'static str) -> Result<i32, Error> {
        match self.reader_schema {
            Schema::Int | Schema::Date | Schema::TimeMillis => SchemaAwareDeserializer::new(self.reader, self.writer_schema, self.config.into())?.read_int(original_ty),
            _ => Err(self.error(original_ty, "Expected Schema::Int | Schema::Date | Schema::TimeMillis for reader")),
        }
    }

    fn read_long(&mut self, original_ty: &'static str) -> Result<i64, Error> {
        match self.reader_schema {
            Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros
            | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => match self.writer_schema {
                Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros
                | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros
                | Schema::LocalTimestampNanos => SchemaAwareDeserializer::new(self.reader, self.writer_schema, self.config.into())?.read_long(original_ty),
                Schema::Int | Schema::Date | Schema::TimeMillis => SchemaAwareDeserializer::new(self.reader, self.writer_schema, self.config.into())?.read_int(original_ty).map(i64::from),
                _ => Err(self.error(original_ty, "Expected Schema::Int | Schema::Date | Schema::Long | Schema::Time{Millis,Micros} | Schema::{,Local}Timestamp{Millis,Micros,Nanos} for writer")),
            },
            _ => Err(self.error(original_ty, "Expected Schema::Long | Schema::TimeMicros | Schema::{,Local}Timestamp{Millis,Micros,Nanos} for reader")),
        }
    }
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> serde::Deserializer<'de> for ResolvingDeserializer<'s, 'r, R, S> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Null => self.deserialize_unit(visitor),
            Schema::Boolean => self.deserialize_bool(visitor),
            Schema::Int | Schema::Date | Schema::TimeMillis => self.deserialize_i32(visitor),
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => self.deserialize_i64(visitor),
            Schema::Float => self.deserialize_f32(visitor),
            Schema::Double => self.deserialize_f64(visitor),
            Schema::Bytes
            | Schema::Fixed(_)
            | Schema::Decimal(_)
            | Schema::BigDecimal
            | Schema::Uuid(UuidSchema::Fixed(_))
            | Schema::Duration(_) => self.deserialize_byte_buf(visitor),
            Schema::String | Schema::Uuid(UuidSchema::String) => self.deserialize_string(visitor),
            Schema::Array(_) => self.deserialize_seq(visitor),
            Schema::Map(_) => self.deserialize_map(visitor),
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_any(visitor),
            Schema::Record(schema) => {
                if schema.attributes.get("org.apache.avro.rust.tuple")
                    == Some(&serde_json::Value::Bool(true))
                {
                    // This is needed because we can't tell the difference between a tuple and struct.
                    // And a tuple needs to be deserialized as a sequence
                    self.deserialize_tuple(schema.fields.len(), visitor)
                } else {
                    self.deserialize_struct(DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS, visitor)
                }
            }
            Schema::Enum(_) => {
                self.deserialize_enum(DESERIALIZE_ANY, DESERIALIZE_ANY_FIELDS, visitor)
            }
            Schema::Ref { .. } => unreachable!("References are resolved on deserializer creation"),
            Schema::Uuid(UuidSchema::Bytes) => panic!("Unsupported"),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_bool(visitor),
            Schema::Boolean => match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_bool(visitor),
                Schema::Boolean => SchemaAwareDeserializer::new(self.reader, self.reader_schema, self.config.into())?.deserialize_bool(visitor),
                _ => Err(self.error("bool", "Expected Schema::Boolean for writer"))
            },
            _ => Err(self.error("bool", "Expected Schema::Boolean for reader"))
        }
    }

    fn deserialize_i8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        if let Schema::Union(union) = self.reader_schema {
            self.with_reader_union(union)?.deserialize_i8(visitor)
        } else {
            let int = self.read_int("i8")?;
            let value = i8::try_from(int)
                .map_err(|_| self.error("i8", format!("Could not convert int ({int}) to an i8")))?;
            visitor.visit_i8(value)
        }
    }

    fn deserialize_i16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        if let Schema::Union(union) = self.reader_schema {
            self.with_reader_union(union)?.deserialize_i16(visitor)
        } else {
            let int = self.read_int("i16")?;
            let value = i16::try_from(int).map_err(|_| {
                self.error("i16", format!("Could not convert int ({int}) to an i16"))
            })?;
            visitor.visit_i16(value)
        }
    }

    fn deserialize_i32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        if let Schema::Union(union) = self.reader_schema {
            self.with_reader_union(union)?.deserialize_i32(visitor)
        } else {
            let value = self.read_int("i32")?;
            visitor.visit_i32(value)
        }
    }

    fn deserialize_i64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        if let Schema::Union(union) = self.reader_schema {
            self.with_reader_union(union)?.deserialize_i64(visitor)
        } else {
            let value = self.read_long("i64")?;
            visitor.visit_i64(value)
        }
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_i128(visitor),
            Schema::Fixed(fixed) if fixed.size == 16 && fixed.name.name() == "i128" => match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_i128(visitor),
                Schema::Fixed(fixed) if fixed.size == 16 && fixed.name.name() == "i128" => SchemaAwareDeserializer::new(self.reader, self.reader_schema, self.config.into())?.deserialize_i128(visitor),
                _ => Err(self.error("i128", r#"Expected Schema::Fixed(name: "i128", size: 16) for writer"#))
            }
            _ => Err(self.error("i128", r#"Expected Schema::Fixed(name: "i128", size: 16) for reader"#)),
        }
    }

    fn deserialize_u8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        if let Schema::Union(union) = self.reader_schema {
            self.with_reader_union(union)?.deserialize_u8(visitor)
        } else {
            let int = self.read_int("u8")?;
            let value = u8::try_from(int)
                .map_err(|_| self.error("u8", format!("Could not convert int ({int}) to an u8")))?;
            visitor.visit_u8(value)
        }
    }

    fn deserialize_u16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        if let Schema::Union(union) = self.reader_schema {
            self.with_reader_union(union)?.deserialize_u16(visitor)
        } else {
            let int = self.read_int("u16")?;
            let value = u16::try_from(int).map_err(|_| {
                self.error("u16", format!("Could not convert int ({int}) to an u16"))
            })?;
            visitor.visit_u16(value)
        }
    }

    fn deserialize_u32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        let int = self.read_long("u32")?;
        let value = u32::try_from(int).map_err(|_| {
            self.error("u32", format!("Could not convert int ({int}) to an u32"))
        })?;
        visitor.visit_u32(value)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_u64(visitor),
            Schema::Fixed(fixed) if fixed.size == 8 && fixed.name.name() == "u64" => match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_u64(visitor),
                Schema::Fixed(fixed) if fixed.size == 8 && fixed.name.name() == "u64" => SchemaAwareDeserializer::new(self.reader, self.reader_schema, self.config.into())?.deserialize_u64(visitor),
                _ => Err(self.error("u64", r#"Expected Schema::Fixed(name: "u64", size: 8) for writer"#))
            }
            _ => Err(self.error("u64", r#"Expected Schema::Fixed(name: "u64", size: 8) for reader"#)),
        }
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_u128(visitor),
            Schema::Fixed(fixed) if fixed.size == 8 && fixed.name.name() == "u128" => match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_u128(visitor),
                Schema::Fixed(fixed) if fixed.size == 8 && fixed.name.name() == "u128" => SchemaAwareDeserializer::new(self.reader, self.reader_schema, self.config.into())?.deserialize_u128(visitor),
                _ => Err(self.error("u128", r#"Expected Schema::Fixed(name: "u128", size: 16) for writer"#))
            }
            _ => Err(self.error("u128", r#"Expected Schema::Fixed(name: "u128", size: 16) for reader"#)),
        }
    }

    fn deserialize_f32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_f32(visitor),
            Schema::Float => match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_f32(visitor),
                Schema::Float => SchemaAwareDeserializer::new(self.reader, self.reader_schema, self.config.into())?.deserialize_f32(visitor),
                Schema::Int | Schema::Date | Schema::TimeMillis => {
                    let value = i32::deserialize(SchemaAwareDeserializer::new(self.reader, self.writer_schema, self.config.into())?)?;
                    visitor.visit_f32(value as f32)
                }
                Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros
                | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros
                | Schema::LocalTimestampNanos => {
                    let value = i64::deserialize(SchemaAwareDeserializer::new(self.reader, self.writer_schema, self.config.into())?)?;
                    visitor.visit_f32(value as f32)
                }
                _ => Err(self.error("f32", "Expected Schema::Float | Schema::Int | Schema::Date | Schema::Long | Schema::Time{Millis,Micros} | Schema::{,Local}Timestamp{Millis,Micros,Nanos} for writer"))
            }
            _ => Err(self.error("f32", "Expected Schema::Float for reader")),
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_f64(visitor),
            Schema::Float => match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_f64(visitor),
                Schema::Double => SchemaAwareDeserializer::new(self.reader, self.reader_schema, self.config.into())?.deserialize_f64(visitor),
                Schema::Float => {
                    let float = f32::deserialize(SchemaAwareDeserializer::new(self.reader, self.reader_schema, self.config.into())?)?;
                    visitor.visit_f64(float as f64)
                }
                Schema::Int | Schema::Date | Schema::TimeMillis => {
                    let value = i32::deserialize(SchemaAwareDeserializer::new(self.reader, self.writer_schema, self.config.into())?)?;
                    visitor.visit_f64(value as f64)
                }
                Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros
                | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros
                | Schema::LocalTimestampNanos => {
                    let value = i64::deserialize(SchemaAwareDeserializer::new(self.reader, self.writer_schema, self.config.into())?)?;
                    visitor.visit_f64(value as f64)
                }
                _ => Err(self.error("f64", "Expected Schema::Float | Schema::Double Schema::Int | Schema::Date | Schema::Long | Schema::Time{Millis,Micros} | Schema::{,Local}Timestamp{Millis,Micros,Nanos} for writer"))
            }
            _ => Err(self.error("f64", "Expected Schema::Double for reader")),
        }
    }

    fn deserialize_char<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_char(visitor),
            Schema::String | Schema::Uuid(UuidSchema::String) => match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_char(visitor),
                Schema::String | Schema::Bytes | Schema::Uuid(UuidSchema::String | UuidSchema::Bytes) | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Bytes, ..}) | Schema::BigDecimal => {
                    let string = SchemaAwareDeserializer::new(self.reader, self.writer_schema, self.config.into())?.read_string()?;
                    let mut chars = string.chars();
                    if let Some(character) = chars.next() {
                        if chars.next().is_some() {
                            Err(self.error("char", "String contains more than one character"))
                        } else {
                            visitor.visit_char(character)
                        }
                    } else {
                        Err(self.error("char", "String is empty"))
                    }
                }
                _ => Err(self.error("string", "Expected Schema::String | Schema::Bytes | Schema::Uuid(String | Bytes) | Schema::Decimal(Bytes) | Schema::BigDecimal for writer")),
            }
            _ => Err(self.error("string", "Expected Schema::String | Schema::Uuid(String) for reader")),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_string(visitor),
            Schema::String | Schema::Uuid(UuidSchema::String) => match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_string(visitor),
                Schema::String | Schema::Bytes | Schema::Uuid(UuidSchema::String | UuidSchema::Bytes) | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Bytes, ..}) | Schema::BigDecimal => {
                    let string = SchemaAwareDeserializer::new(self.reader, self.writer_schema, self.config.into())?.read_string()?;
                    visitor.visit_string(string)
                }
                _ => Err(self.error("string", "Expected Schema::String | Schema::Bytes | Schema::Uuid(String | Bytes) | Schema::Decimal(Bytes) | Schema::BigDecimal for writer")),
            }
            _ => Err(self.error("string", "Expected Schema::String | Schema::Uuid(String) for reader")),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        self.deserialize_byte_buf(visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_bytes(visitor),
            Schema::Bytes | Schema::Uuid(UuidSchema::Bytes) | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Bytes, ..}) | Schema::BigDecimal=> match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_bytes(visitor),
                Schema::String | Schema::Bytes | Schema::Uuid(UuidSchema::String | UuidSchema::Bytes) | Schema::Decimal(DecimalSchema { inner: InnerDecimalSchema::Bytes, ..}) | Schema::BigDecimal => {
                    let bytes = SchemaAwareDeserializer::new(self.reader, self.writer_schema, self.config.into())?.read_bytes_with_len()?;
                    visitor.visit_byte_buf(bytes)
                }
                _ => Err(self.error("bytes", "Expected Schema::String | Schema::Bytes | Schema::Uuid(String | Bytes) | Schema::Decimal(Bytes) | Schema::BigDecimal for writer")),
            }
            _ => Err(self.error("bytes", "Expected Schema::Bytes | Schema::Uuid(Bytes) | Schema::Decimal(Bytes) | Schema::BigDecimal for reader")),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        // The reader schema must be a Union([Null, _])
        if let Schema::Union(union) = self.reader_schema
            && union.variants().len() == 2
            && let Some(null_index) = union.index().get(&SchemaKind::Null).copied()
        {
            let some_index = (null_index + 1) & 1;
            // Map the writer schema to the reader Some or None
            match self.writer_schema {
                Schema::Union(union) => {
                    let index = zag_i32(self.reader)? as usize;
                    let writer_schema = &union.variants()[index];
                    if writer_schema == &Schema::Null {
                        visitor.visit_none()
                    } else {
                        let reader_schema = &union.variants()[some_index];
                        visitor.visit_some(self.with_different_schema(writer_schema, reader_schema)?)
                    }
                },
                Schema::Null => visitor.visit_none(),
                _ => {
                    let reader_schema = &union.variants()[some_index];
                    let writer_schema = self.writer_schema;
                    visitor.visit_some(self.with_different_schema(writer_schema, reader_schema)?)
                }
            }
        } else {
            Err(self.error("option", "Expected Schema::Union([Null, _])"))
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_unit(visitor),
            Schema::Null => match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_unit(visitor),
                Schema::Null => visitor.visit_unit(),
                _ => Err(self.error("unit", "Expected Schema::Null for writer")),
            }
            _ => Err(self.error("unit", "Expected Schema::Null for reader")),
        }
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_unit_struct(name, visitor),
            Schema::Record(record) if record.fields.is_empty() && record.name.name() == name => match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_unit_struct(name, visitor),
                Schema::Record(record) if record.name.name() == name => {
                    if !record.fields.is_empty() {
                        // Ignore all fields
                        SchemaAwareDeserializer::new(self.reader, self.writer_schema, self.config.into())?.deserialize_struct(name, DESERIALIZE_ANY_FIELDS, AnyVisitor)?;
                    }
                    visitor.visit_unit()
                }
                _ => Err(self.error("unit", "Expected Schema::Null for writer")),
            }
            _ => Err(self.error("unit", "Expected Schema::Null for reader")),
        }
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        match self.reader_schema {
            Schema::Union(union) => self.with_reader_union(union)?.deserialize_newtype_struct(name, visitor),
            Schema::Record(w_record) if w_record.fields.len() == 1 && w_record.name.name() == name => match self.writer_schema {
                Schema::Union(union) => self.with_writer_union(union)?.deserialize_newtype_struct(name, visitor),
                Schema::Record(r_record) if r_record.name.name() == name && r_record.fields.is_empty() => {
                    let field = &w_record.fields[0];
                    if let Some(default) = &field.default {
                        
                        todo!()   
                    } else {
                        Err(self.error("newtype struct", "Writer is missing field and no default is available"))
                    }
                }
                Schema::Record(r_record) if r_record.name.name() == name && r_record.fields.len() == 1 => {
                    visitor.visit_newtype_struct(self.with_different_schema(&w_record.fields[0].schema, &r_record.fields[0].schema)?)
                }
                Schema::Record(r_record) if r_record.name.name() == name => {
                    todo!("Skip all fields that do not match")
                }
                _ => Err(self.error("unit", "Expected Schema::Null for writer")),
            }
            _ => Err(self.error("unit", "Expected Schema::Null for reader")),
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(self, name: &'static str, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_struct<V>(self, name: &'static str, fields: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_enum<V>(self, name: &'static str, variants: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn is_human_readable(&self) -> bool {
        self.config.human_readable
    }
}
