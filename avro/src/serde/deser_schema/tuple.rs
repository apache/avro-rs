use crate::schema::RecordSchema;
use crate::serde::deser_schema::union::UnionDeserializer;
use crate::serde::deser_schema::{Config, SchemaAwareDeserializer};
use crate::{Error, Schema};
use serde::de::{DeserializeSeed, SeqAccess};
use std::io::Read;

pub struct ManyTupleDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    schema: &'s RecordSchema,
    config: Config<'s>,
    current_field: usize,
}

impl<'s, 'r, R: Read> ManyTupleDeserializer<'s, 'r, R> {
    pub fn new(reader: &'r mut R, schema: &'s RecordSchema, config: Config<'s>) -> Self {
        Self {
            reader,
            schema,
            config,
            current_field: 0,
        }
    }
}

impl<'de, 's, 'r, R: Read> SeqAccess<'de> for ManyTupleDeserializer<'s, 'r, R> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.current_field < self.schema.fields.len() {
            let schema = &self.schema.fields[self.current_field].schema;
            let v = match schema {
                Schema::Union(union) => {
                    seed.deserialize(UnionDeserializer::new(self.reader, union, self.config)?)?
                }
                schema => seed.deserialize(SchemaAwareDeserializer::new(
                    self.reader,
                    schema,
                    self.config,
                )?)?,
            };
            self.current_field += 1;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }
}

pub struct UnitTupleDeserializer;
impl<'de> SeqAccess<'de> for UnitTupleDeserializer {
    type Error = Error;

    fn next_element_seed<T>(&mut self, _seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        Ok(None)
    }
}

pub struct OneTupleDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    schema: &'s Schema,
    config: Config<'s>,
    field_read: bool,
}

impl<'s, 'r, R: Read> OneTupleDeserializer<'s, 'r, R> {
    pub fn new(reader: &'r mut R, schema: &'s Schema, config: Config<'s>) -> Self {
        Self {
            reader,
            schema,
            config,
            field_read: false,
        }
    }
}

impl<'de, 's, 'r, R: Read> SeqAccess<'de> for OneTupleDeserializer<'s, 'r, R> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.field_read {
            Ok(None)
        } else {
            let v = match self.schema {
                Schema::Union(union) => {
                    seed.deserialize(UnionDeserializer::new(self.reader, union, self.config)?)?
                }
                schema => seed.deserialize(SchemaAwareDeserializer::new(
                    self.reader,
                    schema,
                    self.config,
                )?)?,
            };
            self.field_read = true;
            Ok(Some(v))
        }
    }
}
