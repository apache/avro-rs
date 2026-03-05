use crate::schema::RecordSchema;
use crate::serde::deser_schema::{Config, SchemaAwareDeserializer};
use crate::{Error, Schema};
use serde::de::{DeserializeSeed, SeqAccess};
use std::borrow::Borrow;
use std::io::Read;

pub struct ManyTupleDeserializer<'s, 'r, R: Read, S: Borrow<Schema>> {
    reader: &'r mut R,
    schema: &'s RecordSchema,
    config: Config<'s, S>,
    current_field: usize,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> ManyTupleDeserializer<'s, 'r, R, S> {
    pub fn new(reader: &'r mut R, schema: &'s RecordSchema, config: Config<'s, S>) -> Self {
        Self {
            reader,
            schema,
            config,
            current_field: 0,
        }
    }
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> SeqAccess<'de>
    for ManyTupleDeserializer<'s, 'r, R, S>
{
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.current_field < self.schema.fields.len() {
            let schema = &self.schema.fields[self.current_field].schema;
            let v = seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                schema,
                self.config,
            )?)?;
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

pub struct OneTupleDeserializer<'s, 'r, R: Read, S: Borrow<Schema>> {
    reader: &'r mut R,
    schema: &'s Schema,
    config: Config<'s, S>,
    field_read: bool,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> OneTupleDeserializer<'s, 'r, R, S> {
    pub fn new(reader: &'r mut R, schema: &'s Schema, config: Config<'s, S>) -> Self {
        Self {
            reader,
            schema,
            config,
            field_read: false,
        }
    }
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> SeqAccess<'de>
    for OneTupleDeserializer<'s, 'r, R, S>
{
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.field_read {
            Ok(None)
        } else {
            let v = seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                self.schema,
                self.config,
            )?)?;
            self.field_read = true;
            Ok(Some(v))
        }
    }
}
