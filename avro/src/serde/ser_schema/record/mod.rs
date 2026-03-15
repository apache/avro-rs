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

mod field_default;

use std::{borrow::Borrow, cmp::Ordering, collections::HashMap, io::Write};

use serde::{
    Serialize,
    ser::{SerializeMap, SerializeStruct, SerializeStructVariant},
};

use super::{Config, SchemaAwareSerializer};
use crate::{
    Error, Schema,
    error::Details,
    schema::RecordSchema,
    serde::{
        ser_schema::record::field_default::SchemaAwareRecordFieldDefault, util::StringSerializer,
    },
};

pub struct RecordSerializer<'s, 'w, W: Write, S: Borrow<Schema>> {
    writer: &'w mut W,
    record: &'s RecordSchema,
    config: Config<'s, S>,
    /// Cache fields received out-of-order
    cache: HashMap<usize, Vec<u8>>,
    /// The position of the current map entry being written
    map_position: Option<usize>,
    /// The field that should be written now.
    field_position: usize,
    bytes_written: usize,
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> RecordSerializer<'s, 'w, W, S> {
    pub fn new(
        writer: &'w mut W,
        record: &'s RecordSchema,
        config: Config<'s, S>,
        bytes_written: Option<usize>,
    ) -> Self {
        Self {
            writer,
            record,
            config,
            cache: HashMap::new(),
            map_position: None,
            field_position: 0,
            bytes_written: bytes_written.unwrap_or(0),
        }
    }

    fn field_error(&self, position: usize, error: Error) -> Error {
        let field = &self.record.fields[position];
        let error = match error.into_details() {
            Details::SerializeValueWithSchema {
                value_type,
                value,
                schema: _,
            } => format!("Failed to serialize value of type `{value_type}`: {value}"),
            Details::SerializeRecordFieldWithSchema {
                field_name,
                record_schema,
                error,
            } => format!(
                "Failed to serialize field '{field_name}' of record {}: {error}",
                record_schema.name
            ),
            Details::MissingDefaultForSkippedField { field_name, schema } => {
                format!(
                    "Missing default for skipped field '{field_name}' for record {}",
                    schema.name
                )
            }
            details => format!("{details:?}"),
        };
        Error::new(Details::SerializeRecordFieldWithSchema {
            field_name: field.name.clone(),
            record_schema: self.record.clone(),
            error,
        })
    }

    fn serialize_next_field<T: ?Sized + Serialize>(
        &mut self,
        position: usize,
        value: &T,
    ) -> Result<(), Error> {
        let field = &self.record.fields[position];
        match self.field_position.cmp(&position) {
            Ordering::Equal => {
                // Field received in the right order
                self.bytes_written += value
                    .serialize(SchemaAwareSerializer::new(
                        self.writer,
                        &field.schema,
                        self.config,
                    )?)
                    .map_err(|e| self.field_error(self.field_position, e))?;
                self.field_position += 1;

                // Write any fields that were already received and can now be written
                while let Some(bytes) = self.cache.remove(&self.field_position) {
                    self.writer.write_all(&bytes).map_err(Details::WriteBytes)?;
                    self.bytes_written += bytes.len();
                    self.field_position += 1;
                }

                Ok(())
            }
            Ordering::Less => {
                // Another field needs to be written first, so cache this field
                let mut bytes = Vec::new();
                value
                    .serialize(SchemaAwareSerializer::new(
                        &mut bytes,
                        &field.schema,
                        self.config,
                    )?)
                    .map_err(|e| self.field_error(self.field_position, e))?;
                if self.cache.insert(position, bytes).is_some() {
                    Err(Details::FieldNameDuplicate(field.name.clone()).into())
                } else {
                    Ok(())
                }
            }
            Ordering::Greater => {
                // This field is already written to the writer so we got a duplicate
                Err(Details::FieldNameDuplicate(field.name.clone()).into())
            }
        }
    }

    fn serialize_default(&mut self, position: usize) -> Result<(), Error> {
        let field = &self.record.fields[position];
        if let Some(default) = &field.default {
            self.serialize_next_field(
                position,
                &SchemaAwareRecordFieldDefault::new(default, &field.schema),
            )
            .map_err(|e| self.field_error(position, e))
        } else {
            Err(Details::MissingDefaultForSkippedField {
                field_name: field.name.clone(),
                schema: self.record.clone(),
            }
            .into())
        }
    }

    fn end(mut self) -> Result<usize, Error> {
        // Write any fields that were skipped by `#[serde(skip)]` or `#[serde(skip_serializing{,_if}]`
        while self.field_position != self.record.fields.len() {
            self.serialize_default(self.field_position)?;
        }

        Ok(self.bytes_written)
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeStruct for RecordSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        if let Some(position) = self.record.lookup.get(key).copied() {
            self.serialize_next_field(position, value)
        } else {
            Err(Details::GetField(key.to_string()).into())
        }
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), Self::Error> {
        if let Some(position) = self.record.lookup.get(key).copied() {
            self.serialize_default(position)
        } else {
            Err(Details::GetField(key.to_string()).into())
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeMap for RecordSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let name = key.serialize(StringSerializer)?;
        if let Some(position) = self.record.lookup.get(&name).copied() {
            self.map_position = Some(position);
            Ok(())
        } else {
            Err(Details::FieldName(name.to_string()).into())
        }
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_next_field(
            self.map_position
                .expect("serialze_value called without calling serialize_key"),
            value,
        )
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }

    fn serialize_entry<K, V>(&mut self, key: &K, value: &V) -> Result<(), Self::Error>
    where
        K: ?Sized + Serialize,
        V: ?Sized + Serialize,
    {
        let name = key.serialize(StringSerializer)?;
        if let Some(position) = self.record.lookup.get(&name).copied() {
            self.serialize_next_field(position, value)
        } else {
            Err(Details::FieldName(name.to_string()).into())
        }
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeStructVariant
    for RecordSerializer<'s, 'w, W, S>
{
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        <Self as SerializeStruct>::serialize_field(self, key, value)
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), Self::Error> {
        <Self as SerializeStruct>::skip_field(self, key)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}
