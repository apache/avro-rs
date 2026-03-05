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

use std::borrow::Borrow;
use std::fmt::{Debug, Formatter};
use std::io::Read;

use serde::de::MapAccess;

use super::Config;
use crate::serde::deser_schema::identifier::IdentifierDeserializer;
use crate::{Error, Schema, schema::RecordSchema, serde::deser_schema::SchemaAwareDeserializer};

pub struct RecordDeserializer<'s, 'r, R: Read, S: Borrow<Schema>> {
    reader: &'r mut R,
    schema: &'s RecordSchema,
    config: Config<'s, S>,
    current_field: State,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> Debug for RecordDeserializer<'s, 'r, R, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordDeserializer")
            .field("schema", &self.schema)
            .field("current_field", &self.current_field)
            .finish()
    }
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> RecordDeserializer<'s, 'r, R, S> {
    pub fn new(reader: &'r mut R, schema: &'s RecordSchema, config: Config<'s, S>) -> Self {
        Self {
            reader,
            schema,
            config,
            current_field: State::Key(0),
        }
    }
}

#[derive(Debug)]
enum State {
    Key(usize),
    Value(usize),
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> MapAccess<'de> for RecordDeserializer<'s, 'r, R, S> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        println!("next_key_seed: {self:?}");
        let State::Key(index) = self.current_field else {
            panic!("`next_key_seed` and `next_value_seed` where called in the wrong error")
        };
        if index >= self.schema.fields.len() {
            // Finished reading this record
            Ok(None)
        } else {
            let v = seed.deserialize(IdentifierDeserializer::string(
                &self.schema.fields[index].name,
            ))?;
            self.current_field = State::Value(index);
            Ok(Some(v))
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        println!("next_value_seed: {self:?}");
        let State::Value(index) = self.current_field else {
            panic!("`next_key_seed` and `next_value_seed` where called in the wrong error")
        };
        let schema = &self.schema.fields[index].schema;
        let v = if let Schema::Record(record) = schema
            && record.fields.len() == 1
            && record.fields[0].name == "field_0"
            && record
                .attributes
                .get("org.apache.avro.rust.union_of_records")
                == Some(&serde_json::Value::Bool(true))
        {
            // Most likely a Union of Records
            seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                &record.fields[0].schema,
                self.config,
            )?)?
        } else {
            seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                schema,
                self.config,
            )?)?
        };
        self.current_field = State::Key(index + 1);
        Ok(v)
    }

    fn size_hint(&self) -> Option<usize> {
        match self.current_field {
            State::Key(index) | State::Value(index) => Some(self.schema.fields.len() - index),
        }
    }
}
