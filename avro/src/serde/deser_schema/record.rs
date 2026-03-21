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

use std::{borrow::Borrow, io::Read};

use serde::de::{DeserializeSeed, MapAccess};

use crate::{
    Error, Schema,
    schema::RecordSchema,
    serde::deser_schema::{Config, SchemaAwareDeserializer, identifier::IdentifierDeserializer},
};

pub struct RecordDeserializer<'s, 'r, R: Read, S: Borrow<Schema>> {
    reader: &'r mut R,
    schema: &'s RecordSchema,
    config: Config<'s, S>,
    current_field: usize,
}

impl<'s, 'r, R: Read, S: Borrow<Schema>> RecordDeserializer<'s, 'r, R, S> {
    pub fn new(reader: &'r mut R, schema: &'s RecordSchema, config: Config<'s, S>) -> Self {
        Self {
            reader,
            schema,
            config,
            current_field: 0,
        }
    }
}

impl<'de, 's, 'r, R: Read, S: Borrow<Schema>> MapAccess<'de> for RecordDeserializer<'s, 'r, R, S> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.current_field >= self.schema.fields.len() {
            // Finished reading this record
            Ok(None)
        } else {
            seed.deserialize(IdentifierDeserializer::string(
                &self.schema.fields[self.current_field].name,
            ))
            .map(Some)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let schema = &self.schema.fields[self.current_field].schema;
        let value = if let Schema::Record(record) = schema
            && record.fields.len() == 1
            && record
                .attributes
                .get("org.apache.avro.rust.union_of_records")
                == Some(&serde_json::Value::Bool(true))
        {
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
        self.current_field += 1;
        Ok(value)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.schema.fields.len() - self.current_field)
    }
}
