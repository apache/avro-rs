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

use std::fmt::{Debug, Formatter};
use std::io::Read;

use serde::{Deserializer, de::MapAccess};

use super::Config;
use crate::serde::deser_schema::union::UnionDeserializer;
use crate::{
    Error, Schema, error::Details, schema::RecordSchema,
    serde::deser_schema::SchemaAwareDeserializer,
};

pub struct RecordDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    schema: &'s RecordSchema,
    config: Config<'s>,
    current_field: State,
}

impl<'s, 'r, R: Read> Debug for RecordDeserializer<'s, 'r, R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordDeserializer")
            .field("schema", &self.schema)
            .field("config", &self.config)
            .field("current_field", &self.current_field)
            .finish()
    }
}

impl<'s, 'r, R: Read> RecordDeserializer<'s, 'r, R> {
    pub fn new(reader: &'r mut R, schema: &'s RecordSchema, config: Config<'s>) -> Self {
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

impl<'de, 's, 'r, R: Read> MapAccess<'de> for RecordDeserializer<'s, 'r, R> {
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
            let v = seed.deserialize(FieldName {
                name: &self.schema.fields[index].name,
            })?;
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
        let v = match &self.schema.fields[index].schema {
            Schema::Union(union) => {
                seed.deserialize(UnionDeserializer::new(self.reader, union, self.config)?)?
            }
            schema => seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                schema,
                self.config,
            )?)?,
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

/// "Deserializer" for the field name
struct FieldName<'s> {
    name: &'s str,
}

impl<'de, 's> Deserializer<'de> for FieldName<'s> {
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_any".into()).into())
    }

    fn deserialize_bool<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_bool".into()).into())
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_i8".into()).into())
    }

    fn deserialize_i16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_i16".into()).into())
    }

    fn deserialize_i32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_i32".into()).into())
    }

    fn deserialize_i64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_i64".into()).into())
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_u8".into()).into())
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_u16".into()).into())
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_u32".into()).into())
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_u64".into()).into())
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_f32".into()).into())
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_f64".into()).into())
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_char".into()).into())
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_str".into()).into())
    }

    fn deserialize_string<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_string".into()).into())
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_bytes".into()).into())
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_byte_buf".into()).into())
    }

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_option".into()).into())
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_unit".into()).into())
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey(format!("deserialize_unit_struct(name: {name})")).into())
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey(format!("deserialize_newtype_struct(name: {name})")).into())
    }

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_seq".into()).into())
    }

    fn deserialize_tuple<V>(self, len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey(format!("deserialize_tuple(len: {len})")).into())
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey(format!(
            "deserialize_tuple_struct(name: {name}, len: {len})"
        ))
        .into())
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_map".into()).into())
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey(format!(
            "deserialize_struct(name: {name}, fields: {fields:?})"
        ))
        .into())
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey(format!(
            "deserialize_enum(name: {name}, variants: {variants:?})"
        ))
        .into())
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        println!("deserializing_identifier: {}", self.name);
        visitor.visit_str(self.name)
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Details::DeserializeKey("deserialize_ignored_any".into()).into())
    }
}
