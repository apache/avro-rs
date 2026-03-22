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

use std::{borrow::Borrow, io::Write};

use serde::{
    Serialize,
    ser::{SerializeTuple, SerializeTupleStruct, SerializeTupleVariant},
};

use super::{Config, SchemaAwareSerializer, union::UnionSerializer};
use crate::{
    Error, Schema,
    error::Details,
    schema::{RecordSchema, UnionSchema},
};

#[expect(
    private_interfaces,
    reason = "One{,Union}TupleSerializer should not be used directly"
)]
pub enum TupleSerializer<'s, 'w, W: Write, S: Borrow<Schema>> {
    Unit(usize),
    One(OneTupleSerializer<'s, 'w, W, S>),
    /// This exists because we can't create a `&Schema::Union` from a `&UnionSchema`
    OneUnion(OneUnionTupleSerializer<'s, 'w, W, S>),
    Many(ManyTupleSerializer<'s, 'w, W, S>),
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> TupleSerializer<'s, 'w, W, S> {
    pub fn unit(bytes_written: Option<usize>) -> Self {
        Self::Unit(bytes_written.unwrap_or(0))
    }

    pub fn one(
        writer: &'w mut W,
        schema: &'s Schema,
        config: Config<'s, S>,
        bytes_written: Option<usize>,
    ) -> Self {
        Self::One(OneTupleSerializer::new(
            writer,
            schema,
            config,
            bytes_written,
        ))
    }

    pub fn one_union(writer: &'w mut W, union: &'s UnionSchema, config: Config<'s, S>) -> Self {
        Self::OneUnion(OneUnionTupleSerializer::new(writer, union, config))
    }

    pub fn many(
        writer: &'w mut W,
        schema: &'s RecordSchema,
        config: Config<'s, S>,
        bytes_written: Option<usize>,
    ) -> Self {
        Self::Many(ManyTupleSerializer::new(
            writer,
            schema,
            config,
            bytes_written,
        ))
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeTuple for TupleSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self {
            TupleSerializer::Unit(_) => Err(Error::new(Details::SerializeValueWithSchema {
                value_type: "tuple",
                value: "Expected no elements for the unit tuple".into(),
                schema: Schema::Null,
            })),
            TupleSerializer::One(one) => one.serialize_element(value),
            TupleSerializer::OneUnion(one) => one.serialize_element(value),
            TupleSerializer::Many(many) => many.serialize_element(value),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self {
            TupleSerializer::Unit(bytes_written) => Ok(bytes_written),
            TupleSerializer::One(one) => one.end(),
            TupleSerializer::OneUnion(one) => one.end(),
            TupleSerializer::Many(many) => SerializeTuple::end(many),
        }
    }
}

pub struct ManyTupleSerializer<'s, 'w, W: Write, S: Borrow<Schema>> {
    writer: &'w mut W,
    schema: &'s RecordSchema,
    config: Config<'s, S>,
    field_position: usize,
    bytes_written: usize,
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> ManyTupleSerializer<'s, 'w, W, S> {
    pub fn new(
        writer: &'w mut W,
        schema: &'s RecordSchema,
        config: Config<'s, S>,
        bytes_written: Option<usize>,
    ) -> Self {
        Self {
            writer,
            schema,
            config,
            field_position: 0,
            bytes_written: bytes_written.unwrap_or(0),
        }
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeTuple for ManyTupleSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let schema = &self
            .schema
            .fields
            .get(self.field_position)
            .ok_or_else(|| Details::SerializeRecordUnknownFieldIndex {
                position: self.field_position,
                schema: self.schema.clone(),
            })?
            .schema;
        self.bytes_written += value.serialize(SchemaAwareSerializer::new(
            self.writer,
            schema,
            self.config,
        )?)?;
        self.field_position += 1;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        if self.field_position != self.schema.fields.len() {
            Err(Details::SerializeTupleMissingElements {
                position: self.field_position,
                total_elements: self.schema.fields.len(),
            }
            .into())
        } else {
            Ok(self.bytes_written)
        }
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeTupleStruct
    for ManyTupleSerializer<'s, 'w, W, S>
{
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        SerializeTuple::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        SerializeTuple::end(self)
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeTupleVariant
    for ManyTupleSerializer<'s, 'w, W, S>
{
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        SerializeTuple::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        SerializeTuple::end(self)
    }
}

struct OneTupleSerializer<'s, 'w, W: Write, S: Borrow<Schema>> {
    writer: &'w mut W,
    schema: &'s Schema,
    config: Config<'s, S>,
    bytes_written: usize,
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> OneTupleSerializer<'s, 'w, W, S> {
    pub fn new(
        writer: &'w mut W,
        schema: &'s Schema,
        config: Config<'s, S>,
        bytes_written: Option<usize>,
    ) -> Self {
        Self {
            writer,
            schema,
            config,
            bytes_written: bytes_written.unwrap_or(0),
        }
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeTuple for OneTupleSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self.schema {
            Schema::Union(union) => {
                self.bytes_written +=
                    value.serialize(UnionSerializer::new(self.writer, union, self.config))?;
            }
            schema => {
                self.bytes_written += value.serialize(SchemaAwareSerializer::new(
                    self.writer,
                    schema,
                    self.config,
                )?)?;
            }
        }
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.bytes_written)
    }
}

struct OneUnionTupleSerializer<'s, 'w, W: Write, S: Borrow<Schema>> {
    writer: &'w mut W,
    union: &'s UnionSchema,
    config: Config<'s, S>,
    bytes_written: usize,
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> OneUnionTupleSerializer<'s, 'w, W, S> {
    pub fn new(writer: &'w mut W, union: &'s UnionSchema, config: Config<'s, S>) -> Self {
        Self {
            writer,
            union,
            config,
            bytes_written: 0,
        }
    }
}

impl<'s, 'w, W: Write, S: Borrow<Schema>> SerializeTuple for OneUnionTupleSerializer<'s, 'w, W, S> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.bytes_written +=
            value.serialize(UnionSerializer::new(self.writer, self.union, self.config))?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.bytes_written)
    }
}
