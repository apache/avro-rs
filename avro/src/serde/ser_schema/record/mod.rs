use std::{cmp::Ordering, collections::HashMap, io::Write};

use serde::ser::{Serialize, SerializeMap, SerializeStruct, SerializeStructVariant};

use super::{Config, SchemaAwareSerializer};
use crate::serde::ser_schema::record::field_default::SchemaAwareDefault;
use crate::{
    Error, Schema,
    error::Details,
    schema::{RecordField, RecordSchema},
    serde::util::StringSerializer,
};

mod field_default;

pub struct RecordSerializer<'s, 'w, W: Write> {
    writer: &'w mut W,
    schema: &'s RecordSchema,
    config: Config<'s>,
    /// Fields we received in the wrong order
    field_cache: HashMap<usize, Vec<u8>>,
    /// The current field name when serializing from a map (for `flatten` support).
    map_field_name: Option<String>,
    field_position: usize,
    bytes_written: usize,
}

impl<'s, 'w, W: Write> RecordSerializer<'s, 'w, W> {
    pub fn new(
        writer: &'w mut W,
        schema: &'s RecordSchema,
        config: Config<'s>,
        bytes_written: Option<usize>,
    ) -> Self {
        Self {
            writer,
            schema,
            config,
            field_cache: HashMap::new(),
            map_field_name: None,
            field_position: 0,
            bytes_written: bytes_written.unwrap_or(0),
        }
    }

    fn serialize_next_field<T>(
        &mut self,
        field: &RecordField,
        position: usize,
        value: &T,
    ) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        match self.field_position.cmp(&position) {
            Ordering::Equal => {
                // If we receive fields in order, write them directly to the main writer
                let value_ser =
                    SchemaAwareSerializer::new(&mut *self.writer, &field.schema, self.config)?;
                self.bytes_written += value.serialize(value_ser)?;

                self.field_position += 1;
                while let Some(bytes) = self.field_cache.remove(&self.field_position) {
                    self.writer.write_all(&bytes).map_err(Details::WriteBytes)?;
                    self.bytes_written += bytes.len();
                    self.field_position += 1;
                }
                Ok(())
            }
            Ordering::Less => {
                // Current field position is smaller than this field position,
                // so we're still missing at least one field, save this field temporarily
                let mut bytes = Vec::new();
                let value_ser = SchemaAwareSerializer::new(&mut bytes, &field.schema, self.config)?;
                value.serialize(value_ser)?;
                if self.field_cache.insert(position, bytes).is_some() {
                    Err(Details::FieldNameDuplicate(field.name.clone()).into())
                } else {
                    Ok(())
                }
            }
            Ordering::Greater => {
                // Current field position is greater than this field position,
                // so we've already had this field
                Err(Details::FieldNameDuplicate(field.name.clone()).into())
            }
        }
    }

    fn end(mut self) -> Result<usize, Error> {
        // Write any fields that are `serde(skip)` or `serde(skip_serializing)`
        while self.field_position != self.schema.fields.len() {
            let field_info = &self.schema.fields[self.field_position];
            if let Some(bytes) = self.field_cache.remove(&self.field_position) {
                self.writer.write_all(&bytes).map_err(Details::WriteBytes)?;
                self.bytes_written += bytes.len();
                self.field_position += 1;
            } else if let Some(default) = &field_info.default {
                self.serialize_next_field(
                    field_info,
                    self.field_position,
                    &SchemaAwareDefault::new(default, &field_info.schema),
                )
                .map_err(|e| Details::SerializeRecordFieldWithSchema {
                    field_name: field_info.name.clone(),
                    record_schema: Schema::Record(self.schema.clone()),
                    error: Box::new(e),
                })?;
            } else {
                return Err(Details::MissingDefaultForSkippedField {
                    field_name: field_info.name.clone(),
                    schema: Schema::Record(self.schema.clone()),
                }
                .into());
            }
        }

        debug_assert!(
            self.field_cache.is_empty(),
            "There should be no more unwritten fields at this point: {:?}",
            self.field_cache
        );
        debug_assert!(
            self.map_field_name.is_none(),
            "There should be no field name at this point: field {:?}",
            self.map_field_name
        );
        Ok(self.bytes_written)
    }
}

impl<'s, 'w, W: Write> SerializeStruct for RecordSerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        if let Some(position) = self.schema.lookup.get(key).copied() {
            let field = &self.schema.fields[position];
            self.serialize_next_field(field, position, value)
                .map_err(|e| {
                    Details::SerializeRecordFieldWithSchema {
                        field_name: key.to_string(),
                        record_schema: Schema::Record(self.schema.clone()),
                        error: Box::new(e),
                    }
                    .into()
                })
        } else {
            Err(Details::FieldName(String::from(key)).into())
        }
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), Self::Error> {
        if let Some(position) = self.schema.lookup.get(key).copied() {
            let field = &self.schema.fields[position];
            if let Some(default) = &field.default {
                self.serialize_next_field(field, position, default)
                    .map_err(|e| {
                        Details::SerializeRecordFieldWithSchema {
                            field_name: key.to_string(),
                            record_schema: Schema::Record(self.schema.clone()),
                            error: Box::new(e),
                        }
                        .into()
                    })
            } else {
                Err(Details::MissingDefaultForSkippedField {
                    field_name: key.to_string(),
                    schema: Schema::Record(self.schema.clone()),
                }
                .into())
            }
        } else {
            Err(Details::GetField(key.to_string()).into())
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

/// This implementation is used to support `#[serde(flatten)]` as that uses [`SerializeMap`] instead of [`SerializeStruct`].
///
/// [`SerializeMap`](ser::SerializeMap)
/// [`SerializeStruct`](ser::SerializeStruct)
impl<'s, 'w, W: Write> SerializeMap for RecordSerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let name = key.serialize(StringSerializer)?;
        let old = self.map_field_name.replace(name);
        debug_assert!(
            old.is_none(),
            "Expected a value instead of a key: old key: {old:?}, new key: {:?}",
            self.map_field_name
        );
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let key = self.map_field_name.take().ok_or(Details::MapNoKey)?;
        if let Some(position) = self.schema.lookup.get(&key).copied() {
            let field = &self.schema.fields[position];
            self.serialize_next_field(field, position, value)
                .map_err(|e| {
                    Details::SerializeRecordFieldWithSchema {
                        field_name: key.to_string(),
                        record_schema: Schema::Record(self.schema.clone()),
                        error: Box::new(e),
                    }
                    .into()
                })
        } else {
            Err(Details::FieldName(key).into())
        }
    }

    // TODO: Implement serialize_entry, which allows us to skip storing the key

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}

impl<'s, 'w, W: Write> SerializeStructVariant for RecordSerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        SerializeStruct::serialize_field(self, key, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.end()
    }
}
