use super::array::ArraySerializer;
use super::map::MapSerializer;
use super::record::RecordSerializer;
use super::tuple::{
    ManyTupleSerializer, OneUnionTupleSerializer, TupleSerializer, UnitTupleSerializer,
};
use super::{Config, MapOrRecordSerializer, SchemaAwareSerializer};
use crate::encode::{encode_int, encode_long};
use crate::error::Details;
use crate::schema::{RecordSchema, SchemaKind, UnionSchema};
use crate::serde::with::{BytesType, SER_BYTES_TYPE};
use crate::{Error, Schema};
use serde::{Serialize, Serializer};
use std::io::Write;

pub struct UnionAwareSerializer<'s, 'w, W: Write> {
    writer: &'w mut W,
    union_schema: &'s UnionSchema,
    config: Config<'s>,
}

impl<'s, 'w, W: Write> UnionAwareSerializer<'s, 'w, W> {
    pub fn new(writer: &'w mut W, union_schema: &'s UnionSchema, config: Config<'s>) -> Self {
        Self {
            writer,
            union_schema,
            config,
        }
    }

    fn error(&self, ty: &'static str, error: impl Into<String>) -> Error {
        Error::new(Details::SerializeValueWithSchema {
            value_type: ty,
            value: error.into(),
            schema: Schema::Union(self.union_schema.clone()),
        })
    }

    pub(super) fn serialize_int(self, original_ty: &'static str, v: i32) -> Result<usize, Error> {
        let Some(index) = self.union_schema.index().get(&SchemaKind::Int).copied() else {
            return Err(self.error(
                original_ty,
                "Expected Schema::Int | Schema::Date | Schema::TimeMillis in variants",
            ));
        };
        let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
        bytes_written += encode_int(v, &mut *self.writer)?;
        Ok(bytes_written)
    }

    pub(super) fn serialize_long(self, original_ty: &'static str, v: i64) -> Result<usize, Error> {
        let Some(index) = self.union_schema.index().get(&SchemaKind::Long).copied() else {
            return Err(self.error(original_ty, "Expected Schema::Long | Schema::TimeMicros | Schema::{,Local}Timestamp{Millis,Micros,Nanos} in variants"));
        };
        let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
        bytes_written += encode_long(v, &mut *self.writer)?;
        Ok(bytes_written)
    }

    fn find_named_schema(&self, name: &str) -> Result<Option<(usize, &'s Schema)>, Error> {
        self.union_schema
            .variants()
            .iter()
            .enumerate()
            .find(|(_i, s)| s.name().is_some_and(|n| n.name() == name))
            .map(|(i, s)| self.resolve_schema(s).map(|s| (i, s)))
            .transpose()
    }

    fn find_fixed_n(&self, len: usize) -> Result<Option<(usize, &'s Schema)>, Error> {
        for (index, schema) in self.union_schema.variants().iter().enumerate() {
            if let Schema::Fixed(fixed) = self.resolve_schema(schema)?
                && fixed.size == len
            {
                return Ok(Some((index, schema)));
            }
        }
        Ok(None)
    }

    fn find_record_n_fields(&self, len: usize) -> Result<Option<(usize, &'s RecordSchema)>, Error> {
        for (index, schema) in self.union_schema.variants().iter().enumerate() {
            if let Schema::Record(record) = self.resolve_schema(schema)?
                && record.fields.len() == len
            {
                return Ok(Some((index, record)));
            }
        }
        Ok(None)
    }

    fn resolve_schema(&self, schema: &'s Schema) -> Result<&'s Schema, Error> {
        if let Schema::Ref { name } = schema {
            self.config
                .names
                .get(name)
                .copied()
                .ok_or_else(|| Details::SchemaResolutionError(name.clone()).into())
        } else {
            Ok(schema)
        }
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        self.writer.write_all(bytes).map_err(Details::WriteBytes)?;

        Ok(bytes.len())
    }

    fn write_bytes_with_len(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        let mut bytes_written: usize = 0;

        bytes_written += encode_long(bytes.len() as i64, &mut *self.writer)?;
        bytes_written += self.write_bytes(bytes)?;

        Ok(bytes_written)
    }
}

impl<'s, 'w, W: Write> Serializer for UnionAwareSerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;
    type SerializeSeq = ArraySerializer<'s, 'w, W>;
    type SerializeTuple = TupleSerializer<'s, 'w, W>;
    type SerializeTupleStruct = ManyTupleSerializer<'s, 'w, W>;
    type SerializeTupleVariant = ManyTupleSerializer<'s, 'w, W>;
    type SerializeMap = MapOrRecordSerializer<'s, 'w, W>;
    type SerializeStruct = RecordSerializer<'s, 'w, W>;
    type SerializeStructVariant = RecordSerializer<'s, 'w, W>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        let Some(index) = self.union_schema.index().get(&SchemaKind::Boolean).copied() else {
            return Err(self.error("bool", "Expected Schema::Boolean in variants"));
        };
        let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
        self.writer
            .write_all(&[v as u8])
            .map_err(Details::WriteBytes)?;
        bytes_written += 1;
        Ok(bytes_written)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_int("i8", i32::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_int("i16", i32::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.serialize_int("i32", v)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.serialize_long("i64", v)
    }

    fn serialize_i128(mut self, v: i128) -> Result<Self::Ok, Self::Error> {
        if let Some((index, Schema::Fixed(fixed))) = self.find_named_schema("i128")?
            && fixed.size == 16
        {
            let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
            let bytes = v.to_le_bytes();
            bytes_written += self.write_bytes(&bytes)?;
            Ok(bytes_written)
        } else {
            Err(self.error(
                "i128",
                r#"Expected Schema::Fixed(name: "i128", size: 16) in variants"#,
            ))
        }
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_int("u8", i32::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_int("u16", i32::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.serialize_long("u32", i64::from(v))
    }

    fn serialize_u64(mut self, v: u64) -> Result<Self::Ok, Self::Error> {
        if let Some((index, Schema::Fixed(fixed))) = self.find_named_schema("u64")?
            && fixed.size == 8
        {
            let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
            let bytes = v.to_le_bytes();
            bytes_written += self.write_bytes(&bytes)?;
            Ok(bytes_written)
        } else {
            Err(self.error(
                "u64",
                r#"Expected Schema::Fixed(name: "u64", size: 8) in variants"#,
            ))
        }
    }

    fn serialize_u128(mut self, v: u128) -> Result<Self::Ok, Self::Error> {
        if let Some((index, Schema::Fixed(fixed))) = self.find_named_schema("u128")?
            && fixed.size == 16
        {
            let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
            let bytes = v.to_le_bytes();
            bytes_written += self.write_bytes(&bytes)?;
            Ok(bytes_written)
        } else {
            Err(self.error(
                "u64",
                r#"Expected Schema::Fixed(name: "u128", size: 16) in variants"#,
            ))
        }
    }

    fn serialize_f32(mut self, v: f32) -> Result<Self::Ok, Self::Error> {
        let Some(index) = self.union_schema.index().get(&SchemaKind::Float).copied() else {
            return Err(self.error("f32", "Expected Schema::Float in variants"));
        };
        let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
        let bytes = v.to_le_bytes();
        bytes_written += self.write_bytes(&bytes)?;
        Ok(bytes_written)
    }

    fn serialize_f64(mut self, v: f64) -> Result<Self::Ok, Self::Error> {
        let Some(index) = self.union_schema.index().get(&SchemaKind::Double).copied() else {
            return Err(self.error("f64", "Expected Schema::Double in variants"));
        };
        let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
        let bytes = v.to_le_bytes();
        bytes_written += self.write_bytes(&bytes)?;
        Ok(bytes_written)
    }

    fn serialize_char(mut self, v: char) -> Result<Self::Ok, Self::Error> {
        let Some(index) = self.union_schema.index().get(&SchemaKind::String).copied() else {
            return Err(self.error("char", "Expected Schema::String in variants"));
        };
        let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
        let bytes = v.to_string().into_bytes();
        bytes_written += self.write_bytes_with_len(&bytes)?;
        Ok(bytes_written)
    }

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        let Some(index) = self.union_schema.index().get(&SchemaKind::String).copied() else {
            return Err(self.error("str", "Expected Schema::String in variants"));
        };
        let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
        let bytes = v.as_bytes();
        bytes_written += self.write_bytes_with_len(&bytes)?;
        Ok(bytes_written)
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        let (index, schema) = match SER_BYTES_TYPE.get() {
            BytesType::Bytes => {
                let Some(index) = self.union_schema.index().get(&SchemaKind::Bytes).copied() else {
                    return Err(self.error("bytes", "Expected Schema::Bytes in variants"));
                };
                (index, &Schema::Bytes)
            }
            BytesType::Fixed => {
                let Some((index, schema)) = self.find_fixed_n(v.len())? else {
                    return Err(self.error(
                        "bytes",
                        format!("Expected Schema::Fixed(size = {}) in variants", v.len()),
                    ));
                };
                (index, schema)
            }
            BytesType::Unset => {
                let potential_bytes_index =
                    self.union_schema.index().get(&SchemaKind::Bytes).copied();
                let potential_fixed_index = self.find_fixed_n(v.len())?;
                match (potential_bytes_index, potential_fixed_index) {
                    (Some(bytes_index), Some((fixed_index, fixed_schema))) => {
                        if bytes_index < fixed_index {
                            (bytes_index, &Schema::Bytes)
                        } else {
                            (fixed_index, fixed_schema)
                        }
                    }
                    (Some(bytes_index), None) => (bytes_index, &Schema::Bytes),
                    (None, Some((fixed_index, fixed_schema))) => (fixed_index, fixed_schema),
                    (None, None) => {
                        return Err(self.error(
                            "bytes",
                            format!(
                                "Expected Schema::Bytes or Schema::Fixed(size: {}) in variants",
                                v.len()
                            ),
                        ));
                    }
                }
            }
        };
        let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
        match schema {
            Schema::Bytes => {
                bytes_written += self.write_bytes_with_len(v)?;
            }
            Schema::Fixed(_) => {
                bytes_written += self.write_bytes(v)?;
            }
            _ => unreachable!(),
        }
        Ok(bytes_written)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(self.error("None", "Nested unions are not supported"))
    }

    fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(self.error("Some", "Nested unions are not supported"))
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        let Some(index) = self.union_schema.index().get(&SchemaKind::Null).copied() else {
            return Err(self.error("()", "Expected Schema::Null in variants"));
        };
        encode_int(index as i32, self.writer)
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        let Some((index, Schema::Record(schema))) = self.find_named_schema(name)? else {
            return Err(self.error(
                "unit struct",
                format!("Expected Schema::Record(name: {name}, fields: []) in variants"),
            ));
        };
        if !schema.fields.is_empty() {
            return Err(self.error(
                "unit struct",
                format!("Expected Schema::Record(name: {name}, fields: []) in variants"),
            ));
        }
        encode_int(index as i32, self.writer)
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        if let Some((index, Schema::Enum(schema))) = self.find_named_schema(name)?
            && schema.symbols[variant_index as usize] == variant
        {
            let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
            bytes_written += encode_int(variant_index as i32, &mut *self.writer)?;
            Ok(bytes_written)
        } else {
            Err(self.error(
                "unit variant",
                format!(
                    "Expected Enum(name: {name}, symbols[{variant_index}] == {variant}) in variants"
                ),
            ))
        }
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        if let Some((index, Schema::Record(record))) = self.find_named_schema(name)?
            && record.fields.len() == 1
        {
            let mut bytes_written = encode_int(index as i32, &mut *self.writer)?;
            let ser =
                SchemaAwareSerializer::new(self.writer, &record.fields[0].schema, self.config)?;
            bytes_written += value.serialize(ser)?;
            Ok(bytes_written)
        } else {
            Err(self.error(
                "newtype struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == 1) in variants"),
            ))
        }
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(self.error("newtype struct", "Nested unions are not supported"))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let Some(index) = self.union_schema.index().get(&SchemaKind::Array).copied() else {
            return Err(self.error("seq", "Expected Schema::Array in variants"));
        };
        let Some(Schema::Array(schema)) = self.union_schema.variants().get(index) else {
            unreachable!("SchemaKind::Array at this index, so there must be a Schema:Array");
        };
        let bytes_written = encode_int(index as i32, &mut *self.writer)?;
        ArraySerializer::new(self.writer, schema, self.config, len, Some(bytes_written))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        if len == 0 {
            let index = self
                .union_schema
                .index()
                .get(&SchemaKind::Null)
                .copied()
                .ok_or_else(|| self.error("tuple", "Expected Schema::Null in variants"))?;
            let bytes_written = encode_int(index as i32, &mut *self.writer)?;
            Ok(TupleSerializer::Unit(UnitTupleSerializer::new(Some(
                bytes_written,
            ))))
        } else if len == 1 {
            Ok(TupleSerializer::OneUnion(OneUnionTupleSerializer::new(
                self.writer,
                self.union_schema,
                self.config,
            )))
        } else if let Some((index, record)) = self.find_record_n_fields(len)? {
            let bytes_written = encode_int(index as i32, &mut *self.writer)?;
            Ok(TupleSerializer::Many(ManyTupleSerializer::new(
                self.writer,
                record,
                self.config,
                Some(bytes_written),
            )))
        } else {
            Err(self.error(
                "tuple",
                format!(
                    "Expected Schema::Null | Schema::Record(fields.len() == {len}) in variants"
                ),
            ))
        }
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        if let Some((index, Schema::Record(schema))) = self.find_named_schema(name)?
            && schema.fields.len() == len
        {
            let bytes_written = encode_int(index as i32, &mut *self.writer)?;
            Ok(ManyTupleSerializer::new(
                self.writer,
                schema,
                self.config,
                Some(bytes_written),
            ))
        } else {
            Err(self.error(
                "newtype struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == {len}) in variants"),
            ))
        }
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(self.error("tuple variant", "Nested unions are not supported"))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        if let Some(index) = self.union_schema.index().get(&SchemaKind::Map).copied() {
            let Some(Schema::Map(map)) = self.union_schema.variants().get(index) else {
                unreachable!("SchemaKind::Map at this index, so there must be a Schema:Map");
            };
            let bytes_written = encode_int(index as i32, &mut *self.writer)?;
            Ok(MapOrRecordSerializer::Map(MapSerializer::new(
                self.writer,
                map,
                self.config,
                len,
                Some(bytes_written),
            )?))
        } else if let Some((index, record)) = self.find_record_n_fields(len.unwrap())? {
            let bytes_written = encode_int(index as i32, &mut *self.writer)?;
            Ok(MapOrRecordSerializer::Record(RecordSerializer::new(
                self.writer,
                record,
                self.config,
                Some(bytes_written),
            )))
        } else {
            Err(self.error("map", "Expected Schema::Map in variants"))
        }
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        if let Some((index, Schema::Record(schema))) = self.find_named_schema(name)? {
            let bytes_written = encode_int(index as i32, &mut *self.writer)?;
            Ok(RecordSerializer::new(
                self.writer,
                schema,
                self.config,
                Some(bytes_written),
            ))
        } else {
            Err(self.error(
                "struct",
                format!("Expect Schema::Record(name: {name}) in variants"),
            ))
        }
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(self.error("struct variant", "Nested unions are not supported"))
    }
}
