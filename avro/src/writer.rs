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

//! Logic handling writing in Avro format at user level.
use crate::{
    AvroResult, Codec, Error,
    encode::{encode, encode_internal, encode_to_vec},
    error::Details,
    headers::{HeaderBuilder, RabinFingerprintHeader},
    schema::{NamesRef, ResolvedOwnedSchema, ResolvedSchema, Schema},
    serde::{AvroSchema, ser_schema::SchemaAwareWriteSerializer},
    types::Value,
};
use serde::Serialize;
use std::{
    collections::HashMap, io::Write, marker::PhantomData, mem::ManuallyDrop, ops::RangeInclusive,
};

const DEFAULT_BLOCK_SIZE: usize = 16000;
const AVRO_OBJECT_HEADER: &[u8] = b"Obj\x01";

/// Main interface for writing Avro formatted values.
///
/// It is critical to call flush before `Writer<W>` is dropped. Though dropping will attempt to flush
/// the contents of the buffer, any errors that happen in the process of dropping will be ignored.
/// Calling flush ensures that the buffer is empty and thus dropping will not even attempt file operations.
pub struct Writer<'a, W: Write> {
    schema: &'a Schema,
    writer: W,
    resolved_schema: ResolvedSchema<'a>,
    codec: Codec,
    block_size: usize,
    buffer: Vec<u8>,
    num_values: usize,
    marker: [u8; 16],
    has_header: bool,
    user_metadata: HashMap<String, Value>,
}

#[bon::bon]
impl<'a, W: Write> Writer<'a, W> {
    #[builder]
    pub fn builder(
        schema: &'a Schema,
        schemata: Option<Vec<&'a Schema>>,
        writer: W,
        #[builder(default = Codec::Null)] codec: Codec,
        #[builder(default = DEFAULT_BLOCK_SIZE)] block_size: usize,
        #[builder(default = generate_sync_marker())] marker: [u8; 16],
        /// Has the header already been written.
        ///
        /// To disable writing the header, this can be set to `true`.
        #[builder(default = false)]
        has_header: bool,
        #[builder(default)] user_metadata: HashMap<String, Value>,
    ) -> AvroResult<Self> {
        let resolved_schema = if let Some(schemata) = schemata {
            ResolvedSchema::try_from(schemata)?
        } else {
            ResolvedSchema::try_from(schema)?
        };
        Ok(Self {
            schema,
            writer,
            resolved_schema,
            codec,
            block_size,
            buffer: Vec::with_capacity(block_size),
            num_values: 0,
            marker,
            has_header,
            user_metadata,
        })
    }
}

impl<'a, W: Write> Writer<'a, W> {
    /// Creates a `Writer` given a `Schema` and something implementing the `io::Write` trait to write
    /// to.
    /// No compression `Codec` will be used.
    pub fn new(schema: &'a Schema, writer: W) -> AvroResult<Self> {
        Writer::with_codec(schema, writer, Codec::Null)
    }

    /// Creates a `Writer` with a specific `Codec` given a `Schema` and something implementing the
    /// `io::Write` trait to write to.
    pub fn with_codec(schema: &'a Schema, writer: W, codec: Codec) -> AvroResult<Self> {
        Self::builder()
            .schema(schema)
            .writer(writer)
            .codec(codec)
            .build()
    }

    /// Creates a `Writer` with a specific `Codec` given a `Schema` and something implementing the
    /// `io::Write` trait to write to.
    /// If the `schema` is incomplete, i.e. contains `Schema::Ref`s then all dependencies must
    /// be provided in `schemata`.
    pub fn with_schemata(
        schema: &'a Schema,
        schemata: Vec<&'a Schema>,
        writer: W,
        codec: Codec,
    ) -> AvroResult<Self> {
        Self::builder()
            .schema(schema)
            .schemata(schemata)
            .writer(writer)
            .codec(codec)
            .build()
    }

    /// Creates a `Writer` that will append values to already populated
    /// `std::io::Write` using the provided `marker`
    /// No compression `Codec` will be used.
    pub fn append_to(schema: &'a Schema, writer: W, marker: [u8; 16]) -> AvroResult<Self> {
        Writer::append_to_with_codec(schema, writer, Codec::Null, marker)
    }

    /// Creates a `Writer` that will append values to already populated
    /// `std::io::Write` using the provided `marker`
    pub fn append_to_with_codec(
        schema: &'a Schema,
        writer: W,
        codec: Codec,
        marker: [u8; 16],
    ) -> AvroResult<Self> {
        Self::builder()
            .schema(schema)
            .writer(writer)
            .codec(codec)
            .marker(marker)
            .has_header(true)
            .build()
    }

    /// Creates a `Writer` that will append values to already populated
    /// `std::io::Write` using the provided `marker`
    pub fn append_to_with_codec_schemata(
        schema: &'a Schema,
        schemata: Vec<&'a Schema>,
        writer: W,
        codec: Codec,
        marker: [u8; 16],
    ) -> AvroResult<Self> {
        Self::builder()
            .schema(schema)
            .schemata(schemata)
            .writer(writer)
            .codec(codec)
            .marker(marker)
            .has_header(true)
            .build()
    }

    /// Get a reference to the `Schema` associated to a `Writer`.
    pub fn schema(&self) -> &'a Schema {
        self.schema
    }

    /// Deprecated. Use [`Writer::append_value`] instead.
    #[deprecated(since = "0.22.0", note = "Use `Writer::append_value` instead")]
    pub fn append<T: Into<Value>>(&mut self, value: T) -> AvroResult<usize> {
        self.append_value(value)
    }

    /// Append a value to the `Writer`, also performs schema validation.
    ///
    /// Returns the number of bytes written (it might be 0, see below).
    ///
    /// **NOTE**: This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](Writer::flush).
    pub fn append_value<T: Into<Value>>(&mut self, value: T) -> AvroResult<usize> {
        let avro = value.into();
        self.append_value_ref(&avro)
    }

    /// Append a compatible value to a `Writer`, also performs schema validation.
    ///
    /// Returns the number of bytes written (it might be 0, see below).
    ///
    /// **NOTE**: This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](Writer::flush).
    pub fn append_value_ref(&mut self, value: &Value) -> AvroResult<usize> {
        if let Some(reason) = value.validate_internal(
            self.schema,
            self.resolved_schema.get_names(),
            self.schema.namespace(),
        ) {
            return Err(Details::ValidationWithReason {
                value: value.clone(),
                schema: self.schema.clone(),
                reason,
            }
            .into());
        }
        self.unvalidated_append_value_ref(value)
    }

    /// Append a compatible value to a `Writer`.
    ///
    /// This function does **not** validate that the provided value matches the schema. If it does
    /// not match, the file will contain corrupt data. Use [`Writer::append_value`] to have the
    /// value validated during write or use [`Value::validate`] to validate the value.
    ///
    /// Returns the number of bytes written (it might be 0, see below).
    ///
    /// **NOTE**: This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](Writer::flush).
    pub fn unvalidated_append_value<T: Into<Value>>(&mut self, value: T) -> AvroResult<usize> {
        let value = value.into();
        self.unvalidated_append_value_ref(&value)
    }

    /// Append a compatible value to a `Writer`.
    ///
    /// This function does **not** validate that the provided value matches the schema. If it does
    /// not match, the file will contain corrupt data. Use [`Writer::append_value_ref`] to have the
    /// value validated during write or use [`Value::validate`] to validate the value.
    ///
    /// Returns the number of bytes written (it might be 0, see below).
    ///
    /// **NOTE**: This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](Writer::flush).
    pub fn unvalidated_append_value_ref(&mut self, value: &Value) -> AvroResult<usize> {
        let n = self.maybe_write_header()?;
        encode_internal(
            value,
            self.schema,
            self.resolved_schema.get_names(),
            self.schema.namespace(),
            &mut self.buffer,
        )?;

        self.num_values += 1;

        if self.buffer.len() >= self.block_size {
            return self.flush().map(|b| b + n);
        }

        Ok(n)
    }

    /// Append anything implementing the `Serialize` trait to a `Writer` for
    /// [`serde`](https://docs.serde.rs/serde/index.html) compatibility, also performing schema
    /// validation.
    ///
    /// Returns the number of bytes written.
    ///
    /// **NOTE**: This function is not guaranteed to perform any actual write, since it relies on
    /// internal buffering for performance reasons. If you want to be sure the value has been
    /// written, then call [`flush`](Writer::flush).
    pub fn append_ser<S: Serialize>(&mut self, value: S) -> AvroResult<usize> {
        let n = self.maybe_write_header()?;

        let mut serializer = SchemaAwareWriteSerializer::new(
            &mut self.buffer,
            self.schema,
            self.resolved_schema.get_names(),
            None,
        );
        value.serialize(&mut serializer)?;
        self.num_values += 1;

        if self.buffer.len() >= self.block_size {
            return self.flush().map(|b| b + n);
        }

        Ok(n)
    }

    /// Extend a `Writer` with an `Iterator` of values, also performs schema validation.
    ///
    /// Returns the number of bytes written.
    ///
    /// **NOTE**: This function forces the written data to be flushed (an implicit
    /// call to [`flush`](Writer::flush) is performed).
    pub fn extend<I, T: Into<Value>>(&mut self, values: I) -> AvroResult<usize>
    where
        I: IntoIterator<Item = T>,
    {
        /*
        https://github.com/rust-lang/rfcs/issues/811 :(
        let mut stream = values
            .filter_map(|value| value.serialize(&mut self.serializer).ok())
            .map(|value| value.encode(self.schema))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| err_msg("value does not match given schema"))?
            .into_iter()
            .fold(Vec::new(), |mut acc, stream| {
                num_values += 1;
                acc.extend(stream); acc
            });
        */

        let mut num_bytes = 0;
        for value in values {
            num_bytes += self.append_value(value)?;
        }
        num_bytes += self.flush()?;

        Ok(num_bytes)
    }

    /// Extend a `Writer` with an `Iterator` of anything implementing the `Serialize` trait for
    /// [`serde`](https://docs.serde.rs/serde/index.html) compatibility, also performing schema
    /// validation.
    ///
    /// Returns the number of bytes written.
    ///
    /// **NOTE**: This function forces the written data to be flushed (an implicit
    /// call to [`flush`](Writer::flush) is performed).
    pub fn extend_ser<I, T: Serialize>(&mut self, values: I) -> AvroResult<usize>
    where
        I: IntoIterator<Item = T>,
    {
        /*
        https://github.com/rust-lang/rfcs/issues/811 :(
        let mut stream = values
            .filter_map(|value| value.serialize(&mut self.serializer).ok())
            .map(|value| value.encode(self.schema))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| err_msg("value does not match given schema"))?
            .into_iter()
            .fold(Vec::new(), |mut acc, stream| {
                num_values += 1;
                acc.extend(stream); acc
            });
        */

        let mut num_bytes = 0;
        for value in values {
            num_bytes += self.append_ser(value)?;
        }
        num_bytes += self.flush()?;

        Ok(num_bytes)
    }

    /// Extend a `Writer` by appending each `Value` from a slice, while also performing schema
    /// validation on each value appended.
    ///
    /// Returns the number of bytes written.
    ///
    /// **NOTE**: This function forces the written data to be flushed (an implicit
    /// call to [`flush`](Writer::flush) is performed).
    pub fn extend_from_slice(&mut self, values: &[Value]) -> AvroResult<usize> {
        let mut num_bytes = 0;
        for value in values {
            num_bytes += self.append_value_ref(value)?;
        }
        num_bytes += self.flush()?;

        Ok(num_bytes)
    }

    /// Flush the content to the inner `Writer`.
    ///
    /// Call this function to make sure all the content has been written before releasing the `Writer`.
    /// This will also write the header if it wasn't written yet and hasn't been disabled using
    /// [`WriterBuilder::has_header`].
    ///
    /// Returns the number of bytes written.
    pub fn flush(&mut self) -> AvroResult<usize> {
        let mut num_bytes = self.maybe_write_header()?;
        if self.num_values == 0 {
            return Ok(num_bytes);
        }

        self.codec.compress(&mut self.buffer)?;

        let num_values = self.num_values;
        let stream_len = self.buffer.len();

        num_bytes += self.append_raw(&num_values.try_into()?, &Schema::Long)?
            + self.append_raw(&stream_len.try_into()?, &Schema::Long)?
            + self
                .writer
                .write(self.buffer.as_ref())
                .map_err(Details::WriteBytes)?
            + self.append_marker()?;

        self.buffer.clear();
        self.num_values = 0;

        self.writer.flush().map_err(Details::FlushWriter)?;

        Ok(num_bytes)
    }

    /// Return what the `Writer` is writing to, consuming the `Writer` itself.
    ///
    /// **NOTE**: This function forces the written data to be flushed (an implicit
    /// call to [`flush`](Writer::flush) is performed).
    pub fn into_inner(mut self) -> AvroResult<W> {
        self.maybe_write_header()?;
        self.flush()?;

        let mut this = ManuallyDrop::new(self);

        // Extract every member that is not Copy and therefore should be dropped
        let _buffer = std::mem::take(&mut this.buffer);
        let _user_metadata = std::mem::take(&mut this.user_metadata);
        // SAFETY: resolved schema is not accessed after this and won't be dropped because of ManuallyDrop
        unsafe { std::ptr::drop_in_place(&mut this.resolved_schema) };

        // SAFETY: double-drops are prevented by putting `this` in a ManuallyDrop that is never dropped
        let writer = unsafe { std::ptr::read(&this.writer) };

        Ok(writer)
    }

    /// Gets a reference to the underlying writer.
    ///
    /// **NOTE**: There is likely data still in the buffer. To have all the data
    /// in the writer call [`flush`](Writer::flush) first.
    pub fn get_ref(&self) -> &W {
        &self.writer
    }

    /// Gets a mutable reference to the underlying writer.
    ///
    /// It is inadvisable to directly write to the underlying writer.
    ///
    /// **NOTE**: There is likely data still in the buffer. To have all the data
    /// in the writer call [`flush`](Writer::flush) first.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Generate and append synchronization marker to the payload.
    fn append_marker(&mut self) -> AvroResult<usize> {
        // using .writer.write directly to avoid mutable borrow of self
        // with ref borrowing of self.marker
        self.writer
            .write(&self.marker)
            .map_err(|e| Details::WriteMarker(e).into())
    }

    /// Append a raw Avro Value to the payload avoiding to encode it again.
    fn append_raw(&mut self, value: &Value, schema: &Schema) -> AvroResult<usize> {
        self.append_bytes(encode_to_vec(value, schema)?.as_ref())
    }

    /// Append pure bytes to the payload.
    fn append_bytes(&mut self, bytes: &[u8]) -> AvroResult<usize> {
        self.writer
            .write(bytes)
            .map_err(|e| Details::WriteBytes(e).into())
    }

    /// Adds custom metadata to the file.
    /// This method could be used only before adding the first record to the writer.
    pub fn add_user_metadata<T: AsRef<[u8]>>(&mut self, key: String, value: T) -> AvroResult<()> {
        if !self.has_header {
            if key.starts_with("avro.") {
                return Err(Details::InvalidMetadataKey(key).into());
            }
            self.user_metadata
                .insert(key, Value::Bytes(value.as_ref().to_vec()));
            Ok(())
        } else {
            Err(Details::FileHeaderAlreadyWritten.into())
        }
    }

    /// Create an Avro header based on schema, codec and sync marker.
    fn header(&self) -> Result<Vec<u8>, Error> {
        let schema_bytes = serde_json::to_string(self.schema)
            .map_err(Details::ConvertJsonToString)?
            .into_bytes();

        let mut metadata = HashMap::with_capacity(2);
        metadata.insert("avro.schema", Value::Bytes(schema_bytes));
        if self.codec != Codec::Null {
            metadata.insert("avro.codec", self.codec.into());
        }
        match self.codec {
            #[cfg(feature = "bzip")]
            Codec::Bzip2(settings) => {
                metadata.insert(
                    "avro.codec.compression_level",
                    Value::Bytes(vec![settings.compression_level]),
                );
            }
            #[cfg(feature = "xz")]
            Codec::Xz(settings) => {
                metadata.insert(
                    "avro.codec.compression_level",
                    Value::Bytes(vec![settings.compression_level]),
                );
            }
            #[cfg(feature = "zstandard")]
            Codec::Zstandard(settings) => {
                metadata.insert(
                    "avro.codec.compression_level",
                    Value::Bytes(vec![settings.compression_level]),
                );
            }
            _ => {}
        }

        for (k, v) in &self.user_metadata {
            metadata.insert(k.as_str(), v.clone());
        }

        let mut header = Vec::new();
        header.extend_from_slice(AVRO_OBJECT_HEADER);
        encode(
            &metadata.into(),
            &Schema::map(Schema::Bytes).build(),
            &mut header,
        )?;
        header.extend_from_slice(&self.marker);

        Ok(header)
    }

    fn maybe_write_header(&mut self) -> AvroResult<usize> {
        if !self.has_header {
            let header = self.header()?;
            let n = self.append_bytes(header.as_ref())?;
            self.has_header = true;
            Ok(n)
        } else {
            Ok(0)
        }
    }
}

/// A buffer that can be cleared.
pub trait Clearable {
    /// Clear the buffer.
    fn clear(&mut self);
}

impl Clearable for Vec<u8> {
    fn clear(&mut self) {
        Vec::clear(self);
    }
}

impl<'a, W: Clearable + Write> Writer<'a, W> {
    /// Reset the writer.
    ///
    /// This will clear the underlying writer, the internal buffer, and the user metadata.
    /// It will also generate a new sync marker.
    ///
    /// # Example
    /// ```
    /// # use apache_avro::{Writer, Schema, Error};
    /// # let schema = Schema::Boolean;
    /// # let values = [true, false];
    /// # fn send(_: &Vec<u8>) {}
    /// let mut writer = Writer::new(&schema, Vec::new())?;
    ///
    /// // Write some values
    /// for value in values {
    ///     writer.append_value(value)?;
    /// }
    ///
    /// // Flush the buffer and only then do something with buffer
    /// writer.flush()?;
    /// send(writer.get_ref());
    ///
    /// // Reset the writer
    /// writer.reset();
    ///
    /// // Write some values again
    /// for value in values {
    ///     writer.append_value(value)?;
    /// }
    ///
    /// # Ok::<(), Error>(())
    /// ```
    ///
    /// # Warning
    /// Any data that has been appended but not yet flushed will be silently
    /// discarded. Call [`flush`](Writer::flush) before `reset()` if you need
    /// to preserve in-flight records.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.writer.clear();
        self.has_header = false;
        self.num_values = 0;
        self.user_metadata.clear();
        self.marker = generate_sync_marker();
    }
}

impl<W: Write> Drop for Writer<'_, W> {
    /// Drop the writer, will try to flush ignoring any errors.
    fn drop(&mut self) {
        let _ = self.maybe_write_header();
        let _ = self.flush();
    }
}

/// Encode a value into raw Avro data, also performs schema validation.
///
/// This is an internal function which gets the bytes buffer where to write as parameter instead of
/// creating a new one like `to_avro_datum`.
fn write_avro_datum<T: Into<Value>, W: Write>(
    schema: &Schema,
    value: T,
    writer: &mut W,
) -> Result<(), Error> {
    let avro = value.into();
    if !avro.validate(schema) {
        return Err(Details::Validation.into());
    }
    encode(&avro, schema, writer)?;
    Ok(())
}

fn write_avro_datum_schemata<T: Into<Value>>(
    schema: &Schema,
    schemata: Vec<&Schema>,
    value: T,
    buffer: &mut Vec<u8>,
) -> AvroResult<usize> {
    let avro = value.into();
    let rs = ResolvedSchema::try_from(schemata)?;
    let names = rs.get_names();
    let enclosing_namespace = schema.namespace();
    if let Some(_err) = avro.validate_internal(schema, names, enclosing_namespace) {
        return Err(Details::Validation.into());
    }
    encode_internal(&avro, schema, names, enclosing_namespace, buffer)
}

/// Writer that encodes messages according to the single object encoding v1 spec
/// Uses an API similar to the current File Writer
/// Writes all object bytes at once, and drains internal buffer
pub struct GenericSingleObjectWriter {
    buffer: Vec<u8>,
    resolved: ResolvedOwnedSchema,
}

impl GenericSingleObjectWriter {
    pub fn new_with_capacity(
        schema: &Schema,
        initial_buffer_cap: usize,
    ) -> AvroResult<GenericSingleObjectWriter> {
        let header_builder = RabinFingerprintHeader::from_schema(schema);
        Self::new_with_capacity_and_header_builder(schema, initial_buffer_cap, header_builder)
    }

    pub fn new_with_capacity_and_header_builder<HB: HeaderBuilder>(
        schema: &Schema,
        initial_buffer_cap: usize,
        header_builder: HB,
    ) -> AvroResult<GenericSingleObjectWriter> {
        let mut buffer = Vec::with_capacity(initial_buffer_cap);
        let header = header_builder.build_header();
        buffer.extend_from_slice(&header);

        Ok(GenericSingleObjectWriter {
            buffer,
            resolved: ResolvedOwnedSchema::try_from(schema.clone())?,
        })
    }

    const HEADER_LENGTH_RANGE: RangeInclusive<usize> = 10_usize..=20_usize;

    /// Write the referenced Value to the provided Write object. Returns a result with the number of bytes written including the header
    pub fn write_value_ref<W: Write>(&mut self, v: &Value, writer: &mut W) -> AvroResult<usize> {
        let original_length = self.buffer.len();
        if !Self::HEADER_LENGTH_RANGE.contains(&original_length) {
            Err(Details::IllegalSingleObjectWriterState.into())
        } else {
            write_value_ref_owned_resolved(&self.resolved, v, &mut self.buffer)?;
            writer
                .write_all(&self.buffer)
                .map_err(Details::WriteBytes)?;
            let len = self.buffer.len();
            self.buffer.truncate(original_length);
            Ok(len)
        }
    }

    /// Write the Value to the provided Write object. Returns a result with the number of bytes written including the header
    pub fn write_value<W: Write>(&mut self, v: Value, writer: &mut W) -> AvroResult<usize> {
        self.write_value_ref(&v, writer)
    }
}

/// Writer that encodes messages according to the single object encoding v1 spec
pub struct SpecificSingleObjectWriter<T>
where
    T: AvroSchema,
{
    resolved: ResolvedOwnedSchema,
    header: Vec<u8>,
    _model: PhantomData<T>,
}

impl<T> SpecificSingleObjectWriter<T>
where
    T: AvroSchema,
{
    pub fn new() -> AvroResult<Self> {
        let schema = T::get_schema();
        let header = RabinFingerprintHeader::from_schema(&schema).build_header();
        let resolved = ResolvedOwnedSchema::new(schema)?;
        // We don't use Self::new_with_header_builder as that would mean calling T::get_schema() twice
        Ok(Self {
            resolved,
            header,
            _model: PhantomData,
        })
    }

    pub fn new_with_header_builder(header_builder: impl HeaderBuilder) -> AvroResult<Self> {
        let header = header_builder.build_header();
        let resolved = ResolvedOwnedSchema::new(T::get_schema())?;
        Ok(Self {
            resolved,
            header,
            _model: PhantomData,
        })
    }

    /// Deprecated. Use [`SpecificSingleObjectWriter::new`] instead.
    #[deprecated(since = "0.22.0", note = "Use new() instead")]
    pub fn with_capacity(_buffer_cap: usize) -> AvroResult<Self> {
        Self::new()
    }
}

impl<T> SpecificSingleObjectWriter<T>
where
    T: AvroSchema + Into<Value>,
{
    /// Write the value to the writer
    ///
    /// Returns the number of bytes written.
    ///
    /// Each call writes a complete single-object encoded message (header + data),
    /// making each message independently decodable.
    pub fn write_value<W: Write>(&self, data: T, writer: &mut W) -> AvroResult<usize> {
        writer
            .write_all(&self.header)
            .map_err(Details::WriteBytes)?;
        let value: Value = data.into();
        let bytes = write_value_ref_owned_resolved(&self.resolved, &value, writer)?;
        Ok(bytes + self.header.len())
    }
}

impl<T> SpecificSingleObjectWriter<T>
where
    T: AvroSchema + Serialize,
{
    /// Write the object to the writer.
    ///
    /// Returns the number of bytes written.
    ///
    /// Each call writes a complete single-object encoded message (header + data),
    /// making each message independently decodable.
    pub fn write_ref<W: Write>(&self, data: &T, writer: &mut W) -> AvroResult<usize> {
        writer
            .write_all(&self.header)
            .map_err(Details::WriteBytes)?;

        let bytes = write_avro_datum_ref(
            self.resolved.get_root_schema(),
            self.resolved.get_names(),
            data,
            writer,
        )?;

        Ok(bytes + self.header.len())
    }

    /// Write the object to the writer.
    ///
    /// Returns the number of bytes written.
    ///
    /// Each call writes a complete single-object encoded message (header + data),
    /// making each message independently decodable.
    pub fn write<W: Write>(&self, data: T, writer: &mut W) -> AvroResult<usize> {
        self.write_ref(&data, writer)
    }
}

fn write_value_ref_owned_resolved<W: Write>(
    resolved_schema: &ResolvedOwnedSchema,
    value: &Value,
    writer: &mut W,
) -> AvroResult<usize> {
    let root_schema = resolved_schema.get_root_schema();
    if let Some(reason) = value.validate_internal(
        root_schema,
        resolved_schema.get_names(),
        root_schema.namespace(),
    ) {
        return Err(Details::ValidationWithReason {
            value: value.clone(),
            schema: root_schema.clone(),
            reason,
        }
        .into());
    }
    encode_internal(
        value,
        root_schema,
        resolved_schema.get_names(),
        root_schema.namespace(),
        writer,
    )
}

/// Encode a value into raw Avro data, also performs schema validation.
///
/// **NOTE**: This function has a quite small niche of usage and does NOT generate headers and sync
/// markers; use [`Writer`] to be fully Avro-compatible if you don't know what
/// you are doing, instead.
pub fn to_avro_datum<T: Into<Value>>(schema: &Schema, value: T) -> AvroResult<Vec<u8>> {
    let mut buffer = Vec::new();
    write_avro_datum(schema, value, &mut buffer)?;
    Ok(buffer)
}

/// Write the referenced [Serialize]able object to the provided [Write] object.
///
/// Returns a result with the number of bytes written.
///
/// **NOTE**: This function has a quite small niche of usage and does **NOT** generate headers and sync
/// markers; use [`append_ser`](Writer::append_ser) to be fully Avro-compatible
/// if you don't know what you are doing, instead.
pub fn write_avro_datum_ref<T: Serialize, W: Write>(
    schema: &Schema,
    names: &NamesRef,
    data: &T,
    writer: &mut W,
) -> AvroResult<usize> {
    let mut serializer = SchemaAwareWriteSerializer::new(writer, schema, names, None);
    data.serialize(&mut serializer)
}

/// Encode a value into raw Avro data, also performs schema validation.
///
/// If the provided `schema` is incomplete then its dependencies must be
/// provided in `schemata`
pub fn to_avro_datum_schemata<T: Into<Value>>(
    schema: &Schema,
    schemata: Vec<&Schema>,
    value: T,
) -> AvroResult<Vec<u8>> {
    let mut buffer = Vec::new();
    write_avro_datum_schemata(schema, schemata, value, &mut buffer)?;
    Ok(buffer)
}

#[cfg(not(target_arch = "wasm32"))]
fn generate_sync_marker() -> [u8; 16] {
    rand::random()
}

#[cfg(target_arch = "wasm32")]
fn generate_sync_marker() -> [u8; 16] {
    let mut marker = [0_u8; 16];
    std::iter::repeat_with(quad_rand::rand)
        .take(4)
        .flat_map(|i| i.to_be_bytes())
        .enumerate()
        .for_each(|(i, n)| marker[i] = n);
    marker
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use super::*;
    use crate::{
        Reader,
        decimal::Decimal,
        duration::{Days, Duration, Millis, Months},
        headers::GlueSchemaUuidHeader,
        rabin::Rabin,
        schema::{DecimalSchema, FixedSchema, Name},
        types::Record,
        util::zig_i64,
    };
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use crate::schema::InnerDecimalSchema;
    use crate::{codec::DeflateSettings, error::Details};
    use apache_avro_test_helper::TestResult;

    const AVRO_OBJECT_HEADER_LEN: usize = AVRO_OBJECT_HEADER.len();

    const SCHEMA: &str = r#"
    {
      "type": "record",
      "name": "test",
      "fields": [
        {
          "name": "a",
          "type": "long",
          "default": 42
        },
        {
          "name": "b",
          "type": "string"
        }
      ]
    }
    "#;

    const UNION_SCHEMA: &str = r#"["null", "long"]"#;

    #[test]
    fn test_to_avro_datum() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        let mut expected = Vec::new();
        zig_i64(27, &mut expected)?;
        zig_i64(3, &mut expected)?;
        expected.extend([b'f', b'o', b'o']);

        assert_eq!(to_avro_datum(&schema, record)?, expected);

        Ok(())
    }

    #[test]
    fn avro_rs_193_write_avro_datum_ref() -> TestResult {
        #[derive(Serialize)]
        struct TestStruct {
            a: i64,
            b: String,
        }

        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer: Vec<u8> = Vec::new();
        let data = TestStruct {
            a: 27,
            b: "foo".to_string(),
        };

        let mut expected = Vec::new();
        zig_i64(27, &mut expected)?;
        zig_i64(3, &mut expected)?;
        expected.extend([b'f', b'o', b'o']);

        let bytes = write_avro_datum_ref(&schema, &HashMap::new(), &data, &mut writer)?;

        assert_eq!(bytes, expected.len());
        assert_eq!(writer, expected);

        Ok(())
    }

    #[test]
    fn avro_rs_220_flush_write_header() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;

        // By default flush should write the header even if nothing was added yet
        let mut writer = Writer::new(&schema, Vec::new())?;
        writer.flush()?;
        let result = writer.into_inner()?;
        assert_eq!(result.len(), 147);

        // Unless the user indicates via the builder that the header has already been written
        let mut writer = Writer::builder()
            .has_header(true)
            .schema(&schema)
            .writer(Vec::new())
            .build()?;
        writer.flush()?;
        let result = writer.into_inner()?;
        assert_eq!(result.len(), 0);

        Ok(())
    }

    #[test]
    fn test_union_not_null() -> TestResult {
        let schema = Schema::parse_str(UNION_SCHEMA)?;
        let union = Value::Union(1, Box::new(Value::Long(3)));

        let mut expected = Vec::new();
        zig_i64(1, &mut expected)?;
        zig_i64(3, &mut expected)?;

        assert_eq!(to_avro_datum(&schema, union)?, expected);

        Ok(())
    }

    #[test]
    fn test_union_null() -> TestResult {
        let schema = Schema::parse_str(UNION_SCHEMA)?;
        let union = Value::Union(0, Box::new(Value::Null));

        let mut expected = Vec::new();
        zig_i64(0, &mut expected)?;

        assert_eq!(to_avro_datum(&schema, union)?, expected);

        Ok(())
    }

    fn logical_type_test<T: Into<Value> + Clone>(
        schema_str: &'static str,

        expected_schema: &Schema,
        value: Value,

        raw_schema: &Schema,
        raw_value: T,
    ) -> TestResult {
        let schema = Schema::parse_str(schema_str)?;
        assert_eq!(&schema, expected_schema);
        // The serialized format should be the same as the schema.
        let ser = to_avro_datum(&schema, value.clone())?;
        let raw_ser = to_avro_datum(raw_schema, raw_value)?;
        assert_eq!(ser, raw_ser);

        // Should deserialize from the schema into the logical type.
        let mut r = ser.as_slice();
        let de = crate::from_avro_datum(&schema, &mut r, None)?;
        assert_eq!(de, value);
        Ok(())
    }

    #[test]
    fn date() -> TestResult {
        logical_type_test(
            r#"{"type": "int", "logicalType": "date"}"#,
            &Schema::Date,
            Value::Date(1_i32),
            &Schema::Int,
            1_i32,
        )
    }

    #[test]
    fn time_millis() -> TestResult {
        logical_type_test(
            r#"{"type": "int", "logicalType": "time-millis"}"#,
            &Schema::TimeMillis,
            Value::TimeMillis(1_i32),
            &Schema::Int,
            1_i32,
        )
    }

    #[test]
    fn time_micros() -> TestResult {
        logical_type_test(
            r#"{"type": "long", "logicalType": "time-micros"}"#,
            &Schema::TimeMicros,
            Value::TimeMicros(1_i64),
            &Schema::Long,
            1_i64,
        )
    }

    #[test]
    fn timestamp_millis() -> TestResult {
        logical_type_test(
            r#"{"type": "long", "logicalType": "timestamp-millis"}"#,
            &Schema::TimestampMillis,
            Value::TimestampMillis(1_i64),
            &Schema::Long,
            1_i64,
        )
    }

    #[test]
    fn timestamp_micros() -> TestResult {
        logical_type_test(
            r#"{"type": "long", "logicalType": "timestamp-micros"}"#,
            &Schema::TimestampMicros,
            Value::TimestampMicros(1_i64),
            &Schema::Long,
            1_i64,
        )
    }

    #[test]
    fn decimal_fixed() -> TestResult {
        let size = 30;
        let fixed = FixedSchema {
            name: Name::new("decimal")?,
            aliases: None,
            doc: None,
            size,
            attributes: Default::default(),
        };
        let inner = InnerDecimalSchema::Fixed(fixed.clone());
        let value = vec![0u8; size];
        logical_type_test(
            r#"{"type": {"type": "fixed", "size": 30, "name": "decimal"}, "logicalType": "decimal", "precision": 20, "scale": 5}"#,
            &Schema::Decimal(DecimalSchema {
                precision: 20,
                scale: 5,
                inner,
            }),
            Value::Decimal(Decimal::from(value.clone())),
            &Schema::Fixed(fixed),
            Value::Fixed(size, value),
        )
    }

    #[test]
    fn decimal_bytes() -> TestResult {
        let value = vec![0u8; 10];
        logical_type_test(
            r#"{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 3}"#,
            &Schema::Decimal(DecimalSchema {
                precision: 4,
                scale: 3,
                inner: InnerDecimalSchema::Bytes,
            }),
            Value::Decimal(Decimal::from(value.clone())),
            &Schema::Bytes,
            value,
        )
    }

    #[test]
    fn duration() -> TestResult {
        let inner = Schema::Fixed(FixedSchema {
            name: Name::new("duration")?,
            aliases: None,
            doc: None,
            size: 12,
            attributes: Default::default(),
        });
        let value = Value::Duration(Duration::new(
            Months::new(256),
            Days::new(512),
            Millis::new(1024),
        ));
        logical_type_test(
            r#"{"type": {"type": "fixed", "name": "duration", "size": 12}, "logicalType": "duration"}"#,
            &Schema::Duration(FixedSchema {
                name: Name::try_from("duration").expect("Name is valid"),
                aliases: None,
                doc: None,
                size: 12,
                attributes: Default::default(),
            }),
            value,
            &inner,
            Value::Fixed(12, vec![0, 1, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0]),
        )
    }

    #[test]
    fn test_writer_append() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        let n1 = writer.append_value(record.clone())?;
        let n2 = writer.append_value(record.clone())?;
        let n3 = writer.flush()?;
        let result = writer.into_inner()?;

        assert_eq!(n1 + n2 + n3, result.len());

        let mut data = Vec::new();
        zig_i64(27, &mut data)?;
        zig_i64(3, &mut data)?;
        data.extend(b"foo");
        data.extend(data.clone());

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );

        Ok(())
    }

    #[test]
    fn test_writer_extend() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        let record_copy = record.clone();
        let records = vec![record, record_copy];

        let n1 = writer.extend(records)?;
        let n2 = writer.flush()?;
        let result = writer.into_inner()?;

        assert_eq!(n1 + n2, result.len());

        let mut data = Vec::new();
        zig_i64(27, &mut data)?;
        zig_i64(3, &mut data)?;
        data.extend(b"foo");
        data.extend(data.clone());

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );

        Ok(())
    }

    #[derive(Debug, Clone, Deserialize, Serialize)]
    struct TestSerdeSerialize {
        a: i64,
        b: String,
    }

    #[test]
    fn test_writer_append_ser() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;

        let record = TestSerdeSerialize {
            a: 27,
            b: "foo".to_owned(),
        };

        let n1 = writer.append_ser(record)?;
        let n2 = writer.flush()?;
        let result = writer.into_inner()?;

        assert_eq!(n1 + n2, result.len());

        let mut data = Vec::new();
        zig_i64(27, &mut data)?;
        zig_i64(3, &mut data)?;
        data.extend(b"foo");

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );

        Ok(())
    }

    #[test]
    fn test_writer_extend_ser() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;

        let record = TestSerdeSerialize {
            a: 27,
            b: "foo".to_owned(),
        };
        let record_copy = record.clone();
        let records = vec![record, record_copy];

        let n1 = writer.extend_ser(records)?;
        let n2 = writer.flush()?;
        let result = writer.into_inner()?;

        assert_eq!(n1 + n2, result.len());

        let mut data = Vec::new();
        zig_i64(27, &mut data)?;
        zig_i64(3, &mut data)?;
        data.extend(b"foo");
        data.extend(data.clone());

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );

        Ok(())
    }

    fn make_writer_with_codec(schema: &Schema) -> AvroResult<Writer<'_, Vec<u8>>> {
        Writer::with_codec(
            schema,
            Vec::new(),
            Codec::Deflate(DeflateSettings::default()),
        )
    }

    fn make_writer_with_builder(schema: &Schema) -> AvroResult<Writer<'_, Vec<u8>>> {
        Writer::builder()
            .writer(Vec::new())
            .schema(schema)
            .codec(Codec::Deflate(DeflateSettings::default()))
            .block_size(100)
            .build()
    }

    fn check_writer(mut writer: Writer<'_, Vec<u8>>, schema: &Schema) -> TestResult {
        let mut record = Record::new(schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        let n1 = writer.append_value(record.clone())?;
        let n2 = writer.append_value(record.clone())?;
        let n3 = writer.flush()?;
        let result = writer.into_inner()?;

        assert_eq!(n1 + n2 + n3, result.len());

        let mut data = Vec::new();
        zig_i64(27, &mut data)?;
        zig_i64(3, &mut data)?;
        data.extend(b"foo");
        data.extend(data.clone());
        Codec::Deflate(DeflateSettings::default()).compress(&mut data)?;

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );

        Ok(())
    }

    #[test]
    fn test_writer_with_codec() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let writer = make_writer_with_codec(&schema)?;
        check_writer(writer, &schema)
    }

    #[test]
    fn test_writer_with_builder() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let writer = make_writer_with_builder(&schema)?;
        check_writer(writer, &schema)
    }

    #[test]
    fn test_logical_writer() -> TestResult {
        const LOGICAL_TYPE_SCHEMA: &str = r#"
        {
          "type": "record",
          "name": "logical_type_test",
          "fields": [
            {
              "name": "a",
              "type": [
                "null",
                {
                  "type": "long",
                  "logicalType": "timestamp-micros"
                }
              ]
            }
          ]
        }
        "#;
        let codec = Codec::Deflate(DeflateSettings::default());
        let schema = Schema::parse_str(LOGICAL_TYPE_SCHEMA)?;
        let mut writer = Writer::builder()
            .schema(&schema)
            .codec(codec)
            .writer(Vec::new())
            .build()?;

        let mut record1 = Record::new(&schema).unwrap();
        record1.put(
            "a",
            Value::Union(1, Box::new(Value::TimestampMicros(1234_i64))),
        );

        let mut record2 = Record::new(&schema).unwrap();
        record2.put("a", Value::Union(0, Box::new(Value::Null)));

        let n1 = writer.append_value(record1)?;
        let n2 = writer.append_value(record2)?;
        let n3 = writer.flush()?;
        let result = writer.into_inner()?;

        assert_eq!(n1 + n2 + n3, result.len());

        let mut data = Vec::new();
        // byte indicating not null
        zig_i64(1, &mut data)?;
        zig_i64(1234, &mut data)?;

        // byte indicating null
        zig_i64(0, &mut data)?;
        codec.compress(&mut data)?;

        // starts with magic
        assert_eq!(&result[..AVRO_OBJECT_HEADER_LEN], AVRO_OBJECT_HEADER);
        // ends with data and sync marker
        let last_data_byte = result.len() - 16;
        assert_eq!(
            &result[last_data_byte - data.len()..last_data_byte],
            data.as_slice()
        );

        Ok(())
    }

    #[test]
    fn test_avro_3405_writer_add_metadata_success() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;

        writer.add_user_metadata("stringKey".to_string(), String::from("stringValue"))?;
        writer.add_user_metadata("strKey".to_string(), "strValue")?;
        writer.add_user_metadata("bytesKey".to_string(), b"bytesValue")?;
        writer.add_user_metadata("vecKey".to_string(), vec![1, 2, 3])?;

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        writer.append_value(record.clone())?;
        writer.append_value(record.clone())?;
        writer.flush()?;
        let result = writer.into_inner()?;

        assert_eq!(result.len(), 244);

        Ok(())
    }

    #[test]
    fn test_avro_3881_metadata_empty_body() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;
        writer.add_user_metadata("a".to_string(), "b")?;
        let result = writer.into_inner()?;

        let reader = Reader::builder(&result[..])
            .reader_schema(&schema)
            .build()?;
        let mut expected = HashMap::new();
        expected.insert("a".to_string(), vec![b'b']);
        assert_eq!(reader.user_metadata(), &expected);
        assert_eq!(reader.into_iter().count(), 0);

        Ok(())
    }

    #[test]
    fn test_avro_3405_writer_add_metadata_failure() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        writer.append_value(record.clone())?;

        match writer
            .add_user_metadata("stringKey".to_string(), String::from("value2"))
            .map_err(Error::into_details)
        {
            Err(e @ Details::FileHeaderAlreadyWritten) => {
                assert_eq!(e.to_string(), "The file metadata is already flushed.")
            }
            Err(e) => panic!("Unexpected error occurred while writing user metadata: {e:?}"),
            Ok(_) => panic!("Expected an error that metadata cannot be added after adding data"),
        }

        Ok(())
    }

    #[test]
    fn test_avro_3405_writer_add_metadata_reserved_prefix_failure() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;

        let key = "avro.stringKey".to_string();
        match writer
            .add_user_metadata(key.clone(), "value")
            .map_err(Error::into_details)
        {
            Err(ref e @ Details::InvalidMetadataKey(_)) => {
                assert_eq!(
                    e.to_string(),
                    format!(
                        "Metadata keys starting with 'avro.' are reserved for internal usage: {key}."
                    )
                )
            }
            Err(e) => panic!(
                "Unexpected error occurred while writing user metadata with reserved prefix ('avro.'): {e:?}"
            ),
            Ok(_) => {
                panic!("Expected an error that the metadata key cannot be prefixed with 'avro.'")
            }
        }

        Ok(())
    }

    #[test]
    fn test_avro_3405_writer_add_metadata_with_builder_api_success() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;

        let mut user_meta_data: HashMap<String, Value> = HashMap::new();
        user_meta_data.insert(
            "stringKey".to_string(),
            Value::String("stringValue".to_string()),
        );
        user_meta_data.insert("bytesKey".to_string(), Value::Bytes(b"bytesValue".to_vec()));
        user_meta_data.insert("vecKey".to_string(), Value::Bytes(vec![1, 2, 3]));

        let writer: Writer<'_, Vec<u8>> = Writer::builder()
            .writer(Vec::new())
            .schema(&schema)
            .user_metadata(user_meta_data.clone())
            .build()?;

        assert_eq!(writer.user_metadata, user_meta_data);

        Ok(())
    }

    #[derive(Serialize, Clone)]
    struct TestSingleObjectWriter {
        a: i64,
        b: f64,
        c: Vec<String>,
    }

    impl AvroSchema for TestSingleObjectWriter {
        fn get_schema() -> Schema {
            let schema = r#"
            {
                "type":"record",
                "name":"TestSingleObjectWrtierSerialize",
                "fields":[
                    {
                        "name":"a",
                        "type":"long"
                    },
                    {
                        "name":"b",
                        "type":"double"
                    },
                    {
                        "name":"c",
                        "type":{
                            "type":"array",
                            "items":"string"
                        }
                    }
                ]
            }
            "#;
            Schema::parse_str(schema).unwrap()
        }
    }

    impl From<TestSingleObjectWriter> for Value {
        fn from(obj: TestSingleObjectWriter) -> Value {
            Value::Record(vec![
                ("a".into(), obj.a.into()),
                ("b".into(), obj.b.into()),
                (
                    "c".into(),
                    Value::Array(obj.c.into_iter().map(|s| s.into()).collect()),
                ),
            ])
        }
    }

    #[test]
    fn test_single_object_writer() -> TestResult {
        let mut buf: Vec<u8> = Vec::new();
        let obj = TestSingleObjectWriter {
            a: 300,
            b: 34.555,
            c: vec!["cat".into(), "dog".into()],
        };
        let mut writer = GenericSingleObjectWriter::new_with_capacity(
            &TestSingleObjectWriter::get_schema(),
            1024,
        )
        .expect("Should resolve schema");
        let value = obj.into();
        let written_bytes = writer
            .write_value_ref(&value, &mut buf)
            .expect("Error serializing properly");

        assert!(buf.len() > 10, "no bytes written");
        assert_eq!(buf.len(), written_bytes);
        assert_eq!(buf[0], 0xC3);
        assert_eq!(buf[1], 0x01);
        assert_eq!(
            &buf[2..10],
            &TestSingleObjectWriter::get_schema()
                .fingerprint::<Rabin>()
                .bytes[..]
        );
        let mut msg_binary = Vec::new();
        encode(
            &value,
            &TestSingleObjectWriter::get_schema(),
            &mut msg_binary,
        )
        .expect("encode should have failed by here as a dependency of any writing");
        assert_eq!(&buf[10..], &msg_binary[..]);

        Ok(())
    }

    #[test]
    fn test_single_object_writer_with_header_builder() -> TestResult {
        let mut buf: Vec<u8> = Vec::new();
        let obj = TestSingleObjectWriter {
            a: 300,
            b: 34.555,
            c: vec!["cat".into(), "dog".into()],
        };
        let schema_uuid = Uuid::parse_str("b2f1cf00-0434-013e-439a-125eb8485a5f")?;
        let header_builder = GlueSchemaUuidHeader::from_uuid(schema_uuid);
        let mut writer = GenericSingleObjectWriter::new_with_capacity_and_header_builder(
            &TestSingleObjectWriter::get_schema(),
            1024,
            header_builder,
        )
        .expect("Should resolve schema");
        let value = obj.into();
        writer
            .write_value_ref(&value, &mut buf)
            .expect("Error serializing properly");

        assert_eq!(buf[0], 0x03);
        assert_eq!(buf[1], 0x00);
        assert_eq!(buf[2..18], schema_uuid.into_bytes()[..]);
        Ok(())
    }

    #[test]
    fn test_writer_parity() -> TestResult {
        let obj1 = TestSingleObjectWriter {
            a: 300,
            b: 34.555,
            c: vec!["cat".into(), "dog".into()],
        };

        let mut buf1: Vec<u8> = Vec::new();
        let mut buf2: Vec<u8> = Vec::new();
        let mut buf3: Vec<u8> = Vec::new();
        let mut buf4: Vec<u8> = Vec::new();

        let mut generic_writer = GenericSingleObjectWriter::new_with_capacity(
            &TestSingleObjectWriter::get_schema(),
            1024,
        )
        .expect("Should resolve schema");
        let specific_writer = SpecificSingleObjectWriter::<TestSingleObjectWriter>::new()
            .expect("Resolved should pass");
        specific_writer
            .write_ref(&obj1, &mut buf1)
            .expect("Serialization expected");
        specific_writer
            .write_ref(&obj1, &mut buf2)
            .expect("Serialization expected");
        specific_writer
            .write_value(obj1.clone(), &mut buf3)
            .expect("Serialization expected");

        generic_writer
            .write_value(obj1.into(), &mut buf4)
            .expect("Serialization expected");

        assert_eq!(buf1, buf2);
        assert_eq!(buf2, buf3);
        assert_eq!(buf3, buf4);

        Ok(())
    }

    #[test]
    fn avro_3894_take_aliases_into_account_when_serializing() -> TestResult {
        const SCHEMA: &str = r#"
  {
      "type": "record",
      "name": "Conference",
      "fields": [
          {"type": "string", "name": "name"},
          {"type": ["null", "long"], "name": "date", "aliases" : [ "time2", "time" ]}
      ]
  }"#;

        #[derive(Debug, PartialEq, Eq, Clone, Serialize)]
        pub struct Conference {
            pub name: String,
            pub time: Option<i64>,
        }

        let conf = Conference {
            name: "RustConf".to_string(),
            time: Some(1234567890),
        };

        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;

        let bytes = writer.append_ser(conf)?;

        assert_eq!(182, bytes);

        Ok(())
    }

    #[test]
    fn avro_4014_validation_returns_a_detailed_error() -> TestResult {
        const SCHEMA: &str = r#"
  {
      "type": "record",
      "name": "Conference",
      "fields": [
          {"type": "string", "name": "name"},
          {"type": ["null", "long"], "name": "date", "aliases" : [ "time2", "time" ]}
      ]
  }"#;

        #[derive(Debug, PartialEq, Clone, Serialize)]
        pub struct Conference {
            pub name: String,
            pub time: Option<f64>, // wrong type: f64 instead of i64
        }

        let conf = Conference {
            name: "RustConf".to_string(),
            time: Some(12345678.90),
        };

        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new())?;

        match writer.append_ser(conf) {
            Ok(bytes) => panic!("Expected an error, but got {bytes} bytes written"),
            Err(e) => {
                assert_eq!(
                    e.to_string(),
                    r#"Failed to serialize field 'time' for record Record(RecordSchema { name: Name { name: "Conference", .. }, fields: [RecordField { name: "name", schema: String, position: 0, .. }, RecordField { name: "date", aliases: ["time2", "time"], schema: Union(UnionSchema { schemas: [Null, Long] }), position: 1, .. }], .. }): Failed to serialize value of type f64 using schema Union(UnionSchema { schemas: [Null, Long] }): 12345678.9. Cause: Cannot find a Double schema in [Null, Long]"#
                );
            }
        }
        Ok(())
    }

    #[test]
    fn avro_4063_flush_applies_to_inner_writer() -> TestResult {
        const SCHEMA: &str = r#"
        {
            "type": "record",
            "name": "ExampleSchema",
            "fields": [
                {"name": "exampleField", "type": "string"}
            ]
        }
        "#;

        #[derive(Clone, Default)]
        struct TestBuffer(Rc<RefCell<Vec<u8>>>);

        impl TestBuffer {
            fn len(&self) -> usize {
                self.0.borrow().len()
            }
        }

        impl Write for TestBuffer {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.borrow_mut().write(buf)
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let shared_buffer = TestBuffer::default();

        let buffered_writer = std::io::BufWriter::new(shared_buffer.clone());

        let schema = Schema::parse_str(SCHEMA)?;

        let mut writer = Writer::new(&schema, buffered_writer)?;

        let mut record = Record::new(writer.schema()).unwrap();
        record.put("exampleField", "value");

        writer.append_value(record)?;
        writer.flush()?;

        assert_eq!(
            shared_buffer.len(),
            151,
            "the test buffer was not fully written to after Writer::flush was called"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_439_specific_single_object_writer_ref() -> TestResult {
        #[derive(Serialize)]
        struct Recursive {
            field: bool,
            recurse: Option<Box<Recursive>>,
        }

        impl AvroSchema for Recursive {
            fn get_schema() -> Schema {
                Schema::parse_str(
                    r#"{
                    "name": "Recursive",
                    "type": "record",
                    "fields": [
                        { "name": "field", "type": "boolean" },
                        { "name": "recurse", "type": ["null", "Recursive"] }
                    ]
                }"#,
                )
                .unwrap()
            }
        }

        let mut buffer = Vec::new();
        let writer = SpecificSingleObjectWriter::new()?;

        writer.write(
            Recursive {
                field: true,
                recurse: Some(Box::new(Recursive {
                    field: false,
                    recurse: None,
                })),
            },
            &mut buffer,
        )?;
        assert_eq!(
            buffer,
            &[195, 1, 83, 223, 43, 26, 181, 179, 227, 224, 1, 2, 0, 0][..]
        );

        Ok(())
    }

    #[test]
    fn avro_rs_310_append_unvalidated_value() -> TestResult {
        let schema = Schema::String;
        let value = Value::Int(1);

        let mut writer = Writer::new(&schema, Vec::new())?;
        writer.unvalidated_append_value_ref(&value)?;
        writer.unvalidated_append_value(value)?;
        let buffer = writer.into_inner()?;

        // Check the last two bytes for the sync marker
        assert_eq!(&buffer[buffer.len() - 18..buffer.len() - 16], &[2, 2]);

        let mut writer = Writer::new(&schema, Vec::new())?;
        let value = Value::Int(1);
        let err = writer.append_value_ref(&value).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Value Int(1) does not match schema String: Reason: Unsupported value-schema combination! Value: Int(1), schema: String"
        );
        let err = writer.append_value(value).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Value Int(1) does not match schema String: Reason: Unsupported value-schema combination! Value: Int(1), schema: String"
        );

        Ok(())
    }

    #[test]
    fn avro_rs_469_reset_writer() -> TestResult {
        let schema = Schema::Boolean;
        let values = [true, false, true, false];
        let mut writer = Writer::new(&schema, Vec::new())?;

        for value in values {
            writer.append_value(value)?;
        }

        writer.flush()?;
        let first_buffer = writer.get_ref().clone();

        writer.reset();
        assert_eq!(writer.get_ref().len(), 0);

        for value in values {
            writer.append_value(value)?;
        }

        writer.flush()?;
        let second_buffer = writer.get_ref().clone();
        assert_eq!(first_buffer.len(), second_buffer.len());
        // File structure:
        // Header: ? bytes
        // Sync marker: 16 bytes
        // Data: 6 bytes
        // Sync marker: 16 bytes
        let len = first_buffer.len();
        let header = len - 16 - 6 - 16;
        let data = header + 16;
        assert_eq!(
            first_buffer[..header],
            second_buffer[..header],
            "Written header must be the same, excluding sync marker"
        );
        assert_ne!(
            first_buffer[header..data],
            second_buffer[header..data],
            "Sync markers should be different"
        );
        assert_eq!(
            first_buffer[data..data + 6],
            second_buffer[data..data + 6],
            "Written data must be the same"
        );
        assert_ne!(
            first_buffer[len - 16..],
            second_buffer[len - 16..],
            "Sync markers should be different"
        );

        Ok(())
    }
}
