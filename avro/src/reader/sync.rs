use oval::Buffer;
use serde::Deserialize;
use std::{collections::HashMap, io::Read};

use crate::{
    AvroResult, Error, Schema,
    decode::{
        ItemRead, StateMachine, StateMachineControlFlow,
        commands::CommandTape,
        datum::DatumStateMachine,
        deserialize_from_tape,
        object_container_file::{
            ObjectContainerFileBodyStateMachine, ObjectContainerFileHeader,
            ObjectContainerFileHeaderStateMachine,
        },
        value_from_tape,
    },
    error::Details,
    schema::{resolve_names, resolve_names_with_schemata},
    types::Value,
};

/// Main interface for reading Avro formatted values.
///
/// To be used as an iterator:
///
/// ```no_run
/// # use apache_avro::Reader;
/// # use std::io::Cursor;
/// # let input = Cursor::new(Vec::<u8>::new());
/// for value in Reader::new(input).unwrap() {
///     match value {
///         Ok(v) => println!("{:?}", v),
///         Err(e) => println!("Error: {}", e),
///     };
/// }
/// ```
pub struct Reader<'a, R> {
    reader_schema: Option<&'a Schema>,
    header: ObjectContainerFileHeader,
    fsm: Option<ObjectContainerFileBodyStateMachine>,
    reader: R,
    buffer: Buffer,
}

impl<'a, R: Read> Reader<'a, R> {
    /// Creates a [`Reader`] that will use the schema from the file header.
    ///
    /// No reader [`Schema`] will be set.
    ///
    /// **NOTE** The Avro header is going to be read automatically upon creation of the [`Reader`].
    pub fn new(reader: R) -> Result<Self, Error> {
        Self::new_inner(reader, None, Vec::new())
    }

    /// Creates a [`Reader`] that will use the given schema for schema resolution.
    ///
    /// **NOTE** The Avro header is going to be read automatically upon creation of the [`Reader`].
    pub fn with_schema(schema: &'a Schema, reader: R) -> Result<Self, Error> {
        Self::new_inner(reader, Some(schema), Vec::new())
    }

    /// Creates a [`Reader`] that will use the given schema for schema resolution.
    ///
    /// `schema` must be in `schemata`. Otherwise, any self-referential [`Schema::Ref`]s can't be
    /// resolved and an error will be returned.
    ///
    /// Any [`Schema::Ref`] will be resolved using the schemata.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the [`Reader`].
    pub fn with_schemata(
        schema: &'a Schema,
        schemata: Vec<&'a Schema>,
        reader: R,
    ) -> Result<Self, Error> {
        Self::new_inner(reader, Some(schema), schemata)
    }

    /// Get a reference to the writer [`Schema`].
    pub fn writer_schema(&self) -> &Schema {
        &self.header.schema
    }

    /// Get a reference to the optional reader [`Schema`].
    ///
    /// This will only be set if there was a reader schema provided *and* it differed from the
    /// writer schema.
    pub fn reader_schema(&self) -> Option<&'a Schema> {
        self.reader_schema
    }

    /// Get a reference to the user metadata.
    pub fn user_metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.header.metadata
    }

    /// Get a reference to the file header.
    pub fn header(&self) -> &ObjectContainerFileHeader {
        &self.header
    }

    fn new_inner(
        mut reader: R,
        reader_schema: Option<&'a Schema>,
        schemata: Vec<&'a Schema>,
    ) -> Result<Self, Error> {
        // Read a maximum of 2Kb per read
        let mut buffer = Buffer::with_capacity(2 * 1024);

        // Parse the header
        let mut fsm = ObjectContainerFileHeaderStateMachine::new(schemata);
        let header = loop {
            // Fill the buffer
            let n = reader.read(buffer.space()).map_err(Details::ReadHeader)?;
            if n == 0 {
                return Err(Details::ReadHeader(std::io::ErrorKind::UnexpectedEof.into()).into());
            }
            buffer.fill(n);

            // Start/continue the state machine
            match fsm.parse(&mut buffer)? {
                StateMachineControlFlow::NeedMore(new_fsm) => fsm = new_fsm,
                StateMachineControlFlow::Done(header) => break header,
            }
        };

        let tape = CommandTape::build_from_schema(&header.schema, &header.names)?;

        let reader_schema = if let Some(schema) = reader_schema
            && schema != &header.schema
        {
            Some(schema)
        } else {
            None
        };

        Ok(Self {
            reader_schema,
            fsm: Some(ObjectContainerFileBodyStateMachine::new(
                tape,
                header.sync,
                header.codec,
            )),
            header,
            reader,
            buffer,
        })
    }

    /// Get the next object in the file
    fn next_object(&mut self) -> Option<Result<Vec<ItemRead>, Error>> {
        if let Some(mut fsm) = self.fsm.take() {
            loop {
                match fsm.parse(&mut self.buffer) {
                    Ok(StateMachineControlFlow::NeedMore(new_fsm)) => {
                        fsm = new_fsm;
                        let n = match self.reader.read(self.buffer.space()) {
                            Ok(0) => {
                                return Some(Err(Details::ReadIntoBuf(
                                    std::io::ErrorKind::UnexpectedEof.into(),
                                )
                                .into()));
                            }
                            Ok(n) => n,
                            Err(e) => return Some(Err(Details::ReadIntoBuf(e).into())),
                        };
                        self.buffer.fill(n);
                    }
                    Ok(StateMachineControlFlow::Done(Some((object, fsm)))) => {
                        self.fsm.replace(fsm);
                        return Some(Ok(object));
                    }
                    Ok(StateMachineControlFlow::Done(None)) => {
                        return None;
                    }
                    Err(e) => {
                        return Some(Err(e));
                    }
                }
            }
        }
        None
    }

    /// Deserialize the next object directly to `T`.
    ///
    /// This function goes immediately from the inner representation to `T` without going through
    /// [`Value`] first. It does not support schema resolution using a reader [`Schema`].
    ///
    /// # Panics
    /// Will panic if a reader [`Schema`] was supplied when creating the [`Reader`].
    pub fn next_serde<'b, T: Deserialize<'b>>(&mut self) -> Option<Result<T, Error>> {
        assert!(
            self.reader_schema.is_none(),
            "Schema resolution is not supported with this function!"
        );
        self.next_object()
            .map(|r| r.and_then(|mut tape| deserialize_from_tape(&mut tape, &self.header.schema)))
    }
}

impl<R: Read> Iterator for Reader<'_, R> {
    type Item = Result<Value, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_object().map(|r| {
            r.and_then(|mut tape| {
                value_from_tape(&mut tape, &self.header.schema, &self.header.names)
            })
            .and_then(|v| {
                if let Some(schema) = &self.reader_schema {
                    v.resolve_internal(schema, &self.header.names, &None, &None)
                } else {
                    Ok(v)
                }
            })
        })
    }
}

/// Decode a raw Avro datum using the provided [`Schema`].
///
/// In case a reader [`Schema`] is provided, schema resolution will be performed.
///
/// **NOTE** This function is very niche and does NOT take care of reading the header and
/// consecutive data blocks. use [`Reader`] if you just want to read an Avro encoded file.
pub fn from_avro_datum<R: Read>(
    writer_schema: &Schema,
    reader: &mut R,
    reader_schema: Option<&Schema>,
) -> AvroResult<Value> {
    from_avro_datum_reader_schemata(writer_schema, Vec::new(), reader, reader_schema, Vec::new())
}

/// Decode a raw Avro datum using the provided [`Schema`] and schemata.
///
/// `schema` must be in `schemata`. Otherwise, any self-referential [`Schema::Ref`]s can't be
/// resolved and an error will be returned.
///
/// If the writer schema contains any [`Schema::Ref`] then it will use the provided
/// schemata to resolve any dependencies.
///
/// In case a reader [`Schema`] is provided, schema resolution will be performed.
pub fn from_avro_datum_schemata<R: Read>(
    writer_schema: &Schema,
    writer_schemata: Vec<&Schema>,
    reader: &mut R,
    reader_schema: Option<&Schema>,
) -> AvroResult<Value> {
    from_avro_datum_reader_schemata(
        writer_schema,
        writer_schemata,
        reader,
        reader_schema,
        Vec::new(),
    )
}

/// Decode a raw Avro datum using the provided [`Schema`] and schemata.
///
/// `schema` must be in `schemata`. Otherwise, any self-referential [`Schema::Ref`]s can't be
/// resolved and an error will be returned.
///
/// If the writer schema contains any [`Schema::Ref`] then it will use the provided
/// schemata to resolve any dependencies.
///
/// In case a reader [`Schema`] is provided, schema resolution will be performed.
// TODO: These should really be a reusable reader, as quite a lot of work is done on creation
pub fn from_avro_datum_reader_schemata<R: Read>(
    writer_schema: &Schema,
    writer_schemata: Vec<&Schema>,
    reader: &mut R,
    reader_schema: Option<&Schema>,
    reader_schemata: Vec<&Schema>,
) -> AvroResult<Value> {
    let mut names = HashMap::new();
    if writer_schemata.is_empty() {
        resolve_names(writer_schema, &mut names, &None)?;
    } else {
        resolve_names_with_schemata(&writer_schemata, &mut names, &None)?;
    }

    let tape = CommandTape::build_from_schema(writer_schema, &names)?;

    println!("{tape:#?}");

    // Read a maximum of 2Kb per read
    let mut buffer = Buffer::with_capacity(2 * 1024);
    let mut fsm = DatumStateMachine::new(tape);
    let value = loop {
        // Fill the buffer
        let n = reader.read(buffer.space()).map_err(Details::ReadIntoBuf)?;
        if n == 0 {
            // If the writer schema is null, this is expected and we just return a null value
            if matches!(writer_schema, &Schema::Null) {
                break Value::Null;
            }
            return Err(Details::ReadIntoBuf(std::io::ErrorKind::UnexpectedEof.into()).into());
        }
        buffer.fill(n);

        match fsm.parse(&mut buffer)? {
            StateMachineControlFlow::NeedMore(new_fsm) => {
                fsm = new_fsm;
            }
            StateMachineControlFlow::Done(mut tape) => {
                break value_from_tape(&mut tape, writer_schema, &names)?;
            }
        }
    };
    match reader_schema {
        Some(schema) => {
            if reader_schemata.is_empty() {
                value.resolve(schema)
            } else {
                value.resolve_schemata(schema, reader_schemata)
            }
        }
        None => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Schema, Writer, from_value,
        types::{Record, Value},
    };
    use apache_avro_test_helper::TestResult;
    use pretty_assertions::assert_eq;
    use serde::Deserialize;
    use std::{collections::HashMap, io::Cursor};

    /// Test it reads all the sync markers
    #[test]
    fn sync_markers() {
        let mut writer = Writer::new(&Schema::String, Vec::new()).unwrap();
        writer.append(Value::String("Hello".to_string())).unwrap();
        writer.flush().unwrap();
        writer.append(Value::String("World".to_string())).unwrap();
        writer.flush().unwrap();
        let mut written = Cursor::new(writer.into_inner().unwrap());

        let mut reader = Reader::new(&mut written).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::String("Hello".to_string())
        );
        assert_eq!(
            reader.next().unwrap().unwrap(),
            Value::String("World".to_string())
        );

        drop(reader);
        let position = written.position();
        let expected = written.into_inner().len();
        assert_eq!(position, expected as u64);
    }

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
    const ENCODED: &[u8] = &[
        79u8, 98u8, 106u8, 1u8, 4u8, 22u8, 97u8, 118u8, 114u8, 111u8, 46u8, 115u8, 99u8, 104u8,
        101u8, 109u8, 97u8, 222u8, 1u8, 123u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8,
        114u8, 101u8, 99u8, 111u8, 114u8, 100u8, 34u8, 44u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8,
        58u8, 34u8, 116u8, 101u8, 115u8, 116u8, 34u8, 44u8, 34u8, 102u8, 105u8, 101u8, 108u8,
        100u8, 115u8, 34u8, 58u8, 91u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8,
        97u8, 34u8, 44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 108u8, 111u8, 110u8,
        103u8, 34u8, 44u8, 34u8, 100u8, 101u8, 102u8, 97u8, 117u8, 108u8, 116u8, 34u8, 58u8, 52u8,
        50u8, 125u8, 44u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8, 98u8, 34u8,
        44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 115u8, 116u8, 114u8, 105u8,
        110u8, 103u8, 34u8, 125u8, 93u8, 125u8, 20u8, 97u8, 118u8, 114u8, 111u8, 46u8, 99u8, 111u8,
        100u8, 101u8, 99u8, 8u8, 110u8, 117u8, 108u8, 108u8, 0u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8, 4u8, 20u8, 54u8,
        6u8, 102u8, 111u8, 111u8, 84u8, 6u8, 98u8, 97u8, 114u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8,
    ];

    #[test]
    fn test_from_avro_datum() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let mut encoded: &'static [u8] = &[54, 6, 102, 111, 111];

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        let expected = record.into();

        assert_eq!(from_avro_datum(&schema, &mut encoded, None)?, expected);

        Ok(())
    }

    #[test]
    fn test_from_avro_datum_with_union_to_struct() -> TestResult {
        // TODO: Inform @ultrabug that their fix has been reverted and they need to use from_avro_datum_reader_schemata
        const TEST_RECORD_WRITER_SCHEMA_3240: &str = r#"
    {
      "type": "record",
      "name": "test",
      "fields": [
        {
          "name": "a",
          "type": "long"
        },
        {
          "name": "b",
          "type": "string"
        }
      ]
    }
        "#;

        const TEST_RECORD_READER_SCHEMA_3240: &str = r#"
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
        },
        {
            "name": "a_nullable_array",
            "type": ["null", {"type": "array", "items": {"type": "string"}}],
            "default": null
        },
        {
            "name": "a_nullable_boolean",
            "type": ["null", {"type": "boolean"}],
            "default": null
        },
        {
            "name": "a_nullable_string",
            "type": ["null", {"type": "string"}],
            "default": null
        }
      ]
    }
    "#;
        const TEST_DATUM_3240: &[u8] = &[54, 6, 102, 111, 111];

        #[derive(Default, Debug, Deserialize, PartialEq, Eq)]
        struct TestRecord3240 {
            a: i64,
            b: String,
            a_nullable_array: Option<Vec<String>>,
            // we are missing the 'a_nullable_boolean' field to simulate missing keys
            // a_nullable_boolean: Option<bool>,
            a_nullable_string: Option<String>,
        }

        let reader_schema = Schema::parse_str(TEST_RECORD_READER_SCHEMA_3240)?;
        let writer_schema = Schema::parse_str(TEST_RECORD_WRITER_SCHEMA_3240)?;

        // The schema used to read is not compatible with what is written
        assert!(from_avro_datum(&reader_schema, &mut Cursor::new(TEST_DATUM_3240), None).is_err());
        // If the writer schema is used, it is compatible
        assert!(from_avro_datum(&writer_schema, &mut Cursor::new(TEST_DATUM_3240), None).is_ok());

        // For schema compatibility use the writer and reader schema
        let avro_datum = from_avro_datum_reader_schemata(
            &writer_schema,
            Vec::new(),
            &mut Cursor::new(TEST_DATUM_3240),
            Some(&reader_schema),
            Vec::new(),
        )?;

        let expected_record: TestRecord3240 = TestRecord3240 {
            a: 27i64,
            b: String::from("foo"),
            a_nullable_array: None,
            a_nullable_string: None,
        };

        let parsed_record: TestRecord3240 = match &avro_datum {
            Value::Record(_) => from_value::<TestRecord3240>(&avro_datum)?,
            unexpected => {
                panic!("could not map avro data to struct, found unexpected: {unexpected:?}")
            }
        };

        assert_eq!(parsed_record, expected_record);

        Ok(())
    }

    #[test]
    fn test_null_union() -> TestResult {
        let schema = Schema::parse_str(UNION_SCHEMA)?;
        let mut encoded: &'static [u8] = &[2, 0];

        assert_eq!(
            from_avro_datum(&schema, &mut encoded, None)?,
            Value::Union(1, Box::new(Value::Long(0)))
        );

        Ok(())
    }

    #[test]
    fn test_reader_iterator() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let reader = Reader::with_schema(&schema, ENCODED)?;

        let mut record1 = Record::new(&schema).unwrap();
        record1.put("a", 27i64);
        record1.put("b", "foo");

        let mut record2 = Record::new(&schema).unwrap();
        record2.put("a", 42i64);
        record2.put("b", "bar");

        let expected = [record1.into(), record2.into()];

        for (i, value) in reader.enumerate() {
            assert_eq!(value?, expected[i]);
        }

        Ok(())
    }

    #[test]
    fn test_reader_invalid_header() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let invalid = ENCODED.iter().copied().skip(1).collect::<Vec<u8>>();
        assert!(Reader::with_schema(&schema, &invalid[..]).is_err());

        Ok(())
    }

    #[test]
    fn test_reader_invalid_block() -> TestResult {
        let schema = Schema::parse_str(SCHEMA)?;
        let invalid = ENCODED
            .iter()
            .copied()
            .rev()
            .skip(19)
            .collect::<Vec<u8>>()
            .into_iter()
            .rev()
            .collect::<Vec<u8>>();
        let mut reader = Reader::with_schema(&schema, &invalid[..])?;

        // The block says it contains 2 values, but only contains one.
        // The first value is successfully decoded
        let _v = reader.next().unwrap().unwrap();
        // The second fails with an unexpected end of file error.
        assert!(reader.next().unwrap().is_err());

        Ok(())
    }

    #[test]
    fn test_reader_empty_buffer() -> TestResult {
        let empty = Cursor::new(Vec::new());
        assert!(Reader::new(empty).is_err());

        Ok(())
    }

    #[test]
    fn test_reader_only_header() -> TestResult {
        let invalid = ENCODED.iter().copied().take(165).collect::<Vec<u8>>();
        let reader = Reader::new(&invalid[..])?;
        for value in reader {
            assert!(value.is_err());
        }

        Ok(())
    }

    #[test]
    fn test_avro_3405_read_user_metadata_success() -> TestResult {
        use crate::writer::Writer;

        let schema = Schema::parse_str(SCHEMA)?;
        let mut writer = Writer::new(&schema, Vec::new()).unwrap();

        let mut user_meta_data: HashMap<String, Vec<u8>> = HashMap::new();
        user_meta_data.insert("stringKey".to_string(), b"stringValue".to_vec());
        user_meta_data.insert("bytesKey".to_string(), b"bytesValue".to_vec());
        user_meta_data.insert("vecKey".to_string(), vec![1, 2, 3]);

        for (k, v) in user_meta_data.iter() {
            writer.add_user_metadata(k.to_string(), v)?;
        }

        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        writer.append(record.clone())?;
        writer.append(record.clone())?;
        writer.flush()?;
        let result = writer.into_inner()?;

        let reader = Reader::new(&result[..])?;
        assert_eq!(reader.user_metadata(), &user_meta_data);

        Ok(())
    }

    #[cfg(not(feature = "snappy"))]
    #[test]
    fn test_avro_3549_read_not_enabled_codec() {
        let snappy_compressed_avro = vec![
            79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 210, 1, 123,
            34, 102, 105, 101, 108, 100, 115, 34, 58, 91, 123, 34, 110, 97, 109, 101, 34, 58, 34,
            110, 117, 109, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 115, 116, 114, 105, 110,
            103, 34, 125, 93, 44, 34, 110, 97, 109, 101, 34, 58, 34, 101, 118, 101, 110, 116, 34,
            44, 34, 110, 97, 109, 101, 115, 112, 97, 99, 101, 34, 58, 34, 101, 120, 97, 109, 112,
            108, 101, 110, 97, 109, 101, 115, 112, 97, 99, 101, 34, 44, 34, 116, 121, 112, 101, 34,
            58, 34, 114, 101, 99, 111, 114, 100, 34, 125, 20, 97, 118, 114, 111, 46, 99, 111, 100,
            101, 99, 12, 115, 110, 97, 112, 112, 121, 0, 213, 209, 241, 208, 200, 110, 164, 47,
            203, 25, 90, 235, 161, 167, 195, 177, 2, 20, 4, 12, 6, 49, 50, 51, 115, 38, 58, 0, 213,
            209, 241, 208, 200, 110, 164, 47, 203, 25, 90, 235, 161, 167, 195, 177,
        ];

        if let Err(err) = Reader::new(snappy_compressed_avro.as_slice()) {
            pretty_assertions::assert_eq!(
                "Codec 'snappy' is not supported/enabled",
                err.to_string()
            );
        } else {
            panic!("Expected an error in the reading of the codec!");
        }
    }
}
