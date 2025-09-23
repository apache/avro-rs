use oval::Buffer;
use serde::Deserialize;
use std::{collections::HashMap, io::Read};

use crate::{
    AvroResult, Error, Schema,
    error::Details,
    schema::{resolve_names, resolve_names_with_schemata},
    state_machines::reading::{
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
    use crate::{Schema, Writer, state_machines::reading::sync::Reader, types::Value};
    use std::io::Cursor;

    /// Test it reads all the sync markers
    #[test]
    fn sync_markers() {
        let mut writer = Writer::new(&Schema::String, Vec::new());
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
}
