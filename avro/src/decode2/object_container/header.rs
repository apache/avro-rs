
use std::collections::HashMap;
use std::io::Read;
use std::str::FromStr;
use log::warn;
use oval::Buffer;
use crate::{Codec, Error, Schema};
use crate::decode2::complex::block::MapFsm;
use crate::decode2::{Fsm, FsmControlFlow, FsmResult};
use crate::decode2::object_container::Header;
use crate::error::Details;
use crate::schema::{resolve_names, resolve_names_with_schemata, Names, ResolvedSchema};
use crate::types::Value;

/// Decode the header of an Object Container File.
///
/// The output of this state machine can be used with [`DataBlockFsm`] to parse the body.
///
/// [`DataBlockFsm`]: super::DataBlockFsm
pub struct HeaderFsm<'a> {
    /// We wrap around inner to hide implementation details from the user.
    fsm: InnerHeaderFsm<'a>,
}
impl<'a> HeaderFsm<'a> {
    /// Create a new decoder.
    pub fn new() -> Self {
        Self {
            fsm: InnerHeaderFsm::new(),
        }
    }

    /// Create a new decoder with schemata.
    pub fn new_with_schemata(schemata: Vec<&'a Schema>) -> Self {
        Self {
            fsm: InnerHeaderFsm::new_with_schemata(schemata)
        }
    }
}
impl Default for HeaderFsm<'_> {
    fn default() -> Self {
        Self::new()
    }
}
impl<'a> Fsm for HeaderFsm<'a> {
    type Output = Header;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.fsm.parse(buffer)?.map(|fsm| Self { fsm }, |h| h))
    }
}

/// The actual decoder for the header.
enum InnerHeaderFsm<'a> {
    /// We start with reading the magic number and verifying this is an Object Container File.
    ReadMagic {
        schemata: Vec<&'a Schema>
    },
    /// Then we read in all the metadata.
    Metadata {
        fsm: MapFsm<'static>,
        schemata: Vec<&'a Schema>,
    },
    /// Finally, we need to read the sync marker.
    Sync {
        metadata: HashMap<String, Value>,
        schemata: Vec<&'a Schema>,
    },
}
impl<'a> InnerHeaderFsm<'a> {
    pub fn new() -> Self {
        Self::ReadMagic { schemata: Vec::new() }
    }

    pub fn new_with_schemata(schemata: Vec<&'a Schema>) -> Self {
        Self::ReadMagic { schemata }
    }
}

impl<'a> Fsm for InnerHeaderFsm<'a> {
    type Output = Header;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        loop {
            match self {
                InnerHeaderFsm::ReadMagic { schemata } => {
                    if buffer.available_data() < 4 {
                        return Ok(FsmControlFlow::NeedMore(Self::ReadMagic { schemata }));
                    }
                    if buffer.data()[0..4] != [b'O', b'b', b'j', 1] {
                        return Err(Details::HeaderMagic.into());
                    }
                    buffer.consume(4);
                    self = Self::Metadata { fsm: MapFsm::new(&Schema::Bytes), schemata };
                }
                InnerHeaderFsm::Metadata { fsm, schemata } => {
                    match fsm.parse(buffer)? {
                        FsmControlFlow::NeedMore(fsm) => {
                            return Ok(FsmControlFlow::NeedMore(Self::Metadata { fsm, schemata }));
                        }
                        FsmControlFlow::Done(value) => {
                            let Value::Map(metadata) = value else { unreachable!() };
                            self = Self::Sync { metadata, schemata};
                        }
                    }
                }
                InnerHeaderFsm::Sync { metadata, schemata } => {
                    if buffer.available_data() < 16 {
                        return Ok(FsmControlFlow::NeedMore(Self::Sync { metadata, schemata }));
                    }
                    let mut sync = [0; 16];
                    buffer.read_exact(&mut sync).unwrap_or_else(|_| unreachable!());
                    return Ok(FsmControlFlow::Done(create_header(metadata, sync, schemata)?))
                }
            }

        }
    }
}

fn create_header(map: HashMap<String, Value>, sync: [u8; 16], mut schemata: Vec<&Schema>) -> Result<Header, Error> {
    let mut schema = None;
    let mut codec = None;
    let mut found_compression_level = false;
    let mut metadata = HashMap::new();
    let mut names = HashMap::new();

    for (key, value) in map {
        let Value::Bytes(value) = value else { unreachable!() };
        match key.as_ref() {
            "avro.schema" => {
                if schema.is_some() {
                    // Duplicate key
                    return Err(Details::GetHeaderMetadata.into());
                }
                let json: serde_json::Value =
                    serde_json::from_slice(&value).map_err(Details::ParseSchemaJson)?;

                if !schemata.is_empty() {
                    // TODO: Make parse_with_names accept NamesRef
                    let schemata = std::mem::take(&mut schemata);
                    resolve_names_with_schemata(&schemata, &mut names, &None)?;

                    // TODO: Maybe we can not do this, and just past &names to Schema::parse_with_names
                    let rs = ResolvedSchema::try_from(schemata)?;
                    let names: Names = rs
                        .get_names()
                        .iter()
                        .map(|(name, &schema)| (name.clone(), schema.clone()))
                        .collect();

                    let parsed_schema = Schema::parse_with_names(&json, names)?;
                    schema.replace(parsed_schema);
                } else {
                    let parsed_schema = Schema::parse(&json)?;
                    resolve_names(&parsed_schema, &mut names, &None)?;
                    schema.replace(parsed_schema);
                }
            }
            "avro.codec" => {
                let string = String::from_utf8(value).map_err(Details::ConvertToUtf8)?;
                let parsed_codec = Codec::from_str(&string)
                    .map_err(|_| Details::CodecNotSupported(string))?;
                if codec.replace(parsed_codec).is_some() {
                    // Duplicate key
                    return Err(Details::GetHeaderMetadata.into());
                }
            }
            "avro.codec.compression_level" => {
                // Compression level is not useful for decoding
                if found_compression_level {
                    // Duplicate key
                    return Err(Details::GetHeaderMetadata.into());
                }
                found_compression_level = true;
            }
            _ => {
                if key.starts_with("avro.") {
                    warn!("Ignoring unknown metadata key: {key}");
                }
                if metadata.insert(key, value).is_some() {
                    // Duplicate key
                    return Err(Details::GetHeaderMetadata.into());
                }
            }
        }
    }


    let Some(schema) = schema else {
        return Err(Details::GetHeaderMetadata.into());
    };
    let codec = codec.unwrap_or(Codec::Null);
    Ok(Header {
        schema,
        names,
        codec,
        sync,
        metadata,
    })
}
