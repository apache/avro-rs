use std::collections::HashMap;
use crate::{Codec, Schema};
use crate::schema::Names;

/// Decoder for the Object Container File header.
mod header;
/// Decoder for the Object Container File data blocks.
mod data;

/// The header as read from an Object Container File.
pub struct Header {
    /// The schema used to write the file.
    pub schema: Schema,
    pub names: Names,
    /// The compression used.
    pub codec: Codec,
    /// The sync marker used between blocks
    pub sync: [u8; 16],
    /// User metadata in the header
    pub metadata: HashMap<String, Vec<u8>>,
}

pub use header::HeaderFsm;
pub use data::DataBlockFsm;
