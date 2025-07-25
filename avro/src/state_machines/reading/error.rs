use crate::{Schema, state_machines::reading::ItemRead};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ValueFromTapeError {
    #[error("Unexpected end of tape while building Value")]
    UnexpectedEndOfTape,
    #[error(
        "Mismatch between tape and schema while building Value: schema {schema}, tape: {item:?}"
    )]
    TapeSchemaMismatch { schema: Schema, item: ItemRead },
    #[error(
        "Mismatch between tape and schema while building Value: Schema::Fixed expected {expected} bytes, but tape had {actual}"
    )]
    TapeSchemaMismatchFixed { expected: usize, actual: usize },
}
