use std::io::Read;
use oval::Buffer;
use crate::decode2::object_container::Header;
use crate::decode2::{Fsm, FsmControlFlow, FsmResult, SubFsm};
use crate::decode2::codec::CodecStateMachine;
use crate::decode2::{decode_zigzag_buffer};
use crate::error::Details;
use crate::Schema;
use crate::types::Value;

pub struct DataBlockFsm<'a> {
    schema: &'a Schema,
    fsm: CodecStateMachine<SubFsm<'a>>,
    sync: [u8; 16],
    left_in_block: usize,
    need_to_read_block_byte_size: bool,
    need_to_read_sync: bool,
}
impl<'a> DataBlockFsm<'a> {
    pub fn new(header: &'a Header) -> Self {
        let fsm = CodecStateMachine::new(SubFsm::from(&header.schema), header.codec);
        Self {
            schema: &header.schema,
            fsm,
            sync: header.sync,
            left_in_block: 0,
            need_to_read_block_byte_size: false,
            need_to_read_sync: false,
        }
    }
}
impl<'a> Fsm for DataBlockFsm<'a> {
    type Output = Option<(Value, Self)>;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        // If we have just finished a block or have just been created we need to read the block metadata
        if self.left_in_block == 0 {
            // At the end of a block we need to read the sync marker
            if self.need_to_read_sync {
                if buffer.available_data() < 16 {
                    return Ok(FsmControlFlow::NeedMore(self));
                }

                let mut sync = [0; 16];
                buffer.read_exact(&mut sync).unwrap_or_else(|_| unreachable!());

                if sync != self.sync {
                    return Err(Details::GetBlockMarker.into());
                }
                self.need_to_read_sync = false;
            }

            // Read the amount of items in the block
            let Some(block) = decode_zigzag_buffer(buffer)? else {
                return Ok(FsmControlFlow::NeedMore(self));
            };

            let abs_block = block.unsigned_abs();
            let abs_block =
                usize::try_from(abs_block).map_err(|e| Details::ConvertU64ToUsize(e, abs_block))?;
            if abs_block == 0 {
                // Finished reading the file
                return Ok(FsmControlFlow::Done(None));
            }
            self.need_to_read_block_byte_size = true;
            // This will only be done after left_in_block hits 0
            self.need_to_read_sync = true;
            self.left_in_block = abs_block;
        }

        if self.need_to_read_block_byte_size {
            let Some(block) = decode_zigzag_buffer(buffer)? else {
                // Not enough data left in the buffer
                return Ok(FsmControlFlow::NeedMore(self));
            };
            // We can't use the size, but we should check that it is valid
            let _size = usize::try_from(block).map_err(|e| Details::ConvertI64ToUsize(e, block))?;
            self.need_to_read_block_byte_size = false;
        }

        match self.fsm.parse(buffer)? {
            FsmControlFlow::NeedMore(fsm) => {
                self.fsm = fsm;
                Ok(FsmControlFlow::NeedMore(self))
            }
            FsmControlFlow::Done((value, fsm)) => {
                self.left_in_block -= 1;

                // Codec's inner FSM is finished so needs to be reset
                self.fsm = fsm;
                self.fsm.reset(SubFsm::from(self.schema));

                Ok(FsmControlFlow::Done(Some((value, self))))
            }
        }
    }
}