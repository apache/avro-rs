use std::collections::HashMap;
use oval::Buffer;
use crate::decode2::{Fsm, FsmControlFlow, FsmResult, SubFsm};
use crate::decode2::primitive::bytes::StringFsm;
use crate::decode2::decode_zigzag_buffer;
use crate::error::Details;
use crate::Schema;
use crate::types::Value;

pub struct ArrayFsm<'a> {
    schema: &'a Schema,
    sub_fsm: Option<Box<SubFsm<'a>>>,
    left_in_current_block: usize,
    need_to_read_block_byte_size: bool,
    values: Vec<Value>,
}
impl<'a> ArrayFsm<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            sub_fsm: None,
            left_in_current_block: 0,
            need_to_read_block_byte_size: false,
            values: Vec::new(),
        }
    }
}
impl<'a> Fsm for ArrayFsm<'a> {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        // We loop until we're finished or we need more data
        loop {
            // If we finished the last block (or are newly created) read the block info
            if self.left_in_current_block == 0 {
                let Some(block) = decode_zigzag_buffer(buffer)? else {
                    return Ok(FsmControlFlow::NeedMore(self));
                };

                // Done parsing the blocks
                if block == 0 {
                    return Ok(FsmControlFlow::Done(Value::Array(self.values)));
                }

                // Need to read the block byte size when block is negative
                self.need_to_read_block_byte_size = block.is_negative();

                // We do the rest with the absolute block size
                let abs_block = usize::try_from(block.unsigned_abs())
                    .map_err(|e| Details::ConvertU64ToUsize(e, block.unsigned_abs()))?;

                self.left_in_current_block = abs_block;
                self.values.reserve(abs_block);
            }

            // If the block length was negative we need to read the block size
            if self.need_to_read_block_byte_size {
                let Some(block) = decode_zigzag_buffer(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(FsmControlFlow::NeedMore(self));
                };

                // Make sure the value is sane
                // TODO: Maybe use safe_len here?
                let _ = usize::try_from(block)
                    .map_err(|e| Details::ConvertI64ToUsize(e, block))?;

                // This is not necessary, as it will be overwritten before being read again
                // but it does show the intent more clearly
                self.need_to_read_block_byte_size = false;
            }

            // Check if we already have a state machine to run
            if let Some(sub_fsm) = self.sub_fsm.as_deref_mut() {
                let fsm = std::mem::take(sub_fsm);
                match fsm.parse(buffer)? {
                    FsmControlFlow::NeedMore(fsm) => {
                        let _ = std::mem::replace(sub_fsm, fsm);
                        return Ok(FsmControlFlow::NeedMore(self));
                    }
                    FsmControlFlow::Done(value) => {
                        self.left_in_current_block -= 1;
                        self.values.push(value);
                        // Assume we need to read another value
                        // We do this to reuse the Box we already have and therefore preventing a lot
                        // of allocations
                        let _ = std::mem::replace(sub_fsm, SubFsm::from(self.schema));
                        // Continue the loop
                        continue;
                    }
                }
            } else {
                let fsm = SubFsm::from(self.schema);
                match fsm.parse(buffer)? {
                    FsmControlFlow::NeedMore(fsm) => {
                        // Save the current progress and ask for more bytes
                        self.sub_fsm = Some(Box::new(fsm));
                        return Ok(FsmControlFlow::NeedMore(self));
                    }
                    FsmControlFlow::Done(value) => {
                        self.left_in_current_block -= 1;
                        self.values.push(value);
                        // As we don't have a box yet, we don't create the next state machine as that
                        // allocation might be unnecessary
                        continue;
                    }
                }
            }
        }
    }
}

pub struct MapFsm<'a> {
    schema: &'a Schema,
    sub_fsm: Option<Box<SubFsm<'a>>>,
    left_in_current_block: usize,
    need_to_read_block_byte_size: bool,
    current_key: Option<String>,
    values: HashMap<String, Value>,
}
impl<'a> MapFsm<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            sub_fsm: None,
            left_in_current_block: 0,
            need_to_read_block_byte_size: false,
            current_key: None,
            values: HashMap::new(),
        }
    }
}
impl<'a> Fsm for MapFsm<'a> {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        // We loop until we're finished or we need more data
        loop {
            // If we finished the last block (or are newly created) read the block info
            if self.left_in_current_block == 0 {
                let Some(block) = decode_zigzag_buffer(buffer)? else {
                    return Ok(FsmControlFlow::NeedMore(self));
                };

                // Done parsing the blocks
                if block == 0 {
                    return Ok(FsmControlFlow::Done(Value::Map(self.values)));
                }

                // Need to read the block byte size when block is negative
                self.need_to_read_block_byte_size = block.is_negative();

                // We do the rest with the absolute block size
                let abs_block = usize::try_from(block.unsigned_abs())
                    .map_err(|e| Details::ConvertU64ToUsize(e, block.unsigned_abs()))?;

                self.left_in_current_block = abs_block;
                self.values.reserve(abs_block);
            }

            // If the block length was negative we need to read the block size
            if self.need_to_read_block_byte_size {
                let Some(block) = decode_zigzag_buffer(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(FsmControlFlow::NeedMore(self));
                };

                // Make sure the value is sane
                // TODO: Maybe use safe_len here?
                let _ = usize::try_from(block)
                    .map_err(|e| Details::ConvertI64ToUsize(e, block))?;

                // This is not necessary, as it will be overwritten before being read again
                // but it does show the intent more clearly
                self.need_to_read_block_byte_size = false;
            }

            if let Some(key) = self.current_key.take() {
                // Check if we already have a state machine to run
                if let Some(sub_fsm) = self.sub_fsm.as_deref_mut() {
                    let fsm = std::mem::take(sub_fsm);
                    match fsm.parse(buffer)? {
                        FsmControlFlow::NeedMore(fsm) => {
                            let _ = std::mem::replace(sub_fsm, fsm);
                            self.current_key = Some(key);
                            return Ok(FsmControlFlow::NeedMore(self));
                        }
                        FsmControlFlow::Done(value) => {
                            self.left_in_current_block -= 1;
                            self.values.insert(key, value);
                            // Assume we need to read another key
                            // We do this to reuse the Box we already have and therefore preventing a lot
                            // of allocations
                            let _ = std::mem::replace(sub_fsm, SubFsm::String(StringFsm::default()));
                            // Continue the loop
                            continue;
                        }
                    }
                } else {
                    let fsm = SubFsm::from(self.schema);
                    match fsm.parse(buffer)? {
                        FsmControlFlow::NeedMore(fsm) => {
                            // Save the current progress and ask for more bytes
                            self.sub_fsm = Some(Box::new(fsm));
                            self.current_key = Some(key);
                            return Ok(FsmControlFlow::NeedMore(self));
                        }
                        FsmControlFlow::Done(value) => {
                            self.left_in_current_block -= 1;
                            self.values.insert(key, value);
                            // As we don't have a box yet, we don't create the next state machine as that
                            // allocation might be unnecessary
                            continue;
                        }
                    }
                }
            } else {
                // Check if we already have a state machine to run
                if let Some(sub_fsm) = self.sub_fsm.as_deref_mut() {
                    let fsm = std::mem::take(sub_fsm);
                    match fsm.parse(buffer)? {
                        FsmControlFlow::NeedMore(fsm) => {
                            let _ = std::mem::replace(sub_fsm, fsm);
                            return Ok(FsmControlFlow::NeedMore(self));
                        }
                        FsmControlFlow::Done(value) => {
                            let Value::String(key) = value else {
                                unreachable!()
                            };
                            self.current_key = Some(key);
                            // Now we need to read the value, might as well reuse the allocation
                            let _ = std::mem::replace(sub_fsm, SubFsm::from(self.schema));
                            // Continue the loop
                            continue;
                        }
                    }
                } else {
                    let fsm = SubFsm::String(StringFsm::default());
                    match fsm.parse(buffer)? {
                        FsmControlFlow::NeedMore(fsm) => {
                            // Save the current progress and ask for more bytes
                            self.sub_fsm = Some(Box::new(fsm));
                            return Ok(FsmControlFlow::NeedMore(self));
                        }
                        FsmControlFlow::Done(value) => {
                            let Value::String(key) = value else {
                                unreachable!()
                            };
                            self.current_key = Some(key);
                            // We don't have an allocation so there is nothing to reuse
                            continue;
                        }
                    }
                }
            }
        }
    }
}