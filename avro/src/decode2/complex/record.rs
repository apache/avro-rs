use oval::Buffer;
use crate::decode2::{Fsm, FsmControlFlow, FsmResult, SubFsm};
use crate::schema::RecordSchema;
use crate::types::Value;

pub struct RecordFsm<'a> {
    schema: &'a RecordSchema,
    current_field: usize,
    sub_fsm: Option<Box<SubFsm<'a>>>,
    fields: Vec<(String, Value)>,
}
impl<'a> RecordFsm<'a> {
    pub fn new(schema: &'a RecordSchema) -> Self {
        Self {
            schema,
            current_field: 0,
            sub_fsm: None,
            fields: Vec::new(),
        }
    }
}

impl<'a> Fsm for RecordFsm<'a> {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        // All fields are there, this should only be possible for an empty record. Which should
        // not exist
        if self.current_field >= self.schema.fields.len() {
            return Ok(FsmControlFlow::Done(Value::Record(self.fields)));
        }
        loop {
            if let Some(sub_fsm) = self.sub_fsm.as_deref_mut() {
                let fsm = std::mem::take(sub_fsm);
                match fsm.parse(buffer)? {
                    FsmControlFlow::NeedMore(fsm) => {
                        let _ = std::mem::replace(sub_fsm, fsm);
                        return Ok(FsmControlFlow::NeedMore(self));
                    }
                    FsmControlFlow::Done(value) => {
                        // Finished reading a field, add the name and value to the list
                        let field_name = self.schema.fields[self.current_field].name.clone();
                        self.fields.push((field_name, value));
                        assert_eq!(self.current_field, self.fields.len() - 1);

                        self.current_field += 1;

                        // If there is a next field, prepare the state machine in the same box
                        if let Some(field) = self.schema.fields.get(self.current_field) {
                            let _ = std::mem::replace(sub_fsm, SubFsm::from(&field.schema));
                            // Restart the loop
                            continue;
                        } else {
                            assert_eq!(self.fields.len(), self.schema.fields.len());
                            return Ok(FsmControlFlow::Done(Value::Record(self.fields)));
                        }
                    }
                }
            } else {
                let schema = &self.schema.fields[self.current_field].schema;
                match SubFsm::from(schema).parse(buffer)? {
                    FsmControlFlow::NeedMore(fsm) => {
                        self.sub_fsm = Some(Box::new(fsm));
                        return Ok(FsmControlFlow::NeedMore(self));
                    }
                    FsmControlFlow::Done(value) => {
                        // Finished reading a field, add the name and value to the list
                        let field_name = self.schema.fields[self.current_field].name.clone();
                        self.fields.push((field_name, value));
                        assert_eq!(self.current_field, self.fields.len() - 1);

                        self.current_field += 1;

                        // If there is no next field, return the record
                        if self.schema.fields.get(self.current_field).is_none() {
                            assert_eq!(self.fields.len(), self.schema.fields.len());
                            return Ok(FsmControlFlow::Done(Value::Record(self.fields)));
                        }
                    }
                }

            }
        }
    }
}