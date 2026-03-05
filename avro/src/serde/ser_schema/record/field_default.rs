use crate::schema::SchemaKind;
use crate::{Schema, serde::ser_schema::SERIALIZING_SCHEMA_DEFAULT};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug)]
pub struct SchemaAwareDefault<'v, 's> {
    value: &'v Value,
    schema: &'s Schema,
}

impl<'v, 's> SchemaAwareDefault<'v, 's> {
    pub fn new(value: &'v Value, schema: &'s Schema) -> Self {
        Self { value, schema }
    }

    fn serialize_as_newtype_variant<S>(
        &self,
        serializer: S,
        index: usize,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let Schema::Union(union) = self.schema else {
            panic!("This function should only be called if self.schema is a Union");
        };
        let value = Self::new(self.value, &union.variants()[index]);
        let index = index as u32;
        serializer.serialize_newtype_variant(
            SERIALIZING_SCHEMA_DEFAULT,
            index,
            SERIALIZING_SCHEMA_DEFAULT,
            &value,
        )
    }

    fn recursive_type_check(value: &Value, schema: &Schema) -> bool {
        match (value, schema) {
            (Value::Null, Schema::Null)
            | (Value::Bool(_), Schema::Boolean)
            | (Value::String(_), Schema::Bytes | Schema::String) => true,
            (Value::Number(n), Schema::Int | Schema::Date | Schema::TimeMillis) if n.is_i64() => {
                let long = n.as_i64().unwrap();
                i32::try_from(long).is_ok()
            }
            (
                Value::Number(n),
                Schema::Long
                | Schema::TimeMicros
                | Schema::TimestampMillis
                | Schema::TimestampMicros
                | Schema::TimestampNanos
                | Schema::LocalTimestampMillis
                | Schema::LocalTimestampMicros
                | Schema::LocalTimestampNanos,
            ) if n.is_i64() => true,
            (Value::Number(n), Schema::Float | Schema::Double) if n.is_f64() => true,
            (Value::String(s), Schema::Fixed(fixed)) => s.len() == fixed.size,
            (Value::String(s), Schema::Enum(enum_schema)) => enum_schema.symbols.contains(s),
            (Value::Object(o), Schema::Record(record)) => record.fields.iter().all(|field| {
                if let Some(value) = o.get(&field.name) {
                    Self::recursive_type_check(value, &field.schema)
                } else {
                    field.default.is_some()
                }
            }),
            (Value::Object(o), Schema::Map(map)) => o
                .values()
                .all(|value| Self::recursive_type_check(value, &map.types)),
            (Value::Array(a), Schema::Array(array)) => a
                .iter()
                .all(|value| Self::recursive_type_check(value, &array.items)),
            (_, Schema::Union(union)) => union
                .variants()
                .iter()
                .any(|variant| Self::recursive_type_check(value, variant)),
            _ => false,
        }
    }
}

impl<'v, 's> Serialize for SchemaAwareDefault<'v, 's> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;
        match (&self.value, self.schema) {
            (Value::Null, Schema::Null) => serializer.serialize_unit(),
            (Value::Bool(boolean), Schema::Boolean) => serializer.serialize_bool(*boolean),
            (Value::Number(n), Schema::Int | Schema::Date | Schema::TimeMillis) if n.is_i64() => {
                let long = n.as_i64().unwrap();
                let int = i32::try_from(long).map_err(|_| {
                    S::Error::custom(format!("Default {long} is too large for {:?}", self.schema))
                })?;
                serializer.serialize_i32(int)
            }
            (
                Value::Number(n),
                Schema::Long
                | Schema::TimeMicros
                | Schema::TimestampMillis
                | Schema::TimestampMicros
                | Schema::TimestampNanos
                | Schema::LocalTimestampMillis
                | Schema::LocalTimestampMicros
                | Schema::LocalTimestampNanos,
            ) if n.is_i64() => {
                let long = n.as_i64().unwrap();
                serializer.serialize_i64(long)
            }
            (Value::Number(n), Schema::Float) if n.is_f64() => {
                let float = n.as_f64().unwrap();
                serializer.serialize_f32(float as f32)
            }
            (Value::Number(n), Schema::Double) if n.is_f64() => {
                let double = n.as_f64().unwrap();
                serializer.serialize_f64(double)
            }
            (Value::String(s), Schema::Bytes | Schema::Fixed(_)) => {
                serializer.serialize_bytes(s.as_bytes())
            }
            (Value::String(s), Schema::String) => serializer.serialize_str(s),
            (Value::String(s), Schema::Enum(enum_schema)) => {
                let Some((variant_index, _)) = enum_schema
                    .symbols
                    .iter()
                    .enumerate()
                    .find(|(_i, symbol)| *symbol == s)
                else {
                    return Err(S::Error::custom(format!(
                        "Could not find `{s}` in enum: {enum_schema:?}"
                    )));
                };

                serializer.serialize_unit_variant(
                    SERIALIZING_SCHEMA_DEFAULT,
                    variant_index as u32,
                    SERIALIZING_SCHEMA_DEFAULT,
                )
            }
            // This abuses the support for flattened fields, which are also serialized as a map.
            (Value::Object(o), Schema::Record(record)) => serializer.collect_map(
                o.iter()
                    .enumerate()
                    .map(|(i, (k, v))| (k, Self::new(v, &record.fields[i].schema))),
            ),
            (Value::Object(o), Schema::Map(map)) => {
                serializer.collect_map(o.iter().map(|(k, v)| (k, Self::new(v, &map.types))))
            }
            (Value::Array(a), Schema::Array(array)) => {
                serializer.collect_seq(a.iter().map(|v| Self::new(v, &array.items)))
            }
            (_, Schema::Union(union)) => {
                if union.variants().len() == 2
                    && let Some(index) = union.index().get(&SchemaKind::Null)
                {
                    // Fast path for options
                    if self.value == &Value::Null {
                        serializer.serialize_none()
                    } else {
                        let other_index = (index + 1) & 1;
                        let value = Self::new(self.value, &union.variants()[other_index]);
                        serializer.serialize_some(&value)
                    }
                } else {
                    // Find the first variant that can match this value
                    for (index, variant) in union.variants().iter().enumerate() {
                        match (self.value, variant) {
                            (Value::Null, Schema::Null) => {
                                let index = index as u32;
                                return serializer.serialize_unit_variant(
                                    SERIALIZING_SCHEMA_DEFAULT,
                                    index,
                                    SERIALIZING_SCHEMA_DEFAULT,
                                );
                            }
                            _ if Self::recursive_type_check(self.value, variant) => {
                                return self.serialize_as_newtype_variant(serializer, index);
                            }
                            _ => {}
                        }
                    }
                    Err(S::Error::custom(format!(
                        "Could not match default to any variant of {:?}, default: {:?}",
                        self.schema, self.value
                    )))
                }
            }
            _ => Err(S::Error::custom(format!(
                "Unexpected default for {:?}, default: {:?}",
                self.schema, self.value
            ))),
        }
    }
}
