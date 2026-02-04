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

use std::cell::Cell;

thread_local! {
    /// A thread local that is used to decide if Rust bytes need to be serialized to
    /// [`Value::Bytes`] or [`Value::Fixed`].
    ///
    /// Relies on the fact that serde's serialization process is single-threaded.
    pub(crate) static SER_BYTES_TYPE: Cell<BytesType> = const { Cell::new(BytesType::Bytes) };

    /// A thread local that is used to decide if a [`Value::Bytes`] needs to be deserialized to
    /// a [`Vec`] or slice.
    ///
    /// Relies on the fact that serde's deserialization process is single-threaded.
    pub(crate) static DE_BYTES_BORROWED: Cell<bool> = const { Cell::new(false) };
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum BytesType {
    Bytes,
    Fixed,
}

struct BytesTypeGuard(BytesType);
impl BytesTypeGuard {
    fn set(temp: BytesType) -> Self {
        let prev = SER_BYTES_TYPE.get();
        SER_BYTES_TYPE.set(temp);
        Self(prev)
    }
}

impl Drop for BytesTypeGuard {
    fn drop(&mut self) {
        SER_BYTES_TYPE.set(self.0);
    }
}

struct BorrowedGuard(bool);
impl BorrowedGuard {
    fn set(temp: bool) -> Self {
        let prev = DE_BYTES_BORROWED.get();
        DE_BYTES_BORROWED.set(temp);
        Self(prev)
    }
}

impl Drop for BorrowedGuard {
    fn drop(&mut self) {
        DE_BYTES_BORROWED.set(self.0);
    }
}

/// Efficient (de)serialization of Avro bytes values.
///
/// This module is intended to be used through the Serde `with` attribute.
/// Use [`apache_avro::serde::bytes_opt`] for optional bytes.
///
/// When used with different serialization formats, this is equivalent to [`serde_bytes`].
///
/// See usage with below example:
/// ```
/// # use apache_avro::AvroSchema;
/// # use serde::{Deserialize, Serialize};
/// #[derive(AvroSchema, Serialize, Deserialize)]
/// struct StructWithBytes {
///     #[avro(with)]
///     #[serde(with = "apache_avro::serde::bytes")]
///     vec_field: Vec<u8>,
///
///     #[avro(with = apache_avro::serde::fixed::get_schema_in_ctxt::<6>)]
///     #[serde(with = "apache_avro::serde::fixed")]
///     fixed_field: [u8; 6],
/// }
/// ```
///
/// [`apache_avro::serde::bytes_opt`]: bytes_opt
pub mod bytes {
    use serde::{Deserializer, Serializer};

    use crate::{
        Schema,
        schema::{Names, Namespace, RecordField},
    };

    /// Returns [`Schema::Bytes`]
    pub fn get_schema_in_ctxt(_: &mut Names, _: &Namespace) -> Schema {
        Schema::Bytes
    }

    /// Returns `None`
    pub fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut Names,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_bytes::serialize(bytes, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_bytes::deserialize(deserializer)
    }
}

/// Efficient (de)serialization of optional Avro bytes values.
///
/// This module is intended to be used through the Serde `with` attribute.
/// Use [`apache_avro::serde::bytes`] for non-optional bytes.
///
/// When used with different serialization formats, this is equivalent to [`serde_bytes`].
///
/// See usage with below example:
/// ```
/// # use apache_avro::AvroSchema;
/// # use serde::{Deserialize, Serialize};
/// #[derive(AvroSchema, Serialize, Deserialize)]
/// struct StructWithBytes {
///     #[avro(with)]
///     #[serde(with = "apache_avro::serde::bytes_opt")]
///     vec_field: Option<Vec<u8>>,
///
///     #[avro(with = apache_avro::serde::fixed_opt::get_schema_in_ctxt::<6>)]
///     #[serde(with = "apache_avro::serde::fixed_opt")]
///     fixed_field: Option<[u8; 6]>,
/// }
/// ```
///
/// [`apache_avro::serde::bytes`]: bytes
pub mod bytes_opt {
    use serde::{Deserializer, Serializer};
    use std::borrow::Borrow;

    use crate::{
        Schema,
        schema::{Names, Namespace, RecordField, UnionSchema},
    };

    /// Returns `Schema::Union(Schema::Null, Schema::Bytes)`
    pub fn get_schema_in_ctxt(_: &mut Names, _: &Namespace) -> Schema {
        Schema::Union(
            UnionSchema::new(vec![Schema::Null, Schema::Bytes]).expect("This is a valid union"),
        )
    }

    /// Returns `None`
    pub fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut Names,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }

    pub fn serialize<S, B>(bytes: &Option<B>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        B: Borrow<[u8]> + serde_bytes::Serialize,
    {
        serde_bytes::serialize(bytes, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_bytes::deserialize(deserializer)
    }
}

/// Efficient (de)serialization of Avro fixed values.
///
/// This module is intended to be used through the Serde `with` attribute.
/// Use [`apache_avro::serde::fixed_opt`] for optional fixed values.
///
/// When used with different serialization formats, this is equivalent to [`serde_bytes`].
///
/// See usage with below example:
/// ```
/// # use apache_avro::AvroSchema;
/// # use serde::{Deserialize, Serialize};
/// #[derive(AvroSchema, Serialize, Deserialize)]
/// struct StructWithBytes {
///     #[avro(with)]
///     #[serde(with = "apache_avro::serde::bytes")]
///     vec_field: Vec<u8>,
///
///     #[avro(with = apache_avro::serde::fixed::get_schema_in_ctxt::<6>)]
///     #[serde(with = "apache_avro::serde::fixed")]
///     fixed_field: [u8; 6],
/// }
/// ```
///
/// [`apache_avro::serde::fixed_opt`]: fixed_opt
pub mod fixed {
    use super::BytesType;
    use serde::{Deserializer, Serializer};

    use crate::{
        Schema,
        schema::{FixedSchema, Name, Names, Namespace, RecordField},
    };

    /// Returns `Schema::Fixed(N)` named `serde_avro_fixed_{N}`
    #[expect(clippy::map_entry, reason = "We don't use the value from the map")]
    pub fn get_schema_in_ctxt<const N: usize>(
        named_schemas: &mut Names,
        enclosing_namespace: &Namespace,
    ) -> Schema {
        let name = Name::new(&format!("serde_avro_fixed_{N}"))
            .expect("Name is valid")
            .fully_qualified_name(enclosing_namespace);
        if named_schemas.contains_key(&name) {
            Schema::Ref { name }
        } else {
            let schema = Schema::Fixed(FixedSchema::builder().name(name.clone()).size(N).build());
            named_schemas.insert(name, schema.clone());
            schema
        }
    }

    /// Returns `None`
    pub fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut Names,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let _guard = super::BytesTypeGuard::set(BytesType::Fixed);
        serde_bytes::serialize(bytes, serializer)
    }

    pub fn deserialize<'de, D, const N: usize>(deserializer: D) -> Result<[u8; N], D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_bytes::deserialize(deserializer)
    }
}

/// Efficient (de)serialization of optional Avro fixed values.
///
/// This module is intended to be used through the Serde `with` attribute.
/// Use [`apache_avro::serde::fixed`] for non-optional fixed values.
///
/// When used with different serialization formats, this is equivalent to [`serde_bytes`].
///
/// See usage with below example:
/// ```
/// # use apache_avro::AvroSchema;
/// # use serde::{Deserialize, Serialize};
/// #[derive(AvroSchema, Serialize, Deserialize)]
/// struct StructWithBytes {
///     #[avro(with)]
///     #[serde(with = "apache_avro::serde::bytes_opt")]
///     vec_field: Option<Vec<u8>>,
///
///     #[avro(with = apache_avro::serde::fixed_opt::get_schema_in_ctxt::<6>)]
///     #[serde(with = "apache_avro::serde::fixed_opt")]
///     fixed_field: Option<[u8; 6]>,
/// }
/// ```
///
/// [`apache_avro::serde::fixed`]: fixed
pub mod fixed_opt {
    use super::BytesType;
    use serde::{Deserializer, Serializer};
    use std::borrow::Borrow;

    use crate::{
        Schema,
        schema::{Names, Namespace, RecordField, UnionSchema},
    };

    /// Returns `Schema::Union(Schema::Null, Schema::Fixed(N))` where the fixed schema is named `serde_avro_fixed_{N}`
    pub fn get_schema_in_ctxt<const N: usize>(
        named_schemas: &mut Names,
        enclosing_namespace: &Namespace,
    ) -> Schema {
        Schema::Union(
            UnionSchema::new(vec![
                Schema::Null,
                super::fixed::get_schema_in_ctxt::<N>(named_schemas, enclosing_namespace),
            ])
            .expect("This is a valid union"),
        )
    }

    /// Returns `None`
    pub fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut Names,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }

    pub fn serialize<S, B>(bytes: &Option<B>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        B: Borrow<[u8]> + serde_bytes::Serialize,
    {
        let _guard = super::BytesTypeGuard::set(BytesType::Fixed);
        serde_bytes::serialize(bytes, serializer)
    }

    pub fn deserialize<'de, D, const N: usize>(deserializer: D) -> Result<Option<[u8; N]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_bytes::deserialize(deserializer)
    }
}

/// Efficient (de)serialization of Avro bytes/fixed borrowed values.
///
/// This module is intended to be used through the Serde `with` attribute.
///
/// Note that `&[u8]` are always serialized as [`Value::Bytes`]. However,
/// both [`Value::Bytes`] and [`Value::Fixed`] can be deserialized as `&[u8]`.
///
/// Use [`apache_avro::serde::slice_opt`] for optional bytes/fixed borrowed values.
///
/// When used with different serialization formats, this is equivalent to [`serde_bytes`].
///
/// See usage with below example:
/// ```
/// # use apache_avro::AvroSchema;
/// # use serde::{Deserialize, Serialize};
/// #[derive(AvroSchema, Serialize, Deserialize)]
/// struct StructWithBytes<'a> {
///     #[avro(with)]
///     #[serde(with = "apache_avro::serde::slice")]
///     slice_field: &'a [u8],
/// }
/// ```
///
/// [`Value::Bytes`]: crate::types::Value::Bytes
/// [`Value::Fixed`]: crate::types::Value::Fixed
/// [`apache_avro::serde::slice_opt`]: slice_opt
pub mod slice {
    use serde::{Deserializer, Serializer};

    use crate::{
        Schema,
        schema::{Names, Namespace, RecordField},
    };

    /// Returns [`Schema::Bytes`]
    pub fn get_schema_in_ctxt(_: &mut Names, _: &Namespace) -> Schema {
        Schema::Bytes
    }

    /// Returns `None`
    pub fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut Names,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_bytes::serialize(bytes, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<&'de [u8], D::Error>
    where
        D: Deserializer<'de>,
    {
        let _guard = super::BorrowedGuard::set(true);
        serde_bytes::deserialize(deserializer)
    }
}

/// Efficient (de)serialization of optional Avro bytes/fixed borrowed values.
///
/// This module is intended to be used through the Serde `with` attribute.
///
/// Note that `&[u8]` are always serialized as [`Value::Bytes`]. However,
/// both [`Value::Bytes`] and [`Value::Fixed`] can be deserialized as `&[u8]`.
///
/// Use [`apache_avro::serde::slice`] for non-optional bytes/fixed borrowed values.
///
/// When used with different serialization formats, this is equivalent to [`serde_bytes`].
///
/// See usage with below example:
/// ```
/// # use apache_avro::AvroSchema;
/// # use serde::{Deserialize, Serialize};
/// #[derive(AvroSchema, Serialize, Deserialize)]
/// struct StructWithBytes<'a> {
///     #[avro(with)]
///     #[serde(with = "apache_avro::serde::slice_opt")]
///     slice_field: Option<&'a [u8]>,
/// }
/// ```
///
/// [`Value::Bytes`]: crate::types::Value::Bytes
/// [`Value::Fixed`]: crate::types::Value::Fixed
/// [`apache_avro::serde::slice`]: mod@slice
pub mod slice_opt {
    use serde::{Deserializer, Serializer};
    use std::borrow::Borrow;

    use crate::{
        Schema,
        schema::{Names, Namespace, RecordField, UnionSchema},
    };

    /// Returns `Schema::Union(Schema::Null, Schema::Bytes)`
    pub fn get_schema_in_ctxt(_: &mut Names, _: &Namespace) -> Schema {
        Schema::Union(
            UnionSchema::new(vec![Schema::Null, Schema::Bytes]).expect("This is a valid union"),
        )
    }

    /// Returns `None`
    pub fn get_record_fields_in_ctxt(
        _: usize,
        _: &mut Names,
        _: &Namespace,
    ) -> Option<Vec<RecordField>> {
        None
    }

    pub fn serialize<S, B>(bytes: &Option<B>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        B: Borrow<[u8]> + serde_bytes::Serialize,
    {
        serde_bytes::serialize(&bytes, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<&'de [u8]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let _guard = super::BorrowedGuard::set(true);
        serde_bytes::deserialize(deserializer)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Schema, from_value, to_value, types::Value};
    use serde::{Deserialize, Serialize};

    #[test]
    fn avro_3631_validate_schema_for_struct_with_byte_types() {
        #[derive(Debug, Serialize)]
        struct TestStructWithBytes<'a> {
            #[serde(with = "crate::serde::bytes")]
            vec_field: Vec<u8>,
            #[serde(with = "crate::serde::bytes_opt")]
            vec_field_opt: Option<Vec<u8>>,

            #[serde(with = "crate::serde::fixed")]
            fixed_field: [u8; 6],
            #[serde(with = "crate::serde::fixed_opt")]
            fixed_field_opt: Option<[u8; 7]>,

            #[serde(with = "crate::serde::slice")]
            slice_field: &'a [u8],
            #[serde(with = "crate::serde::slice_opt")]
            slice_field_opt: Option<&'a [u8]>,
        }

        let test = TestStructWithBytes {
            vec_field: vec![2, 3, 4],
            vec_field_opt: Some(vec![2, 3, 4]),
            fixed_field: [1; 6],
            fixed_field_opt: Some([1; 7]),
            slice_field: &[1, 2, 3],
            slice_field_opt: Some(&[1, 2, 3]),
        };
        let value: Value = to_value(test).unwrap();
        let schema = Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "TestStructWithBytes",
              "fields": [ {
                "name": "vec_field",
                "type": "bytes"
              }, {
                "name": "vec_field_opt",
                "type": ["null", "bytes"]
              }, {
                "name": "fixed_field",
                "type": {
                  "name": "ByteData",
                  "type": "fixed",
                  "size": 6
                }
              }, {
                "name": "fixed_field_opt",
                "type": ["null", {
                  "name": "ByteData2",
                  "type": "fixed",
                  "size": 7
                } ]
              }, {
                "name": "slice_field",
                "type": "bytes"
              }, {
                "name": "slice_field_opt",
                "type": ["null", "bytes"]
              } ]
            }"#,
        )
        .unwrap();
        assert!(value.validate(&schema));
    }

    #[test]
    fn avro_3631_deserialize_value_to_struct_with_byte_types() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct TestStructWithBytes<'a> {
            #[serde(with = "crate::serde::bytes")]
            vec_field: Vec<u8>,
            #[serde(with = "crate::serde::bytes_opt")]
            vec_field_opt: Option<Vec<u8>>,
            #[serde(with = "crate::serde::bytes_opt")]
            vec_field_opt2: Option<Vec<u8>>,

            #[serde(with = "crate::serde::fixed")]
            fixed_field: [u8; 6],
            #[serde(with = "crate::serde::fixed_opt")]
            fixed_field_opt: Option<[u8; 7]>,
            #[serde(with = "crate::serde::fixed_opt")]
            fixed_field_opt2: Option<[u8; 8]>,

            #[serde(with = "crate::serde::slice")]
            slice_bytes_field: &'a [u8],
            #[serde(with = "crate::serde::slice_opt")]
            slice_bytes_field_opt: Option<&'a [u8]>,
            #[serde(with = "crate::serde::slice_opt")]
            slice_bytes_field_opt2: Option<&'a [u8]>,

            #[serde(with = "crate::serde::slice")]
            slice_fixed_field: &'a [u8],
            #[serde(with = "crate::serde::slice_opt")]
            slice_fixed_field_opt: Option<&'a [u8]>,
            #[serde(with = "crate::serde::slice_opt")]
            slice_fixed_field_opt2: Option<&'a [u8]>,
        }

        let expected = TestStructWithBytes {
            vec_field: vec![3, 33],
            vec_field_opt: Some(vec![4, 44]),
            vec_field_opt2: None,
            fixed_field: [1; 6],
            fixed_field_opt: Some([7; 7]),
            fixed_field_opt2: None,
            slice_bytes_field: &[1, 11, 111],
            slice_bytes_field_opt: Some(&[5, 5, 5, 5, 5]),
            slice_bytes_field_opt2: None,
            slice_fixed_field: &[2, 22, 222],
            slice_fixed_field_opt: Some(&[3, 3, 3]),
            slice_fixed_field_opt2: None,
        };

        let value = Value::Record(vec![
            (
                "vec_field".to_owned(),
                Value::Bytes(expected.vec_field.clone()),
            ),
            (
                "vec_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Bytes(
                        expected.vec_field_opt.as_ref().unwrap().clone(),
                    )),
                ),
            ),
            (
                "vec_field_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "fixed_field".to_owned(),
                Value::Fixed(expected.fixed_field.len(), expected.fixed_field.to_vec()),
            ),
            (
                "fixed_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Fixed(
                        expected.fixed_field_opt.as_ref().unwrap().len(),
                        expected.fixed_field_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "fixed_field_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "slice_bytes_field".to_owned(),
                Value::Bytes(expected.slice_bytes_field.to_vec()),
            ),
            (
                "slice_bytes_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Bytes(
                        expected.slice_bytes_field_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "slice_bytes_field_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "slice_fixed_field".to_owned(),
                Value::Fixed(
                    expected.slice_fixed_field.len(),
                    expected.slice_fixed_field.to_vec(),
                ),
            ),
            (
                "slice_fixed_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Fixed(
                        expected.slice_fixed_field_opt.as_ref().unwrap().len(),
                        expected.slice_fixed_field_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "slice_fixed_field_opt2".to_owned(),
                Value::Union(1, Box::new(Value::Null)),
            ),
        ]);
        assert_eq!(expected, from_value(&value).unwrap());
    }

    #[test]
    fn avro_3631_serialize_struct_to_value_with_byte_types() {
        #[derive(Debug, Serialize)]
        struct TestStructWithBytes<'a> {
            array_field: &'a [u8],
            vec_field: Vec<u8>,

            #[serde(with = "crate::serde::fixed")]
            vec_field2: Vec<u8>,
            #[serde(with = "crate::serde::fixed_opt")]
            vec_field2_opt: Option<Vec<u8>>,
            #[serde(with = "crate::serde::fixed_opt")]
            vec_field2_opt2: Option<Vec<u8>>,

            #[serde(with = "crate::serde::bytes")]
            vec_field3: Vec<u8>,
            #[serde(with = "crate::serde::bytes_opt")]
            vec_field3_opt: Option<Vec<u8>>,
            #[serde(with = "crate::serde::bytes_opt")]
            vec_field3_opt2: Option<Vec<u8>>,

            #[serde(with = "crate::serde::fixed")]
            fixed_field: [u8; 6],
            #[serde(with = "crate::serde::fixed_opt")]
            fixed_field_opt: Option<[u8; 5]>,
            #[serde(with = "crate::serde::fixed_opt")]
            fixed_field_opt2: Option<[u8; 4]>,

            #[serde(with = "crate::serde::fixed")]
            fixed_field2: &'a [u8],
            #[serde(with = "crate::serde::fixed_opt")]
            fixed_field2_opt: Option<&'a [u8]>,
            #[serde(with = "crate::serde::fixed_opt")]
            fixed_field2_opt2: Option<&'a [u8]>,

            #[serde(with = "crate::serde::bytes")]
            bytes_field: &'a [u8],
            #[serde(with = "crate::serde::bytes_opt")]
            bytes_field_opt: Option<&'a [u8]>,
            #[serde(with = "crate::serde::bytes_opt")]
            bytes_field_opt2: Option<&'a [u8]>,

            #[serde(with = "crate::serde::bytes")]
            bytes_field2: [u8; 6],
            #[serde(with = "crate::serde::bytes_opt")]
            bytes_field2_opt: Option<[u8; 7]>,
            #[serde(with = "crate::serde::bytes_opt")]
            bytes_field2_opt2: Option<[u8; 8]>,
        }

        let test = TestStructWithBytes {
            array_field: &[1, 11, 111],
            vec_field: vec![3, 33],
            vec_field2: vec![4, 44],
            vec_field2_opt: Some(vec![14, 144]),
            vec_field2_opt2: None,
            vec_field3: vec![5, 55],
            vec_field3_opt: Some(vec![15, 155]),
            vec_field3_opt2: None,
            fixed_field: [1; 6],
            fixed_field_opt: Some([6; 5]),
            fixed_field_opt2: None,
            fixed_field2: &[6, 66],
            fixed_field2_opt: Some(&[7, 77]),
            fixed_field2_opt2: None,
            bytes_field: &[2, 22, 222],
            bytes_field_opt: Some(&[3, 33, 233]),
            bytes_field_opt2: None,
            bytes_field2: [2; 6],
            bytes_field2_opt: Some([2; 7]),
            bytes_field2_opt2: None,
        };
        let expected = Value::Record(vec![
            (
                "array_field".to_owned(),
                Value::Array(
                    test.array_field
                        .iter()
                        .map(|&i| Value::Int(i as i32))
                        .collect(),
                ),
            ),
            (
                "vec_field".to_owned(),
                Value::Array(
                    test.vec_field
                        .iter()
                        .map(|&i| Value::Int(i as i32))
                        .collect(),
                ),
            ),
            (
                "vec_field2".to_owned(),
                Value::Fixed(test.vec_field2.len(), test.vec_field2.clone()),
            ),
            (
                "vec_field2_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Fixed(
                        test.vec_field2_opt.as_ref().unwrap().len(),
                        test.vec_field2_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "vec_field2_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "vec_field3".to_owned(),
                Value::Bytes(test.vec_field3.clone()),
            ),
            (
                "vec_field3_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Bytes(test.vec_field3_opt.as_ref().unwrap().clone())),
                ),
            ),
            (
                "vec_field3_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "fixed_field".to_owned(),
                Value::Fixed(test.fixed_field.len(), test.fixed_field.to_vec()),
            ),
            (
                "fixed_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Fixed(
                        test.fixed_field_opt.as_ref().unwrap().len(),
                        test.fixed_field_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "fixed_field_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "fixed_field2".to_owned(),
                Value::Fixed(test.fixed_field2.len(), test.fixed_field2.to_vec()),
            ),
            (
                "fixed_field2_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Fixed(
                        test.fixed_field2_opt.as_ref().unwrap().len(),
                        test.fixed_field2_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "fixed_field2_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "bytes_field".to_owned(),
                Value::Bytes(test.bytes_field.to_vec()),
            ),
            (
                "bytes_field_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Bytes(
                        test.bytes_field_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "bytes_field_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "bytes_field2".to_owned(),
                Value::Bytes(test.bytes_field2.to_vec()),
            ),
            (
                "bytes_field2_opt".to_owned(),
                Value::Union(
                    1,
                    Box::new(Value::Bytes(
                        test.bytes_field2_opt.as_ref().unwrap().to_vec(),
                    )),
                ),
            ),
            (
                "bytes_field2_opt2".to_owned(),
                Value::Union(0, Box::new(Value::Null)),
            ),
        ]);
        assert_eq!(expected, to_value(test).unwrap());
    }
}
