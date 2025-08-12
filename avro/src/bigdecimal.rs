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

pub use bigdecimal::BigDecimal;

#[synca::synca(
  #[cfg(feature = "tokio")]
  pub mod tokio { },
  #[cfg(feature = "sync")]
  pub mod sync {
    sync!();
    replace!(
      crate::bigdecimal::tokio => crate::bigdecimal::sync,
      crate::codec::tokio => crate::codec::sync,
      crate::decode::tokio => crate::decode::sync,
      crate::encode::tokio => crate::encode::sync,
      crate::error::tokio => crate::error::sync,
      crate::schema::tokio => crate::schema::sync,
      crate::reader::tokio => crate::reader::sync,
      crate::util::tokio => crate::util::sync,
      crate::types::tokio => crate::types::sync,
      crate::writer::tokio => crate::writer::sync,
      #[tokio::test] => #[test]
    );
  }
)]
mod bigdecimal {
    use crate::AvroResult;
    use crate::{
        decode::tokio::{decode_len, decode_long},
        encode::tokio::{encode_bytes, encode_long},
        error::Details,
        types::Value,
    };
    use bigdecimal::BigDecimal;
    use num_bigint::BigInt;

    pub(crate) async fn big_decimal_as_bytes(decimal: &BigDecimal) -> AvroResult<Vec<u8>> {
        let mut buffer: Vec<u8> = Vec::new();
        let (big_int, exponent): (BigInt, i64) = decimal.as_bigint_and_exponent();
        let big_endian_value: Vec<u8> = big_int.to_signed_bytes_be();
        encode_bytes(&big_endian_value, &mut buffer).await?;
        encode_long(exponent, &mut buffer).await?;

        Ok(buffer)
    }

    pub(crate) async fn serialize_big_decimal(decimal: &BigDecimal) -> AvroResult<Vec<u8>> {
        // encode big decimal, without global size
        let buffer = big_decimal_as_bytes(decimal).await?;

        // encode global size and content
        let mut final_buffer: Vec<u8> = Vec::new();
        encode_bytes(&buffer, &mut final_buffer).await?;

        Ok(final_buffer)
    }

    pub(crate) async fn deserialize_big_decimal(bytes: &Vec<u8>) -> AvroResult<BigDecimal> {
        let mut bytes: &[u8] = bytes.as_slice();
        let mut big_decimal_buffer = match decode_len(&mut bytes).await {
            Ok(size) => vec![0u8; size],
            Err(err) => return Err(Details::BigDecimalLen(Box::new(err)).into()),
        };

        #[synca::cfg(sync)]
        use std::io::Read;
        #[synca::cfg(tokio)]
        use tokio::io::AsyncRead;
        #[synca::cfg(tokio)]
        use tokio::io::AsyncReadExt;

        bytes
            .read_exact(&mut big_decimal_buffer[..]).await
            .map_err(Details::ReadDouble)?;

        match decode_long(&mut bytes).await {
            Ok(Value::Long(scale_value)) => {
                let big_int: BigInt = BigInt::from_signed_bytes_be(&big_decimal_buffer);
                let decimal = BigDecimal::new(big_int, scale_value);
                Ok(decimal)
            }
            _ => Err(Details::BigDecimalScale.into()),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::{
            codec::tokio::Codec, error::Error, reader::tokio::Reader,
            schema::tokio::SchemaExt, types::Record, writer::tokio::Writer,
        };
        use apache_avro_test_helper::TestResult;
        use bigdecimal::{One, Zero};
        #[synca::cfg(tokio)]
        use futures::StreamExt;
        use pretty_assertions::assert_eq;
        #[synca::cfg(sync)]
        use std::fs::File;
        #[synca::cfg(sync)]
        use std::io::BufReader;
        use std::{
            ops::{Div, Mul},
            str::FromStr,
        };
        #[synca::cfg(tokio)]
        use tokio::fs::File;
        #[synca::cfg(tokio)]
        use tokio::io::BufReader;

        #[tokio::test]
        async fn test_avro_3779_bigdecimal_serial() -> TestResult {
            let value: BigDecimal =
                bigdecimal::BigDecimal::from(-1421).div(bigdecimal::BigDecimal::from(2));
            let mut current: BigDecimal = BigDecimal::one();

            for iter in 1..180 {
                let buffer: Vec<u8> = serialize_big_decimal(&current).await?;

                let mut as_slice = buffer.as_slice();
                decode_long(&mut as_slice).await?;

                let mut result: Vec<u8> = Vec::new();
                result.extend_from_slice(as_slice);

                let deserialize_big_decimal: Result<BigDecimal, Error> =
                    deserialize_big_decimal(&result).await;
                assert!(
                    deserialize_big_decimal.is_ok(),
                    "can't deserialize for iter {iter}"
                );
                assert_eq!(current, deserialize_big_decimal?, "not equals for {iter}");
                current = current.mul(&value);
            }

            let buffer: Vec<u8> = serialize_big_decimal(&BigDecimal::zero()).await?;
            let mut as_slice = buffer.as_slice();
            decode_long(&mut as_slice).await?;

            let mut result: Vec<u8> = Vec::new();
            result.extend_from_slice(as_slice);

            let deserialize_big_decimal: Result<BigDecimal, Error> =
                deserialize_big_decimal(&result).await;
            assert!(
                deserialize_big_decimal.is_ok(),
                "can't deserialize for zero"
            );
            assert_eq!(
                BigDecimal::zero(),
                deserialize_big_decimal?,
                "not equals for zero"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_avro_3779_record_with_bg() -> TestResult {
            let schema_str = r#"
        {
          "type": "record",
          "name": "test",
          "fields": [
            {
              "name": "field_name",
              "type": "bytes",
              "logicalType": "big-decimal"
            }
          ]
        }
        "#;
            let schema = SchemaExt::parse_str(schema_str).await?;

            // build record with big decimal value
            let mut record = Record::new(&schema).unwrap();
            let val = BigDecimal::new(BigInt::from(12), 2);
            record.put("field_name", val.clone());

            // write a record
            let codec = Codec::Null;
            let mut writer = Writer::builder()
                .schema(&schema)
                .codec(codec)
                .writer(Vec::new())
                .build();

            writer.append(record.clone()).await?;
            writer.flush().await?;

            // read record
            let wrote_data = writer.into_inner().await?;
            let mut reader = Reader::new(&wrote_data[..]).await?;

            let value = reader.next().await.unwrap()?;

            // extract field value
            let big_decimal_value: &Value = match value {
                Value::Record(ref fields) => Ok(&fields[0].1),
                other => Err(format!("Expected a Value::Record, got: {other:?}")),
            }?;

            let x1res: &BigDecimal = match big_decimal_value {
                Value::BigDecimal(s) => Ok(s),
                other => Err(format!("Expected Value::BigDecimal, got: {other:?}")),
            }?;
            assert_eq!(&val, x1res);

            Ok(())
        }

        #[tokio::test]
        async fn test_avro_3779_from_java_file() -> TestResult {
            // Open file generated with Java code to ensure compatibility
            // with Java big decimal logical type.
            let file = File::open("./tests/bigdec.avro").await?;
            let mut reader = Reader::new(BufReader::new(file)).await?;
            let next_element = reader.next().await;
            assert!(next_element.is_some());
            let value = next_element.unwrap()?;
            let bg = match value {
                Value::Record(ref fields) => Ok(&fields[0].1),
                other => Err(format!("Expected a Value::Record, got: {other:?}")),
            }?;
            let value_big_decimal = match bg {
                Value::BigDecimal(val) => Ok(val),
                other => Err(format!("Expected a Value::BigDecimal, got: {other:?}")),
            }?;

            let ref_value = BigDecimal::from_str("2.24")?;
            assert_eq!(&ref_value, value_big_decimal);

            Ok(())
        }
    }
}
