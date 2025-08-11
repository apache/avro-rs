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

#[synca::synca(
  #[cfg(feature = "tokio")]
  pub mod tokio {},
  #[cfg(feature = "sync")]
  pub mod sync {
    sync!();
    replace!(
      tokio::io::AsyncRead + Unpin => std::io::Read,
      bigdecimal::tokio => bigdecimal::sync,
      decode::tokio => decode::sync,
      encode::tokio => encode::sync,
      error::tokio => error::sync,
      schema::tokio => schema::sync,
      util::tokio => util::sync,
      #[tokio::test] => #[test]
    );
  }
)]
mod decimal {

    use crate::{AvroResult, error::tokio::Details, error::tokio::Error};
    use num_bigint::{BigInt, Sign};
    use serde::{Deserialize, Serialize, Serializer, de::SeqAccess};

    #[derive(Debug, Clone, Eq)]
    pub struct Decimal {
        value: BigInt,
        len: usize,
    }

    impl Serialize for Decimal {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match self.to_vec() {
                Ok(ref bytes) => serializer.serialize_bytes(bytes),
                Err(e) => Err(serde::ser::Error::custom(e)),
            }
        }
    }
    impl<'de> Deserialize<'de> for Decimal {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct DecimalVisitor;
            impl<'de> serde::de::Visitor<'de> for DecimalVisitor {
                type Value = Decimal;

                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("a byte slice or seq of bytes")
                }

                fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    Ok(Decimal::from(v))
                }
                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: SeqAccess<'de>,
                {
                    let mut bytes = Vec::new();
                    while let Some(value) = seq.next_element::<u8>()? {
                        bytes.push(value);
                    }

                    Ok(Decimal::from(bytes))
                }
            }
            deserializer.deserialize_bytes(DecimalVisitor)
        }
    }

    // We only care about value equality, not byte length. Can two equal `BigInt`s have two different
    // byte lengths?
    impl PartialEq for Decimal {
        fn eq(&self, other: &Self) -> bool {
            self.value == other.value
        }
    }

    impl Decimal {
        pub(crate) fn len(&self) -> usize {
            self.len
        }

        pub(crate) fn to_vec(&self) -> AvroResult<Vec<u8>> {
            self.to_sign_extended_bytes_with_len(self.len)
        }

        pub(crate) fn to_sign_extended_bytes_with_len(&self, len: usize) -> AvroResult<Vec<u8>> {
            let sign_byte = 0xFF * u8::from(self.value.sign() == Sign::Minus);
            let mut decimal_bytes = vec![sign_byte; len];
            let raw_bytes = self.value.to_signed_bytes_be();
            let num_raw_bytes = raw_bytes.len();
            let start_byte_index = len.checked_sub(num_raw_bytes).ok_or(Details::SignExtend {
                requested: len,
                needed: num_raw_bytes,
            })?;
            decimal_bytes[start_byte_index..].copy_from_slice(&raw_bytes);
            Ok(decimal_bytes)
        }
    }

    impl From<Decimal> for BigInt {
        fn from(decimal: Decimal) -> Self {
            decimal.value
        }
    }

    /// Gets the internal byte array representation of a referenced decimal.
    /// Usage:
    /// ```
    /// use apache_avro::Decimal;
    /// use std::convert::TryFrom;
    ///
    /// let decimal = Decimal::from(vec![1, 24]);
    /// let maybe_bytes = <Vec<u8>>::try_from(&decimal);
    /// ```
    impl std::convert::TryFrom<&Decimal> for Vec<u8> {
        type Error = Error;

        fn try_from(decimal: &Decimal) -> Result<Self, Self::Error> {
            decimal.to_vec()
        }
    }

    /// Gets the internal byte array representation of an owned decimal.
    /// Usage:
    /// ```
    /// use apache_avro::Decimal;
    /// use std::convert::TryFrom;
    ///
    /// let decimal = Decimal::from(vec![1, 24]);
    /// let maybe_bytes = <Vec<u8>>::try_from(decimal);
    /// ```
    impl std::convert::TryFrom<Decimal> for Vec<u8> {
        type Error = Error;

        fn try_from(decimal: Decimal) -> Result<Self, Self::Error> {
            decimal.to_vec()
        }
    }

    impl<T: AsRef<[u8]>> From<T> for Decimal {
        fn from(bytes: T) -> Self {
            let bytes_ref = bytes.as_ref();
            Self {
                value: BigInt::from_signed_bytes_be(bytes_ref),
                len: bytes_ref.len(),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use apache_avro_test_helper::TestResult;
        use pretty_assertions::assert_eq;

        #[test]
        fn test_decimal_from_bytes_from_ref_decimal() -> TestResult {
            let input = vec![1, 24];
            let d = Decimal::from(&input);

            let output = <Vec<u8>>::try_from(&d)?;
            assert_eq!(output, input);

            Ok(())
        }

        #[test]
        fn test_decimal_from_bytes_from_owned_decimal() -> TestResult {
            let input = vec![1, 24];
            let d = Decimal::from(&input);

            let output = <Vec<u8>>::try_from(d)?;
            assert_eq!(output, input);

            Ok(())
        }

        #[test]
        fn avro_3949_decimal_serde() -> TestResult {
            let decimal = Decimal::from(&[1, 2, 3]);

            let ser = serde_json::to_string(&decimal)?;
            let de = serde_json::from_str(&ser)?;
            std::assert_eq!(decimal, de);

            Ok(())
        }
    }
}
