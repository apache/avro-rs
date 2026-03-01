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

//! Deprecated. Use [`apache_avro::serde::*`] instead.
//!
//! [`apache_avro::serde::*`](crate::serde)

#[cfg(doc)]
use crate as apache_avro;

/// Deprecated. Use [`apache_avro::serde::bytes`] instead.
#[deprecated(since = "0.22.0", note = "Use `apache_avro::serde::bytes` instead")]
pub mod serde_avro_bytes {
    #[cfg(doc)]
    use crate as apache_avro;
    use serde::{Deserializer, Serializer};

    /// Deprecated. Use [`apache_avro::serde::bytes::serialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::bytes::serialize` instead"
    )]
    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        crate::serde::bytes::serialize(bytes, serializer)
    }

    /// Deprecated. Use [`apache_avro::serde::bytes::deserialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::bytes::deserialize` instead"
    )]
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::serde::bytes::deserialize(deserializer)
    }
}

/// Deprecated. Use [`apache_avro::serde::bytes_opt`] instead.
///
/// [`apache_avro::serde::bytes_opt`](crate::serde::bytes_opt)
#[deprecated(since = "0.22.0", note = "Use `apache_avro::serde::bytes_opt` instead")]
pub mod serde_avro_bytes_opt {
    #[cfg(doc)]
    use crate as apache_avro;
    use serde::{Deserializer, Serializer};
    use std::borrow::Borrow;

    /// Deprecated. Use [`apache_avro::serde::bytes_opt::serialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::bytes_opt::serialize` instead"
    )]
    #[expect(clippy::ref_option, reason = "Required by the Serde API")]
    pub fn serialize<S, B>(bytes: &Option<B>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        B: Borrow<[u8]> + serde_bytes::Serialize,
    {
        crate::serde::bytes_opt::serialize(bytes, serializer)
    }

    /// Deprecated. Use [`apache_avro::serde::bytes_opt::deserialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::bytes_opt::deserialize` instead"
    )]
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::serde::bytes_opt::deserialize(deserializer)
    }
}

/// Deprecated. Use [`apache_avro::serde::fixed`] instead.
#[deprecated(since = "0.22.0", note = "Use `apache_avro::serde::fixed` instead")]
pub mod serde_avro_fixed {
    #[cfg(doc)]
    use crate as apache_avro;
    use serde::{Deserializer, Serializer};

    /// Deprecated. Use [`apache_avro::serde::fixed::serialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::fixed::serialize` instead"
    )]
    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        crate::serde::fixed::serialize(bytes, serializer)
    }

    /// Deprecated. Use [`apache_avro::serde::fixed::deserialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::fixed::deserialize` instead"
    )]
    pub fn deserialize<'de, D, const N: usize>(deserializer: D) -> Result<[u8; N], D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::serde::fixed::deserialize(deserializer)
    }
}

/// Deprecated. Use [`apache_avro::serde::fixed_opt`] instead.
#[deprecated(since = "0.22.0", note = "Use `apache_avro::serde::fixed_opt` instead")]
pub mod serde_avro_fixed_opt {
    #[cfg(doc)]
    use crate as apache_avro;
    use serde::{Deserializer, Serializer};
    use std::borrow::Borrow;

    /// Deprecated. Use [`apache_avro::serde::fixed_opt::serialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::fixed_opt::serialize` instead"
    )]
    #[expect(clippy::ref_option, reason = "Required by the Serde API")]
    pub fn serialize<S, B>(bytes: &Option<B>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        B: Borrow<[u8]> + serde_bytes::Serialize,
    {
        crate::serde::fixed_opt::serialize(bytes, serializer)
    }

    /// Deprecated. Use [`apache_avro::serde::fixed_opt::deserialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::fixed_opt::deserialize` instead"
    )]
    pub fn deserialize<'de, D, const N: usize>(deserializer: D) -> Result<Option<[u8; N]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::serde::fixed_opt::deserialize(deserializer)
    }
}

/// Deprecated. Use [`apache_avro::serde::slice`] instead.
#[deprecated(since = "0.22.0", note = "Use `apache_avro::serde::slice` instead")]
pub mod serde_avro_slice {
    #[cfg(doc)]
    use crate as apache_avro;
    use serde::{Deserializer, Serializer};

    /// Deprecated. Use [`apache_avro::serde::slice::serialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::slice::serialize` instead"
    )]
    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        crate::serde::slice::serialize(bytes, serializer)
    }

    /// Deprecated. Use [`apache_avro::serde::slice::deserialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::slice::deserialize` instead"
    )]
    pub fn deserialize<'de, D>(deserializer: D) -> Result<&'de [u8], D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::serde::slice::deserialize(deserializer)
    }
}

/// Deprecated. Use [`apache_avro::serde::slice_opt`] instead.
#[deprecated(since = "0.22.0", note = "Use `apache_avro::serde::slice_opt` instead")]
pub mod serde_avro_slice_opt {
    #[cfg(doc)]
    use crate as apache_avro;
    use serde::{Deserializer, Serializer};
    use std::borrow::Borrow;

    /// Deprecated. Use [`apache_avro::serde::slice_opt::serialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::slice_opt::serialize` instead"
    )]
    #[expect(clippy::ref_option, reason = "Required by the Serde API")]
    pub fn serialize<S, B>(bytes: &Option<B>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        B: Borrow<[u8]> + serde_bytes::Serialize,
    {
        crate::serde::slice_opt::serialize(bytes, serializer)
    }

    /// Deprecated. Use [`apache_avro::serde::slice_opt::deserialize`] instead.
    #[deprecated(
        since = "0.22.0",
        note = "Use `apache_avro::serde::slice_opt::deserialize` instead"
    )]
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<&'de [u8]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::serde::slice_opt::deserialize(deserializer)
    }
}
