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

use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

use crate::{
    AvroResult, Error, Schema,
    error::Details,
    util::MapHelper,
    validator::{validate_namespace, validate_schema_name},
};

/// Represents names for `record`, `enum` and `fixed` Avro schemas.
///
/// Each of these `Schema`s have a `fullname` composed of two parts:
///   * a name
///   * a namespace
///
/// `aliases` can also be defined, to facilitate schema evolution.
///
/// More information about schema names can be found in the
/// [Avro specification](https://avro.apache.org/docs/++version++/specification/#names)
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Name {
    /// The full name
    namespace_and_name: String,
    /// Start byte of the name part
    ///
    /// If this is zero, then there is no namespace.
    index_of_name: usize,
}

/// Represents the aliases for Named Schema
pub type Aliases = Option<Vec<Alias>>;
/// Represents Schema lookup within a schema env
pub type Names = HashMap<Name, Schema>;
/// Represents Schema lookup within a schema
pub type NamesRef<'a> = HashMap<Name, &'a Schema>;
/// Represents the namespace for Named Schema
pub type Namespace = Option<String>;
/// Represents the namespace for Named Schema
pub type NamespaceRef<'a> = Option<&'a str>;

impl Name {
    /// Create a new `Name`.
    /// Parses the optional `namespace` from the `name` string.
    /// `aliases` will not be defined.
    pub fn new(name: impl Into<String> + AsRef<str>) -> AvroResult<Self> {
        Self::new_with_enclosing_namespace(name, None)
    }

    /// Create a new `Name` using the namespace from `enclosing_namespace` if absent.
    pub fn new_with_enclosing_namespace(
        name: impl Into<String> + AsRef<str>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Self> {
        // Having both `Into<String>` and `AsRef<str>` allows optimal use in both of these cases:
        // - `name` is a `String`. We can reuse the allocation when `enclosing_namespace` is `None`
        //   or `name` already has a namespace.
        // - `name` is a `str`. With only `Into<String` we need an extra allocation in the case `name`
        //   doesn't have namespace and `enclosing_namespace` is `Some`. Having `AsRef<str>` allows
        //   skipping that allocation.
        let name_ref = name.as_ref();
        let index_of_name = validate_schema_name(name_ref)?;

        if index_of_name == 0
            && let Some(namespace) = enclosing_namespace
            && !namespace.is_empty()
        {
            validate_namespace(namespace)?;
            Ok(Self {
                namespace_and_name: format!("{namespace}.{name_ref}"),
                index_of_name: namespace.len() + 1,
            })
        } else if index_of_name == 1 {
            // Name has a leading dot
            Ok(Self {
                namespace_and_name: name.as_ref()[1..].into(),
                index_of_name: 0,
            })
        } else {
            Ok(Self {
                namespace_and_name: name.into(),
                index_of_name,
            })
        }
    }

    /// Parse a `serde_json::Value` into a `Name`.
    pub(crate) fn parse(
        complex: &Map<String, Value>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Self> {
        let name_field = complex.name().ok_or(Details::GetNameField)?;
        Self::new_with_enclosing_namespace(
            name_field,
            complex.string("namespace").or(enclosing_namespace),
        )
    }

    pub fn name(&self) -> &str {
        &self.namespace_and_name[self.index_of_name..]
    }

    pub fn namespace(&self) -> NamespaceRef<'_> {
        if self.index_of_name == 0 {
            None
        } else {
            Some(&self.namespace_and_name[..(self.index_of_name - 1)])
        }
    }

    /// Return the `fullname` of this `Name`
    ///
    /// More information about fullnames can be found in the
    /// [Avro specification](https://avro.apache.org/docs/++version++/specification/#names)
    pub fn fullname(&self, enclosing_namespace: NamespaceRef) -> String {
        if self.index_of_name == 0
            && let Some(namespace) = enclosing_namespace
            && !namespace.is_empty()
        {
            format!("{namespace}.{}", self.namespace_and_name)
        } else {
            self.namespace_and_name.clone()
        }
    }

    /// Construct the fully qualified name
    ///
    /// ```
    /// # use apache_avro::{Error, schema::Name};
    /// assert_eq!(
    ///     Name::new("some_name")?.fully_qualified_name(Some("some_namespace")).into_owned(),
    ///     Name::new("some_namespace.some_name")?
    /// );
    /// assert_eq!(
    ///     Name::new("some_namespace.some_name")?.fully_qualified_name(Some("other_namespace")).into_owned(),
    ///     Name::new("some_namespace.some_name")?
    /// );
    /// # Ok::<(), Error>(())
    /// ```
    pub fn fully_qualified_name(&self, enclosing_namespace: NamespaceRef) -> Cow<'_, Name> {
        if self.index_of_name == 0
            && let Some(namespace) = enclosing_namespace
            && !namespace.is_empty()
        {
            Cow::Owned(Self {
                namespace_and_name: format!("{namespace}.{}", self.namespace_and_name),
                index_of_name: namespace.len() + 1,
            })
        } else {
            Cow::Borrowed(self)
        }
    }

    /// Create an empty name.
    ///
    /// This name is invalid and should never be used anywhere! The only valid use is filling
    /// a `Name` field that will not be used.
    ///
    /// Using this name will cause a panic.
    pub(crate) fn invalid_empty_name() -> Self {
        Self {
            namespace_and_name: String::new(),
            index_of_name: usize::MAX,
        }
    }
}

impl TryFrom<&str> for Name {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<String> for Name {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(&value)
    }
}

impl FromStr for Name {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl Debug for Name {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("Name");
        debug.field("name", &self.name());
        if self.index_of_name != 0 {
            debug.field("namespace", &self.namespace());
            debug.finish()
        } else {
            debug.finish_non_exhaustive()
        }
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.namespace_and_name)
    }
}

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        Value::deserialize(deserializer).and_then(|value| {
            use serde::de::Error;
            if let Value::Object(json) = value {
                Name::parse(&json, None).map_err(Error::custom)
            } else {
                Err(Error::custom(format!("Expected a JSON object: {value:?}")))
            }
        })
    }
}

/// Newtype pattern for `Name` to better control the `serde_json::Value` representation.
/// Aliases are serialized as an array of plain strings in the JSON representation.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Alias(Name);

impl Alias {
    pub fn new(name: &str) -> AvroResult<Self> {
        Name::new(name).map(Self)
    }

    pub fn name(&self) -> &str {
        self.0.name()
    }

    pub fn namespace(&self) -> NamespaceRef<'_> {
        self.0.namespace()
    }

    pub fn fullname(&self, enclosing_namespace: NamespaceRef) -> String {
        self.0.fullname(enclosing_namespace)
    }

    pub fn fully_qualified_name(&self, default_namespace: NamespaceRef) -> Cow<'_, Name> {
        self.0.fully_qualified_name(default_namespace)
    }
}

impl TryFrom<&str> for Alias {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<String> for Alias {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(&value)
    }
}

impl FromStr for Alias {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl Serialize for Alias {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.fullname(None))
    }
}

#[cfg(test)]
mod tests {
    use crate::Error;

    use super::*;
    use apache_avro_test_helper::TestResult;

    #[test]
    /// Zero-length namespace is considered as no-namespace.
    fn test_namespace_from_name_with_empty_value() -> TestResult {
        let name = Name::new(".name")?;
        assert_eq!(name.namespace_and_name, "name");
        assert_eq!(name.index_of_name, 0);

        Ok(())
    }

    #[test]
    /// Whitespace is not allowed in the name.
    fn test_name_with_whitespace_value() {
        match Name::new(" ").map_err(Error::into_details) {
            Err(Details::InvalidSchemaName(_, _)) => {}
            _ => panic!("Expected an Details::InvalidSchemaName!"),
        }
    }

    #[test]
    /// The name must be non-empty.
    fn test_name_with_no_name_part() {
        match Name::new("space.").map_err(Error::into_details) {
            Err(Details::InvalidSchemaName(_, _)) => {}
            _ => panic!("Expected an Details::InvalidSchemaName!"),
        }
    }

    /// A test cases showing that names and namespaces can be constructed
    /// entirely by underscores.
    #[test]
    fn test_avro_3897_funny_valid_names_and_namespaces() -> TestResult {
        for funny_name in ["_", "_._", "__._", "_.__", "_._._"] {
            let name = Name::new(funny_name);
            assert!(name.is_ok());
        }
        Ok(())
    }
}
