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
use std::collections::HashMap;
use std::fmt;
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
/// [Avro specification](https://avro.apache.org/docs/current/specification/#names)
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Name {
    pub name: String,
    pub namespace: Namespace,
}

/// Represents the aliases for Named Schema
pub type Aliases = Option<Vec<Alias>>;
/// Represents Schema lookup within a schema env
pub type Names = HashMap<Name, Schema>;
/// Represents Schema lookup within a schema
pub type NamesRef<'a> = HashMap<Name, &'a Schema>;
/// Represents the namespace for Named Schema
pub type Namespace = Option<String>;

impl Name {
    /// Create a new `Name`.
    /// Parses the optional `namespace` from the `name` string.
    /// `aliases` will not be defined.
    pub fn new(name: &str) -> AvroResult<Self> {
        let (name, namespace) = Name::get_name_and_namespace(name)?;
        Ok(Self {
            name,
            namespace: namespace.filter(|ns| !ns.is_empty()),
        })
    }

    fn get_name_and_namespace(name: &str) -> AvroResult<(String, Namespace)> {
        validate_schema_name(name)
    }

    /// Parse a `serde_json::Value` into a `Name`.
    pub(crate) fn parse(
        complex: &Map<String, Value>,
        enclosing_namespace: &Namespace,
    ) -> AvroResult<Self> {
        let (name, namespace_from_name) = complex
            .name()
            .map(|name| Name::get_name_and_namespace(name.as_str()).unwrap())
            .ok_or(Details::GetNameField)?;
        // FIXME Reading name from the type is wrong ! The name there is just a metadata (AVRO-3430)
        let type_name = match complex.get("type") {
            Some(Value::Object(complex_type)) => complex_type.name().or(None),
            _ => None,
        };

        let namespace = namespace_from_name
            .or_else(|| {
                complex
                    .string("namespace")
                    .or_else(|| enclosing_namespace.clone())
            })
            .filter(|ns| !ns.is_empty());

        if let Some(ref ns) = namespace {
            validate_namespace(ns)?;
        }

        Ok(Self {
            name: type_name.unwrap_or(name),
            namespace,
        })
    }

    /// Return the `fullname` of this `Name`
    ///
    /// More information about fullnames can be found in the
    /// [Avro specification](https://avro.apache.org/docs/current/specification/#names)
    pub fn fullname(&self, default_namespace: Namespace) -> String {
        if self.name.contains('.') {
            self.name.clone()
        } else {
            let namespace = self.namespace.clone().or(default_namespace);

            match namespace {
                Some(ref namespace) if !namespace.is_empty() => {
                    format!("{}.{}", namespace, self.name)
                }
                _ => self.name.clone(),
            }
        }
    }

    /// Return the fully qualified name needed for indexing or searching for the schema within a schema/schema env context. Puts the enclosing namespace into the name's namespace for clarity in schema/schema env parsing
    /// ```ignore
    /// use apache_avro::schema::Name;
    ///
    /// assert_eq!(
    /// Name::new("some_name")?.fully_qualified_name(&Some("some_namespace".into())),
    /// Name::new("some_namespace.some_name")?
    /// );
    /// assert_eq!(
    /// Name::new("some_namespace.some_name")?.fully_qualified_name(&Some("other_namespace".into())),
    /// Name::new("some_namespace.some_name")?
    /// );
    /// ```
    pub fn fully_qualified_name(&self, enclosing_namespace: &Namespace) -> Name {
        Name {
            name: self.name.clone(),
            namespace: self
                .namespace
                .clone()
                .or_else(|| enclosing_namespace.clone().filter(|ns| !ns.is_empty())),
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

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.fullname(None)[..])
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
                Name::parse(&json, &None).map_err(Error::custom)
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

    pub fn name(&self) -> String {
        self.0.name.clone()
    }

    pub fn namespace(&self) -> Namespace {
        self.0.namespace.clone()
    }

    pub fn fullname(&self, default_namespace: Namespace) -> String {
        self.0.fullname(default_namespace)
    }

    pub fn fully_qualified_name(&self, default_namespace: &Namespace) -> Name {
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
        assert_eq!(name.name, "name");
        assert_eq!(name.namespace, None);

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
