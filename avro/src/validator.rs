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

//! # Custom name validation
//!
//! By default, the library follows the rules specified in the [Avro specification](https://avro.apache.org/docs/++version++/specification/#names).
//!
//! Some of the other Apache Avro language SDKs are more flexible in their name validation. For
//! interoperability with those SDKs, the library provides a way to customize the name validation.
//!
//! ```
//! # use apache_avro::{AvroResult, validator::{SchemaNameValidator, set_schema_name_validator}};
//! # use regex_lite::Regex;
//! # use std::sync::OnceLock;
//! struct DontAllowNamespaces;
//!
//! impl SchemaNameValidator for DontAllowNamespaces {
//!     fn regex(&self) -> &'static Regex {
//!         static SCHEMA_NAME_ONCE: OnceLock<Regex> = OnceLock::new();
//!         SCHEMA_NAME_ONCE.get_or_init(|| {
//!             Regex::new(
//!                 // Disallows any namespace. By naming the group `name`, the default
//!                 // implementation of `SchemaNameValidator::validate` can be reused.
//!                 r"^(?P<name>[A-Za-z_][A-Za-z0-9_]*)$",
//!             ).expect("Regex is valid")
//!         })
//!     }
//! }
//!
//! // don't parse any schema before registering the custom validator(s)!
//!
//! if set_schema_name_validator(Box::new(DontAllowNamespaces)).is_err() {
//!     // `.unwrap()` doesn't work as the return type does not implement `Debug`
//!     panic!("There was already a schema validator configured")
//! }
//!
//! // ... use the library
//! ```
//!
//! Similar logic could be applied to the schema namespace, enum symbols and field names validation.
//!
//! **Note**: the library allows to set a validator only once per the application lifetime!
//! If the application parses schemas before setting a validator, the default validator will be
//! registered and used!

use crate::{AvroResult, error::Details};
use log::debug;
use regex_lite::Regex;
use std::sync::OnceLock;

/// A validator that validates names and namespaces according to the Avro specification.
struct SpecificationValidator;

/// A trait that validates schema names.
///
/// To register a custom one use [`set_schema_name_validator`].
pub trait SchemaNameValidator: Send + Sync {
    /// The regex used to validate the schema name.
    ///
    /// When the name part of the full name is provided as a capture group named `name`, the
    /// default implementation of [`Self::validate`] can be used.
    ///
    /// The default implementation uses the Avro specified regex.
    fn regex(&self) -> &'static Regex {
        static SCHEMA_NAME_ONCE: OnceLock<Regex> = OnceLock::new();
        SCHEMA_NAME_ONCE.get_or_init(|| {
            Regex::new(
                // An optional namespace (with optional dots) followed by a name without any dots in it.
                r"^((?P<namespace>([A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*)?)\.)?(?P<name>[A-Za-z_][A-Za-z0-9_]*)$",
            )
                .unwrap()
        })
    }

    /// Validates the schema name and returns the start byte of the name.
    ///
    /// Requires that the implementation of [`Self::regex`] provides a capture group named `name`
    /// that captures the name part of the full name.
    ///
    /// Should return [`Details::InvalidSchemaName`] if it is invalid.
    fn validate(&self, schema_name: &str) -> AvroResult<usize> {
        let regex = SchemaNameValidator::regex(self);
        let caps = regex
            .captures(schema_name)
            .ok_or_else(|| Details::InvalidSchemaName(schema_name.to_string(), regex.as_str()))?;
        Ok(caps
            .name("name")
            .expect("Regex has no group named `name`")
            .start())
    }
}

impl SchemaNameValidator for SpecificationValidator {}

static NAME_VALIDATOR_ONCE: OnceLock<Box<dyn SchemaNameValidator + Send + Sync>> = OnceLock::new();

/// Sets a custom schema name validator.
///
/// Returns `Err(validator)` if a validator is already configured.
///
/// **Note**: This function must be called before parsing any schema because this will
/// register the default validator and the registration is one time only!
pub fn set_schema_name_validator(
    validator: Box<dyn SchemaNameValidator + Send + Sync>,
) -> Result<(), Box<dyn SchemaNameValidator + Send + Sync>> {
    debug!("Setting a custom schema name validator.");
    NAME_VALIDATOR_ONCE.set(validator)
}

pub(crate) fn validate_schema_name(schema_name: &str) -> AvroResult<usize> {
    NAME_VALIDATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default name validator.");
            Box::new(SpecificationValidator)
        })
        .validate(schema_name)
}

/// A trait that validates schema namespaces.
///
/// To register a custom one use [`set_schema_namespace_validator`].
pub trait SchemaNamespaceValidator: Send + Sync {
    /// The regex used to validate the schema namespace.
    ///
    /// The default implementation uses the Avro specified regex.
    fn regex(&self) -> &'static Regex {
        static NAMESPACE_ONCE: OnceLock<Regex> = OnceLock::new();
        NAMESPACE_ONCE.get_or_init(|| {
            Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*)?$").unwrap()
        })
    }

    /// Validates a schema namespace.
    ///
    /// Should return [`Details::InvalidNamespace`] if it is invalid.
    fn validate(&self, namespace: &str) -> AvroResult<()> {
        let regex = SchemaNamespaceValidator::regex(self);
        if !regex.is_match(namespace) {
            Err(Details::InvalidNamespace(namespace.to_string(), regex.as_str()).into())
        } else {
            Ok(())
        }
    }
}

impl SchemaNamespaceValidator for SpecificationValidator {}

static NAMESPACE_VALIDATOR_ONCE: OnceLock<Box<dyn SchemaNamespaceValidator + Send + Sync>> =
    OnceLock::new();

/// Sets a custom schema namespace validator.
///
/// Returns `Err(validator)` if a validator is already configured.
///
/// **Note**: This function must be called before parsing any schema because this will
/// register the default validator and the registration is one time only!
pub fn set_schema_namespace_validator(
    validator: Box<dyn SchemaNamespaceValidator + Send + Sync>,
) -> Result<(), Box<dyn SchemaNamespaceValidator + Send + Sync>> {
    NAMESPACE_VALIDATOR_ONCE.set(validator)
}

pub(crate) fn validate_namespace(ns: &str) -> AvroResult<()> {
    NAMESPACE_VALIDATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default namespace validator.");
            Box::new(SpecificationValidator)
        })
        .validate(ns)
}

/// A trait that validates enum symbol names.
///
/// To register a custom one use [`set_enum_symbol_name_validator`].
pub trait EnumSymbolNameValidator: Send + Sync {
    /// The regex used to validate the symbols of enums.
    ///
    /// The default implementation uses the Avro specified regex.
    fn regex(&self) -> &'static Regex {
        static ENUM_SYMBOL_NAME_ONCE: OnceLock<Regex> = OnceLock::new();
        ENUM_SYMBOL_NAME_ONCE.get_or_init(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap())
    }

    /// Validate the symbol of an enum.
    ///
    /// Should return [`Details::EnumSymbolName`] if it is invalid.
    fn validate(&self, symbol: &str) -> AvroResult<()> {
        let regex = EnumSymbolNameValidator::regex(self);
        if !regex.is_match(symbol) {
            return Err(Details::EnumSymbolName(symbol.to_string()).into());
        }

        Ok(())
    }
}

impl EnumSymbolNameValidator for SpecificationValidator {}

static ENUM_SYMBOL_NAME_VALIDATOR_ONCE: OnceLock<Box<dyn EnumSymbolNameValidator + Send + Sync>> =
    OnceLock::new();

/// Sets a custom enum symbol name validator.
///
/// Returns `Err(validator)` if a validator is already configured.
///
/// **Note**: This function must be called before parsing any schema because this will
/// register the default validator and the registration is one time only!
pub fn set_enum_symbol_name_validator(
    validator: Box<dyn EnumSymbolNameValidator + Send + Sync>,
) -> Result<(), Box<dyn EnumSymbolNameValidator + Send + Sync>> {
    ENUM_SYMBOL_NAME_VALIDATOR_ONCE.set(validator)
}

pub(crate) fn validate_enum_symbol_name(symbol: &str) -> AvroResult<()> {
    ENUM_SYMBOL_NAME_VALIDATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default enum symbol name validator.");
            Box::new(SpecificationValidator)
        })
        .validate(symbol)
}

/// A trait that validates record field names.
///
/// To register a custom one use [`set_record_field_name_validator`].
pub trait RecordFieldNameValidator: Send + Sync {
    /// The regex used to validate the record field names.
    ///
    /// The default implementation uses the Avro specified regex.
    fn regex(&self) -> &'static Regex {
        static FIELD_NAME_ONCE: OnceLock<Regex> = OnceLock::new();
        FIELD_NAME_ONCE.get_or_init(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap())
    }

    /// Validate the name of a record field.
    ///
    /// Should return [`Details::FieldName`] if it is invalid.
    fn validate(&self, field_name: &str) -> AvroResult<()> {
        let regex = RecordFieldNameValidator::regex(self);
        if !regex.is_match(field_name) {
            return Err(Details::FieldName(field_name.to_string()).into());
        }

        Ok(())
    }
}

impl RecordFieldNameValidator for SpecificationValidator {}

static RECORD_FIELD_NAME_VALIDATOR_ONCE: OnceLock<Box<dyn RecordFieldNameValidator + Send + Sync>> =
    OnceLock::new();

/// Sets a custom record field name validator.
///
/// Returns `Err(validator)` if a validator is already configured.
///
/// **Note**: This function must be called before parsing any schema because this will
/// register the default validator and the registration is one time only!
pub fn set_record_field_name_validator(
    validator: Box<dyn RecordFieldNameValidator + Send + Sync>,
) -> Result<(), Box<dyn RecordFieldNameValidator + Send + Sync>> {
    RECORD_FIELD_NAME_VALIDATOR_ONCE.set(validator)
}

pub(crate) fn validate_record_field_name(field_name: &str) -> AvroResult<()> {
    RECORD_FIELD_NAME_VALIDATOR_ONCE
        .get_or_init(|| {
            debug!("Going to use the default record field name validator.");
            Box::new(SpecificationValidator)
        })
        .validate(field_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Name;
    use apache_avro_test_helper::TestResult;

    #[test]
    fn avro_3900_default_name_validator_with_valid_ns() -> TestResult {
        validate_schema_name("example")?;
        Ok(())
    }

    #[test]
    fn avro_3900_default_name_validator_with_invalid_ns() -> TestResult {
        assert!(validate_schema_name("com-example").is_err());
        Ok(())
    }

    #[test]
    fn test_avro_3897_disallow_invalid_namespaces_in_fully_qualified_name() -> TestResult {
        let full_name = "ns.0.record1";
        let name = Name::new(full_name);
        assert!(name.is_err());
        let validator = SpecificationValidator;
        let expected = Details::InvalidSchemaName(
            full_name.to_string(),
            SchemaNameValidator::regex(&validator).as_str(),
        )
        .to_string();
        let err = name.map_err(|e| e.to_string()).err().unwrap();
        pretty_assertions::assert_eq!(expected, err);

        let full_name = "ns..record1";
        let name = Name::new(full_name);
        assert!(name.is_err());
        let expected = Details::InvalidSchemaName(
            full_name.to_string(),
            SchemaNameValidator::regex(&validator).as_str(),
        )
        .to_string();
        let err = name.map_err(|e| e.to_string()).err().unwrap();
        pretty_assertions::assert_eq!(expected, err);
        Ok(())
    }

    #[test]
    fn avro_3900_default_namespace_validator_with_valid_ns() -> TestResult {
        validate_namespace("com.example")?;
        Ok(())
    }

    #[test]
    fn avro_3900_default_namespace_validator_with_invalid_ns() -> TestResult {
        assert!(validate_namespace("com-example").is_err());
        Ok(())
    }

    #[test]
    fn avro_3900_default_enum_symbol_validator_with_valid_symbol_name() -> TestResult {
        validate_enum_symbol_name("spades")?;
        Ok(())
    }

    #[test]
    fn avro_3900_default_enum_symbol_validator_with_invalid_symbol_name() -> TestResult {
        assert!(validate_enum_symbol_name("com-example").is_err());
        Ok(())
    }

    #[test]
    fn avro_3900_default_record_field_validator_with_valid_name() -> TestResult {
        validate_record_field_name("test")?;
        Ok(())
    }

    #[test]
    fn avro_3900_default_record_field_validator_with_invalid_name() -> TestResult {
        assert!(validate_record_field_name("com-example").is_err());
        Ok(())
    }
}
