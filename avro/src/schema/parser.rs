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

use crate::error::Details;
use crate::schema::{
    Alias, Aliases, ArraySchema, DecimalMetadata, DecimalSchema, EnumSchema, FixedSchema,
    MapSchema, Name, Names, NamespaceRef, Precision, RecordField, RecordSchema, Scale, Schema,
    SchemaKind, UnionSchema, UuidSchema,
};
use crate::types;
use crate::util::MapHelper;
use crate::validator::validate_enum_symbol_name;
use crate::{AvroResult, Error};
use log::{debug, error, warn};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Default)]
pub(crate) struct Parser {
    input_schemas: HashMap<Name, Value>,
    /// Used to resolve cyclic references, i.e. when a
    /// field's type is a reference to its record's type
    resolving_schemas: Names,
    input_order: Vec<Name>,
    /// Used to avoid parsing the same schema twice
    parsed_schemas: Names,
}

impl Parser {
    pub(crate) fn new(
        input_schemas: HashMap<Name, Value>,
        input_order: Vec<Name>,
        parsed_schemas: Names,
    ) -> Self {
        Self {
            input_schemas,
            resolving_schemas: HashMap::default(),
            input_order,
            parsed_schemas,
        }
    }

    pub(crate) fn get_parsed_schemas(&mut self) -> &Names {
        &self.parsed_schemas
    }

    /// Create a `Schema` from a string representing a JSON Avro schema.
    pub(super) fn parse_str(&mut self, input: &str) -> AvroResult<Schema> {
        let value = serde_json::from_str(input).map_err(Details::ParseSchemaJson)?;
        self.parse(&value, None)
    }

    /// Create an array of `Schema`s from an iterator of JSON Avro schemas.
    ///
    /// It is allowed that the schemas have cross-dependencies; these will be resolved during parsing.
    pub(super) fn parse_list(&mut self) -> AvroResult<Vec<Schema>> {
        self.parse_input_schemas()?;

        let mut parsed_schemas = Vec::with_capacity(self.parsed_schemas.len());
        for name in self.input_order.drain(0..) {
            let parsed = self
                .parsed_schemas
                .remove(&name)
                .expect("One of the input schemas was unexpectedly not parsed");
            parsed_schemas.push(parsed);
        }
        Ok(parsed_schemas)
    }

    /// Convert the input schemas to `parsed_schemas`.
    pub(super) fn parse_input_schemas(&mut self) -> Result<(), Error> {
        while !self.input_schemas.is_empty() {
            let next_name = self
                .input_schemas
                .keys()
                .next()
                .expect("Input schemas unexpectedly empty")
                .to_owned();
            let (name, value) = self
                .input_schemas
                .remove_entry(&next_name)
                .expect("Key unexpectedly missing");
            let parsed = self.parse(&value, None)?;
            self.parsed_schemas
                .insert(self.get_schema_type_name(name, value), parsed);
        }
        Ok(())
    }

    /// Create a `Schema` from a `serde_json::Value` representing a JSON Avro schema.
    pub(super) fn parse(
        &mut self,
        value: &Value,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        match *value {
            Value::String(ref t) => self.parse_known_schema(t.as_str(), enclosing_namespace),
            Value::Object(ref data) => self.parse_complex(data, enclosing_namespace),
            Value::Array(ref data) => self.parse_union(data, enclosing_namespace),
            _ => Err(Details::ParseSchemaFromValidJson.into()),
        }
    }

    /// Parse a string as a primitive type or reference to `parsed_schemas`.
    fn parse_known_schema(
        &mut self,
        name: &str,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        match name {
            "null" => Ok(Schema::Null),
            "boolean" => Ok(Schema::Boolean),
            "int" => Ok(Schema::Int),
            "long" => Ok(Schema::Long),
            "double" => Ok(Schema::Double),
            "float" => Ok(Schema::Float),
            "bytes" => Ok(Schema::Bytes),
            "string" => Ok(Schema::String),
            _ => self.fetch_schema_ref(name, enclosing_namespace),
        }
    }

    /// Given a name, tries to retrieve the parsed schema from `parsed_schemas`.
    ///
    /// If a parsed schema is not found, it checks if a currently resolving
    /// schema with that name exists.
    /// If a resolving schema is not found, it checks if a JSON with that name exists
    /// in `input_schemas` and then parses it (removing it from `input_schemas`)
    /// and adds the parsed schema to `parsed_schemas`.
    ///
    /// This method allows schemas definitions that depend on other types to
    /// parse their dependencies (or look them up if already parsed).
    pub(super) fn fetch_schema_ref(
        &mut self,
        name: &str,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        fn get_schema_ref(parsed: &Schema) -> Schema {
            match parsed {
                &Schema::Record(RecordSchema { ref name, .. })
                | &Schema::Enum(EnumSchema { ref name, .. })
                | &Schema::Fixed(FixedSchema { ref name, .. }) => {
                    Schema::Ref { name: name.clone() }
                }
                _ => parsed.clone(),
            }
        }

        let fully_qualified_name = Name::new_with_enclosing_namespace(name, enclosing_namespace)?;

        if self.parsed_schemas.contains_key(&fully_qualified_name) {
            return Ok(Schema::Ref {
                name: fully_qualified_name,
            });
        }
        if let Some(resolving_schema) = self.resolving_schemas.get(&fully_qualified_name) {
            return Ok(resolving_schema.clone());
        }

        // For good error reporting we add this check
        match fully_qualified_name.name() {
            "record" | "enum" | "fixed" => {
                return Err(
                    Details::InvalidSchemaRecord(fully_qualified_name.name().to_string()).into(),
                );
            }
            _ => (),
        }

        let value = self
            .input_schemas
            .remove(&fully_qualified_name)
            // TODO make a better descriptive error message here that conveys that a named schema cannot be found
            .ok_or_else(|| {
                let full_name = fully_qualified_name.fullname(None);
                if full_name == "bool" {
                    Details::ParsePrimitiveSimilar(full_name, "boolean")
                } else {
                    Details::ParsePrimitive(full_name)
                }
            })?;

        // parsing a full schema from inside another schema. Other full schema will not inherit namespace
        let parsed = self.parse(&value, None)?;
        self.parsed_schemas.insert(
            self.get_schema_type_name(fully_qualified_name, value),
            parsed.clone(),
        );

        Ok(get_schema_ref(&parsed))
    }

    fn get_decimal_integer(
        &self,
        complex: &Map<String, Value>,
        key: &'static str,
    ) -> AvroResult<DecimalMetadata> {
        match complex.get(key) {
            Some(Value::Number(value)) => self.parse_json_integer_for_decimal(value),
            None => {
                if key == "scale" {
                    Ok(0)
                } else {
                    Err(Details::GetDecimalMetadataFromJson(key).into())
                }
            }
            Some(value) => Err(Details::GetDecimalMetadataValueFromJson {
                key: key.into(),
                value: value.clone(),
            }
            .into()),
        }
    }

    fn parse_precision_and_scale(
        &self,
        complex: &Map<String, Value>,
    ) -> AvroResult<(Precision, Scale)> {
        let precision = self.get_decimal_integer(complex, "precision")?;
        let scale = self.get_decimal_integer(complex, "scale")?;

        if precision < 1 {
            return Err(Details::DecimalPrecisionMuBePositive { precision }.into());
        }

        if precision < scale {
            Err(Details::DecimalPrecisionLessThanScale { precision, scale }.into())
        } else {
            Ok((precision, scale))
        }
    }

    /// Parse a `serde_json::Value` representing a complex Avro type into a `Schema`.
    ///
    /// Avro supports "recursive" definition of types.
    /// e.g: `{"type": {"type": "string"}}`
    pub(super) fn parse_complex(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        // Try to parse this as a native complex type.
        fn parse_as_native_complex(
            complex: &Map<String, Value>,
            parser: &mut Parser,
            enclosing_namespace: NamespaceRef,
        ) -> AvroResult<Schema> {
            match complex.get("type") {
                Some(value) => match value {
                    Value::String(s) if s == "fixed" => {
                        parser.parse_fixed(complex, enclosing_namespace)
                    }
                    _ => parser.parse(value, enclosing_namespace),
                },
                None => Err(Details::GetLogicalTypeField.into()),
            }
        }

        // This crate supports some logical types natively, and this function tries to convert
        // a native complex type with a logical type attribute to these logical types.
        // This function:
        // 1. Checks whether the native complex type is in the supported kinds.
        // 2. If it is, using the convert function to convert the native complex type to
        // a logical type.
        fn try_convert_to_logical_type<F>(
            logical_type: &str,
            schema: Schema,
            supported_schema_kinds: &[SchemaKind],
            convert: F,
        ) -> AvroResult<Schema>
        where
            F: Fn(Schema) -> AvroResult<Schema>,
        {
            let kind = SchemaKind::from(schema.clone());
            if supported_schema_kinds.contains(&kind) {
                convert(schema)
            } else {
                warn!(
                    "Ignoring unknown logical type '{logical_type}' for schema of type: {schema:?}!"
                );
                Ok(schema)
            }
        }

        match complex.get("logicalType") {
            Some(Value::String(t)) => match t.as_str() {
                "decimal" => {
                    return try_convert_to_logical_type(
                        "decimal",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Fixed, SchemaKind::Bytes],
                        |inner| -> AvroResult<Schema> {
                            match self.parse_precision_and_scale(complex) {
                                Ok((precision, scale)) => Ok(Schema::Decimal(DecimalSchema {
                                    precision,
                                    scale,
                                    inner: inner.try_into()?,
                                })),
                                Err(err) => {
                                    warn!("Ignoring invalid decimal logical type: {err}");
                                    Ok(inner)
                                }
                            }
                        },
                    );
                }
                "big-decimal" => {
                    return try_convert_to_logical_type(
                        "big-decimal",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Bytes],
                        |_| -> AvroResult<Schema> { Ok(Schema::BigDecimal) },
                    );
                }
                "uuid" => {
                    return try_convert_to_logical_type(
                        "uuid",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::String, SchemaKind::Fixed, SchemaKind::Bytes],
                        |schema| match schema {
                            Schema::String => Ok(Schema::Uuid(UuidSchema::String)),
                            Schema::Fixed(fixed @ FixedSchema { size: 16, .. }) => {
                                Ok(Schema::Uuid(UuidSchema::Fixed(fixed)))
                            }
                            Schema::Fixed(FixedSchema { size, .. }) => {
                                warn!(
                                    "Ignoring uuid logical type for a Fixed schema because its size ({size:?}) is not 16! Schema: {schema:?}"
                                );
                                Ok(schema)
                            }
                            Schema::Bytes => Ok(Schema::Uuid(UuidSchema::Bytes)),
                            _ => {
                                warn!("Ignoring invalid uuid logical type for schema: {schema:?}");
                                Ok(schema)
                            }
                        },
                    );
                }
                "date" => {
                    return try_convert_to_logical_type(
                        "date",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Int],
                        |_| -> AvroResult<Schema> { Ok(Schema::Date) },
                    );
                }
                "time-millis" => {
                    return try_convert_to_logical_type(
                        "date",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Int],
                        |_| -> AvroResult<Schema> { Ok(Schema::TimeMillis) },
                    );
                }
                "time-micros" => {
                    return try_convert_to_logical_type(
                        "time-micros",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::TimeMicros) },
                    );
                }
                "timestamp-millis" => {
                    return try_convert_to_logical_type(
                        "timestamp-millis",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::TimestampMillis) },
                    );
                }
                "timestamp-micros" => {
                    return try_convert_to_logical_type(
                        "timestamp-micros",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::TimestampMicros) },
                    );
                }
                "timestamp-nanos" => {
                    return try_convert_to_logical_type(
                        "timestamp-nanos",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::TimestampNanos) },
                    );
                }
                "local-timestamp-millis" => {
                    return try_convert_to_logical_type(
                        "local-timestamp-millis",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::LocalTimestampMillis) },
                    );
                }
                "local-timestamp-micros" => {
                    return try_convert_to_logical_type(
                        "local-timestamp-micros",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::LocalTimestampMicros) },
                    );
                }
                "local-timestamp-nanos" => {
                    return try_convert_to_logical_type(
                        "local-timestamp-nanos",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Long],
                        |_| -> AvroResult<Schema> { Ok(Schema::LocalTimestampNanos) },
                    );
                }
                "duration" => {
                    return try_convert_to_logical_type(
                        "duration",
                        parse_as_native_complex(complex, self, enclosing_namespace)?,
                        &[SchemaKind::Fixed],
                        |schema| -> AvroResult<Schema> {
                            match schema {
                                Schema::Fixed(fixed @ FixedSchema { size: 12, .. }) => {
                                    Ok(Schema::Duration(fixed))
                                }
                                Schema::Fixed(FixedSchema { size, .. }) => {
                                    warn!(
                                        "Ignoring duration logical type on fixed type because size ({size}) is not 12! Schema: {schema:?}"
                                    );
                                    Ok(schema)
                                }
                                _ => {
                                    warn!(
                                        "Ignoring invalid duration logical type for schema: {schema:?}"
                                    );
                                    Ok(schema)
                                }
                            }
                        },
                    );
                }
                // In this case, of an unknown logical type, we just pass through the underlying
                // type.
                _ => {}
            },
            // The spec says to ignore invalid logical types and just pass through the
            // underlying type. It is unclear whether that applies to this case or not, where the
            // `logicalType` is not a string.
            Some(value) => return Err(Details::GetLogicalTypeFieldType(value.clone()).into()),
            _ => {}
        }
        match complex.get("type") {
            Some(Value::String(t)) => match t.as_str() {
                "record" => self.parse_record(complex, enclosing_namespace),
                "enum" => self.parse_enum(complex, enclosing_namespace),
                "array" => self.parse_array(complex, enclosing_namespace),
                "map" => self.parse_map(complex, enclosing_namespace),
                "fixed" => self.parse_fixed(complex, enclosing_namespace),
                other => self.parse_known_schema(other, enclosing_namespace),
            },
            Some(Value::Object(data)) => self.parse_complex(data, enclosing_namespace),
            Some(Value::Array(variants)) => self.parse_union(variants, enclosing_namespace),
            Some(unknown) => Err(Details::GetComplexType(unknown.clone()).into()),
            None => Err(Details::GetComplexTypeField.into()),
        }
    }

    fn register_resolving_schema(&mut self, name: &Name, aliases: &Aliases) {
        let resolving_schema = Schema::Ref { name: name.clone() };
        self.resolving_schemas
            .insert(name.clone(), resolving_schema.clone());

        let namespace = name.namespace();

        if let Some(aliases) = aliases {
            aliases.iter().for_each(|alias| {
                let alias_fullname = alias.fully_qualified_name(namespace).into_owned();
                self.resolving_schemas
                    .insert(alias_fullname, resolving_schema.clone());
            });
        }
    }

    fn register_parsed_schema(
        &mut self,
        fully_qualified_name: &Name,
        schema: &Schema,
        aliases: &Aliases,
    ) {
        // FIXME, this should be globally aware, so if there is something overwriting something
        // else then there is an ambiguous schema definition. An appropriate error should be thrown
        self.parsed_schemas
            .insert(fully_qualified_name.clone(), schema.clone());
        self.resolving_schemas.remove(fully_qualified_name);

        let namespace = fully_qualified_name.namespace();

        if let Some(aliases) = aliases {
            aliases.iter().for_each(|alias| {
                let alias_fullname = alias.fully_qualified_name(namespace);
                self.resolving_schemas.remove(&alias_fullname);
                self.parsed_schemas
                    .insert(alias_fullname.into_owned(), schema.clone());
            });
        }
    }

    /// Returns already parsed schema or a schema that is currently being resolved.
    fn get_already_seen_schema(
        &self,
        complex: &Map<String, Value>,
        enclosing_namespace: NamespaceRef,
    ) -> Option<&Schema> {
        match complex.get("type") {
            Some(Value::String(typ)) => {
                let name =
                    Name::new_with_enclosing_namespace(typ.as_str(), enclosing_namespace).unwrap();
                self.resolving_schemas
                    .get(&name)
                    .or_else(|| self.parsed_schemas.get(&name))
            }
            _ => None,
        }
    }

    /// Parse a `serde_json::Value` representing an Avro record type into a `Schema`.
    fn parse_record(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let fields_opt = complex.get("fields");

        if fields_opt.is_none()
            && let Some(seen) = self.get_already_seen_schema(complex, enclosing_namespace)
        {
            return Ok(seen.clone());
        }

        let fully_qualified_name = Name::parse(complex, enclosing_namespace)?;
        let aliases =
            self.fix_aliases_namespace(complex.aliases(), fully_qualified_name.namespace());

        let mut lookup = BTreeMap::new();

        self.register_resolving_schema(&fully_qualified_name, &aliases);

        debug!("Going to parse record schema: {:?}", &fully_qualified_name);

        let fields: Vec<RecordField> = fields_opt
            .and_then(|fields| fields.as_array())
            .ok_or_else(|| Error::new(Details::GetRecordFieldsJson))
            .and_then(|fields| {
                fields
                    .iter()
                    .filter_map(|field| field.as_object())
                    .map(|field| RecordField::parse(field, self, &fully_qualified_name))
                    .collect::<Result<_, _>>()
            })?;

        for (position, field) in fields.iter().enumerate() {
            if let Some(_old) = lookup.insert(field.name.clone(), position) {
                return Err(Details::FieldNameDuplicate(field.name.clone()).into());
            }

            for alias in &field.aliases {
                lookup.insert(alias.clone(), position);
            }
        }

        let schema = Schema::Record(RecordSchema {
            name: fully_qualified_name.clone(),
            aliases: aliases.clone(),
            doc: complex.doc(),
            fields,
            lookup,
            attributes: self.get_custom_attributes(complex, vec!["fields"]),
        });

        self.register_parsed_schema(&fully_qualified_name, &schema, &aliases);
        Ok(schema)
    }

    fn get_custom_attributes(
        &self,
        complex: &Map<String, Value>,
        excluded: Vec<&'static str>,
    ) -> BTreeMap<String, Value> {
        let mut custom_attributes: BTreeMap<String, Value> = BTreeMap::new();
        for (key, value) in complex {
            match key.as_str() {
                "type" | "name" | "namespace" | "doc" | "aliases" | "logicalType" => continue,
                candidate if excluded.contains(&candidate) => continue,
                _ => custom_attributes.insert(key.clone(), value.clone()),
            };
        }
        custom_attributes
    }

    /// Parse a `serde_json::Value` representing a Avro enum type into a `Schema`.
    fn parse_enum(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let symbols_opt = complex.get("symbols");

        if symbols_opt.is_none()
            && let Some(seen) = self.get_already_seen_schema(complex, enclosing_namespace)
        {
            return Ok(seen.clone());
        }

        let fully_qualified_name = Name::parse(complex, enclosing_namespace)?;
        let aliases =
            self.fix_aliases_namespace(complex.aliases(), fully_qualified_name.namespace());

        let symbols: Vec<String> = symbols_opt
            .and_then(|v| v.as_array())
            .ok_or_else(|| Error::from(Details::GetEnumSymbolsField))
            .and_then(|symbols| {
                symbols
                    .iter()
                    .map(|symbol| symbol.as_str().map(|s| s.to_string()))
                    .collect::<Option<_>>()
                    .ok_or_else(|| Error::from(Details::GetEnumSymbols))
            })?;

        let mut existing_symbols: HashSet<&String> = HashSet::with_capacity(symbols.len());
        for symbol in symbols.iter() {
            validate_enum_symbol_name(symbol)?;

            // Ensure there are no duplicate symbols
            if existing_symbols.contains(&symbol) {
                return Err(Details::EnumSymbolDuplicate(symbol.to_string()).into());
            }

            existing_symbols.insert(symbol);
        }

        let mut default: Option<String> = None;
        if let Some(value) = complex.get("default") {
            if let Value::String(ref s) = *value {
                default = Some(s.clone());
            } else {
                return Err(Details::EnumDefaultWrongType(value.clone()).into());
            }
        }

        if let Some(ref value) = default {
            let resolved = types::Value::from(value.clone())
                .resolve_enum(&symbols, &Some(value.to_string()), &None)
                .is_ok();
            if !resolved {
                return Err(Details::GetEnumDefault {
                    symbol: value.to_string(),
                    symbols,
                }
                .into());
            }
        }

        let schema = Schema::Enum(EnumSchema {
            name: fully_qualified_name.clone(),
            aliases: aliases.clone(),
            doc: complex.doc(),
            symbols,
            default,
            attributes: self.get_custom_attributes(complex, vec!["symbols", "default"]),
        });

        self.register_parsed_schema(&fully_qualified_name, &schema, &aliases);

        Ok(schema)
    }

    /// Parse a `serde_json::Value` representing a Avro array type into a `Schema`.
    fn parse_array(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let items = complex
            .get("items")
            .ok_or_else(|| Details::GetArrayItemsField.into())
            .and_then(|items| self.parse(items, enclosing_namespace))?;
        let default = if let Some(default) = complex.get("default").cloned() {
            if let Value::Array(_) = default {
                let crate::types::Value::Array(array) = crate::types::Value::try_from(default)?
                else {
                    unreachable!("JsonValue::Array can only become a Value::Array")
                };
                // Check that the default type matches the schema type
                if let Some(value) = array.iter().find(|v| {
                    v.validate_internal(&items, &self.parsed_schemas, enclosing_namespace)
                        .is_some()
                }) {
                    return Err(Details::ArrayDefaultWrongInnerType(items, value.clone()).into());
                }
                Some(array)
            } else {
                return Err(Details::ArrayDefaultWrongType(default).into());
            }
        } else {
            None
        };
        Ok(Schema::Array(ArraySchema {
            items: Box::new(items),
            default,
            attributes: self.get_custom_attributes(complex, vec!["items", "default"]),
        }))
    }

    /// Parse a `serde_json::Value` representing a Avro map type into a `Schema`.
    fn parse_map(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let types = complex
            .get("values")
            .ok_or_else(|| Details::GetMapValuesField.into())
            .and_then(|types| self.parse(types, enclosing_namespace))?;

        let default = if let Some(default) = complex.get("default").cloned() {
            if let Value::Object(_) = default {
                let crate::types::Value::Map(map) = crate::types::Value::try_from(default)? else {
                    unreachable!("JsonValue::Object can only become a Value::Map")
                };
                // Check that the default type matches the schema type
                if let Some(value) = map.values().find(|v| {
                    v.validate_internal(&types, &self.parsed_schemas, enclosing_namespace)
                        .is_some()
                }) {
                    return Err(Details::MapDefaultWrongInnerType(types, value.clone()).into());
                }
                Some(map)
            } else {
                return Err(Details::MapDefaultWrongType(default).into());
            }
        } else {
            None
        };

        Ok(Schema::Map(MapSchema {
            types: Box::new(types),
            default,
            attributes: self.get_custom_attributes(complex, vec!["values", "default"]),
        }))
    }

    /// Parse a `serde_json::Value` representing a Avro union type into a `Schema`.
    fn parse_union(
        &mut self,
        items: &[Value],
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        items
            .iter()
            .map(|v| self.parse(v, enclosing_namespace))
            .collect::<Result<Vec<_>, _>>()
            .and_then(|schemas| {
                if schemas.is_empty() {
                    error!(
                        "Union schemas should have at least two members! \
                    Please enable debug logging to find out which Record schema \
                    declares the union with 'RUST_LOG=apache_avro::schema=debug'."
                    );
                } else if schemas.len() == 1 {
                    warn!(
                        "Union schema with just one member! Consider dropping the union! \
                    Please enable debug logging to find out which Record schema \
                    declares the union with 'RUST_LOG=apache_avro::schema=debug'."
                    );
                }
                Ok(Schema::Union(UnionSchema::new(schemas)?))
            })
    }

    /// Parse a `serde_json::Value` representing a Avro fixed type into a `Schema`.
    fn parse_fixed(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let size_opt = complex.get("size");
        if size_opt.is_none()
            && let Some(seen) = self.get_already_seen_schema(complex, enclosing_namespace)
        {
            return Ok(seen.clone());
        }

        let doc = complex.get("doc").and_then(|v| match &v {
            &Value::String(docstr) => Some(docstr.clone()),
            _ => None,
        });

        let size = match size_opt {
            Some(size) => size
                .as_u64()
                .ok_or_else(|| Details::GetFixedSizeFieldPositive(size.clone())),
            None => Err(Details::GetFixedSizeField),
        }?;

        let fully_qualified_name = Name::parse(complex, enclosing_namespace)?;
        let aliases =
            self.fix_aliases_namespace(complex.aliases(), fully_qualified_name.namespace());

        let schema = Schema::Fixed(FixedSchema {
            name: fully_qualified_name.clone(),
            aliases: aliases.clone(),
            doc,
            size: size as usize,
            attributes: self.get_custom_attributes(complex, vec!["size"]),
        });

        self.register_parsed_schema(&fully_qualified_name, &schema, &aliases);

        Ok(schema)
    }

    // A type alias may be specified either as a fully namespace-qualified, or relative
    // to the namespace of the name it is an alias for. For example, if a type named "a.b"
    // has aliases of "c" and "x.y", then the fully qualified names of its aliases are "a.c"
    // and "x.y".
    // https://avro.apache.org/docs/++version++/specification/#aliases
    fn fix_aliases_namespace(
        &self,
        aliases: Option<Vec<String>>,
        namespace: NamespaceRef,
    ) -> Aliases {
        aliases.map(|aliases| {
            aliases
                .iter()
                .map(|alias| {
                    if alias.find('.').is_none() {
                        match namespace {
                            Some(ns) => format!("{ns}.{alias}"),
                            None => alias.clone(),
                        }
                    } else {
                        alias.clone()
                    }
                })
                .map(|alias| Alias::new(alias.as_str()).unwrap())
                .collect()
        })
    }

    fn get_schema_type_name(&self, name: Name, value: Value) -> Name {
        match value.get("type") {
            Some(Value::Object(complex_type)) => match complex_type.name() {
                Some(name) => Name::new(name).unwrap(),
                _ => name,
            },
            _ => name,
        }
    }

    fn parse_json_integer_for_decimal(
        &self,
        value: &serde_json::Number,
    ) -> AvroResult<DecimalMetadata> {
        Ok(if value.is_u64() {
            let num = value
                .as_u64()
                .ok_or_else(|| Details::GetU64FromJson(value.clone()))?;
            num.try_into()
                .map_err(|e| Details::ConvertU64ToUsize(e, num))?
        } else if value.is_i64() {
            let num = value
                .as_i64()
                .ok_or_else(|| Details::GetI64FromJson(value.clone()))?;
            num.try_into()
                .map_err(|e| Details::ConvertI64ToUsize(e, num))?
        } else {
            return Err(Details::GetPrecisionOrScaleFromJson(value.clone()).into());
        })
    }
}
