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
    self, Alias, Aliases, ArraySchema, DecimalMetadata, DecimalSchema, DefaultToResolve, EnumSchema, FixedSchema, MapSchema, Name, NamespaceRef, Precision, RecordField, RecordSchema, Scale, Schema, SchemaKind, SchemaWithSymbols, UnionSchema, UuidSchema
};
use crate::serde::fixed;
use crate::types;
use crate::util::MapHelper;
use crate::validator::validate_enum_symbol_name;
use crate::{AvroResult, Error};
use log::{debug, error, warn};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

enum ReservedSchema {
    Reserved,
    Completed(Arc<Schema>)
}


#[derive(Default)]
pub(crate) struct Parser {
    /// Keeps track of the names defined for this schema, as well as where it is located
    /// in the schema tree
    defined_names: HashMap<Arc<Name>, ReservedSchema>,
    /// Keeps track of the names references by this schema, as well as where they are located
    /// in the schema tree
    referenced_names: HashSet<Arc<Name>>,
    /// Keeps track of fields that have a defualt value we need to resolve against.
    pub(crate) field_defaults_to_resolve: Vec<DefaultToResolve>
}

impl Parser {
    /// Create a `Schema` from a string representing a JSON Avro schema.
    /// Consumes the parser in the process.
    pub(super) fn parse_str(self, input: &str) -> Result<SchemaWithSymbols, Error> {
        let value = serde_json::from_str(input).map_err(Details::ParseSchemaJson)?;
        self.parse(&value, None)
    }

    /// Create a `SchemaWithSymbols` from a `serde_json::Value` representing a JSON Avro
    /// schema.
    pub(super) fn parse(mut self, value: &Value, enclosing_namespace: NamespaceRef) -> AvroResult<SchemaWithSymbols>{
        let schema = self.parse_schema(&value, enclosing_namespace)?;
        let defined_names : HashMap<Arc<Name>, Arc<Schema>> = HashMap::from_iter(self.defined_names.drain().map(|(key, val)| {
            match val {
                ReservedSchema::Reserved => {panic!("reserved schema encountered that was not provided a definition")} // KTODO: clean this message up!!
                ReservedSchema::Completed(schema_ref) => (key, schema_ref)
            }
        }));

        let referenced_names : HashSet<Arc<Name>> = self.referenced_names.drain().filter(|name|{!defined_names.contains_key(name)}).collect();

        Ok(SchemaWithSymbols{
            field_defaults_to_resolve: self.field_defaults_to_resolve.into(),
            defined_names,
            referenced_names,
            stub: Arc::new(schema)
        })
    }

    /// Create a `Schema` from a `serde_json::Value` representing a JSON Avro
    /// schema.
    pub(crate) fn parse_schema(&mut self, value: &Value, enclosing_namespace: NamespaceRef) -> AvroResult<Schema> {
        match *value {
            Value::String(ref t) => self.parse_known_schema(t.as_str(), enclosing_namespace),
            Value::Object(ref data) => {
                self.parse_json_object_as_schema(data, enclosing_namespace)
            }
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

    /// Registers that we are referencing a schema by name
    fn fetch_schema_ref(
        &mut self,
        name: &str,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let fully_qualified_name = Arc::new(Name::new_with_enclosing_namespace(name, enclosing_namespace)?);
        self.referenced_names.insert(Arc::clone(&fully_qualified_name));
        Ok(Schema::Ref { name: fully_qualified_name })
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
    pub(super) fn parse_json_object_as_schema(
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
                    _ => parser.parse_schema(value, enclosing_namespace),
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
            mut convert: F,
        ) -> AvroResult<Schema>
        where
            F: FnMut(Schema) -> AvroResult<Schema>,
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
                                Ok((precision, scale)) => {
                                    match inner{
                                        Schema::Fixed(inner) => {
                                            let schema = Schema::Decimal(DecimalSchema {
                                                precision,
                                                scale,
                                                inner: schema::InnerDecimalSchema::Fixed(inner.clone()),
                                            });
                                            let _ = self.insert_parsed_for_reserved(&inner.name, &inner.aliases, &Arc::new(schema))?;
                                            Ok(Schema::Ref{name: Arc::clone(&inner.name)})
                                        },
                                        Schema::Bytes => {
                                            Ok(Schema::Decimal(DecimalSchema {
                                                precision,
                                                scale,
                                                inner: schema::InnerDecimalSchema::Bytes,
                                            }))
                                        }
                                        _ => Err(Details::ResolveDecimalSchema(inner.into()).into())
                                    }
                                },
                                Err(err) => {
                                    warn!("Ignoring invalid decimal logical type: {err}");
                                    if let Schema::Fixed(ref fixed_inner) = inner {
                                        let _ = self.insert_parsed_for_reserved(&fixed_inner.name, &fixed_inner.aliases, &Arc::new(inner.clone()))?;
                                        Ok(Schema::Ref{name: Arc::clone(&fixed_inner.name)})
                                    }else{
                                        Ok(inner)
                                    }
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
                                let schema = Schema::Uuid(UuidSchema::Fixed(fixed.clone()));
                                let _ = self.insert_parsed_for_reserved(&fixed.name, &fixed.aliases, &Arc::new(schema))?;
                                Ok(Schema::Ref{name: Arc::clone(&fixed.name)})
                            }
                            Schema::Fixed(ref fixed) => {
                                warn!(
                                    "Ignoring uuid logical type for a Fixed schema because its size ({:?}) is not 16! Schema: {schema:?}",
                                    fixed.size
                                );
                                let _ = self.insert_parsed_for_reserved(&fixed.name, &fixed.aliases, &Arc::new(schema.clone()))?;
                                Ok(Schema::Ref{name: Arc::clone(&fixed.name)})
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
                                    let schema = Schema::Duration(fixed.clone());
                                    let _ = self.insert_parsed_for_reserved(&fixed.name, &fixed.aliases, &Arc::new(schema))?;
                                    Ok(Schema::Ref{name: Arc::clone(&fixed.name)})
                                }
                                Schema::Fixed(ref fixed) => {
                                    warn!(
                                        "Ignoring duration logical type on fixed type because size ({}) is not 12! Schema: {schema:?}",
                                        fixed.size
                                    );
                                    let _ = self.insert_parsed_for_reserved(&fixed.name, &fixed.aliases, &Arc::new(schema.clone()))?;
                                    Ok(Schema::Ref{name: Arc::clone(&fixed.name)})
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

        match complex.get("type"){
            Some(Value::String(t))=> { match t.as_str() {
                    "null" => Ok(Schema::Null),
                    "boolean" => Ok(Schema::Boolean),
                    "int" => Ok(Schema::Int),
                    "long" => Ok(Schema::Long),
                    "double" => Ok(Schema::Double),
                    "float" => Ok(Schema::Float),
                    "bytes" => Ok(Schema::Bytes),
                    "string" => Ok(Schema::String),
                    "record" => self.parse_record(complex, enclosing_namespace),
                    "enum" => self.parse_enum(complex, enclosing_namespace),
                    "array" => self.parse_array(complex, enclosing_namespace),
                    "map" => self.parse_map(complex, enclosing_namespace),
                    "fixed" => {
                        let fixed = self.parse_fixed(complex, enclosing_namespace)?;
                        match fixed {
                            Schema::Fixed(ref fixed_internal) => {
                                let _ = self.insert_parsed_for_reserved(&fixed_internal.name, &fixed_internal.aliases, &Arc::new(fixed.clone()))?;
                                Ok(Schema::Ref{name: Arc::clone(&fixed_internal.name)})
                            },
                            _ => panic!("Internal error, parse_fixed did not return a schema variant of Schema::Fixed!")
                        }
                    },
                    _ => Err(Details::JsonSchemaTypeNotAllowed { value: serde_json::Value::String(t.clone()) }.into())
                }
            }
            Some(value) => {Err(Details::JsonSchemaTypeNotAllowed { value: value.clone() }.into())}
            None => {Err(Details::GetComplexTypeField.into())}
        }
    }

    /// checks if a fullname or its aliases have been registered with the parser yet. If so, fails
    /// on NameCollision, else, reserves spots in the defined_names map that will be filled in when
    /// the definition is complete.
    fn check_and_reserve_name_and_aliases(&mut self, name: &Arc<Name>, aliases: &Aliases) -> AvroResult<()>{

        if let Option::None = self.defined_names.insert(Arc::clone(name), ReservedSchema::Reserved) {
        }else{
            return Err(Details::NameCollision(name.fullname(Option::None)).into());
        }

        let namespace = name.namespace();

        if let Some(aliases) = aliases {
            for alias in aliases {
                let alias_name : Arc<Name> = Arc::new(alias.fully_qualified_name(namespace).into_owned()); //KTODO: is this okay??

                if let Option::Some(_) = self.defined_names.insert(Arc::clone(&alias_name), ReservedSchema::Reserved) {
                    return Err(Details::NameCollision(alias_name.fullname(Option::None)).into());
                }
            }
        }

        Ok(())
    }

    /// adds the completed parsed schema into a spot previously reserved via
    /// check_and_reserve_name_and_aliases
    fn insert_parsed_for_reserved(&mut self, name: &Arc<Name>, aliases: &Aliases, schema: &Arc<Schema>) -> AvroResult<()>{

        if let Some(ReservedSchema::Reserved) = self.defined_names.insert(Arc::clone(name), ReservedSchema::Completed(Arc::clone(schema))) {
        }else{
            panic!("Attempting to write into a spot that has not been reserved!"); // TODO: IMPROVE THIS MESSAGE
        }

        let namespace = name.namespace();

        if let Some(aliases) = aliases {
            for alias in aliases {
                let alias_name : Arc<Name> = Arc::new(alias.fully_qualified_name(namespace).into_owned()); // KTODO: same with this, is this okay??

                if let Option::None = self.defined_names.insert(Arc::clone(&alias_name), ReservedSchema::Completed(Arc::clone(schema))) {
                    panic!("Attempting to write into a spot that has not been reserved!"); // TODO: IMPROVE THIS MESSAGE
                }
            }
        }

        Ok(())
    }

    /// Parse a `serde_json::Value` representing an Avro record type into a `Schema`.
    fn parse_record(
        &mut self,
        complex: &Map<String, Value>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let fields_opt = complex.get("fields");

        let fully_qualified_name = Arc::new(Name::parse(complex, enclosing_namespace)?);
        let aliases =
            self.fix_aliases_namespace(complex.aliases(), fully_qualified_name.namespace());

        let mut lookup = BTreeMap::new();

        self.check_and_reserve_name_and_aliases(&fully_qualified_name, &aliases)?;

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
            name: Arc::clone(&fully_qualified_name),
            aliases: aliases.clone(),
            doc: complex.doc(),
            fields,
            lookup,
            attributes: self.get_custom_attributes(complex, vec!["fields"]),
        });

        let _ = self.insert_parsed_for_reserved(&fully_qualified_name, &aliases, &Arc::new(schema))?;
        Ok(Schema::Ref{name: fully_qualified_name})
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

        let fully_qualified_name = Arc::new(Name::parse(complex, enclosing_namespace)?);
        let aliases =
            self.fix_aliases_namespace(complex.aliases(), fully_qualified_name.namespace());

        self.check_and_reserve_name_and_aliases(&fully_qualified_name, &aliases)?;

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
                .resolve_enum(&symbols, &Some(value.to_string()))
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
            name: Arc::clone(&fully_qualified_name),
            aliases: aliases.clone(),
            doc: complex.doc(),
            symbols,
            default,
            attributes: self.get_custom_attributes(complex, vec!["symbols", "default"]),
        });

        self.insert_parsed_for_reserved(&fully_qualified_name, &aliases, &Arc::new(schema));
        Ok(Schema::Ref { name: fully_qualified_name })
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
            .and_then(|items| self.parse_schema(items, enclosing_namespace))?;
        Ok(Schema::Array(ArraySchema {
            items: Box::new(items),
            attributes: self.get_custom_attributes(complex, vec!["items"]),
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
            .and_then(|types| self.parse_schema(types, enclosing_namespace))?;

        Ok(Schema::Map(MapSchema {
            types: Box::new(types),
            attributes: self.get_custom_attributes(complex, vec!["values"]),
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
            .map(|v| self.parse_schema(v, enclosing_namespace))
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

        let fully_qualified_name = Arc::new(Name::parse(complex, enclosing_namespace)?);
        let aliases =
            self.fix_aliases_namespace(complex.aliases(), fully_qualified_name.namespace());

        self.check_and_reserve_name_and_aliases(&fully_qualified_name, &aliases)?;

        let schema = Schema::Fixed(FixedSchema {
            name: Arc::clone(&fully_qualified_name),
            aliases: aliases.clone(),
            doc,
            size: size as usize,
            attributes: self.get_custom_attributes(complex, vec!["size"]),
        });

        // note that we do NOT add the fixed we just created into the named spot we created in the
        // symbol table. This is becuase if this fixed is being used by a logical type, we want the
        // name to refer to that type, so it's up to the caller of this function to decide what
        // schema to place in this reserved spot!

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
