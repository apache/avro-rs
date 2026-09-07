use crate::error::Details;
use crate::schema::{
    Alias, ArraySchema, DecimalSchema, EnumSchema, FixedSchema, InnerDecimalSchema, MapSchema,
    Name, NamespaceRef, RecordField, RecordSchema, UnionSchema, UuidSchema,
};
use crate::{AvroResult, Error, Schema};
use log::warn;
use sonic_rs::{ArrayJsonIter, JsonType, JsonValueTrait, LazyValue};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::ops::Deref;
use std::sync::LazyLock;

static BYTES_UUID_WARNING: LazyLock<()> =
    LazyLock::new(|| warn!("`bytes` with a logical type of `uuid` is deprecated"));

#[derive(Default)]
pub struct Parser2 {
    known_names: HashSet<Name>,
    unknown_references: HashSet<Name>,
}

impl Parser2 {
    /// Create a `Schema` from a string representing a JSON Avro schema.
    pub(super) fn parse_str(input: &str) -> AvroResult<Schema> {
        let lazy = sonic_rs::get_from_str::<[usize; 0]>(input, []).map_err(Details::Sonic)?;
        let mut parser = Self::default();
        let schema = parser.parse(lazy, None)?;
        if parser.has_unknown_references() {
            Err(Details::SchemaResolutionError(
                parser.unknown_references.into_iter().next().unwrap(),
            )
            .into())
        } else {
            Ok(schema)
        }
    }

    fn has_unknown_references(&mut self) -> bool {
        self.unknown_references
            .retain(|r| !self.known_names.contains(r));
        !self.unknown_references.is_empty()
    }

    fn add_reference(&mut self, name: &Name) {
        if !self.known_names.contains(name) {
            self.unknown_references.insert(name.clone());
        }
    }

    fn parse(&mut self, value: LazyValue, enclosing_namespace: NamespaceRef) -> AvroResult<Schema> {
        match value.get_type() {
            JsonType::String => {
                self.parse_string(value.as_str().expect("unreachable"), enclosing_namespace)
            }
            JsonType::Object => self.parse_complex(value, enclosing_namespace),
            JsonType::Array => self.parse_union(
                value.into_array_iter().expect("unreachable"),
                enclosing_namespace,
            ),
            _ => Err(Details::ParseSchemaFromValidJson.into()),
        }
    }

    fn parse_string(
        &mut self,
        value: &str,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        match value {
            "null" => Ok(Schema::Null),
            "boolean" => Ok(Schema::Boolean),
            "int" => Ok(Schema::Int),
            "long" => Ok(Schema::Long),
            "double" => Ok(Schema::Double),
            "float" => Ok(Schema::Float),
            "bytes" => Ok(Schema::Bytes),
            "string" => Ok(Schema::String),
            _ => {
                let name = Name::new_with_enclosing_namespace(value, enclosing_namespace)?;
                self.unknown_references.insert(name.clone());
                Ok(Schema::Ref { name })
            }
        }
    }

    fn parse_union(
        &mut self,
        array: ArrayJsonIter,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let mut builder = UnionSchema::builder();
        for value in array {
            let value = value.map_err(Details::Sonic)?;
            let schema = self.parse(value, enclosing_namespace)?;
            builder.variant(schema)?;
        }
        Ok(Schema::Union(builder.build()))
    }

    fn parse_complex(
        &mut self,
        value: LazyValue,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        assert!(value.is_object());

        let ty_value = value
            .pointer(["type"])
            .ok_or(Details::GetComplexTypeField)?;
        let ty = ty_value
            .as_str()
            .ok_or_else(|| Details::GetComplexType2(ty_value.get_type()))?;
        let logical_ty_value = value.pointer(["logicalType"]);
        let logical_ty = if let Some(val) = &logical_ty_value {
            Some(
                val.as_str()
                    .ok_or_else(|| Details::GetLogicalTypeFieldType2(val.get_type()))?,
            )
        } else {
            None
        };

        // This clone is practically a copy
        let object_iter = value
            .clone()
            .into_object_iter()
            .expect("This is an object")
            .filter_map(|r| {
                if let Ok((key, _)) = &r
                    && key == "type"
                {
                    None
                } else {
                    Some(r.map_err(|e| Error::new(Details::Sonic(e))))
                }
            });
        match (ty, logical_ty) {
            ("null", _) => Ok(Schema::Null),
            ("boolean", _) => Ok(Schema::Boolean),
            ("int", Some("date")) => Ok(Schema::Date),
            ("int", Some("time-millis")) => Ok(Schema::TimeMillis),
            ("int", _) => Ok(Schema::Int),
            ("long", Some("time-micros")) => Ok(Schema::TimeMicros),
            ("long", Some("timestamp-millis")) => Ok(Schema::TimestampMillis),
            ("long", Some("timestamp-micros")) => Ok(Schema::TimestampMicros),
            ("long", Some("timestamp-nanos")) => Ok(Schema::TimestampNanos),
            ("long", Some("local-timestamp-millis")) => Ok(Schema::LocalTimestampMillis),
            ("long", Some("local-timestamp-micros")) => Ok(Schema::LocalTimestampMicros),
            ("long", Some("local-timestamp-nanos")) => Ok(Schema::LocalTimestampNanos),
            ("long", _) => Ok(Schema::Long),
            ("float", _) => Ok(Schema::Float),
            ("double", _) => Ok(Schema::Double),
            ("bytes", Some("decimal")) => {
                self.parse_decimal(object_iter, "bytes", enclosing_namespace)
            }
            ("bytes", Some("big-decimal")) => Ok(Schema::BigDecimal),
            ("bytes", Some("uuid")) => {
                let _ = BYTES_UUID_WARNING.deref();
                Ok(Schema::Uuid(UuidSchema::Bytes))
            }
            ("bytes", _) => Ok(Schema::Bytes),
            ("string", Some("uuid")) => Ok(Schema::Uuid(UuidSchema::String)),
            ("string", _) => Ok(Schema::String),
            ("record", _) => self.parse_record(object_iter, enclosing_namespace),
            ("enum", _) => self.parse_enum(object_iter, enclosing_namespace),
            ("array", _) => self.parse_array(object_iter, enclosing_namespace),
            ("maps", _) => self.parse_map(object_iter, enclosing_namespace),
            ("fixed", Some("decimal")) => {
                self.parse_decimal(object_iter, "fixed", enclosing_namespace)
            }
            ("fixed", Some("uuid")) => self.parse_uuid_fixed(object_iter, enclosing_namespace),
            ("fixed", Some("duration")) => self.parse_duration(object_iter, enclosing_namespace),
            ("fixed", _) => self.parse_fixed(object_iter, enclosing_namespace),
            (_, None) => {
                let name = Name::new_with_enclosing_namespace(ty, enclosing_namespace)?;
                self.add_reference(&name);
                Ok(Schema::Ref { name })
            }
            (_, Some(lt)) => {
                panic!(
                    "References cannot be combined with logical types, reference: {ty}, logical type: {lt}"
                );
            }
        }
    }

    fn parse_decimal<'de>(
        &mut self,
        object: impl Iterator<Item = AvroResult<(Cow<'de, str>, LazyValue<'de>)>>,
        ty: &str,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let mut precision = None;
        let mut scale = None;

        let extracted =
            object.filter_map(|r| match r.as_ref().map(|(k, v)| (k.as_ref(), v)) {
                Ok(("precision", value)) if value.is_u64() => {
                    let value = value.as_u64().expect("Is a u64");
                    match usize::try_from(value) {
                        Ok(0) => {
                            Some(Err(Details::Custom("The `precision` for decimal cannot be 0".into()).into()))
                        }
                        Ok(value) => {
                            precision = Some(value);
                            None
                        }
                        Err(err) => Some(Err(Details::ConvertU64ToUsize(err, value).into()))
                    }
                }
                Ok(("precision", value)) if value.is_i64() => Some(Err(Details::Custom("Expected a non-zero positive integer for decimal `precision` got a negative number".into()).into())),
                Ok(("precision", value)) if value.is_f64() => Some(Err(Details::Custom("Expected a non-zero positive integer for decimal `precision` got a floating point number".into()).into())),
                Ok(("precision", value)) => Some(Err(Details::Custom(format!("Expected a non-zero positive integer for decimal `precision` got {:?}", value.get_type())).into())),
                Ok(("scale", value)) if value.is_u64() => {
                    let value = value.as_u64().expect("Is a u64");
                    match usize::try_from(value) {
                        Ok(value) => {
                            scale = Some(value);
                            None
                        }
                        Err(err) => Some(Err(Details::ConvertU64ToUsize(err, value).into()))
                    }
                }
                Ok(("scale", value)) if value.is_i64() => Some(Err(Details::Custom("Expected a positive integer for decimal `scale` got a negative number".into()).into())),
                Ok(("scale", value)) if value.is_f64() => Some(Err(Details::Custom("Expected a positive integer for decimal `scale` got a floating point number".into()).into())),
                Ok(("scale", value)) => Some(Err(Details::Custom(format!("Expected a positive integer for decimal `scale` got {:?}", value.get_type())).into())),
                _ => Some(r),
            });

        match ty {
            "bytes" => {
                for result in extracted {
                    result?;
                }
                let precision = precision
                    .ok_or_else(|| Details::Custom("Missing `precision` for decimal".into()))?;
                let scale = scale.unwrap_or(0);
                if scale > precision {
                    return Err(Details::DecimalPrecisionLessThanScale { scale, precision }.into());
                }

                Ok(Schema::Decimal(DecimalSchema {
                    precision,
                    scale,
                    inner: InnerDecimalSchema::Bytes,
                }))
            }
            "fixed" => {
                let fixed = self.parse_fixed_bare(extracted, enclosing_namespace)?;
                let precision = precision
                    .ok_or_else(|| Details::Custom("Missing `precision` for decimal".into()))?;
                let scale = scale.unwrap_or(0);
                if scale > precision {
                    return Err(Details::DecimalPrecisionLessThanScale { scale, precision }.into());
                }

                let max_precision = (2usize.pow((8 * (fixed.size - 1)) as u32) - 1).ilog10();
                if (max_precision as usize) < precision {
                    return Err(Details::Custom(format!("Maximum precision for Fixed({}) is {max_precision} but precision is {precision}", fixed.size)).into());
                }

                Ok(Schema::Decimal(DecimalSchema {
                    precision,
                    scale,
                    inner: InnerDecimalSchema::Fixed(fixed),
                }))
            }
            _ => unreachable!(),
        }
    }

    fn parse_record<'de>(
        &mut self,
        object: impl Iterator<Item = AvroResult<(Cow<'de, str>, LazyValue<'de>)>>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let mut name = None;
        let mut namespace = None;
        let mut aliases = None;
        let mut doc = None;
        let mut fields = None;
        let mut attributes = BTreeMap::new();

        for result in object {
            let (key, value) = result?;
            // TODO: panics to actual errors
            match (key.as_ref(), value.get_type()) {
                ("name", _) if name.is_some() => panic!("Duplicate `name` field"),
                ("name", JsonType::String) => {
                    name = Some(value.as_str().expect("Is a string").to_string())
                }
                ("name", ty) => panic!("`name` must be a string not a {ty:?}"),
                ("namespace", _) if namespace.is_some() => panic!("Duplicate `namespace` field"),
                ("namespace", JsonType::String) => {
                    namespace = Some(value.as_str().expect("Is a string").to_string())
                }
                ("namespace", ty) => panic!("`namespace` must be a string not a {ty:?}"),
                ("aliases", _) if aliases.is_none() => panic!("Duplicate `aliases` field"),
                ("aliases", JsonType::Array) => {
                    aliases = Some(
                        self.parse_aliases(value.into_array_iter().unwrap(), enclosing_namespace)?,
                    )
                }
                ("aliases", ty) => panic!("`aliases` must be a string not a {ty:?}"),
                ("doc", _) if doc.is_some() => panic!("Duplicate `doc` field"),
                ("doc", JsonType::String) => {
                    doc = Some(value.as_str().expect("Is a string").to_string())
                }
                ("doc", ty) => panic!("`doc` must be a string not a {ty:?}"),
                ("fields", _) if fields.is_some() => panic!("Duplicate `fields` field"),
                ("fields", JsonType::Array) => {
                    fields = Some(self.parse_fields(
                        value.into_array_iter().expect("Is an array"),
                        enclosing_namespace,
                    ))
                }
                ("fields", ty) => panic!("`fields` must be an positive integer not a {ty:?}"),
                (_, _) => {
                    if attributes.contains_key(key.as_ref()) {
                        panic!("Duplicate custom attribute found for {key}");
                    }
                    attributes.insert(key.into_owned(), sonic_value_to_serde_value(value));
                }
            }
        }

        let Some(name) = name else {
            return Err(Details::GetNameField.into());
        };
        let Some(fields) = fields else {
            panic!("`fields` is missing on the struct")
        };
        let name =
            Name::new_with_enclosing_namespace(name, namespace.as_deref().or(enclosing_namespace))?;

        Ok(Schema::Record(
            RecordSchema::builder()
                .name(name)
                .aliases(aliases)
                .doc(doc)
                .fields(fields?)
                .attributes(attributes)
                .build(),
        ))
    }

    fn parse_enum<'de>(
        &mut self,
        object: impl Iterator<Item = AvroResult<(Cow<'de, str>, LazyValue<'de>)>>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let mut name = None;
        let mut namespace = None;
        let mut aliases = None;
        let mut doc = None;
        let mut symbols = None;
        let mut default = None;
        let mut attributes = BTreeMap::new();

        for result in object {
            let (key, value) = result?;
            // TODO: panics to actual errors
            match (key.as_ref(), value.get_type()) {
                ("name", _) if name.is_some() => panic!("Duplicate `name` field"),
                ("name", JsonType::String) => {
                    name = Some(value.as_str().expect("Is a string").to_string())
                }
                ("name", ty) => panic!("`name` must be a string not a {ty:?}"),
                ("namespace", _) if namespace.is_some() => panic!("Duplicate `namespace` field"),
                ("namespace", JsonType::String) => {
                    namespace = Some(value.as_str().expect("Is a string").to_string())
                }
                ("namespace", ty) => panic!("`namespace` must be a string not a {ty:?}"),
                ("aliases", _) if aliases.is_none() => panic!("Duplicate `aliases` field"),
                ("aliases", JsonType::Array) => {
                    aliases = Some(
                        self.parse_aliases(value.into_array_iter().unwrap(), enclosing_namespace)?,
                    )
                }
                ("aliases", ty) => panic!("`aliases` must be a string not a {ty:?}"),
                ("doc", _) if doc.is_some() => panic!("Duplicate `doc` field"),
                ("doc", JsonType::String) => {
                    doc = Some(value.as_str().expect("Is a string").to_string())
                }
                ("doc", ty) => panic!("`doc` must be a string not a {ty:?}"),
                ("symbols", _) if symbols.is_some() => panic!("Duplicate `symbols` field"),
                ("symbols", JsonType::Array) => {
                    symbols = Some(
                        value.into_array_iter().unwrap().map(|r| {
                            r.map_err(Details::Sonic).and_then(|v| v.as_str().map(str::to_string).ok_or_else(|| Details::Custom(format!("`symbols` must be an array of string, but got an {:?} in the array", v.get_type()))))
                        }).collect::<Result<Vec<_>, _>>()?,
                    )
                }
                ("default", _) if default.is_some() => panic!("Duplicate `default` field"),
                ("default", JsonType::String)  => default = Some(value.as_str().expect("Is a string").to_string()),
                ("default", ty) => panic!("`default` must be a string not a {ty:?}"),
                (_, _) => {
                    if attributes.contains_key(key.as_ref()) {
                        panic!("Duplicate custom attribute found for {key}");
                    }
                    attributes.insert(key.into_owned(), sonic_value_to_serde_value(value));
                }
            }
        }

        let Some(name) = name else {
            return Err(Details::GetNameField.into());
        };
        let Some(symbols) = symbols else {
            panic!("`symbols` is missing on the struct")
        };
        let name =
            Name::new_with_enclosing_namespace(name, namespace.as_deref().or(enclosing_namespace))?;
        if let Some(default) = &default
            && !symbols.contains(default)
        {
            panic!("`default` of {default} does not exist in `symbols`")
        }

        Ok(Schema::Enum(EnumSchema {
            name,
            aliases,
            doc,
            symbols,
            default,
            attributes,
        }))
    }

    fn parse_array<'de>(
        &mut self,
        object: impl Iterator<Item = AvroResult<(Cow<'de, str>, LazyValue<'de>)>>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let mut items = None;
        let mut attributes = BTreeMap::new();

        for result in object {
            let (key, value) = result?;
            match key.as_ref() {
                "items" if items.is_some() => panic!("Duplicate `items` field"),
                "items" => items = Some(self.parse(value, enclosing_namespace)?),
                _ => {
                    if attributes.contains_key(key.as_ref()) {
                        panic!("Duplicate custom attribute found for {key}");
                    }
                    attributes.insert(key.into_owned(), sonic_value_to_serde_value(value));
                }
            }
        }

        let Some(items) = items else {
            panic!("Mising `items` for array")
        };

        Ok(Schema::Array(ArraySchema {
            items: Box::new(items),
            attributes,
        }))
    }

    fn parse_map<'de>(
        &mut self,
        object: impl Iterator<Item = AvroResult<(Cow<'de, str>, LazyValue<'de>)>>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let mut ty = None;
        let mut attributes = BTreeMap::new();

        for result in object {
            let (key, value) = result?;
            match key.as_ref() {
                "type" if ty.is_some() => panic!("Duplicate `type` field"),
                "type" => ty = Some(self.parse(value, enclosing_namespace)?),
                _ => {
                    if attributes.contains_key(key.as_ref()) {
                        panic!("Duplicate custom attribute found for {key}");
                    }
                    attributes.insert(key.into_owned(), sonic_value_to_serde_value(value));
                }
            }
        }

        let Some(ty) = ty else {
            panic!("Mising `items` for array")
        };

        Ok(Schema::Map(MapSchema {
            types: Box::new(ty),
            attributes,
        }))
    }

    fn parse_uuid_fixed<'de>(
        &mut self,
        object: impl Iterator<Item = AvroResult<(Cow<'de, str>, LazyValue<'de>)>>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let filtered = object.filter(|r| {
            if let Ok((key, _)) = r
                && key == "logicalType"
            {
                false
            } else {
                true
            }
        });
        let fixed = self.parse_fixed_bare(filtered, enclosing_namespace)?;
        if fixed.size != 16 {
            // TODO: Wrong error?
            Err(Details::ConvertFixedToUuid(fixed.size).into())
        } else {
            Ok(Schema::Uuid(UuidSchema::Fixed(fixed)))
        }
    }

    fn parse_duration<'de>(
        &mut self,
        object: impl Iterator<Item = AvroResult<(Cow<'de, str>, LazyValue<'de>)>>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        let filtered = object.filter(|r| {
            if let Ok((key, _)) = r
                && key == "logicalType"
            {
                false
            } else {
                true
            }
        });
        Ok(Schema::Duration(
            self.parse_fixed_bare(filtered, enclosing_namespace)?,
        ))
    }

    fn parse_fixed<'de>(
        &mut self,
        object: impl Iterator<Item = AvroResult<(Cow<'de, str>, LazyValue<'de>)>>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Schema> {
        self.parse_fixed_bare(object, enclosing_namespace)
            .map(|f| Schema::Uuid(UuidSchema::Fixed(f)))
    }

    fn parse_fixed_bare<'de>(
        &mut self,
        object: impl Iterator<Item = AvroResult<(Cow<'de, str>, LazyValue<'de>)>>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<FixedSchema> {
        let mut name = None;
        let mut namespace = None;
        let mut aliases = None;
        let mut doc = None;
        let mut size = None;
        let mut attributes = BTreeMap::new();

        for result in object {
            let (key, value) = result?;
            // TODO: panics to actual errors
            match (key.as_ref(), value.get_type()) {
                ("name", _) if name.is_some() => panic!("Duplicate `name` field"),
                ("name", JsonType::String) => {
                    name = Some(value.as_str().expect("Is a string").to_string())
                }
                ("name", ty) => panic!("`name` must be a string not a {ty:?}"),
                ("namespace", _) if namespace.is_some() => panic!("Duplicate `namespace` field"),
                ("namespace", JsonType::String) => {
                    namespace = Some(value.as_str().expect("Is a string").to_string())
                }
                ("namespace", ty) => panic!("`namespace` must be a string not a {ty:?}"),
                ("aliases", _) if aliases.is_none() => panic!("Duplicate `aliases` field"),
                ("aliases", JsonType::Array) => {
                    aliases = Some(
                        self.parse_aliases(value.into_array_iter().unwrap(), enclosing_namespace)?,
                    )
                }
                ("aliases", ty) => panic!("`aliases` must be a string not a {ty:?}"),
                ("doc", _) if doc.is_some() => panic!("Duplicate `doc` field"),
                ("doc", JsonType::String) => {
                    doc = Some(value.as_str().expect("Is a string").to_string())
                }
                ("doc", ty) => panic!("`doc` must be a string not a {ty:?}"),
                ("size", _) if size.is_some() => panic!("Duplicate `size` field"),
                ("size", _) if value.is_u64() => size = Some(value.as_u64().expect("Is a u64")),
                ("size", _) if value.is_i64() => panic!("`size` must be an positive integer"),
                ("size", _) if value.is_f64() => {
                    panic!("`size` must be an positive integer not a float")
                }
                ("size", ty) => panic!("`size` must be an positive integer not a {ty:?}"),
                (_, _) => {
                    if attributes.contains_key(key.as_ref()) {
                        panic!("Duplicate custom attribute found for {key}");
                    }
                    attributes.insert(key.into_owned(), sonic_value_to_serde_value(value));
                }
            }
        }

        let Some(name) = name else {
            return Err(Details::GetNameField.into());
        };
        let Some(size) = size else {
            return Err(Details::GetFixedSizeField.into());
        };
        let name =
            Name::new_with_enclosing_namespace(name, namespace.as_deref().or(enclosing_namespace))?;

        Ok(FixedSchema {
            name,
            aliases: None,
            doc,
            size: usize::try_from(size).map_err(|e| Details::ConvertU64ToUsize(e, size))?,
            attributes,
        })
    }

    fn parse_aliases(
        &mut self,
        array: ArrayJsonIter,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Vec<Alias>> {
        // This function is only entered if `aliases` exist, and the most common length of `aliases`
        // is probably 1.
        let mut aliases = Vec::with_capacity(1);

        for result in array {
            let value = result.map_err(Details::Sonic)?;
            if let Some(str) = value.as_str() {
                aliases.push(Alias::new_with_enclosing_namespace(
                    str,
                    enclosing_namespace,
                )?);
            } else {
                panic!(
                    "aliases must be an array of strings, found an {:?} in the array",
                    value.get_type()
                )
            }
        }

        aliases.shrink_to_fit();
        Ok(aliases)
    }

    fn parse_fields(
        &mut self,
        array: ArrayJsonIter,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<Vec<RecordField>> {
        let mut fields = Vec::new();
        for result in array {
            let item = result.map_err(Details::Sonic)?;
            let ty = item.get_type();
            if let Some(object) = item.into_object_iter() {
                let field = self.parse_field(
                    object.map(|r| r.map_err(|e| Error::new(Details::Sonic(e)))),
                    enclosing_namespace,
                )?;
                fields.push(field);
            } else {
                panic!("Expected array of objects for `fields` but found {ty:?} in the array")
            }
        }
        Ok(fields)
    }

    fn parse_field<'de>(
        &mut self,
        object: impl Iterator<Item = AvroResult<(Cow<'de, str>, LazyValue<'de>)>>,
        enclosing_namespace: NamespaceRef,
    ) -> AvroResult<RecordField> {
        let mut name = None;
        let mut doc = None;
        let mut ty = None;
        let mut order_found = false;
        let mut aliases = None;
        let mut default = None;
        let mut attributes = BTreeMap::new();

        for result in object {
            let (key, value) = result?;
            // TODO: panics to actual errors
            match (key.as_ref(), value.get_type()) {
                ("name", _) if name.is_some() => panic!("Duplicate `name` field"),
                ("name", JsonType::String) => {
                    name = Some(value.as_str().expect("Is a string").to_string())
                }
                ("name", ty) => panic!("`name` must be a string not a {ty:?}"),
                ("doc", _) if doc.is_some() => panic!("Duplicate `doc` field"),
                ("doc", JsonType::String) => {
                    doc = Some(value.as_str().expect("Is a string").to_string())
                }
                ("doc", ty) => panic!("`doc` must be a string not a {ty:?}"),
                ("type", _) if ty.is_some() => panic!("Duplicate `type` field"),
                ("type", _) => ty = Some(self.parse(value, enclosing_namespace)),
                ("order", _) if order_found => panic!("Duplicate `order` field"),
                ("order", JsonType::String) => match value.as_str().expect("Is a string") {
                    "ascending" | "descending" | "ignore" => order_found = true,
                    v => panic!("Unexpected value for `order`: {v}"),
                }
                ("order", ty) => panic!("`order` must be a string not a {ty:?}"),
                ("aliases", _) if aliases.is_none() => panic!("Duplicate `aliases` field"),
                ("aliases", JsonType::Array) => {
                    aliases = Some(
                        value.into_array_iter().unwrap().map(|r| {
                            r.map_err(Details::Sonic).and_then(|v| v.as_str().map(str::to_string).ok_or_else(|| Details::Custom(format!("`aliases` must be an array of string, but got an {:?} in the array", v.get_type()))))
                        }).collect::<Result<Vec<_>, _>>()?,
                    )
                }
                ("aliases", ty) => panic!("`aliases` must be a string not a {ty:?}"),
                ("default", _) if default.is_some() => panic!("Duplicate `default` field"),
                ("default", _) => {
                    default = Some(serde_json::from_str(value.as_raw_str()).expect("This is valid JSON"))
                }
                (_, _) => {
                    if attributes.contains_key(key.as_ref()) {
                        panic!("Duplicate custom attribute found for {key}");
                    }
                    attributes.insert(key.into_owned(), sonic_value_to_serde_value(value));
                }
            }
        }

        let Some(name) = name else {
            panic!("`name` is missing for field")
        };
        let Some(ty) = ty else {
            panic!("`ty` is missing for field")
        };

        Ok(RecordField::builder()
            .name(name)
            .doc(doc)
            .maybe_aliases(aliases)
            .maybe_default(default)
            .schema(ty?)
            .custom_attributes(attributes)
            .build())
    }
}

fn sonic_value_to_serde_value(value: LazyValue) -> serde_json::Value {
    serde_json::from_str(value.as_raw_str()).expect("This should parse")
}
