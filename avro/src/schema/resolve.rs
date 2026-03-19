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

// Items used for handling and/or providing named schema resolution.

use serde::Serialize;
use serde_json::Value as JsonValue;
use strum::Display;

use crate::schema::{Aliases, ArraySchema, DecimalSchema, DefaultToResolve, Documentation, EnumSchema, FixedSchema, MapSchema, Name, NameMap, NameSet, RecordField, RecordSchema, Schema, SchemaKind, SchemaWithSymbols, UnionSchema, UuidSchema, unravel_inner};
use crate::{AvroResult, types};
use crate::error::{Details,Error};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::{collections::{HashMap, HashSet}, sync::Arc, iter::once};

/// a map of names and definitions that is consistent: that is, we have all named schemata refered
/// by any of the component schemata has a unique definition in this context.
#[derive(Debug,Clone)]
pub struct ResolvedContext(NameMap);

impl ResolvedContext{
    /// gets the resolved context from a `ResolvedSchema`
    pub fn from_resolved_schema(resolved_schema : &ResolvedSchema) -> ResolvedContext{
        resolved_schema.context.clone()
    }

    pub fn empty() -> ResolvedContext{
        ResolvedContext(HashMap::new().into())
    }

    // convenience method for copying (pointer copy) ony the definitions we need for a given schema
    // KTODO: bug with this, see notes, need to get to a stable testing environment to look at
    // more...
    //pub fn copy_needed_definitions(&self, schema_with_symbols: &SchemaWithSymbols) -> AvroResult<ResolvedContext> {
    //    let needed_references = &schema_with_symbols.referenced_names;
    //    let mut needed_defs : NameMap = HashMap::new();
    //    for needed in needed_references{
    //        if let Some(def) = self.0.get(needed){
    //            needed_defs.insert(Arc::clone(needed), Arc::clone(def));
    //        }else{
    //            return Err(Details::SchemaLookupError(needed.as_ref().clone()).into());
    //        }
    //    };

    //    // *Why we can form another resolved*: since we provided a schema_with_symbols, we are
    //    // guarenteed that we we have the complete set of definitions needed to descend through
    //    // that schema, since part of our parsing is to track this.
    //    Ok(ResolvedContext(needed_defs.into()))
    //}

    // checks that the provided definition names do not conflict with existing definitions.
    fn check_if_conflicts<'a>(defined_names: &NameMap, added: &NameMap) -> AvroResult<()>{
        let mut conflicting_fullnames : Vec<String> = Vec::new();
        for (name, schema) in added{
            if defined_names.contains_key(name) && Some(schema) != defined_names.get(name){
                conflicting_fullnames.push(name.fullname(Option::None));
            }
        }

        if conflicting_fullnames.len() == 0 {
            Ok(())
        }else{
            Err(Details::MultipleNameCollision(conflicting_fullnames).into())
        }
    }

    //pub fn new_context(schemata: impl Iterator<Item = SchemaWithSymbols>) -> AvroResult<ResolvedContext>{
    //    Self::new_context_with_resolver(schemata, &mut DefaultResolver::new())
    //}

    /// KTODO: docs
    pub fn new_context_with_resolver<'s>(schemata: &Vec<SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<ResolvedContext>{
        let mut context : NameMap = HashMap::new();
        let mut defaults : Vec<Arc<Vec<DefaultToResolve>>> = Vec::new();
        Self::add_to_context(&mut context, &mut defaults, schemata, resolver)?;
        let context = ResolvedContext(context);
        Self::check_defaults(&defaults, &context)?;
        Ok(context)
    }

    /// KTODO: docs
    pub fn needed_context_with_resolver<'s>(schema: &'s SchemaWithSymbols, schemata: &Vec<SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<ResolvedContext>{
        let mut context : NameMap = HashMap::new();
        let mut defaults : Vec<Arc<Vec<DefaultToResolve>>> = Vec::new();
        Self::check_if_schemata_redefine_names(schemata.iter())?;
        Self::add_needed_to_context(schema, &mut context, &mut defaults ,schemata, resolver)?;
        let context = ResolvedContext(context);
        Self::check_defaults(&defaults, &context)?;
        Ok(context)
    }

    // add the with_symbols into the defined_names and check for naming conflicts.
    // If we can't resolved from the local context, we will ask the resolved if it can find
    // the schema we need.
    fn add_to_context(defined_names: &mut NameMap, defaults: &mut Vec<Arc<Vec<DefaultToResolve>>>, schemata: &Vec<SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<()>{
        let mut references : HashSet<Arc<Name>> = HashSet::new();

        for schema_with_symbol in schemata{
            defaults.push(Arc::clone(&schema_with_symbol.field_defaults_to_resolve));
            Self::check_if_conflicts(&defined_names, &schema_with_symbol.defined_names)?;
            defined_names.extend(schema_with_symbol.defined_names.clone());
            references.extend(schema_with_symbol.referenced_names.clone());
        }

        for schema_ref in references{
            Self::resolve_name(defined_names, defaults, &schema_ref, resolver)?;
        }

        Ok(())

    }

    /// KTODO: docs
    fn add_needed_to_context(schema: &SchemaWithSymbols, defined_names: &mut NameMap, defaults: &mut Vec<Arc<Vec<DefaultToResolve>>>, schemata: &Vec<SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<()>{
        defined_names.extend(schema.defined_names.clone());
        defaults.push(Arc::clone(&schema.field_defaults_to_resolve));
        let mut worklist: HashSet<Arc<Name>> = schema.referenced_names.clone();

        while let Some(needed) = Self::take_one(&mut worklist) {
            if defined_names.contains_key(&needed) {
                continue;
            }

            if let Some(provider) = schemata.iter().find(|s| s.defined_names.contains_key(&needed)) {
                Self::check_if_conflicts(defined_names, &provider.defined_names)?;
                defined_names.extend(provider.defined_names.clone());
                defaults.push(Arc::clone(&provider.field_defaults_to_resolve));
                worklist.extend(provider.referenced_names.clone());
            } else {
                Self::resolve_name(defined_names, defaults, &needed, resolver)?;
            }
        }

        Ok(())
    }

    fn take_one(set: &mut HashSet<Arc<Name>>) -> Option<Arc<Name>> {
        let name = set.iter().next()?.clone();
        set.remove(&name);
        Some(name)
    }

    // checks if the list we have provided locally has duplicate definitions for
    // a given name.
    fn check_if_schemata_redefine_names<'s>(schemata: impl IntoIterator<Item = &'s SchemaWithSymbols>)-> AvroResult<()>{
        let mut defined_names : HashMap<&Arc<Name>, &Arc<Schema>> = HashMap::new();

        let mut conflicting_fullnames : Vec<String> = Vec::new();

        for schema in schemata{
            for (name, schema) in &schema.defined_names{
                if defined_names.contains_key(name) && Some(schema) != defined_names.get(name).map(|v| &**v){
                    conflicting_fullnames.push(name.fullname(Option::None));
                }
            }
            defined_names.extend(&schema.defined_names);
        }

        if conflicting_fullnames.len() == 0 {
            Ok(())
        }else{
            Err(Details::MultipleNameCollision(conflicting_fullnames).into())
        }
    }

    // attempt to resolve the schema name, first from the known schema definitions, and if that
    // fails, from the provided resolver.
    fn resolve_name(defined_names: &mut NameMap, defaults: &mut Vec<Arc<Vec<DefaultToResolve>>>, name: &Arc<Name>, resolver: &mut impl Resolver) -> AvroResult<()>{
        // first, check if can resolve internally
        if defined_names.contains_key(name) {
            return Ok(());
        }

        // second, use provided resolver
        match resolver.find_schema(name){
            Ok(schema_with_symbols) => {

                // check that what we got back from the resolver actually matches what we expect
                if !schema_with_symbols.defined_names.contains_key(name) {
                    return Err(Details::CustomSchemaResolverMismatch(name.as_ref().clone(),
                            Vec::from_iter(schema_with_symbols.defined_names.keys().map(|key| {key.as_ref().clone()}))).into())
                }
                // matches, lets add this as a schemata that we should have, and recurse in
                Self::add_to_context(defined_names, defaults, &vec![schema_with_symbols], resolver)?;
                Ok(())
            },
            Err(msg) => {
                return Err(Details::SchemaResolutionErrorWithMsg(name.as_ref().clone(), msg).into());
            }
        }
    }

    fn check_defaults(default_vecs: &Vec<Arc<Vec<DefaultToResolve>>>, context: &ResolvedContext) -> AvroResult<()>{
        for default_vec in default_vecs{
            for default in default_vec.as_ref(){
                let avro_value = types::Value::try_from(default.json.clone())?;
                       avro_value.resolve_internal(ResolvedNode::new(&ResolvedSchema{
                           stub: Arc::new(default.schema.clone()), // TODO: would be nice to get rid of clones here!
                           context: context.clone()
                       })).or_else(|error| {
                           Err(Details::DefaultValidationWithReason {
                               record_name: default.record_name.clone(),
                               field_name: default.field_name.clone(),
                               value: default.json.clone() ,
                               schema: default.schema.clone(),
                               reason: error.to_string() })}
                           )?;
            }
        }

        Ok(())
    }
}
/// Trait for types that can be converted into a [`SchemaWithSymbols`] for schema resolution.
///
/// Implemented for:
/// - `&str`, `String`, `&String` (parses JSON schema string)
/// - `&Schema` (clones and converts)
/// - `SchemaWithSymbols` (identity)
pub trait IntoSchemaWithSymbols {
    fn into_schema_with_symbols(self) -> AvroResult<SchemaWithSymbols>;
}

impl<S: AsRef<str>> IntoSchemaWithSymbols for S {
    fn into_schema_with_symbols(self) -> AvroResult<SchemaWithSymbols> {
        SchemaWithSymbols::parse_str(self.as_ref())
    }
}

impl IntoSchemaWithSymbols for &Schema {
    fn into_schema_with_symbols(self) -> AvroResult<SchemaWithSymbols> {
        Ok(self.clone().into())
    }
}

impl IntoSchemaWithSymbols for SchemaWithSymbols {
    fn into_schema_with_symbols(self) -> AvroResult<SchemaWithSymbols> {
        Ok(self)
    }
}

/// Builder for resolving one or more schemas into [`ResolvedSchema`] values.
///
/// Created via [`ResolvedSchema::resolve()`]. Accepts schema strings, `&Schema` references,
/// or [`SchemaWithSymbols`] values as input — all via the [`IntoSchemaWithSymbols`] trait.
///
/// # Examples
///
/// ```rust,ignore
/// // Resolve a single schema:
/// let resolved = ResolvedSchema::resolve().build_one(schema_str)?;
///
/// // Resolve with additional context schemas:
/// let [resolved] = ResolvedSchema::resolve()
///     .additional(vec![context_schema])?
///     .build_array([main_schema])?;
///
/// // With a custom resolver (e.g. schema registry):
/// let [resolved] = ResolvedSchema::resolve()
///     .build_array_with_resolver([schema], &mut my_resolver)?;
/// ```
pub struct ResolvedSchemaBuilder {
    additional: Vec<SchemaWithSymbols>,
}

impl ResolvedSchemaBuilder {
    fn new() -> Self {
        ResolvedSchemaBuilder {
            additional: Vec::new(),
        }
    }

    /// Provide additional schemas that participate in name resolution but are not themselves
    /// returned. Accepts anything that implements [`IntoSchemaWithSymbols`]: JSON strings,
    /// `&Schema` references, or `SchemaWithSymbols` values.
    pub fn additional(mut self, additional: impl IntoIterator<Item = impl IntoSchemaWithSymbols>) -> AvroResult<Self> {
        self.additional = additional
            .into_iter()
            .map(|s| s.into_schema_with_symbols())
            .collect::<AvroResult<Vec<_>>>()?;
        Ok(self)
    }

    /// Resolve a single schema. This is the simplest entry point.
    pub fn build_one(self, to_resolve: impl IntoSchemaWithSymbols) -> AvroResult<ResolvedSchema> {
        let [result] = self.build_array([to_resolve])?;
        Ok(result)
    }

    /// Resolve a single schema with a custom [`Resolver`] for external name lookup.
    pub fn build_one_with_resolver(self, to_resolve: impl IntoSchemaWithSymbols, resolver: &mut impl Resolver) -> AvroResult<ResolvedSchema> {
        let [result] = self.build_array_with_resolver([to_resolve], resolver)?;
        Ok(result)
    }

    /// Resolve a fixed-size array of schemas, preserving the count at compile time.
    /// All elements must be the same type (e.g. all `&str` or all `&Schema`).
    pub fn build_array<const N: usize>(self, to_resolve: [impl IntoSchemaWithSymbols; N]) -> AvroResult<[ResolvedSchema; N]> {
        self.build_array_with_resolver(to_resolve, &mut DefaultResolver::new())
    }

    /// Resolve a fixed-size array of schemas with a custom [`Resolver`].
    pub fn build_array_with_resolver<const N: usize>(self, to_resolve: [impl IntoSchemaWithSymbols; N], resolver: &mut impl Resolver) -> AvroResult<[ResolvedSchema; N]> {
        let converted: AvroResult<Vec<SchemaWithSymbols>> = to_resolve
            .into_iter()
            .map(|s| s.into_schema_with_symbols())
            .collect();
        let converted = converted?;
        let mut iter = converted.into_iter();
        let to_resolve_array: [SchemaWithSymbols; N] = std::array::from_fn(|_| iter.next().unwrap());
        ResolvedSchema::resolve_symbols_array(to_resolve_array, self.additional, resolver)
    }

    /// Resolve a dynamic list of schemas.
    pub fn build_list(self, to_resolve: impl IntoIterator<Item = impl IntoSchemaWithSymbols>) -> AvroResult<Vec<ResolvedSchema>> {
        self.build_list_with_resolver(to_resolve, &mut DefaultResolver::new())
    }

    /// Resolve a dynamic list of schemas with a custom [`Resolver`].
    pub fn build_list_with_resolver(self, to_resolve: impl IntoIterator<Item = impl IntoSchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<Vec<ResolvedSchema>> {
        let converted: AvroResult<Vec<SchemaWithSymbols>> = to_resolve
            .into_iter()
            .map(|s| s.into_schema_with_symbols())
            .collect();
        ResolvedSchema::resolve_symbols_list(converted?, self.additional, resolver)
    }
}

/// Contains a schema with all of the schema definitions it needs to be completely resolved.
///
/// This type is a promise from the API that each named type in the schema has exactly one
/// unique definition and every named reference in the schema can be uniquely resolved to
/// one of these definitions.
///
/// `ResolvedSchema` wraps `Arc` references and is therefore cheap to clone.
///
/// # Construction
///
/// Use [`ResolvedSchema::resolve()`] to get a [`ResolvedSchemaBuilder`], or
/// [`ResolvedSchema::parse_str()`] for the single-schema convenience method.
///
/// ```rust,ignore
/// // Single schema:
/// let resolved = ResolvedSchema::parse_str(r#"{"type":"record", ...}"#)?;
///
/// // Multiple schemas with context:
/// let [a, b] = ResolvedSchema::resolve()
///     .additional(vec![context])?
///     .build_array([schema_a, schema_b])?;
/// ```
#[derive(Debug,Clone)]
pub struct ResolvedSchema{
    pub stub: Arc<Schema>,
    context: ResolvedContext
}

impl ResolvedSchema{

    /// Start building a schema resolution. Returns a [`ResolvedSchemaBuilder`].
    pub fn resolve() -> ResolvedSchemaBuilder {
        ResolvedSchemaBuilder::new()
    }

    pub fn unravel(&self) -> Schema{
        let complete : CompleteSchema = self.into();
        complete.0
    }

    pub fn new_empty() -> Self{
       Schema::Null.try_into().unwrap()
    }

    /// Parse and resolve a single JSON schema string. Convenience for
    /// `ResolvedSchema::resolve().build_one(string)`.
    pub fn parse_str(string: impl AsRef<str>) -> AvroResult<ResolvedSchema>{
        Self::resolve().build_one(string)
    }

    // ----- internal implementation methods -----

    fn resolve_symbols_array<const N : usize>(to_resolve: [SchemaWithSymbols; N], additional: Vec<SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<[ResolvedSchema; N]> {

        let mut all_schemata : Vec<SchemaWithSymbols> = to_resolve.clone().into();
        all_schemata.extend(additional);

        let contexts : Vec<AvroResult<ResolvedContext>> = to_resolve.iter().map(|schema|{
            ResolvedContext::needed_context_with_resolver(schema , &all_schemata, resolver)
        }).collect();

        let context_errs : Vec<_> = contexts.iter().filter_map(|context|{context.as_ref().err()}).collect();

        if !context_errs.is_empty(){
            let context_errs : Vec<_> = contexts.into_iter().filter_map(|context|{context.err()}).collect();
            return Err(Details::ResolvedSchemaCreationError(context_errs).into())
        }

        let mut to_resolve_iter = to_resolve.into_iter();
        let mut context_iter = contexts.into_iter().map(AvroResult::unwrap);
        Ok(std::array::from_fn(|_|{
            let with_symbol = to_resolve_iter.next().unwrap();
            let context = context_iter.next().unwrap();
            ResolvedSchema{
                stub: with_symbol.stub,
                context
            }
        }))
    }

    fn resolve_symbols_list(to_resolve: Vec<SchemaWithSymbols>, additional: Vec<SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<Vec<ResolvedSchema>> {

        let mut all_schemata = to_resolve.clone();
        all_schemata.extend(additional);
        let contexts : Vec<AvroResult<ResolvedContext>> = to_resolve.iter().map(|schema|{
            ResolvedContext::needed_context_with_resolver(schema , &all_schemata, resolver)
        }).collect();

        let context_errs : Vec<_> = contexts.iter().filter_map(|context|{context.as_ref().err()}).collect();

        if !context_errs.is_empty(){
            let context_errs : Vec<_> = contexts.into_iter().filter_map(|context|{context.err()}).collect();
            return Err(Details::ResolvedSchemaCreationError(context_errs).into())
        }

        let schema_and_context = to_resolve.into_iter().zip(contexts.into_iter());
        Ok(schema_and_context.into_iter().map(|(schema_with_symbols, context)|{ResolvedSchema{
                stub: schema_with_symbols.stub,
                context: context.unwrap()
            }}).collect())
    }



    pub fn get_names<'b>(&'b self) -> &'b NameMap{
        &self.context.0
    }

    pub fn get_schemata(&self) -> impl Iterator<Item = &Arc<Schema>>{
        self.context.0.values()
    }
}

#[derive(Clone, Debug)]
pub struct ResolvedArray<'a>{
    pub attributes: &'a BTreeMap<String, JsonValue>,
    items: &'a Schema,
    root: &'a ResolvedSchema,

    schema: &'a Schema,
}

#[derive(Clone, Debug)]
pub struct ResolvedMap<'a>{
    pub attributes: &'a BTreeMap<String, JsonValue>,
    types: &'a Schema,
    root: &'a ResolvedSchema,

    schema: &'a Schema
}

#[derive(Clone, Debug)]
pub struct ResolvedUnion<'a>{
    pub variant_index: &'a BTreeMap<SchemaKind, usize>,

    union_schema: &'a UnionSchema,
    schemas: &'a Vec<Schema>,
    root: &'a ResolvedSchema,

    schema: &'a Schema
}

#[derive(Clone,Debug)]
pub struct ResolvedRecord<'a>{
    pub name: &'a Arc<Name>,
    pub aliases: &'a Aliases,
    pub doc: &'a Documentation,
    pub lookup: &'a BTreeMap<String, usize>,
    pub attributes: &'a BTreeMap<String, JsonValue>,
    pub fields: Vec<ResolvedRecordField<'a>>,

    root: &'a ResolvedSchema,

    schema: &'a Schema,
}

#[derive(Clone,Debug)]
pub struct ResolvedRecordField<'a>{
    pub name: &'a String,
    pub doc: &'a Documentation,
    pub aliases: &'a Vec<String>,
    pub default: Option<crate::types::Value>,
    pub custom_attributes: &'a BTreeMap<String, JsonValue>,

    record_field: &'a RecordField,
    schema: &'a Schema,
    root: &'a ResolvedSchema
}

#[derive(Clone,Debug, Display)]
pub enum ResolvedNode<'a>{
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    BigDecimal,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    TimestampNanos,
    LocalTimestampMillis,
    LocalTimestampMicros,
    LocalTimestampNanos,
    Uuid(&'a UuidSchema),
    Duration(&'a FixedSchema),
    Enum(&'a EnumSchema),
    Fixed(&'a FixedSchema),
    Decimal(&'a DecimalSchema),
    Array(ResolvedArray<'a>),
    Map(ResolvedMap<'a>),
    Union(ResolvedUnion<'a>),
    Record(ResolvedRecord<'a>),
}

/// Represents a node inside a resolved schema.
/// This is can be used when traversing down a resolved schema tree as it couples
/// the root definintion information with a reference into the schema.
impl<'a> ResolvedNode<'a> {
   pub fn new(root: &'a ResolvedSchema)->ResolvedNode<'a>{
       let schema = root.stub.as_ref();
       Self::from_schema(schema, root)
   }

   fn from_schema(schema: &'a Schema, root: &'a ResolvedSchema) -> ResolvedNode<'a>{
       match schema {
        Schema::Map(MapSchema{attributes, types}) => ResolvedNode::Map(ResolvedMap{attributes, types, root, schema}),
        Schema::Union(union_schema) => ResolvedNode::Union(ResolvedUnion{union_schema, schemas: &union_schema.schemas, variant_index: &union_schema.variant_index, root,schema}),
        Schema::Array(ArraySchema { items, attributes }) => ResolvedNode::Array(ResolvedArray{items, attributes, root,schema}),
        Schema::Record(RecordSchema { name, aliases, doc, fields, lookup, attributes }) => {
            let fields = fields.iter()
                .map(|field|{
                    let RecordField {name, doc, aliases, default, schema, custom_attributes} = field;
                    let default_value = default.as_ref().and_then(|json_value|{Some(crate::types::Value::try_from(json_value.clone()).expect("unable to resolve defualt json value into value. This is an internal error and should never be reached."))});
                    ResolvedRecordField{name, doc, aliases, default: default_value, schema, custom_attributes, record_field: field, root}
                }).collect();
            ResolvedNode::Record(ResolvedRecord{ name, aliases, doc, fields, lookup, attributes, root, schema})
        },
        Schema::Ref{name} => Self::from_schema(root.context.0.get(name).unwrap().as_ref(), root),
        Schema::Null => ResolvedNode::Null,
        Schema::Boolean => ResolvedNode::Boolean,
        Schema::Int => ResolvedNode::Int,
        Schema::Long => ResolvedNode::Long,
        Schema::Float => ResolvedNode::Float,
        Schema::Double => ResolvedNode::Double,
        Schema::Bytes => ResolvedNode::Bytes,
        Schema::String => ResolvedNode::String,
        Schema::BigDecimal => ResolvedNode::BigDecimal,
        Schema::Uuid(uuid_schema) => ResolvedNode::Uuid(uuid_schema),
        Schema::Date => ResolvedNode::Date,
        Schema::TimeMillis => ResolvedNode::TimeMillis,
        Schema::TimeMicros => ResolvedNode::TimeMicros,
        Schema::TimestampMillis => ResolvedNode::TimestampMillis,
        Schema::TimestampMicros => ResolvedNode::TimestampMicros,
        Schema::TimestampNanos => ResolvedNode::TimestampNanos,
        Schema::LocalTimestampMillis => ResolvedNode::LocalTimestampMillis,
        Schema::LocalTimestampMicros => ResolvedNode::LocalTimestampMicros,
        Schema::LocalTimestampNanos => ResolvedNode::LocalTimestampNanos,
        Schema::Duration(fixed_schema) => ResolvedNode::Duration(fixed_schema),
        Schema::Enum(enum_schema) => ResolvedNode::Enum(enum_schema),
        Schema::Fixed(fixed_schema) => ResolvedNode::Fixed(fixed_schema),
        Schema::Decimal(decimal_schema) => ResolvedNode::Decimal(decimal_schema)
       }
   }

   /// For a given resolved node, recreates the ResolvedSchema context. KTODO, need better
   /// documentation
   pub fn get_resolved(&self) -> ResolvedSchema{
       match self {
        ResolvedNode::Map(resolved_map) => ResolvedSchema{stub: resolved_map.schema.clone().into(), context: resolved_map.root.context.clone()},
        ResolvedNode::Union(resolved_union) => ResolvedSchema{stub: resolved_union.schema.clone().into(), context: resolved_union.root.context.clone()},
        ResolvedNode::Array(resolved_array) => ResolvedSchema{stub: resolved_array.schema.clone().into(), context: resolved_array.root.context.clone()},
        ResolvedNode::Record(resolved_record) => ResolvedSchema{stub: resolved_record.schema.clone().into(), context: resolved_record.root.context.clone()},
        ResolvedNode::Uuid(uuid_schema) => ResolvedSchema{stub: Schema::Uuid((*uuid_schema).clone()).into(), context: ResolvedContext::empty()},
        ResolvedNode::Duration(fixed_schema) => ResolvedSchema{stub: Schema::Duration((*fixed_schema).clone()).into(), context: ResolvedContext::empty()},
        ResolvedNode::Enum(enum_schema) => ResolvedSchema{stub: Schema::Enum((*enum_schema).clone()).into(), context: ResolvedContext::empty()},
        ResolvedNode::Fixed(fixed_schema) => ResolvedSchema{stub: Schema::Fixed((*fixed_schema).clone()).into(), context: ResolvedContext::empty()},
        ResolvedNode::Decimal(decimal_schema) => ResolvedSchema{stub: Schema::Decimal((*decimal_schema).clone()).into(), context: ResolvedContext::empty()},
        ResolvedNode::Null => ResolvedSchema{stub: Schema::Null.into(), context: ResolvedContext::empty()},
        ResolvedNode::Boolean => ResolvedSchema{stub: Schema::Boolean.into(), context: ResolvedContext::empty()},
        ResolvedNode::Int => ResolvedSchema{stub: Schema::Int.into(), context: ResolvedContext::empty()},
        ResolvedNode::Long => ResolvedSchema{stub: Schema::Long.into(), context: ResolvedContext::empty()},
        ResolvedNode::Float => ResolvedSchema{stub: Schema::Float.into(), context: ResolvedContext::empty()},
        ResolvedNode::Double => ResolvedSchema{stub: Schema::Double.into(), context: ResolvedContext::empty()},
        ResolvedNode::Bytes => ResolvedSchema{stub: Schema::Bytes.into(), context: ResolvedContext::empty()},
        ResolvedNode::String => ResolvedSchema{stub: Schema::String.into(), context: ResolvedContext::empty()},
        ResolvedNode::BigDecimal => ResolvedSchema{stub: Schema::BigDecimal.into(), context: ResolvedContext::empty()},
        ResolvedNode::Date => ResolvedSchema{stub: Schema::Date.into(), context: ResolvedContext::empty()},
        ResolvedNode::TimeMillis => ResolvedSchema{stub: Schema::TimeMillis.into(), context: ResolvedContext::empty()},
        ResolvedNode::TimeMicros => ResolvedSchema{stub: Schema::TimeMicros.into(), context: ResolvedContext::empty()},
        ResolvedNode::TimestampMillis => ResolvedSchema{stub: Schema::TimestampMillis.into(), context: ResolvedContext::empty()},
        ResolvedNode::TimestampMicros => ResolvedSchema{stub: Schema::TimestampMicros.into(), context: ResolvedContext::empty()},
        ResolvedNode::TimestampNanos => ResolvedSchema{stub: Schema::TimestampNanos.into(), context: ResolvedContext::empty()},
        ResolvedNode::LocalTimestampMillis => ResolvedSchema{stub: Schema::LocalTimestampMillis.into(), context: ResolvedContext::empty()},
        ResolvedNode::LocalTimestampMicros => ResolvedSchema{stub: Schema::LocalTimestampMicros.into(), context: ResolvedContext::empty()},
        ResolvedNode::LocalTimestampNanos => ResolvedSchema{stub: Schema::LocalTimestampNanos.into(), context: ResolvedContext::empty()},
       }
   }
}

impl From<&ResolvedNode<'_>> for SchemaKind {
    fn from(value: &ResolvedNode) -> Self {
        match value {
            ResolvedNode::Null => Self::Null,
            ResolvedNode::Boolean => Self::Boolean,
            ResolvedNode::Int => Self::Int,
            ResolvedNode::Long => Self::Long,
            ResolvedNode::Float => Self::Float,
            ResolvedNode::Double => Self::Double,
            ResolvedNode::Bytes => Self::Bytes,
            ResolvedNode::String => Self::String,
            ResolvedNode::Array(_) => Self::Array,
            ResolvedNode::Map(_) => Self::Map,
            ResolvedNode::Union(_) => Self::Union,
            ResolvedNode::Record(_) => Self::Record,
            ResolvedNode::Enum(_) => Self::Enum,
            ResolvedNode::Fixed(_) => Self::Fixed,
            ResolvedNode::Decimal { .. } => Self::Decimal,
            ResolvedNode::BigDecimal => Self::BigDecimal,
            ResolvedNode::Uuid(_) => Self::Uuid,
            ResolvedNode::Date => Self::Date,
            ResolvedNode::TimeMillis => Self::TimeMillis,
            ResolvedNode::TimeMicros => Self::TimeMicros,
            ResolvedNode::TimestampMillis => Self::TimestampMillis,
            ResolvedNode::TimestampMicros => Self::TimestampMicros,
            ResolvedNode::TimestampNanos => Self::TimestampNanos,
            ResolvedNode::LocalTimestampMillis => Self::LocalTimestampMillis,
            ResolvedNode::LocalTimestampMicros => Self::LocalTimestampMicros,
            ResolvedNode::LocalTimestampNanos => Self::LocalTimestampNanos,
            ResolvedNode::Duration { .. } => Self::Duration,
        }
    }
}

impl<'a> ResolvedMap<'a>{
    pub fn resolve_types(&'a self)->ResolvedNode<'a>{
       ResolvedNode::from_schema(&self.types, self.root)
    }
}

impl<'a> ResolvedUnion<'a>{
    pub fn resolve_schemas(&self)->Vec<ResolvedNode<'a>>{
        self.schemas.iter().map(|schema|{
            ResolvedNode::from_schema(schema, self.root)
        }).collect()
    }

    /// For getting the original schema for nice error printing
    /// Other than that, use should be avoided.
    pub(crate) fn get_union_schema(&self) -> &'a UnionSchema{
        self.union_schema
    }

    pub fn structural_match_on_schema(&'a self, value: &types::Value) -> Option<(usize,ResolvedNode<'a>)>{
        let value_schema_kind = SchemaKind::from(value);
        let resolved_nodes = self.resolve_schemas();
        if let Some(i) = self.get_variant_index(&value_schema_kind) {
            // fast path KTODO double check this!
            let variant_clone = resolved_nodes.get(i).unwrap().clone();
            if let Ok(_) = value
                .clone()
                .resolve_internal(variant_clone.clone()){
                Some((i, variant_clone))
            }else{
                None
            }
        } else {
            // slow path (required for matching logical or named types)
            self.resolve_schemas().into_iter().enumerate().find(|(_, resolved_node)| {
                value
                    .clone()
                    .resolve_internal(resolved_node.clone())
                    .is_ok()
            })
        }
    }

    pub fn get_variant_index(&self, schema_kind: &SchemaKind) -> Option<usize>{
       self.variant_index.get(schema_kind).copied()
    }
}

impl<'a> ResolvedArray<'a>{
    pub fn resolve_items(&self)->ResolvedNode<'a>{
        ResolvedNode::from_schema(&self.items, self.root)
    }
}

impl<'a> ResolvedRecordField<'a>{
    pub fn resolve_field(&self)->ResolvedNode<'a>{
        ResolvedNode::from_schema(&self.schema, self.root)
    }

    // delegate to implementation on Schema
    pub fn is_nullable(&self) -> bool {
        self.record_field.is_nullable()
    }
}

/// this is a schema object that is "self contained" in that it contains all named definitions
/// needed to encode/decode form this schema.
#[derive(Debug)]
pub struct CompleteSchema(Schema);

impl CompleteSchema{
    /// Returns the [Parsing Canonical Form] of `self` that is self contained (not dependent on
    /// any definitions in `schemata`)
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas
    pub fn independent_canonical_form(&self) -> Result<String, Error> { // TODO: I think this will
                                                                        // never panic..
        Ok(self.0.canonical_form())
    }
}

impl From<&ResolvedSchema> for CompleteSchema{
    fn from(value: &ResolvedSchema) -> Self {
        let mut stub = value.stub.as_ref().clone();
        unravel_inner(&mut stub, &value.context.0, &mut HashSet::new(),  &mut HashMap::new());
        CompleteSchema(stub)
    }
}

impl TryFrom<Schema> for CompleteSchema{
    type Error = Error;

    fn try_from(value: Schema) -> Result<Self, Error> {
        let resolved : ResolvedSchema = value.try_into()?;
        return Ok((&resolved).into())
    }
}

impl Serialize for CompleteSchema{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
                self.0.serialize(serializer)
    }
}

impl PartialEq<CompleteSchema> for CompleteSchema{
   fn eq(&self, other: &CompleteSchema) -> bool {
        self.0 == other.0
    }
}

impl Serialize for ResolvedSchema{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
       let complete_schema = CompleteSchema::from(self);
       complete_schema.serialize(serializer)
    }
}

impl PartialEq for ResolvedSchema{
    fn eq(&self, other: &Self) -> bool {
        compare_schema_and_context(&self.stub, &other.stub, &self.context.0, &other.context.0, &HashSet::new(), &HashSet::new())
    }
}

/// technically this is a tighter bound that we need, as we may have
/// iterated far enough down that we don't need to check the entire conext definitions.
impl PartialEq for ResolvedNode<'_>{
    fn eq(&self, other: &Self) -> bool {
        let resolved_self = self.get_resolved();
        let resolved_other = other.get_resolved();
        return resolved_other.unravel() == resolved_self.unravel()
    }
}

impl PartialEq for SchemaWithSymbols{
    fn eq(&self, other: &Self) -> bool {
        compare_schema_and_context(&self.stub, &other.stub, &self.defined_names, &other.defined_names, &self.referenced_names, &other.referenced_names)
    }
}

impl TryFrom<SchemaWithSymbols> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: SchemaWithSymbols) -> AvroResult<Self> {
        ResolvedSchema::resolve().build_one(schema)
    }
}

impl TryFrom<&SchemaWithSymbols> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: &SchemaWithSymbols) -> AvroResult<Self> {
        ResolvedSchema::resolve().build_one(schema.clone())
    }
}

impl TryFrom<Schema> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: Schema) -> AvroResult<Self> {
        let with_symbols: SchemaWithSymbols = schema.into();
        ResolvedSchema::resolve().build_one(with_symbols)
    }
}

/// NOTE: this will copy the schema, and is therefore not the most performant option
impl TryFrom<&Schema> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: &Schema) -> AvroResult<Self> {
        ResolvedSchema::resolve().build_one(schema)
    }
}

//impl<'a> TryFrom<UnionSchema> for ResolvedUnion<'a>{ // KTODO: is this something I should do??
//    type Error = Error;
//
//    fn try_from(value: UnionSchema) -> Result<Self, Self::Error> {
//        let [resolved] = ResolvedSchema::from_raw_schema_array([&Schema::Union(value)], vec![])?;
//        let node = ResolvedNode::new(&resolved);
//        match node {
//            ResolvedNode::Union(resolved_union) => {resolved_union},
//            _ => unreachable!()
//        }
//    }
//}

/// trait for implementing a custom schema name resolver. For instance this
/// could be used to create resolvers that lookup schema names
/// from a shcema registry.
pub trait Resolver{
    fn find_schema(&mut self, name: &Arc<Name>) -> Result<SchemaWithSymbols, String>;
}

pub struct DefaultResolver{}
impl Resolver for DefaultResolver{
    fn find_schema(&mut self, _name: &Arc<Name>) -> Result<SchemaWithSymbols, String> {
       Err(String::from("Definition not found, no custom resolver was given for ResolutionContext"))
    }
}

impl DefaultResolver{
    pub fn new()->Self{
        DefaultResolver{}
    }
}

/// compares the given two schema inside the context AND ALSO compares the contexts themselves. That
/// is, ensures the two contexts are identical
fn compare_schema_and_context(schema_one: &Schema, schema_two: &Schema, context_one: &NameMap, context_two: &NameMap, dangling_one: &NameSet, dangling_two: &NameSet) -> bool{
    if dangling_one != dangling_two{
        return false;
    }

    // verify the provided definitions are the same:
    let one_contains_two = context_one.keys().fold(true, |acc, val|{acc && context_two.contains_key(val)});
    let two_contains_one = context_two.keys().fold(true, |acc, val|{acc && context_one.contains_key(val)});

    if !(one_contains_two && two_contains_one){
        return false
    }

    // we now know that the two contexts claim to define the same set of names, lets verify

    if !context_one.iter().fold(true, |acc, (name, schema)|{acc &&
        schema.as_ref() == context_two.get(name).unwrap().as_ref()}) {
        return false
    }

    schema_one == schema_two

}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Arc;

    use super::{Resolver, ResolvedSchema};
    use crate::{
        Schema,
        schema::{Alias, CompleteSchema, Name, RecordField, RecordSchema, ResolvedNode, SchemaWithSymbols, UnionSchema},
    };
    use apache_avro_test_helper::TestResult;
    use log::RecordBuilder;

    // ---------- custom resolver infrastructure for tests ----------

    /// A simple resolver backed by a HashMap of JSON schema strings keyed by fullname.
    struct MapResolver {
        registry: HashMap<String, String>,
        call_log: Vec<String>,
    }

    impl MapResolver {
        fn new(entries: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self {
            MapResolver {
                registry: entries.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
                call_log: Vec::new(),
            }
        }

        fn calls(&self) -> &[String] {
            &self.call_log
        }
    }

    impl Resolver for MapResolver {
        fn find_schema(&mut self, name: &Arc<Name>) -> Result<SchemaWithSymbols, String> {
            let fullname = name.fullname(None);
            self.call_log.push(fullname.clone());
            match self.registry.get(&fullname) {
                Some(json) => SchemaWithSymbols::parse_str(json)
                    .map_err(|e| format!("parse error: {e}")),
                None => Err(format!("not found: {fullname}")),
            }
        }
    }

    // ---------- custom resolver tests ----------

    #[test]
    fn custom_resolver_resolves_missing_record() -> TestResult {
        // A record that references "ext.Address" which is not provided locally.
        let person = r#"{
            "type": "record",
            "name": "Person",
            "namespace": "app",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "address", "type": "ext.Address"}
            ]
        }"#;

        let address = r#"{
            "type": "record",
            "name": "Address",
            "namespace": "ext",
            "fields": [
                {"name": "street", "type": "string"},
                {"name": "city", "type": "string"}
            ]
        }"#;

        let mut resolver = MapResolver::new([("ext.Address", address)]);
        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([person], &mut resolver)?;

        // The resolver was called for ext.Address
        assert!(resolver.calls().contains(&"ext.Address".to_string()));

        // The context should contain both Person and Address
        assert!(rs.get_names().contains_key(&Name::new("app.Person")?));
        assert!(rs.get_names().contains_key(&Name::new("ext.Address")?));
        assert_eq!(rs.get_names().len(), 2);

        Ok(())
    }

    #[test]
    fn custom_resolver_resolves_missing_enum() -> TestResult {
        let order = r#"{
            "type": "record",
            "name": "Order",
            "namespace": "shop",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "status", "type": "shop.OrderStatus"}
            ]
        }"#;

        let status_enum = r#"{
            "type": "enum",
            "name": "OrderStatus",
            "namespace": "shop",
            "symbols": ["PENDING", "SHIPPED", "DELIVERED"]
        }"#;

        let mut resolver = MapResolver::new([("shop.OrderStatus", status_enum)]);
        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([order], &mut resolver)?;

        assert!(resolver.calls().contains(&"shop.OrderStatus".to_string()));
        assert!(rs.get_names().contains_key(&Name::new("shop.Order")?));
        assert!(rs.get_names().contains_key(&Name::new("shop.OrderStatus")?));

        Ok(())
    }

    #[test]
    fn custom_resolver_resolves_missing_fixed() -> TestResult {
        let msg = r#"{
            "type": "record",
            "name": "Message",
            "namespace": "proto",
            "fields": [
                {"name": "payload", "type": "string"},
                {"name": "checksum", "type": "proto.Checksum"}
            ]
        }"#;

        let checksum_fixed = r#"{
            "type": "fixed",
            "name": "Checksum",
            "namespace": "proto",
            "size": 16
        }"#;

        let mut resolver = MapResolver::new([("proto.Checksum", checksum_fixed)]);
        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([msg], &mut resolver)?;

        assert!(resolver.calls().contains(&"proto.Checksum".to_string()));
        assert!(rs.get_names().contains_key(&Name::new("proto.Message")?));
        assert!(rs.get_names().contains_key(&Name::new("proto.Checksum")?));

        Ok(())
    }

    #[test]
    fn custom_resolver_not_called_when_locally_resolved() -> TestResult {
        // Both schemas provided locally — resolver should never be called.
        let person = r#"{
            "type": "record",
            "name": "Person",
            "namespace": "app",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "address", "type": "app.Address"}
            ]
        }"#;

        let address = r#"{
            "type": "record",
            "name": "Address",
            "namespace": "app",
            "fields": [
                {"name": "street", "type": "string"}
            ]
        }"#;

        let mut resolver = MapResolver::new(Vec::<(&str, &str)>::new());
        let [rs] = ResolvedSchema::resolve().additional(vec![address])?.build_array_with_resolver([person], &mut resolver)?;

        assert!(resolver.calls().is_empty(), "resolver should not be called when all names resolve locally");
        assert_eq!(rs.get_names().len(), 2);

        Ok(())
    }

    #[test]
    fn custom_resolver_transitive_resolution() -> TestResult {
        // A references B, B references C. Only A is provided; B and C come from the resolver.
        let schema_a = r#"{
            "type": "record",
            "name": "A",
            "namespace": "chain",
            "fields": [{"name": "b", "type": "chain.B"}]
        }"#;

        let schema_b = r#"{
            "type": "record",
            "name": "B",
            "namespace": "chain",
            "fields": [{"name": "c", "type": "chain.C"}]
        }"#;

        let schema_c = r#"{
            "type": "record",
            "name": "C",
            "namespace": "chain",
            "fields": [{"name": "value", "type": "int"}]
        }"#;

        let mut resolver = MapResolver::new([
            ("chain.B", schema_b),
            ("chain.C", schema_c),
        ]);

        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([schema_a], &mut resolver)?;

        // Both B and C should have been requested
        assert!(resolver.calls().contains(&"chain.B".to_string()));
        assert!(resolver.calls().contains(&"chain.C".to_string()));

        // All three should be in the context
        assert!(rs.get_names().contains_key(&Name::new("chain.A")?));
        assert!(rs.get_names().contains_key(&Name::new("chain.B")?));
        assert!(rs.get_names().contains_key(&Name::new("chain.C")?));
        assert_eq!(rs.get_names().len(), 3);

        Ok(())
    }

    #[test]
    fn custom_resolver_error_propagates() -> TestResult {
        // A references a name that the resolver cannot provide.
        let schema_a = r#"{
            "type": "record",
            "name": "A",
            "namespace": "ns",
            "fields": [{"name": "x", "type": "ns.Missing"}]
        }"#;

        let mut resolver = MapResolver::new(Vec::<(&str, &str)>::new());
        let result = ResolvedSchema::resolve().build_array_with_resolver([schema_a], &mut resolver);

        assert!(result.is_err(), "should fail when resolver cannot find the schema");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("ns.Missing"), "error should mention the missing name, got: {err_msg}");

        Ok(())
    }

    #[test]
    fn custom_resolver_mismatch_error() -> TestResult {
        // The resolver returns a schema that defines "wrong.Name" when asked for "ns.Expected".
        let schema_a = r#"{
            "type": "record",
            "name": "A",
            "namespace": "ns",
            "fields": [{"name": "x", "type": "ns.Expected"}]
        }"#;

        let wrong_schema = r#"{
            "type": "record",
            "name": "WrongName",
            "namespace": "wrong",
            "fields": [{"name": "y", "type": "int"}]
        }"#;

        let mut resolver = MapResolver::new([("ns.Expected", wrong_schema)]);
        let result = ResolvedSchema::resolve().build_array_with_resolver([schema_a], &mut resolver);

        assert!(result.is_err(), "should fail when resolver returns a schema that doesn't define the requested name");

        Ok(())
    }

    #[test]
    fn custom_resolver_with_multiple_schemas_to_resolve() -> TestResult {
        // Two schemas that each reference an external type.
        let schema_a = r#"{
            "type": "record",
            "name": "A",
            "namespace": "ns",
            "fields": [{"name": "shared", "type": "ext.Shared"}]
        }"#;

        let schema_b = r#"{
            "type": "record",
            "name": "B",
            "namespace": "ns",
            "fields": [{"name": "shared", "type": "ext.Shared"}]
        }"#;

        let shared = r#"{
            "type": "record",
            "name": "Shared",
            "namespace": "ext",
            "fields": [{"name": "value", "type": "string"}]
        }"#;

        let mut resolver = MapResolver::new([("ext.Shared", shared)]);
        let resolved = ResolvedSchema::resolve().build_list_with_resolver(
            vec![schema_a, schema_b],
            &mut resolver,
        )?;

        assert_eq!(resolved.len(), 2);
        // Both resolved schemas should have ext.Shared in their context
        for rs in &resolved {
            assert!(rs.get_names().contains_key(&Name::new("ext.Shared")?));
        }

        Ok(())
    }

    #[test]
    fn custom_resolver_mixed_local_and_external() -> TestResult {
        // A references B (provided locally) and C (provided by resolver).
        let schema_a = r#"{
            "type": "record",
            "name": "A",
            "namespace": "ns",
            "fields": [
                {"name": "b", "type": "ns.B"},
                {"name": "c", "type": "ext.C"}
            ]
        }"#;

        let schema_b = r#"{
            "type": "record",
            "name": "B",
            "namespace": "ns",
            "fields": [{"name": "val", "type": "int"}]
        }"#;

        let schema_c = r#"{
            "type": "record",
            "name": "C",
            "namespace": "ext",
            "fields": [{"name": "val", "type": "long"}]
        }"#;

        let mut resolver = MapResolver::new([("ext.C", schema_c)]);
        let [rs] = ResolvedSchema::resolve().additional(vec![schema_b])?.build_array_with_resolver([schema_a], &mut resolver)?;

        // B resolved locally, C externally
        assert!(rs.get_names().contains_key(&Name::new("ns.A")?));
        assert!(rs.get_names().contains_key(&Name::new("ns.B")?));
        assert!(rs.get_names().contains_key(&Name::new("ext.C")?));
        assert_eq!(rs.get_names().len(), 3);

        // Only C should have triggered a resolver call
        assert_eq!(resolver.calls().len(), 1);
        assert_eq!(resolver.calls()[0], "ext.C");

        Ok(())
    }

    #[test]
    fn custom_resolver_resolved_schema_resolves_node_correctly() -> TestResult {
        // Verify that a schema resolved via custom resolver actually works for
        // navigating the schema tree through ResolvedNode.
        let wrapper = r#"{
            "type": "record",
            "name": "Wrapper",
            "namespace": "ns",
            "fields": [
                {"name": "inner", "type": "ext.Inner"}
            ]
        }"#;

        let inner = r#"{
            "type": "record",
            "name": "Inner",
            "namespace": "ext",
            "fields": [
                {"name": "x", "type": "int"},
                {"name": "y", "type": "string"}
            ]
        }"#;

        let mut resolver = MapResolver::new([("ext.Inner", inner)]);
        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([wrapper], &mut resolver)?;

        // Walk the resolved schema tree
        let node = ResolvedNode::new(&rs);
        match node {
            ResolvedNode::Record(record) => {
                assert_eq!(record.fields.len(), 1);
                let inner_node = record.fields[0].resolve_field();
                match inner_node {
                    ResolvedNode::Record(inner_record) => {
                        assert_eq!(inner_record.fields.len(), 2);
                        assert_eq!(inner_record.fields[0].name, "x");
                        assert_eq!(inner_record.fields[1].name, "y");
                    },
                    other => panic!("expected Record for inner, got {other}"),
                }
            },
            other => panic!("expected Record for wrapper, got {other}"),
        }

        Ok(())
    }

    #[test]
    fn custom_resolver_in_union_field() -> TestResult {
        // A field has a union type where one variant comes from the resolver.
        let record = r#"{
            "type": "record",
            "name": "Event",
            "namespace": "ns",
            "fields": [
                {"name": "payload", "type": ["null", "ext.Payload"]}
            ]
        }"#;

        let payload = r#"{
            "type": "record",
            "name": "Payload",
            "namespace": "ext",
            "fields": [{"name": "data", "type": "bytes"}]
        }"#;

        let mut resolver = MapResolver::new([("ext.Payload", payload)]);
        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([record], &mut resolver)?;

        assert!(rs.get_names().contains_key(&Name::new("ext.Payload")?));

        // Walk the tree: Event -> payload field -> union -> second variant should be Record
        let node = ResolvedNode::new(&rs);
        match node {
            ResolvedNode::Record(rec) => {
                let field_node = rec.fields[0].resolve_field();
                match field_node {
                    ResolvedNode::Union(union) => {
                        let variants = union.resolve_schemas();
                        assert_eq!(variants.len(), 2);
                        assert!(matches!(variants[0], ResolvedNode::Null));
                        assert!(matches!(variants[1], ResolvedNode::Record(_)));
                    },
                    other => panic!("expected Union, got {other}"),
                }
            },
            other => panic!("expected Record, got {other}"),
        }

        Ok(())
    }

    #[test]
    fn custom_resolver_in_array_items() -> TestResult {
        // An array whose items type comes from the resolver.
        let record = r#"{
            "type": "record",
            "name": "Container",
            "namespace": "ns",
            "fields": [
                {"name": "items", "type": {"type": "array", "items": "ext.Item"}}
            ]
        }"#;

        let item = r#"{
            "type": "record",
            "name": "Item",
            "namespace": "ext",
            "fields": [{"name": "label", "type": "string"}]
        }"#;

        let mut resolver = MapResolver::new([("ext.Item", item)]);
        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([record], &mut resolver)?;

        assert!(rs.get_names().contains_key(&Name::new("ext.Item")?));

        let node = ResolvedNode::new(&rs);
        match node {
            ResolvedNode::Record(rec) => {
                let field_node = rec.fields[0].resolve_field();
                match field_node {
                    ResolvedNode::Array(arr) => {
                        let items_node = arr.resolve_items();
                        assert!(matches!(items_node, ResolvedNode::Record(_)));
                    },
                    other => panic!("expected Array, got {other}"),
                }
            },
            other => panic!("expected Record, got {other}"),
        }

        Ok(())
    }

    #[test]
    fn custom_resolver_in_map_values() -> TestResult {
        // A map whose value type comes from the resolver.
        let record = r#"{
            "type": "record",
            "name": "Registry",
            "namespace": "ns",
            "fields": [
                {"name": "entries", "type": {"type": "map", "values": "ext.Entry"}}
            ]
        }"#;

        let entry = r#"{
            "type": "record",
            "name": "Entry",
            "namespace": "ext",
            "fields": [{"name": "value", "type": "double"}]
        }"#;

        let mut resolver = MapResolver::new([("ext.Entry", entry)]);
        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([record], &mut resolver)?;

        assert!(rs.get_names().contains_key(&Name::new("ext.Entry")?));

        let node = ResolvedNode::new(&rs);
        match node {
            ResolvedNode::Record(rec) => {
                let field_node = rec.fields[0].resolve_field();
                match field_node {
                    ResolvedNode::Map(map) => {
                        let values_node = map.resolve_types();
                        assert!(matches!(values_node, ResolvedNode::Record(_)));
                    },
                    other => panic!("expected Map, got {other}"),
                }
            },
            other => panic!("expected Record, got {other}"),
        }

        Ok(())
    }

    #[test]
    fn custom_resolver_resolved_schema_equality() -> TestResult {
        // A schema resolved via custom resolver should equal the same schema resolved locally.
        let main = r#"{
            "type": "record",
            "name": "Main",
            "namespace": "ns",
            "fields": [{"name": "dep", "type": "ns.Dep"}]
        }"#;

        let dep = r#"{
            "type": "record",
            "name": "Dep",
            "namespace": "ns",
            "fields": [{"name": "v", "type": "int"}]
        }"#;

        // Resolve via custom resolver
        let mut resolver = MapResolver::new([("ns.Dep", dep)]);
        let [via_resolver] = ResolvedSchema::resolve().build_array_with_resolver([main], &mut resolver)?;

        // Resolve by providing dep locally
        let [via_local] = ResolvedSchema::resolve().additional(vec![dep])?.build_array([main])?;

        assert_eq!(via_resolver, via_local);

        Ok(())
    }

    #[test]
    fn custom_resolver_serialization_roundtrip() -> TestResult {
        // A schema resolved via custom resolver should serialize correctly.
        let main = r#"{
            "type": "record",
            "name": "ns.Main",
            "fields": [{"name": "dep", "type": "ns.Dep"}]
        }"#;

        let dep = r#"{
            "type": "record",
            "name": "ns.Dep",
            "fields": [{"name": "x", "type": "int"}]
        }"#;

        let mut resolver = MapResolver::new([("ns.Dep", dep)]);
        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([main], &mut resolver)?;

        // Serialize and re-parse: the resulting schema should be self-contained.
        let json = serde_json::to_string(&rs)?;
        let reparsed = ResolvedSchema::parse_str(&json)?;
        assert_eq!(rs, reparsed);

        Ok(())
    }

    #[test]
    fn custom_resolver_resolver_returns_schema_with_extra_definitions() -> TestResult {
        // The resolver returns a schema that defines the requested name plus additional
        // names (e.g. a record with an inline enum). Those extra names should also be
        // available in the context.
        let main = r#"{
            "type": "record",
            "name": "Main",
            "namespace": "ns",
            "fields": [{"name": "detail", "type": "ext.Detail"}]
        }"#;

        // Detail defines an inline enum ext.Status
        let detail = r#"{
            "type": "record",
            "name": "Detail",
            "namespace": "ext",
            "fields": [
                {"name": "status", "type": {
                    "type": "enum",
                    "name": "Status",
                    "namespace": "ext",
                    "symbols": ["OK", "FAIL"]
                }}
            ]
        }"#;

        let mut resolver = MapResolver::new([("ext.Detail", detail)]);
        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([main], &mut resolver)?;

        assert!(rs.get_names().contains_key(&Name::new("ns.Main")?));
        assert!(rs.get_names().contains_key(&Name::new("ext.Detail")?));
        assert!(rs.get_names().contains_key(&Name::new("ext.Status")?));

        Ok(())
    }

    #[test]
    fn custom_resolver_deep_transitive_chain() -> TestResult {
        // A -> B -> C -> D, all resolved externally.
        let schema_a = r#"{
            "type": "record", "name": "deep.A",
            "fields": [{"name": "b", "type": "deep.B"}]
        }"#;
        let schema_b = r#"{
            "type": "record", "name": "deep.B",
            "fields": [{"name": "c", "type": "deep.C"}]
        }"#;
        let schema_c = r#"{
            "type": "record", "name": "deep.C",
            "fields": [{"name": "d", "type": "deep.D"}]
        }"#;
        let schema_d = r#"{
            "type": "record", "name": "deep.D",
            "fields": [{"name": "val", "type": "int"}]
        }"#;

        let mut resolver = MapResolver::new([
            ("deep.B", schema_b),
            ("deep.C", schema_c),
            ("deep.D", schema_d),
        ]);

        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([schema_a], &mut resolver)?;

        for name in ["deep.A", "deep.B", "deep.C", "deep.D"] {
            assert!(rs.get_names().contains_key(&Name::new(name)?), "missing {name}");
        }
        assert_eq!(rs.get_names().len(), 4);

        // Walk all the way down
        let node = ResolvedNode::new(&rs);
        let ResolvedNode::Record(a) = node else { panic!("expected Record") };
        let ResolvedNode::Record(b) = a.fields[0].resolve_field() else { panic!("expected Record") };
        let ResolvedNode::Record(c) = b.fields[0].resolve_field() else { panic!("expected Record") };
        let ResolvedNode::Record(d) = c.fields[0].resolve_field() else { panic!("expected Record") };
        let val = d.fields[0].resolve_field();
        assert!(matches!(val, ResolvedNode::Int));

        Ok(())
    }

    #[test]
    fn custom_resolver_with_schema_with_symbols_api() -> TestResult {
        // Test using the SchemaWithSymbols-level API with a custom resolver.
        let main = r#"{
            "type": "record",
            "name": "ns.Main",
            "fields": [{"name": "ref_field", "type": "ns.External"}]
        }"#;

        let external = r#"{
            "type": "enum",
            "name": "ns.External",
            "symbols": ["X", "Y", "Z"]
        }"#;

        let main_sws = SchemaWithSymbols::parse_str(main)?;

        let mut resolver = MapResolver::new([("ns.External", external)]);
        let [rs] = ResolvedSchema::resolve().build_array_with_resolver(
            [main_sws],
            &mut resolver,
        )?;

        assert!(rs.get_names().contains_key(&Name::new("ns.Main")?));
        assert!(rs.get_names().contains_key(&Name::new("ns.External")?));

        // Verify the field resolves to an Enum
        let ResolvedNode::Record(rec) = ResolvedNode::new(&rs) else { panic!("expected Record") };
        assert!(matches!(rec.fields[0].resolve_field(), ResolvedNode::Enum(_)));

        Ok(())
    }

    #[test]
    fn custom_resolver_unravel_produces_self_contained_schema() -> TestResult {
        // After resolving via custom resolver, unravel() should produce a fully
        // self-contained schema with no Schema::Ref nodes.
        let main = r#"{
            "type": "record",
            "name": "ns.Root",
            "fields": [
                {"name": "tag", "type": "ns.Tag"},
                {"name": "meta", "type": "ns.Meta"}
            ]
        }"#;
        let tag = r#"{
            "type": "enum", "name": "ns.Tag",
            "symbols": ["ALPHA", "BETA"]
        }"#;
        let meta = r#"{
            "type": "fixed", "name": "ns.Meta", "size": 8
        }"#;

        let mut resolver = MapResolver::new([
            ("ns.Tag", tag),
            ("ns.Meta", meta),
        ]);
        let [rs] = ResolvedSchema::resolve().build_array_with_resolver([main], &mut resolver)?;

        let unraveled = rs.unravel();

        // The unraveled schema should be a Record with inline definitions, no Ref nodes
        match &unraveled {
            Schema::Record(rec) => {
                assert_eq!(rec.fields.len(), 2);
                assert!(matches!(&rec.fields[0].schema, Schema::Enum(_)));
                assert!(matches!(&rec.fields[1].schema, Schema::Fixed(_)));
            },
            other => panic!("expected Record, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"inner_record_name",
                            "fields":[
                                {
                                    "name":"inner_field_1",
                                    "type":"double"
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_qualified_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"inner_record_name",
                            "fields":[
                                {
                                    "name":"inner_field_1",
                                    "type":"double"
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_enum_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_qualified_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_enum_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_fixed_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_qualified_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_fixed_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"inner_record_name",
                            "namespace":"inner_space",
                            "fields":[
                                {
                                    "name":"inner_field_1",
                                    "type":"double"
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "namespace": "inner_space",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_enum_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "namespace": "inner_space",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_fixed_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_outer_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"middle_record_name",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 3);
        for s in [
            "space.record_name",
            "space.middle_record_name",
            "space.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_middle_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "middle_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 3);
        for s in [
            "space.record_name",
            "middle_namespace.middle_record_name",
            "middle_namespace.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "namespace":"inner_namespace",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 3);
        for s in [
            "space.record_name",
            "middle_namespace.middle_record_name",
            "inner_namespace.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_in_array_resolution_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": {
                  "type":"array",
                  "items":{
                      "type":"record",
                      "name":"in_array_record",
                      "fields": [
                          {
                              "name":"array_record_field",
                              "type":"string"
                          }
                      ]
                  }
              }
            },
            {
                "name":"outer_field_2",
                "type":"in_array_record"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.in_array_record"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_in_map_resolution_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": {
                  "type":"map",
                  "values":{
                      "type":"record",
                      "name":"in_map_record",
                      "fields": [
                          {
                              "name":"map_record_field",
                              "type":"string"
                          }
                      ]
                  }
              }
            },
            {
                "name":"outer_field_2",
                "type":"in_map_record"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.in_map_record"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3466_test_to_json_inner_enum_inner_namespace() -> TestResult {
        let schema = r#"
        {
        "name": "record_name",
        "namespace": "space",
        "type": "record",
        "fields": [
            {
            "name": "outer_field_1",
            "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "namespace": "inner_space",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_enum_name"
            }
        ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;

        // confirm we have expected 2 full-names
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        // convert Schema back to JSON string
        let schema_str = serde_json::to_string(&schema)?;
        let _schema = Schema::parse_str(&schema_str)?;
        assert_eq!(schema, _schema);

        Ok(())
    }

    #[test]
    fn avro_3466_test_to_json_inner_fixed_inner_namespace() -> TestResult {
        let schema = r#"
        {
        "name": "record_name",
        "namespace": "space",
        "type": "record",
        "fields": [
            {
            "name": "outer_field_1",
            "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "namespace": "inner_space",
                            "size":54
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_fixed_name"
            }
        ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let [rs] = ResolvedSchema::resolve().build_array([&schema])?;

        // confirm we have expected 2 full-names
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        // convert Schema back to JSON string
        let schema_str = serde_json::to_string(&schema)?;
        let _schema = Schema::parse_str(&schema_str)?;
        assert_eq!(schema, _schema);

        Ok(())
    }

    #[test]
    fn avro_rs_339_schema_ref_uuid() -> TestResult {
        let schema = r#"{
            "name": "foo",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "uuid",
                        "name": "bar"
                    }
                },
                {
                    "name": "b",
                    "type": "bar"
                }
            ]
        }"#;
        let _resolved = ResolvedSchema::resolve().build_array([&schema])?;

        Ok(())
    }

    #[test]
    fn avro_rs_339_schema_ref_decimal() -> TestResult {
        let schema = r#"{
            "name": "foo",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "decimal",
                        "precision": 4,
                        "scale": 2,
                        "name": "bar"
                    }
                },
                {
                    "name": "b",
                    "type": "bar"
                }
            ]
        }"#;
        let _resolved = ResolvedSchema::resolve().build_array([&schema])?;

        Ok(())
    }

    #[test]
    fn avro_rs_444_do_not_allow_duplicate_names_in_known_schemata() -> TestResult {
        let schema = r#"{
            "name": "foo",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "decimal",
                        "precision": 4,
                        "scale": 2,
                        "name": "bar"
                    }
                },
                {
                    "name": "b",
                    "type": "bar"
                },
                {
                    "name": "c",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "uuid",
                        "name": "duplicated_name"
                    }
                }
            ]
        }"#;

        let other_schema = r#"{
                "name": "duplicated_name",
                "type": "enum",
                "symbols": ["A", "B", "C"]

        }"#;

        let ambig_schema = r#""duplicated_name""#;

        let result = ResolvedSchema::resolve().additional(vec![&schema, &other_schema])?.build_array([&ambig_schema])
            .unwrap_err();

        assert!(
            result.to_string().contains("Schamata with fullnames: [\"duplicated_name\"], already have definitions in this context.")
        );

        Ok(())
    }

    #[test]
    fn allow_identical_definitions() -> TestResult{
        let schema1 = r#"
        {
            "name": "someRecord",
            "type": "record",
            "fields": [
            {
                "name": "field1",
                "type": {
                    "name": "duplicateEnum",
                    "type": "enum",
                    "symbols":["A","B","C"]
                }
            }
            ]
        }
            "#;

        let schema2 = r#"
        {
            "name": "someOtherRecord",
            "type": "record",
            "fields": [
            {
                "name": "otherField1",
                "type": {
                    "name": "duplicateEnum",
                    "type": "enum",
                    "symbols":["A","B","C"]
                }
            }
            ]
        }
            "#;

        let _resolved = ResolvedSchema::resolve().build_array([schema1, schema2])?;

        Ok(())
    }

    #[test]
    fn dont_allow_close_but_not_identical_definitions() -> TestResult{
        let union = r#"[someRecord, someOtherRecord]"#;

        let schema1 = r#"
        {
            "name": "someRecord",
            "type": "record",
            "fields": [
            {
                "name": "field1",
                "type": {
                    "name": "duplicateEnum",
                    "type": "enum",
                    "symbols":["A","B","C"]
                }
            }
            ]
        }
            "#;

        let schema2 = r#"
        {
            "name": "someOtherRecord",
            "type": "record",
            "fields": [
            {
                "name": "otherField1",
                "type": {
                    "name": "duplicateEnum",
                    "type": "enum",
                    "symbols":["A","B"]
                }
            }
            ]
        }
            "#;

        assert_eq!(
            ResolvedSchema::resolve().additional([schema1, schema2])?.build_array([union]).is_err(),
            true);

        Ok(())

    }

    #[test]
    fn unravel_is_alias_aware() -> TestResult{
        let complete = CompleteSchema::try_from(&ResolvedSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "LongList",
              "aliases": ["LinkedLongs"],
              "fields" : [
                {"name": "value", "type": "long"},
                {"name": "next", "type": ["null", "LinkedLongs"]}
              ]
            }
        "#,
        )?)?;

        let mut lookup = BTreeMap::new();
        lookup.insert("value".to_owned(), 0);
        lookup.insert("next".to_owned(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name::new("LongList")?.into(),
            aliases: Some(vec![Alias::new("LinkedLongs").unwrap()]),
            doc: None,
            fields: vec![
                RecordField::builder()
                    .name("value".to_string())
                    .schema(Schema::Long)
                    .build(),
                RecordField::builder()
                    .name("next".to_string())
                    .schema(Schema::Union(UnionSchema::new(vec![
                        Schema::Null,
                        Schema::Ref {
                            name: Name::new("LongList")?.into(),
                        },
                    ])?))
                    .build(),
            ],
            lookup,
            attributes: Default::default(),
        });

        assert_eq!(
                complete.0,
                expected
            );

        Ok(())
    }

    #[test]
    fn correct_comparison_after_resolved_node_traversal() -> TestResult{
        let schema1 = r#"{
            "name": "someRecord",
            "type": "record",
            "fields": [
                {
                    "name": "matchingInner",
                    "type": {
                        "name": "matchingInner",
                        "type": "record",
                        "fields": [{"name": "f1", "type": "long"}]
                    }
                },
                {
                    "name": "someLongField",
                    "type": "long"
                }
            ]
        }"#;

        let schema2 = r#"{
            "name": "someOtherRecord",
            "type": "record",
            "fields": [
                {
                    "name": "matchingInner",
                    "type": {
                        "name": "matchingInner",
                        "type": "record",
                        "fields": [{"name": "f1", "type": "long"}]
                    }
                },
                {
                    "name": "someStringField",
                    "type": "string"
                }
            ]
        }"#;

        let resolved1 = ResolvedSchema::parse_str(schema1)?;
        let resolved2 = ResolvedSchema::parse_str(schema2)?;

        let node1 = ResolvedNode::new(&resolved1);
        let node2 = ResolvedNode::new(&resolved2);

        let matching_inner1 = match node1 {
            ResolvedNode::Record(resolved_record) => {
                Some(resolved_record.fields.iter().find(|field| field.name == "matchingInner").unwrap().resolve_field())
            }
            _ => None
        };

        let matching_inner2 = match node2 {
            ResolvedNode::Record(resolved_record) => {
                Some(resolved_record.fields.iter().find(|field| field.name == "matchingInner").unwrap().resolve_field())
            }
            _ => None
        };

        assert_eq!(
            matching_inner1,
            matching_inner2
        );

        Ok(())
    }

    #[test]
    fn schema_not_needed_are_ignored() -> TestResult{
        let schema1 = r#"["enum1", "enum2", "myrecord"]"#;
        let schema2 = r#"{
            "name": "enum1",
            "type": "enum",
            "symbols": ["A","B","C"]
        }"#;
        let schema3 = r#"{
            "name": "enum2",
            "type": "enum",
            "symbols": ["F","W","D"]
        }"#;
        let schema4 = r#"{
            "name": "myrecord",
            "type": "record",
            "fields": [{"name": "myfield", "type": "long"}]
        }"#;

        let schema5 = r#"{
            "name": "myfixed",
            "type": "fixed",
            "size": 4
        }"#;

        let [rs] = ResolvedSchema::resolve().additional([schema2, schema3, schema4, schema5])?.build_array([schema1])?;
        let my_fixed = rs.context.0.get(&Name::try_from("myfixed")?);

        assert_eq!(
            my_fixed,
            None
        );

        Ok(())
    }

    #[test]
    fn duplicate_in_provided_definitions_is_okay_if_equal() -> TestResult{
        let schema1 = r#"["enum1", "enum2", "myrecord"]"#;
        let schema2 = r#"{
            "name": "enum1",
            "type": "enum",
            "symbols": ["A","B","C"]
        }"#;
        let schema3 = r#"{
            "name": "enum2",
            "type": "enum",
            "symbols": ["F","W","D"]
        }"#;
        let schema4 = r#"{
            "name": "myrecord",
            "type": "record",
            "fields": [{"name": "myfield", "type": "long"}]
        }"#;

        let _rs = ResolvedSchema::resolve().additional([&schema2, &schema3, &schema4, &schema4])?.build_array([&schema1])?;

        Ok(())
    }

    #[test]
    fn test_multiple_resolved_schema_at_once() -> TestResult{
        let union = r#"["enum1", "enum2"]"#;
        let record = r#"{
            "name": "myRecord",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": "enum1"
                },
                {
                    "name": "f2",
                    "type": "enum2"
                }
            ]
        }"#;
        let schema1 = r#"{
            "name": "enum1",
            "type": "enum",
            "symbols": ["A","B","C"]
        }"#;
        let schema2 = r#"{
            "name": "enum2",
            "type": "enum",
            "symbols": ["F","W","D"]
        }"#;

        let _rs = ResolvedSchema::resolve().additional([&schema1, &schema2])?.build_array([&union, &record])?;

        Ok(())
    }

    const BAR_ENUM: &str = r#"{
        "type": "enum",
        "name": "bar",
        "symbols": ["A", "B", "C"]
    }"#;

    const BAZ_FIXED: &str = r#"{
        "type": "fixed",
        "name": "baz",
        "size": 16
    }"#;

    const QUX_ENUM: &str = r#"{
        "type": "enum",
        "name": "qux",
        "symbols": ["X", "Y"]
    }"#;

    /// baz as a record instead of fixed — conflicts with BAZ_FIXED
    const BAZ_RECORD: &str = r#"{
        "type": "record",
        "name": "baz",
        "fields": [{"name": "x", "type": "int"}]
    }"#;

    const FOO_USES_BAR: &str = r#"{
        "type": "record",
        "name": "foo",
        "fields": [
            {"name": "a", "type": "bar"}
        ]
    }"#;

    #[test]
    fn resolved_schema_only_includes_needed_definitions() {
        let [resolved] = ResolvedSchema::resolve()
            .additional(vec![BAR_ENUM, BAZ_FIXED])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        let names: HashSet<String> = resolved
            .get_names()
            .keys()
            .map(|n| n.fullname(None))
            .collect();

        assert!(names.contains("foo"), "should contain root definition 'foo'");
        assert!(names.contains("bar"), "should contain referenced definition 'bar'");
        assert!(!names.contains("baz"), "should NOT contain unreferenced definition 'baz'");
    }

    #[test]
    fn equal_when_different_unused_definitions_in_pool() {
        // resolve foo with {bar, baz}
        let [resolved_a] = ResolvedSchema::resolve()
            .additional(vec![BAR_ENUM, BAZ_FIXED])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        // resolve foo with {bar, qux}
        let [resolved_b] = ResolvedSchema::resolve()
            .additional(vec![BAR_ENUM, QUX_ENUM])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        assert_eq!(
            resolved_a, resolved_b,
            "resolved schemas should be equal when only the unused pool members differ"
        );
    }

    #[test]
    fn equal_despite_conflicting_unused_definitions_across_pools() {
        // pool A has baz as a fixed
        let [resolved_a] = ResolvedSchema::resolve()
            .additional(vec![BAR_ENUM, BAZ_FIXED])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        // pool B has baz as a record — conflicts with pool A's baz,
        // but neither resolution actually needs baz
        let [resolved_b] = ResolvedSchema::resolve()
            .additional(vec![BAR_ENUM, BAZ_RECORD])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        assert_eq!(
            resolved_a, resolved_b,
            "conflicting definitions of 'baz' shouldn't matter since neither schema uses it"
        );
    }

    #[test]
    fn not_equal_when_shared_definition_differs() {
        let bar_enum_alt: &str = r#"{
            "type": "enum",
            "name": "bar",
            "symbols": ["D", "E", "F"]
        }"#;

        let [resolved_a] = ResolvedSchema::resolve()
            .additional(vec![BAR_ENUM])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        let [resolved_b] = ResolvedSchema::resolve()
            .additional(vec![bar_enum_alt])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        assert_ne!(
            resolved_a, resolved_b,
            "resolved schemas should differ when a used definition differs"
        );
    }

    #[test]
    fn transitive_dependencies_included_but_not_unrelated() {
        // bar references baz, so resolving foo -> bar -> baz should pull in both
        let bar_uses_baz: &str = r#"{
            "type": "record",
            "name": "bar",
            "fields": [
                {"name": "b", "type": "baz"}
            ]
        }"#;

        let [resolved] = ResolvedSchema::resolve()
            .additional(vec![bar_uses_baz, BAZ_FIXED, QUX_ENUM])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        let names: HashSet<String> = resolved
            .get_names()
            .keys()
            .map(|n| n.fullname(None))
            .collect();

        assert!(names.contains("foo"), "root should be present");
        assert!(names.contains("bar"), "direct dependency should be present");
        assert!(names.contains("baz"), "transitive dependency should be present");
        assert!(!names.contains("qux"), "unrelated definition should NOT be present");
    }

    #[test]
    fn equal_with_transitive_deps_from_different_pools() {
        let bar_uses_baz: &str = r#"{
            "type": "record",
            "name": "bar",
            "fields": [
                {"name": "b", "type": "baz"}
            ]
        }"#;

        // pool has an extra unrelated schema
        let [resolved_a] = ResolvedSchema::resolve()
            .additional(vec![bar_uses_baz, BAZ_FIXED, QUX_ENUM])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        // pool without the extra
        let [resolved_b] = ResolvedSchema::resolve()
            .additional(vec![bar_uses_baz, BAZ_FIXED])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        assert_eq!(
            resolved_a, resolved_b,
            "transitive resolution should produce equal schemas regardless of extra pool members"
        );
    }

}
