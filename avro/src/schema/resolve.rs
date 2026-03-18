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

use crate::schema::{Aliases, ArraySchema, DecimalSchema, Documentation, EnumSchema,
            FixedSchema, MapSchema, Name, NameMap, NameSet, RecordField, RecordSchema, Schema,
            SchemaKind, SchemaWithSymbols, UnionSchema, UuidSchema};
use crate::{AvroResult, types};
use crate::error::{Details,Error};
use std::collections::BTreeMap;
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

    pub fn from_str_array<const N : usize, S : AsRef<str>>(to_resolve: [S; N] , additional: impl IntoIterator<Item = S>) -> AvroResult<[ResolvedSchema; N]>{
        todo!()
    }

    pub fn from_str_array_with_resolver<const N : usize, S : AsRef<str>>(to_resolve: [S; N] , additional: impl IntoIterator<Item = S>) -> AvroResult<[ResolvedSchema; N]>{
        todo!()
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
    fn check_if_conflicts<'a>(defined_names: &NameMap, names: impl Iterator<Item = &'a Arc<Name> >) -> AvroResult<()>{
        let mut conflicting_fullnames : Vec<String> = Vec::new();
        for name in names{
            if defined_names.contains_key(name){
                conflicting_fullnames.push(name.fullname(Option::None));
            }
        }

        if conflicting_fullnames.len() == 0 {
            Ok(())
        }else{
            Err(Details::MultipleNameCollision(conflicting_fullnames).into())
        }
    }

    pub fn new_context(schemata: impl Iterator<Item = SchemaWithSymbols>) -> AvroResult<ResolvedContext>{
        Self::new_context_with_resolver(schemata, &mut DefaultResolver::new())
    }

    pub fn new_context_with_resolver(schemata: impl Iterator<Item = SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<ResolvedContext>{
        let mut context : NameMap = HashMap::new();
        Self::add_to_context(&mut context, schemata, resolver)?;
        Ok(ResolvedContext(context))
    }

    // add the with_symbols into the defined_names and check for naming conflicts.
    // If we can't resolved from the local context, we will ask the resolved if it can find
    // the schema we need.
    fn add_to_context(defined_names: &mut NameMap, schemata: impl Iterator<Item = SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<()>{
        let mut references : HashSet<Arc<Name>> = HashSet::new();

        for schema_with_symbol in schemata{
            Self::check_if_conflicts(&defined_names, schema_with_symbol.defined_names.keys())?;
            defined_names.extend(schema_with_symbol.defined_names);
            references.extend(schema_with_symbol.referenced_names);
        }

        for schema_ref in references{
            Self::resolve_name(defined_names, &schema_ref, resolver)?;
        }

        Ok(())

    }

    // attempt to resolve the schema name, first from the known schema definitions, and if that
    // fails, from the provided resolver.
    fn resolve_name(defined_names: &mut NameMap, name: &Arc<Name>, resolver: &mut impl Resolver) -> AvroResult<()>{
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
                Self::add_to_context(defined_names, once(schema_with_symbols), resolver)?;
                Ok(())
            },
            Err(msg) => {
                return Err(Details::SchemaResolutionErrorWithMsg(name.as_ref().clone(), msg).into());
            }
        }
    }
}
/// contians a schema with all of the schema
/// definitions it needs to be completely resolved
/// This type is a promise from the API that each named
/// type in the schema has exactly one unique definition
/// and every named reference in the schema can be uniquely
/// resolved to one of these definitions.
/// ResolvedSchema wraps Arc references and is therefore easy to clone.
#[derive(Debug,Clone)]
pub struct ResolvedSchema{
    pub schema: Arc<Schema>, //KTODO maybe this should take a SchemaWithSymbols? Or a
                                   //reference to SchemaWithSymbols?
    context: ResolvedContext
}

impl ResolvedSchema{

   // KTODO: do we want a function like this??
   // pub fn new(schema: SchemaWithSymbols) -> AvroResult<ResolvedSchema>{
   //     let [resolved_schema] = ResolvedSchema::from_schemata_array([schema], vec![])?;
   //     return Ok(resolved_schema)
   // }

    pub fn new_empty() -> Self{
       Schema::Null.try_into().unwrap()
    }

    // methods that take directly from a string

    // convenience method since this is a common scenario
    pub fn parse_str(string: impl AsRef<str>) -> AvroResult<ResolvedSchema>{
        let [resolved] = Self::from_str_array_only([string])?;
        return Ok(resolved)
    }

    // the rest!

    pub fn from_str_array_only<const N : usize, S : AsRef<str>>(to_resolve: [S; N]) -> AvroResult<[ResolvedSchema; N]>{
        let empt : Vec<&str> = Vec::new();
        Self::from_str_array_with_resolver(to_resolve, empt, &mut DefaultResolver::new())
    }

    pub fn from_str_array<const N : usize, S : AsRef<str>>(to_resolve: [S; N] , additional: impl IntoIterator<Item = S>) -> AvroResult<[ResolvedSchema; N]>{
        Self::from_str_array_with_resolver(to_resolve, additional, &mut DefaultResolver::new())
    }

    pub fn from_str_array_with_resolver<const N : usize>(to_resolve:  [impl AsRef<str>; N] , additional: impl IntoIterator<Item = impl AsRef<str>> , resolver: &mut impl Resolver) -> AvroResult<[ResolvedSchema; N]>{
        let to_resolve = SchemaWithSymbols::parse_array(to_resolve)?;
        let additional = SchemaWithSymbols::parse_list(additional)?;
        let resolved = Self::from_with_symbols_array_with_resolver(to_resolve, additional.into_iter(), resolver)?;
        Ok(resolved)
    }

    pub fn from_str_list<T : AsRef<str>>(to_resolve: Vec<T> , additional: Vec<T>) -> AvroResult<Vec<ResolvedSchema>>{
        Self::from_str_list_with_resolver(to_resolve, additional, &mut DefaultResolver::new())
    }

    pub fn from_str_list_with_resolver<T : AsRef<str>>(to_resolve: Vec<T> , additional: Vec<T>, resolver: &mut impl Resolver) -> AvroResult<Vec<ResolvedSchema>>{
        let to_resolve_len : usize = to_resolve.len();
        let schemata_with_symbols = SchemaWithSymbols::parse_list(to_resolve.into_iter().chain(additional.into_iter()))?;
        let mut resolved = Self::from_with_symbols_list_with_resolver(schemata_with_symbols, Vec::new(), resolver)?;
        Ok(resolved.drain(0..to_resolve_len).collect())
    }

    // methods that take directly from schema
    // this is not the recommended way of forming a ResolvedSchema

    pub fn from_schema_array_only<'a,const N: usize>(to_resolve: [&Schema;N]) -> AvroResult<[ResolvedSchema; N]>{
        Self::from_schema_array_with_resolver(to_resolve, Vec::new(), &mut DefaultResolver::new())
    }

    pub fn from_schema_array<'a,const N: usize>(to_resolve: [&Schema;N], schemata: impl IntoIterator<Item = &'a Schema>) -> AvroResult<[ResolvedSchema; N]>{
        Self::from_schema_array_with_resolver(to_resolve, schemata, &mut DefaultResolver::new())
    }

    pub fn from_schema_array_with_resolver<'a,const N: usize>(to_resolve: [&Schema;N], schemata: impl IntoIterator<Item = &'a Schema>, resolver: &mut impl Resolver) -> AvroResult<[ResolvedSchema; N]>{
        let to_resolve_with_symbols : [SchemaWithSymbols; N] = std::array::from_fn(|i|{
            (*to_resolve.get(i).unwrap()).clone().into()
        });
        ResolvedSchema::from_with_symbols_array_with_resolver(to_resolve_with_symbols,
            schemata.into_iter().map(|schema| schema.clone().into()), resolver)
    }

    pub fn from_schema_list<'a>(to_resolve: impl IntoIterator<Item = &'a Schema>, schemata: impl IntoIterator<Item = &'a Schema>) -> AvroResult<Vec<ResolvedSchema>>{
        Self::from_schema_list_with_resolver(to_resolve, schemata, &mut DefaultResolver::new())
    }
    pub fn from_schema_list_with_resolver<'a>(to_resolve: impl IntoIterator<Item = &'a Schema>, schemata: impl IntoIterator<Item = &'a Schema>, resolver: &mut impl Resolver) -> AvroResult<Vec<ResolvedSchema>>{
        let to_resolve_with_symbols : Vec<SchemaWithSymbols> = to_resolve.into_iter().cloned().map(|schema|{schema.into()}).collect();
        ResolvedSchema::from_with_symbols_list_with_resolver(to_resolve_with_symbols,
            schemata.into_iter().map(|schema| schema.clone().into()), resolver)
    }

    // methods that form a resolution context from `SchemaWithSymbols`
    // this is the reccomended way of creating a `ResolvedSchema`
    //
    pub fn from_with_symbols_array_only<const N : usize>(to_resolve: [SchemaWithSymbols;N]) -> AvroResult<[ResolvedSchema; N]>{
        Self::from_with_symbols_array_with_resolver(to_resolve, Vec::new(), &mut DefaultResolver::new())
    }

    pub fn from_with_symbols_array<const N : usize>(to_resolve: [SchemaWithSymbols;N], schemata_with_symbols: impl IntoIterator<Item = SchemaWithSymbols>) -> AvroResult<[ResolvedSchema; N]>{
        Self::from_with_symbols_array_with_resolver(to_resolve, schemata_with_symbols, &mut DefaultResolver::new())
    }

    pub fn from_with_symbols_list(to_resolve: impl IntoIterator<Item = SchemaWithSymbols>, schemata_with_symbols: impl IntoIterator<Item = SchemaWithSymbols>) -> AvroResult<Vec<ResolvedSchema>>{
        Self::from_with_symbols_list_with_resolver(to_resolve, schemata_with_symbols, &mut DefaultResolver::new())
    }

    /// Takes two vectors of schemata. Both of these vectors are checked that they form a complete
    /// schema context in which there every named schema has a unique defition and every schema
    /// reference can be uniquely resolved to one of these definitions. The first vector of
    /// schemata are those in which we want the associated ResolvedSchema forms of the schema to be
    /// returned. The second vector are schemata that are used for schema resolution, but do not
    /// have their ResolvedSchema form returned.
    pub fn from_with_symbols_array_with_resolver<const N : usize>(to_resolve: [SchemaWithSymbols; N], schemata_with_symbols: impl IntoIterator<Item = SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<[ResolvedSchema; N]> {

        let context = ResolvedContext::new_context_with_resolver(to_resolve.clone().into_iter().chain(schemata_with_symbols), resolver)?;

        // check defualts for each with_symbol
        for with_symbol in to_resolve.iter(){
            for (schema, value) in &with_symbol.field_defaults_to_resolve{
               let avro_value = types::Value::try_from(value.clone())?;
               avro_value.resolve_internal(ResolvedNode::new(&ResolvedSchema{
                   schema: Arc::new(schema.clone()), // KTODO: this should be optimized away
                   context: context.clone()
               }))?;
            }
        }

        let mut to_resolve_iter = to_resolve.into_iter();
        Ok(std::array::from_fn(|_|{
            let with_symbol = to_resolve_iter.next().unwrap();
            ResolvedSchema{
                schema: with_symbol.schema,
                context: context.clone()
            }
        }))
    }

    pub fn from_with_symbols_list_with_resolver(to_resolve: impl IntoIterator<Item = SchemaWithSymbols>, schemata_with_symbols: impl IntoIterator<Item = SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<Vec<ResolvedSchema>> {

        let to_resolve : Vec<SchemaWithSymbols> = Vec::from_iter(to_resolve.into_iter());

        let context = ResolvedContext::new_context_with_resolver(to_resolve.iter().cloned().chain(schemata_with_symbols), resolver)?;

        // check defualts for each with_symbol
        for with_symbol in to_resolve.iter(){
            for (schema, value) in &with_symbol.field_defaults_to_resolve{
               let avro_value = types::Value::try_from(value.clone())?;
               avro_value.resolve_internal(ResolvedNode::new(&ResolvedSchema{
                   schema: Arc::new(schema.clone()), // TODO: this should be optimized away
                   context: context.clone()
               }))?;
            }
        }

        Ok(to_resolve.into_iter().map(|schema_with_symbols|{ResolvedSchema{
                schema: schema_with_symbols.schema,
                context: context.clone()
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
       let schema = root.schema.as_ref();
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
        ResolvedNode::Map(resolved_map) => ResolvedSchema{schema: resolved_map.schema.clone().into(), context: resolved_map.root.context.clone()},
        ResolvedNode::Union(resolved_union) => ResolvedSchema{schema: resolved_union.schema.clone().into(), context: resolved_union.root.context.clone()},
        ResolvedNode::Array(resolved_array) => ResolvedSchema{schema: resolved_array.schema.clone().into(), context: resolved_array.root.context.clone()},
        ResolvedNode::Record(resolved_record) => ResolvedSchema{schema: resolved_record.schema.clone().into(), context: resolved_record.root.context.clone()},
        ResolvedNode::Uuid(uuid_schema) => ResolvedSchema{schema: Schema::Uuid((*uuid_schema).clone()).into(), context: ResolvedContext::empty()},
        ResolvedNode::Duration(fixed_schema) => ResolvedSchema{schema: Schema::Duration((*fixed_schema).clone()).into(), context: ResolvedContext::empty()},
        ResolvedNode::Enum(enum_schema) => ResolvedSchema{schema: Schema::Enum((*enum_schema).clone()).into(), context: ResolvedContext::empty()},
        ResolvedNode::Fixed(fixed_schema) => ResolvedSchema{schema: Schema::Fixed((*fixed_schema).clone()).into(), context: ResolvedContext::empty()},
        ResolvedNode::Decimal(decimal_schema) => ResolvedSchema{schema: Schema::Decimal((*decimal_schema).clone()).into(), context: ResolvedContext::empty()},
        ResolvedNode::Null => ResolvedSchema{schema: Schema::Null.into(), context: ResolvedContext::empty()},
        ResolvedNode::Boolean => ResolvedSchema{schema: Schema::Boolean.into(), context: ResolvedContext::empty()},
        ResolvedNode::Int => ResolvedSchema{schema: Schema::Int.into(), context: ResolvedContext::empty()},
        ResolvedNode::Long => ResolvedSchema{schema: Schema::Long.into(), context: ResolvedContext::empty()},
        ResolvedNode::Float => ResolvedSchema{schema: Schema::Float.into(), context: ResolvedContext::empty()},
        ResolvedNode::Double => ResolvedSchema{schema: Schema::Double.into(), context: ResolvedContext::empty()},
        ResolvedNode::Bytes => ResolvedSchema{schema: Schema::Bytes.into(), context: ResolvedContext::empty()},
        ResolvedNode::String => ResolvedSchema{schema: Schema::String.into(), context: ResolvedContext::empty()},
        ResolvedNode::BigDecimal => ResolvedSchema{schema: Schema::BigDecimal.into(), context: ResolvedContext::empty()},
        ResolvedNode::Date => ResolvedSchema{schema: Schema::Date.into(), context: ResolvedContext::empty()},
        ResolvedNode::TimeMillis => ResolvedSchema{schema: Schema::TimeMillis.into(), context: ResolvedContext::empty()},
        ResolvedNode::TimeMicros => ResolvedSchema{schema: Schema::TimeMicros.into(), context: ResolvedContext::empty()},
        ResolvedNode::TimestampMillis => ResolvedSchema{schema: Schema::TimestampMillis.into(), context: ResolvedContext::empty()},
        ResolvedNode::TimestampMicros => ResolvedSchema{schema: Schema::TimestampMicros.into(), context: ResolvedContext::empty()},
        ResolvedNode::TimestampNanos => ResolvedSchema{schema: Schema::TimestampNanos.into(), context: ResolvedContext::empty()},
        ResolvedNode::LocalTimestampMillis => ResolvedSchema{schema: Schema::LocalTimestampMillis.into(), context: ResolvedContext::empty()},
        ResolvedNode::LocalTimestampMicros => ResolvedSchema{schema: Schema::LocalTimestampMicros.into(), context: ResolvedContext::empty()},
        ResolvedNode::LocalTimestampNanos => ResolvedSchema{schema: Schema::LocalTimestampNanos.into(), context: ResolvedContext::empty()},
       }
   }

   pub(crate) fn get_schema(&self) -> Schema{ // KTODO, maybe get rid of this for the future!!
       self.get_resolved().schema.as_ref().clone()
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
            // fast path
            Some((i, resolved_nodes.get(i).unwrap().clone()))
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

        fn unravel(schema: &mut Schema, defined_schemata: &NameMap, placed_schemata: &mut NameSet){
            match schema {
                Schema::Ref{name}=> {
                    if !placed_schemata.contains(name) {
                        let mut definition = defined_schemata.get(name).unwrap().as_ref().clone();
                        unravel(&mut definition, defined_schemata, placed_schemata);
                        *schema = definition;
                    }
                },
                Schema::Record(record_schema) => {
                    if !placed_schemata.insert(Arc::clone(&record_schema.name)) {
                        panic!("When converting to complete schema, attempted to double define a schema when unraveling");
                    }
                    for field in &mut record_schema.fields {
                       unravel(&mut field.schema, defined_schemata, placed_schemata);
                    }
                }
                Schema::Array(array_schema) => {
                    unravel(&mut array_schema.items, defined_schemata, placed_schemata);
                }
                Schema::Map(map_schema) => {
                    unravel(map_schema.types.as_mut(), defined_schemata, placed_schemata);
                }
                Schema::Union(union_schema) => {
                    for mut el_schema in &mut union_schema.schemas {
                        unravel(&mut el_schema, defined_schemata, placed_schemata);
                    }
                },
                Schema::Fixed(fixed_schema) => {
                    if !placed_schemata.insert(Arc::clone(&fixed_schema.name)) {
                        panic!("When converting to complete schema, attempted to double define a schema when unraveling");
                    }
                },
                Schema::Enum(enum_schema) => {
                    if !placed_schemata.insert(Arc::clone(&enum_schema.name)) {
                        panic!("When converting to complete schema, attempted to double define a schema when unraveling");
                    }
                }
                _ => {}
            }
        }

        let mut schema = value.schema.as_ref().clone();
        unravel(&mut schema, &value.context.0, &mut HashSet::new());
        CompleteSchema(schema)
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
        compare_schema_with_context(&self.schema, &other.schema, &self.context.0, &other.context.0, &HashSet::new(), &HashSet::new())
    }
}

/// technically this is a tighter bound that we need, as we may have
/// iterated far enough down that we don't need to check the entire conext definitions.
impl PartialEq for ResolvedNode<'_>{
    fn eq(&self, other: &Self) -> bool {
        let resolved_self = self.get_resolved();
        let resolved_other = other.get_resolved();
        return resolved_other == resolved_self
    }
}

impl PartialEq for SchemaWithSymbols{
    fn eq(&self, other: &Self) -> bool {
        compare_schema_with_context(&self.schema, &other.schema, &self.defined_names, &other.defined_names, &self.referenced_names, &other.referenced_names)
    }
}

impl TryFrom<SchemaWithSymbols> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: SchemaWithSymbols) -> AvroResult<Self> {
        let [resolved_schema] = ResolvedSchema::from_with_symbols_array([schema], Vec::new())?;
        Ok(resolved_schema)
    }
}

impl TryFrom<&SchemaWithSymbols> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: &SchemaWithSymbols) -> AvroResult<Self> {
        let [resolved_schema] = ResolvedSchema::from_with_symbols_array([schema.clone()], Vec::new())?;
        Ok(resolved_schema)
    }
}

impl TryFrom<Schema> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: Schema) -> AvroResult<Self> {
        let with_symbols : SchemaWithSymbols = schema.into();
        let [resolved_schema] = ResolvedSchema::from_with_symbols_array([with_symbols], Vec::new())?;
        Ok(resolved_schema)
    }
}

/// NOTE: this will copy the schema, and is therefore not the most performant option
impl TryFrom<&Schema> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: &Schema) -> AvroResult<Self> {
        let schema = schema.clone();
        let with_symbols : SchemaWithSymbols = schema.into();
        let [resolved_schema] = ResolvedSchema::from_with_symbols_array([with_symbols], Vec::new())?;
        Ok(resolved_schema)
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

fn compare_schema_with_context(schema_one: &Schema, schema_two: &Schema, context_one: &NameMap, context_two: &NameMap, dangling_one: &NameSet, dangling_two: &NameSet) -> bool{
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

    if context_one.iter().fold(true, |acc, (name, schema)|{acc &&
        schema.as_ref() == context_two.get(name).unwrap().as_ref()}) {
        return false
    }

    schema_one == schema_two

}

#[cfg(test)]
mod tests {
    use super::{ResolvedSchema};
    use crate::{
        Schema,
        schema::{Name},
    };
    use apache_avro_test_helper::TestResult;

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
        let [rs] = ResolvedSchema::from_str_array_only([schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_str_array_only([&schema])?;
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
        let [rs] = ResolvedSchema::from_schema_array_only([&schema])?;

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
        let [rs] = ResolvedSchema::from_schema_array_only([&schema])?;

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
        let _resolved = ResolvedSchema::from_str_array_only([&schema])?;

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
        let _resolved = ResolvedSchema::from_str_array_only([&schema])?;

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

        let result = ResolvedSchema::from_str_array([&schema],vec![&other_schema])
            .unwrap_err();

        assert_eq!(
            result.to_string(),
            "Two named schema defined for same fullname: duplicated_name."
        );

        Ok(())
    }
}
