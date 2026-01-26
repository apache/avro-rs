//! # A primer on Apache Avro
//!
//! Avro is a schema based encoding system, like Protobuf. This means that if you have raw Avro data
//! without a schema, you are unable to decode it. It also means that the format is very space
//! efficient.
//!
//! ## Schemas
//! 
//! Schemas are defined in JSON and look like this:
//! ```json
//! {
//!     "type": "record",
//!     "name": "example",
//!     "fields": [
//!         {"name": "a", "type": "long", "default": 42},
//!         {"name": "b", "type": "string"}
//!     ]
//! }
//! ```
//! For all possible types and extra attributes, see [the schema section of the specification].
//! 
//! [the schema section of the specification]: https://avro.apache.org/docs/++version++/specification/#schema-declaration
//! 
//! ## File formats
//! There are three official file formats for Avro. The data in these file formats is all encoded the same, but they differ
//! in how the schema is included.
//! 
//! ### [Object Container File](https://avro.apache.org/docs/++version++/specification/#object-container-files)
//! This is the most common file format used for Avro. It includes the schema in the file, and can therefore be decoded by
//! a reader who doesn't have the schema. It also supports including many records in one file.
//! 
//! ### [Single Object Encoding](https://avro.apache.org/docs/++version++/specification/#single-object-encoding)
//! In this file format, the schema is not included directly. It instead includes a fingerprint of the schema, which a reader
//! can lookup in a schema database or compare with the fingerprint that the reader is expecting. This file format always contains
//! one record.
//! 
//! ### Avro datums
//! This is not really a file format, as it's just the raw Avro encoded data. It does not include a schema and can therefore not be
//! decoded without the reader knowing **exactly** which schema was used to write it.
//! 
