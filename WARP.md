# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

This is the Apache Avro Rust SDK, a comprehensive library for working with Apache Avro data serialization format. It's structured as a Cargo workspace with multiple interdependent crates that provide different aspects of Avro functionality.

## Workspace Structure

This is a multi-crate workspace with the following components:

### Core Crates
- **`avro/`** - Main library (`apache-avro` crate) with full Avro functionality
- **`avro_derive/`** - Proc-macro crate for automatic schema derivation (`apache-avro-derive`)
- **`avro_test_helper/`** - Testing utilities shared across the workspace

### Additional Components
- **`wasm-demo/`** - WebAssembly demonstration application
- **`fuzz/`** - Fuzzing tests (excluded from workspace)

## Common Development Commands

### Building and Testing

```bash
# Build entire workspace
cargo build --all-features

# Build release version
cargo build --all-features --release

# Run all tests (includes doc tests and pre-commit hooks)
make test

# Run tests without pre-commit setup
cargo test --all-features --all-targets
cargo test --doc

# Run specific workspace member
cargo test -p apache-avro
cargo test -p apache-avro-derive

# Build with specific codec features
cargo build --features snappy,zstandard,bzip,xz
```

### Quality and Linting

```bash
# Format all code
make lint
# or directly:
cargo fmt

# Run clippy with strict settings
make clippy
# or directly:
cargo clippy --all-features --all-targets -- -Dclippy::all -Dunused_imports

# Check without building
cargo check --all-features

# Security audit
cargo audit
```

### Documentation and Development Tools

```bash
# Generate and open local documentation
make doc-local

# Generate documentation without opening
make doc

# Update README files from doc comments
make readme

# Run benchmarks
make benchmark
```

### Apache Avro Specific Commands

```bash
# Generate interoperability test data
./build.sh interop-data-generate

# Test interoperability with other Avro implementations
./build.sh interop-data-test

# Distribution build
./build.sh dist
```

### Pre-commit Setup

```bash
# Install pre-commit hooks (includes Python venv setup)
make install-hooks

# Clean pre-commit setup
make clean-hooks

# Manual pre-commit run
.venv/bin/pre-commit run --all-files
```

## Architecture Overview

### Multi-Layer Design

The codebase follows a layered architecture:

1. **Schema Layer** (`schema.rs`) - Avro schema parsing, validation, and representation
2. **Type System** (`types.rs`) - Avro value types and native Rust type mappings
3. **Encoding/Decoding** (`encode.rs`, `decode.rs`) - Low-level binary format handling
4. **Reader/Writer API** (`reader.rs`, `writer.rs`) - High-level streaming interfaces
5. **Serde Integration** (`ser.rs`, `de.rs`) - Rust serde framework integration

### Key Components

#### Schema Management
- JSON schema parsing with dependency resolution
- Programmatic schema construction
- Schema compatibility checking (`schema_compatibility.rs`)
- Custom validation and naming rules (`validator.rs`)

#### Data Processing Approaches
1. **Native Avro Types**: Using `Value`, `Record` types with schema validation
2. **Serde Integration**: Direct serialization/deserialization of Rust structs
3. **Derive Macros**: Automatic schema generation from Rust types

#### Codec Support
Configurable compression codecs via feature flags:
- `snappy` - Google Snappy compression
- `zstandard` - Facebook Zstandard compression  
- `bzip` - BZip2 compression
- `xz` - XZ/LZMA compression
- Built-in: Null (uncompressed) and Deflate

#### Logical Types Support
Built-in support for Avro logical types:
- Decimal (using `num-bigint`)
- UUID (using `uuid` crate)
- Date, Time, Timestamp variants
- Duration with months/days/millis

## Development Considerations

### Feature Flag Strategy
The library uses feature flags for optional functionality:
- Use `--all-features` for comprehensive testing
- Individual codec features for minimal builds
- `derive` feature for procedural macro functionality

### Error Handling Pattern
- Custom `Error` enum with detailed context (replaced `failure` crate in v0.11)
- Extensive use of `thiserror` for error derivation
- Schema validation errors provide precise location information

### Memory Safety and Security
- Built-in allocation limits (default 512MB) to prevent malicious data attacks
- Use `max_allocation_bytes()` before processing if expecting large data
- Robust schema validation prevents many attack vectors

### Compatibility and Migration
- Check `migration_guide.md` for breaking changes between versions
- MSRV (Minimum Supported Rust Version): 1.85.0
- Schema compatibility checking available via `SchemaCompatibility::can_read()`

### Testing Strategy
- Comprehensive unit tests for all modules
- Integration tests with real Avro files
- Interoperability testing with Apache Avro implementations
- Fuzzing tests (separate `fuzz/` directory)
- Property-based testing patterns

### Code Organization Patterns
- Single responsibility modules (encoding, decoding, schema, etc.)
- Trait-based design for extensibility
- Builder patterns for complex configurations
- Iterator-based APIs for memory efficiency

## Customization Points

### Schema Validation
Implement `SchemaNameValidator` trait for custom naming rules:
```rust
set_schema_name_validator(Box::new(MyCustomValidator));
```

### Schema Equality
Implement `SchemataEq` trait for custom schema comparison:
- Default: `StructFieldEq` (fast structural comparison)
- Alternative: `SpecificationEq` (canonical JSON comparison)

### Fingerprinting
Built-in support for schema fingerprinting:
- SHA-256, MD5, Rabin fingerprints
- Used for schema registry integration

## Apache Foundation Requirements

### Licensing
- All files must include Apache 2.0 license headers
- Use provided license header templates
- `deny.toml` enforces allowed license dependencies

### Release Process
- Follow Apache release guidelines in `RELEASE.md`
- Version bumps must be coordinated across workspace
- Update `CHANGELOG.md` for all changes

### Contribution Requirements
- All contributions licensed under Apache 2.0
- Pre-commit hooks enforce formatting and linting
- Consider backward compatibility impact
- Update migration guide for breaking changes

## Performance Considerations

### Benchmarking
- Benchmarks available in `benches/` directory
- Compare against serde_json for baseline performance
- Memory allocation patterns matter for large datasets

### Memory Efficiency
- Streaming APIs for large data processing
- Batch processing capabilities in Reader/Writer
- Allocation limit controls for security

### Feature Selection
- Minimal feature sets for smaller binaries
- Codec features only when compression needed
- WebAssembly compatibility considerations