<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# apache-avro

[![Latest Version](https://img.shields.io/crates/v/apache-avro.svg)](https://crates.io/crates/apache-avro)
[![Rust Continuous Integration](https://github.com/apache/avro-rs/actions/workflows/test-lang-rust-ci.yml/badge.svg)](https://github.com/apache/avro-rs/actions/workflows/test-lang-rust-ci.yml)
[![Latest Documentation](https://docs.rs/apache-avro/badge.svg)](https://docs.rs/apache-avro)
[![Apache License 2.0](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://github.com/apache/avro-rs/blob/main/LICENSE)

[![rust continuous integration][rust continuous integration img]][rust continuous integration]
[![rust clippy check][rust clippy check img]][rust clippy check]
[![rust security audit][rust security audit img]][rust security audit]

[rust continuous integration]: https://github.com/apache/avro-rs/actions/workflows/test-lang-rust-ci.yml
[rust clippy check]:           https://github.com/apache/avro-rs/actions/workflows/test-lang-rust-clippy.yml
[rust security audit]:         https://github.com/apache/avro-rs/actions/workflows/test-lang-rust-audit.yml

[rust continuous integration img]: https://github.com/apache/avro-rs/actions/workflows/test-lang-rust-ci.yml/badge.svg
[rust clippy check img]:           https://github.com/apache/avro-rs/actions/workflows/test-lang-rust-clippy.yml/badge.svg
[rust security audit img]:         https://github.com/apache/avro-rs/actions/workflows/test-lang-rust-audit.yml/badge.svg

A library for working with [Apache Avro](https://avro.apache.org/) in Rust.

Please check our [documentation](https://docs.rs/apache-avro) for examples, tutorials and API reference.

<!-- cargo-rdme start -->

**[Apache Avro](https://avro.apache.org/)** is a data serialization system which provides rich
data structures and a compact, fast, binary data format. If you are not familiar with the data
format, please read [`documentation::primer`] first.

There are two ways of working with Avro data in this crate:

1. Via the generic [`Value`](types::Value) type.
2. Via types implementing `Serialize`, `Deserialize`, and [`AvroSchema`].

The first option is great for dealing with Avro data in a dynamic way. For example, when working
with unknown or rapidly changing schemas or when you don't want or need to map to Rust types. The
module documentation of [`documentation::dynamic`] explains how to work in this dynamic way.

The second option is great when dealing with static schemas that should be decoded to and encoded
from Rust types. The module documentation of [`serde`] explains how to work in this static way.

## Features

- `derive`: enable support for deriving [`AvroSchema`]
- `snappy`: enable support for the Snappy codec
- `zstandard`: enable support for the Zstandard codec
- `bzip`: enable support for the Bzip2 codec
- `xz`: enable support for the Xz codec

## MSRV

The current MSRV is 1.88.0.

The MSRV may be bumped in minor releases.

<!-- cargo-rdme end -->

## License

This project is licensed under [Apache License 2.0](https://github.com/apache/avro/blob/main/LICENSE.txt).

## Contributing

Everyone is encouraged to contribute! You can contribute by forking the GitHub repo and making a pull request or opening
an issue.
All contributions will be licensed under [Apache License 2.0](https://github.com/apache/avro/blob/main/LICENSE.txt).

Please consider adding documentation and tests!
If you introduce a backward-incompatible change, please consider adding instruction to migrate in
the [Migration Guide](migration_guide.md)
If you modify the crate documentation in `lib.rs`, run `make readme` to sync the README file.
