//! Set the `nightly` cfg value on nightly toolchains.
//!
//! We would prefer to just do `#![rustversion::attr(nightly, feature(proc_macro_diagnostic)]`
//! but that's currently not possible, see https://github.com/dtolnay/rustversion/issues/8

#[rustversion::nightly]
fn main() {
    println!("cargo:rustc-cfg=nightly");
}

#[rustversion::not(nightly)]
fn main() {}
