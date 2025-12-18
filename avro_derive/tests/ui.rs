/// These tests only run on nightly as the output can change per compiler version.
///
/// See https://github.com/dtolnay/trybuild/issues/84
#[rustversion::attr(not(nightly), ignore)]
#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
