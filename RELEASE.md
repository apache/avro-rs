# Release of Apache Avro Rust SDK

## Create a Git tag and archive the sources
```
# in avro-rs Git clone
git switch main && git pull --rebase
git archive HEAD -o apache-avro-rs-0.19.0-RC0.tgz
git tag -s rel/release-0.19.0-rc0 -m "Avro-rs 0.19.0 RC0 release."
git push origin rel/release-0.19.0-rc0
```

Note: make sure your GPG key is in https://dist.apache.org/repos/dist/release/avro/KEYS

## Upload the sources to SVN dev area
```
# checkout the 'dev' dist SVN repo
svn co https://dist.apache.org/repos/dist/dev/avro/ dev-avro
cd dev-avro/avro-rs
cp .../apache-avro-rs-0.19.0-RC0.tgz .
sha256sum apache-avro-rs-0.19.0-RC0.tgz > apache-avro-rs-0.19.0-RC0.tgz.sha256
gpg --armor --output apache-avro-rs-0.19.0-RC0.tgz.asc --detach-sig apache-avro-rs-0.19.0-RC0.tgz
svn add apache-avro-rs-0.19.0-RC0.tgz apache-avro-rs-0.19.0-RC0.tgz.sha256 apache-avro-rs-0.19.0-RC0.tgz.asc
svn ci -m "Add sources of avro-rs 0.19.0 for voting"
```

## Send an email with similar content to dev@avro.apache.org:
```
[VOTE] Release Apache Avro-rs 0.19.0

Hi everyone,

I'd like to propose the following RC0 to be released as the official
Apache Avro-rs 0.19.0 release.

The commit id is https://github.com/apache/avro-rs/commit/9cb265d29351d3d0454bba30662f0066e04c171b
* This corresponds to the tag: rel/release-0.19.0-rc0
* https://github.com/apache/avro-rs/releases/tag/rel%2Frelease-0.19.0-rc0

The release tarball, signature, and checksums are here (revision r75797)
* https://dist.apache.org/repos/dist/dev/avro/avro-rs/0.19.0/

You can find the KEYS file here:
* https://dist.apache.org/repos/dist/release/avro/KEYS

This release includes 17 issues:
* https://github.com/apache/avro-rs/issues?q=is%3Aissue%20state%3Aclosed

The easiest way to test the release is:
* mkdir 0.19.0 && cd 0.19.0
* wget -q https://dist.apache.org/repos/dist/dev/avro/avro-rs/0.19.0/apache-avro-rs-0.19.0-RC0.tgz
* tar xvf apache-avro-rs-0.19.0-RC0.tgz 
* cargo fmt --check && cargo test
* (optional) Update your project Cargo.toml to use `apache_avro = { path = "/path/to/0.19.0/avro" } and test your application

Please download, verify, and test. This vote will remain open for at
least 72 hours.

[ ] +1 Release this as Apache Avro-rs 0.19.0
[ ] 0
[ ] -1 Do not release this because...

Regards,
Martin
```



## After a successful vote

```
git checkout -b release-0.19.0-RC1 rel/release-0.19.0-RC1

cargo publish -p apache-avro-derive
cargo publish -p apache-avro-test-helper
cargo publish -p apache-avro

git tag -s rel/release-0.19.0 -m "Apache Avro Rust SDK 0.19.0 release."
push --tags
```


## Create a Github release

1. Go to https://github.com/apache/avro-rs/releases/new
2. Select `rel/release-0.19.0` as the release tag and `auto` as the previous one (or select one manually)
3. Click `Generate release notes` to populate the body
4. Type a `Release title`, e.g. `Apache Avro Rust SDK 0.19.0 release`
5. Make sure `Set as the latest release` checkbox is checked
6. Press the `Publish release` button

## Announce the release

1. Send en email to dev@avro.apache.org and user@avro.apache.org with the following title and body:
1.1. Title: `[ANNOUNCE] Apache Avro Rust SDK 0.19.0 release`
1.2. Body:
```
Hello!

On behalf of the Apache Avro team I am happy to announce the release of Apache Avro Rust SDK version 0.19.0!

It is available at Crates.io:
- https://crates.io/crates/apache-avro
- https://crates.io/crates/apache-avro-derive


What's changed since vX.Y.Z:

* item 1
* item 2

## New Contributors
* contributor 1
* contributor 2

Full Changelog: https://github.com/apache/avro-rs/releases/tag/rel%2Frelease-0.19.0
```

The items could be taken from the body of the Github release ignoring the Dependabot updates:
1. Copy the body of the Github release and paste it in a temporary file, e.g. `/tmp/v0.19.0`
2. `cat /tmp/v0.19.0 | grep -v dependabot`
3. Paste the output in the email body