# Release of Apache Avro Rust SDK

## Create a Git tag and archive the sources
```
# in avro-rs Git clone
git switch main && git rebase --pull
git archive HEAD -o apache-avro-rs-0.18.0-RC0.tgz
git tag -s rel/release-0.18.0-rc0 -m "Avro-rs 0.18.0 RC0 release."
git push origin rel/release-0.18.0-rc0
```

Note: make sure your GPG key is in https://dist.apache.org/repos/dist/release/avro/KEYS

## Upload the sources to SVN dev area
```
# checkout the 'dev' dist SVN repo
svn co https://dist.apache.org/repos/dist/dev/avro/ dev-avro
cd dev-avro
cp .../apache-avro-rs-0.18.0-RC0.tgz .
sha256sum apache-avro-rs-0.18.0-RC0.tgz > apache-avro-rs-0.18.0-RC0.tgz.sha256
gpg --armor --output apache-avro-rs-0.18.0-RC0.tgz.asc --detach-sig apache-avro-rs-0.18.0-RC0.tgz
svn add apache-avro-rs-0.18.0-RC0.tgz apache-avro-rs-0.18.0-RC0.tgz.sha256 apache-avro-rs-0.18.0-RC0.tgz.asc
svn ci -m "Add sources of avro-rs 0.18.0 for voting"
```

## Send an email with similar content to dev@avro.apache.org:
```
[VOTE] Release Apache Avro-rs 0.18.0

Hi everyone,

I'd like to propose the following RC0 to be released as the official
Apache Avro-rs 0.18.0 release.

The commit id is https://github.com/apache/avro-rs/commit/9cb265d29351d3d0454bba30662f0066e04c171b
* This corresponds to the tag: rel/release-0.18.0-rc0
* https://github.com/apache/avro-rs/releases/tag/rel%2Frelease-0.18.0-rc0

The release tarball, signature, and checksums are here (revision r75797)
* https://dist.apache.org/repos/dist/dev/avro/avro-rs/0.18.0/

You can find the KEYS file here:
* https://dist.apache.org/repos/dist/release/avro/KEYS

This release includes 17 issues:
* https://github.com/apache/avro-rs/issues?q=is%3Aissue%20state%3Aclosed

The easiest way to test the release is:
* mkdir 0.18.0 && cd 0.18.0
* wget -q https://dist.apache.org/repos/dist/dev/avro/avro-rs/0.18.0/apache-avro-rs-0.18.0-RC0.tgz
* tar xvf apache-avro-rs-0.18.0-RC0.tgz 
* cargo fmt --check && cargo test
* (optional) Update your project Cargo.toml to use `apache_avro = { path = "/path/to/0.18.0/avro" } and test your application

Please download, verify, and test. This vote will remain open for at
least 72 hours.

[ ] +1 Release this as Apache Avro-rs 0.18.0
[ ] 0
[ ] -1 Do not release this because...

Regards,
Martin
```
