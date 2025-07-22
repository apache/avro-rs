# Possible implementations

## Maintaining two separate implementations

Pros:
 - Easy to implement (just copy-paste the blocking implementation and start inserting `async`/`await`)
 - Allows for optimal performance in both situations
 - *Should* be able to share at least a part of the implementation

Cons:
 - Maintenance, any bug needs to be fixed in both implementations. Same goes for testing.
 - Hard to onboard, new contributors will be confronted with a very large codebase (see [Good ol' copy-pasting](https://nullderef.com/blog/rust-async-sync/#good-ol-copy-pasting))
 - Adding new functionality means implementing it twice.

## Implement in async, use `block_on` for sync implementation

In this implementation, the core codebase is implemented asynchronously. A `blocking` module is provided which wraps
the async functions/types in `block_on` calls. Recreating the runtime on every call is very slow, so to make this work
it would involve spawning a thread for the runtime and using that to spawn the async functions. This is how `reqwest`
implements their async/sync code.

Pros:
 - Only need to maintain/test/upgrade one implementation
 - Optimal performance for async code

Cons:
 - Degrades sync performance
 - Need to pull in a runtime when the `blocking` feature is enabled (`reqwest` use `tokio` but something like `smoll` might make more sense)

## Implement in async, use `maybe_async` to generate sync implementation

[`maybe_async`](https://crates.io/crates/maybe-async) is a proc macro that removes the `.await` from the async code and uses it to generate sync code.

Pros:
 - Only need to maintain/test/upgrade one implementation
 - Optimal performance for both async and sync code

Cons:
 - Crate breaks if both the `sync` and `async` features are enabled

## Sans I/O

Implement the parser as a state machine that can be driven by both async and sync code. This is how [`rc-zip`](https://lib.rs/crates/rc-zip)
is implemented.

Pros:
 - Only need to maintain/test/upgrade one implementation
 - Optimal performance for both async and sync code

Cons:
 - Have to manually implement the state machines
   - In the distant future [it's possible to use coroutines/generators](https://internals.rust-lang.org/t/using-coroutines-for-a-sans-io-parser/22968), but they're currently *very* unstable.

## Do not provide an async implementation

Pros:
 - Easiest option, nothing has to change

Cons:
 - An async implementation is really nice for using Avro over the network

# Serde

One problem not mentioned yet, is that Serde does not have an async interface. This doesn't necessarily have to be a problem.
The current deserialize implementation also first decodes a `avro::Value` and then uses that to deserialize the Serde type (reverse for serialize).
The decoding to `avro::Value` can be made async, and then the serde part can be done in a sync way as it does not use any I/O.

Some alternative options:
- [tokio-serde](https://docs.rs/tokio-serde/latest/tokio_serde/index.html)
  - A wrapper around Serde that requires the user to split the input into frames containing one object.
- [destream](https://docs.rs/destream/0.9.0/destream/index.html)
  - Async versions of the Serde traits, but not compatible with serde so lacks ecosystem support.

# Best option?

I'm currently leaning towards implementing Sans I/O. It provides an (almost) optimal implementation for both async and sync code.
It doesn't duplicate code (except the interfaces) and doesn't require pulling in any runtime (only parts of `futures`).

Care needs to be taken that the state machines are kept small and understandable.

The second-best option is probably using `block_on` in a separate thread. But that seems unnecessarily heavy.

# References

- [Blog post by the maintainer of `RSpotify` who tried multiple of the above options](https://nullderef.com/blog/rust-async-sync/)
- [A discussion about Sans I/O](https://sdr-podcast.com/episodes/sans-io/)
- [A explanation of Sans I/O by the author of `rc-zip`](https://fasterthanli.me/articles/the-case-for-sans-io)
  - The blog post is currently not freely available, but the [video](https://www.youtube.com/watch?v=RYHYiXMJdZI) (which has the exact same content) is freely available
