---
displayed_sidebar: reference
sidebar_position: 8
title: "Rust Style Guide"
---

# Rust Style Guide

The conventions every change to the Angos codebase follows. They adapt the
[Google Rust style guide](https://google.github.io/styleguide/rustguide.html) to an application
codebase; where the two disagree, the project rules below win.

---

## Project rules

These take precedence over everything else on this page.

- **No `unsafe`.** Zero unsafe code is a workspace invariant.
- **No new lint suppressions.** Fix the code instead of adding `#[allow]` or `#[expect]`.
- **No `.unwrap()` or `.expect()` in production code.** Tests may use them.
- **Imports at the top of the module only.** No `use` inside functions, and no fully-qualified paths in code.
- **Make invalid states unrepresentable.** Prefer a newtype whose only constructor validates (`TryFrom`, `FromStr`, `#[serde(try_from = "String")]`) over a `validate_*` helper. Wrap secrets from the outside: `Secret<RedisUrl>`, not `RedisUrl(Secret<String>)`.
- **Avoid single-line functions.** Inline trivial helpers at their call sites. Trait impl methods, functions referenced by name in attributes, and real API accessors are the exceptions.
- **Prefer `pub` over `pub(crate)` or `pub(super)`.**
- **No indirection where none is needed.**
- **Keep comments short.** One sentence if possible, two at most unless the logic is genuinely complex. Don't restate what the code says.

---

## Baseline

- Follow the official Rust Style Guide and the Rust API Guidelines. `cargo fmt` is authoritative.
- Use `#[rustfmt::skip]` only for deliberate alignment (tables, matrices) or generated code, never for taste.
- Favor simplicity and directness over reusability. No premature generalization.

## Organization and imports

- Name a file or module after its primary item (`Foo` lives in `foo.rs`).
- Non-leaf modules hold `mod` declarations and re-exports, not implementations.
- No glob imports, except `use super::*` in `mod tests` and `use Enum::*` inside functions centered on that enum.
- One module per `use` statement: `use foo::bar::{Bar, Baz};`, never `use foo::{Foo, bar::Bar};`.
- Import type names and use them unqualified (`Regex`, not `regex::Regex`). Alias on collisions: `use std::io::Error as IoError`.

## Ownership and signatures

- Prefer unique ownership and borrows. Reach for `Rc` or `Arc` only when the alternative is unreasonably complex.
- Call `.clone()` only for a real copy, not to dodge the borrow checker. Cloning an `Arc` is fine.
- Take `&T`, `&[T]`, `&str` and `Option<&T>`, not `&Box<T>`, `&Vec<T>`, `&String` or `&Option<T>`.
- Returning `Vec<T>` or taking `&[T]` is fine. Use iterators when they actually help.
- No `impl Into<T>` parameters. Take the concrete type most callers already have.
- Prefer an `enum` over a `bool` parameter whose meaning isn't obvious at the call site.
- Use `Option` for absence with an obvious cause and `Result` for recoverable failures that need context.

## Types and traits

- Use a newtype rather than a type alias when semantics differ, the type needs its own API, or values are easy to mix up.
- Prefer named fields over tuples. Tuple structs only for single-field newtypes.
- Implement standard traits (`FromStr`, `Default`, `From`, `Display`) instead of look-alike inherent methods. Prefer `#[derive]` over manual impls.
- Add trait impls when they are needed, not speculatively. Implement `Default` when there is an argument-less `new()`, and `Hash + Eq` for likely map keys.
- Define a trait only for generic bounds, `impl Trait`/`dyn Trait`, or extension traits (named `FooExt`). Never to organize code.
- Prefer generics or `impl Trait` over `dyn Trait` unless dynamic dispatch is genuinely needed.
- When `From` is the main constructor, also provide an inherent `new` or `from_foo`.

## Naming

- Follow standard library conventions: `as_`/`to_`/`into_` with matching `self` receivers; `new`, `new_with_foo`, `try_new` (returns `Result`), `from_foo`.
- Accessors are `foo()`, `foo_mut()`, `set_foo()`, `take_foo()`, `replace_foo()`, and closure accessors `with_foo()`. No `get_` prefix.
- If a type has `len()`, it also has `is_empty()`.
- Give lifetimes on types descriptive names. Use `'a`/`'b` only for simple local relationships, and `'_` wherever it works.
- Keep generic names short and hinting at the bound. `T`/`U` when unbounded.
- No unfamiliar abbreviations or dropped letters.

## Implementation

- Pattern-match instead of check-then-unwrap: `if let Some(v) = opt`, not `if opt.is_some() { opt.unwrap() }`.
- Annotate types where inference hides meaning, especially after `.into()`, `.parse()` and `.collect()`. Prefer a binding annotation over a turbofish.
- Short combinator chains with real logic (`iter().any(..)`) are good. Long chains, chains in `for` headers and `for_each` are not; use a `for` loop.
- Use `format!` or `write!` to join more than two strings or substitute into a literal, not `+` or `str::replace`.
- `assert!` messages add context (the value, who misused what), not a restatement of the condition.
- Overload operators only for their mathematical meaning.
- Put `#[must_use]` on functions whose result can't be ignored correctly, unless the return type already is.
- Reach for macros last: functions, types and traits first, then `macro_rules!`, proc macros only as a last resort.
- No `catch_unwind`, no code before `main()`, no unstable features, and no access to another crate's `#[doc(hidden)]` items.
- Never reach into a poisoned mutex.

## Documentation

- `///` on items, `//!` at the top of a module, `//` for implementation notes. Markdown, full sentences.
- Doc comments say what the code doesn't. Skip trivial ones and self-explanatory enum variants.
- Always document panics and non-obvious invariants.

## Tests

- Every test must be able to fail. A test with no meaningful assertion, or one that only checks what its setup just wrote, is deleted.
- Don't test the obvious: derives, getters, constructors that store their arguments, plain serde structs, or the behaviour of `std` and third-party crates.
- Don't pin behaviour nobody relies on, such as internal error wording, incidental ordering or `Debug` output. Client-visible error bodies and status codes are a contract and are tested.
- Don't duplicate coverage. A new test exercises a branch no other test reaches.
- Backend-dependent tests run against both the filesystem and S3 backends.

---

## Checking a change

Every change must pass, from the repository root:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features
cargo test --workspace --all-features
```

`--workspace` matters: without it, Clippy and the tests skip the member crates.

The S3 and Redis tests need live services, as in CI: Redis on `localhost:6379`, and
[rustfs](https://github.com/rustfs/rustfs) (an S3 server with the conditional writes Angos
relies on) with a `registry` bucket:

```bash
docker run --rm -d -p 6379:6379 redis:7-alpine
docker run --rm -d -p 9000:9000 \
  -e RUSTFS_ACCESS_KEY=root -e RUSTFS_SECRET_KEY=roottoor rustfs/rustfs:1.0.3
AWS_ACCESS_KEY_ID=root AWS_SECRET_ACCESS_KEY=roottoor AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://127.0.0.1:9000 s3api create-bucket --bucket registry
```

Set `ANGOS_TEST_S3_ENDPOINT` if the S3 server is not on `http://127.0.0.1:9000`.
