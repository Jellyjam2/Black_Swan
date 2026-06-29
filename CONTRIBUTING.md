# Contributing to Black Swan

Black Swan welcomes small, focused contributions.

## Good Contribution Areas

- Rust cleanup
- tests
- documentation
- WAL hardening
- replay protection
- Raft scaffolding
- secure transport review

## Development Rules

Before opening a pull request:

```bash
cargo fmt
cargo clippy --workspace --all-targets
cargo test --workspace
```

## Pull Request Style

Keep PRs small.

Good PR:

- one fix
- one test
- clear description

Avoid:

- large rewrites
- unrelated formatting changes
- changing architecture without discussion
- adding secrets or local machine paths

## First Useful Tasks

- Add WAL replay tests
- Add trust-gate tests
- Move the local signed-packet harness into `examples/`
- Replace nonce string matching with `HashSet`
