# Release Checklist

## v0.1.0

Run all checks locally:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo test --workspace
cargo run -p black_swan_coordinator --example local_signed_packet_demo
```

Verify the working tree is clean:

```bash
git status --short
```

Push the release-preparation commit:

```bash
git push origin main
```

Wait for GitHub Actions to pass:

```bash
gh run list --repo Jellyjam2/Black_Swan --limit 5
```

Create the annotated tag only after CI is green:

```bash
git tag -a v0.1.0 -m "Black Swan v0.1.0 foundation release"
git push origin v0.1.0
```

Optional GitHub release:

```bash
gh release create v0.1.0 `
  --repo Jellyjam2/Black_Swan `
  --title "Black Swan v0.1.0" `
  --notes-file RELEASE_NOTES_v0.1.0.md
```

## Release Boundary

v0.1.0 is a foundation release.

It proves the public architecture, CI, tests, security ingress path, WAL path, runtime configuration, AppendEntries scaffold, and snapshot/compaction design contracts.

It does not claim production-complete Raft.