# Security Policy

Black Swan is an experimental secure distributed runtime.

Please do not report security issues publicly before giving the maintainer time to respond.

## Current Security Scope

The current prototype includes:

- Ed25519 signature verification
- sender identity registry
- capability authorization
- timestamp expiry checks
- replay detection
- write-ahead log persistence before state application

## Known Prototype Limitations

The current version is not production-ready.

Known hardening work:

- replace string-based nonce tracking with a structured replay registry
- add TTL cleanup for nonce history
- add persistent identity storage
- add monotonic WAL indices
- add consensus-backed term handling
- add fuzz and integration tests

## Reporting

Open a private report or contact the maintainer directly.

Do not include live private keys, API keys, wallet seed phrases, passwords, or production secrets in any report.
