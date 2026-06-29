# Security Model

Black Swan uses authenticated ingress.

## Packet Security

A packet contains:

- sender identity
- nonce
- timestamp
- raw payload
- Ed25519 signature

The trust gate checks:

1. sender is known
2. packet timestamp is inside the allowed window
3. nonce has not already been used
4. sender has the required capability
5. signature is valid

## Current Trust Boundary

The coordinator trusts only packets that pass the trust gate.

Invalid packets are dropped and the peer connection is closed.

## Known Hardening Work

- Replace string-based nonce tracking with a structured replay registry.
- Add replay-window cleanup.
- Persist registered identities outside process memory.
- Add malformed packet tests.
- Add bad signature tests.
- Add unauthorized capability tests.
