# Cloudflare Durable Object host

`oah-host-cloudflare` is the lease and fencing protocol the workers-rs host wraps. Native tests cover it. This workspace does not build the wasm worker. There is no `wasm32-unknown-unknown` CI target here.

| Operation | Rule |
|---|---|
| `try_claim` | A live foreign owner cannot be stolen. Expiry or same-owner refresh increments `generation`. |
| `heartbeat` | Extends `expires_at_ms` without changing generation. |
| `fence_ok` | Appends must carry the live generation. Stale tokens are rejected. |

Durable Objects run one request at a time, so they do not use the native coordinator's `claim_runnable` CAS. The store adapter (SQLite-equivalent on DO storage, or Postgres via Hyperdrive) must use the same database as the ledger so the append fence can read the submission row in the same transaction.

See `crates/oah-host-cloudflare/src/lib.rs` for the tests.
