# open-agent-harness

Rust workspace. Crates are `oah-*`. The binary is `oah`. The name matches `open-ai-gateway` (`oag-*`). A rename is find-and-replace.

This repo implements a slice of the 2026-09-15 PRD.

Agents are `impl Agent` plus `&mut RenderCx`, or an `AGENT.md` file. A submission is admitted, claimed, processed, then settled. `oah-core` folds the log with a reducer and a recovery classifier.

`POST /agents/{name}/{id}` returns 202. `GET` returns a history snapshot or SSE updates. The paths follow Flue's v1 snapshot shape.

A tool gate can deny a call or ask a human once. Resume runs the stored arguments. It does not ask the model to propose the call again.

Skills load from `SKILL.md`. MCP tools use `mcp__<server>__<tool>`. Cron schedules fire `sched:{id}:{due}` signals. Slack, GitHub, and bearer hooks become channel signals. `POST /ag-ui` emits `RUN_STARTED`. `POST /mcp` is a JSON-RPC door. The `task` tool opens a child session `task:<parent>:task_<ulid>`. `use_sandbox` registers `read`, `write`, `edit`, `bash`, `grep`, and `glob`.

Crash injection fails before each durable write. If a non-durable tool's call is already on a prior attempt with no outcome, resume writes `interrupted` and does not run the tool again.

Inference goes through `hexuria/open-ai-gateway` at `/v1/messages`. Local and demo runs use `MockModel`.

opengrok-server is not in this repo. This code replaces that server's engine. The product UI stays in opengrok-server.

## Run it on your machine

You do not need a gateway key. Debug builds use `MockModel` when `gateway.url` / `gateway.key` are empty.

1. Install [rustup](https://rustup.rs). This repo pins **Rust 1.88** in `rust-toolchain.toml`; rustup installs that channel on first `cargo` in the tree.

```bash
git clone https://github.com/codeitlikemiley/open-agent-harness.git
cd open-agent-harness
# The public clone may not have Cargo.lock. The first cargo command writes one.
```

2. Tests (no server, no key):

```bash
cargo test --workspace
# or: bash scripts/local-smoke.sh
```

Postgres store tests stay skipped unless `OAH_POSTGRES__URL` is set.

3. One conversation, then inspect the SQLite ledger:

```bash
cargo run -p oah -- run support-desk -m "Ticket 42: CSV export is broken" --id ticket-42 --demo
cargo run -p oah -- inspect support-desk/ticket-42
```

Exit `0` means settled completed. Exit `1` means failed.

4. HTTP API + demo console:

```bash
cargo run -p oah -- serve --demo
# or wipe the store first: cargo run -p oah -- dev --fresh
```

Open [http://127.0.0.1:43147](http://127.0.0.1:43147). In the console, send:

> Customer cannot export invoices, ticket 42

The scripted model calls `lookup_ticket` for ticket `42` (Amina Cole / CSV export), then drafts a reply. History is a Flue v1 snapshot. Live updates are SSE.

```bash
curl -sS http://127.0.0.1:43147/health/ready
curl -sS -X POST http://127.0.0.1:43147/agents/support-desk/preview-1 \
  -H 'content-type: application/json' \
  -d '{"message":"Customer cannot export invoices, ticket 42"}'
curl -sS 'http://127.0.0.1:43147/agents/support-desk/preview-1?view=history'
```

`POST` returns `202`. `GET` history should include a `completed` settlement and a `lookup_ticket` tool part.

To point at a live `hexuria/open-ai-gateway` instead of the mock, copy `.env.example` and set `OAH_GATEWAY__URL` / `OAH_GATEWAY__KEY`. A release binary refuses to start in mock mode.

## Configuration

`oah.toml` plus env overrides `OAH_<SECTION>__<FIELD>`:

| Key | Meaning |
|---|---|
| `server.bind` | Listen address (default `0.0.0.0:43147`) |
| `store.backend` | `sqlite` (default) or `postgres` |
| `store.path` | SQLite file (default `.oah/dev.db`) |
| `postgres.url` / `OAH_POSTGRES__URL` | Postgres DSN (LISTEN/NOTIFY wake) |
| `gateway.url` / `gateway.key` | `open-ai-gateway` base URL and `oag_live_…` key |
| `channels.slack` / `github` / `bearer` | Inbound webhook secrets |

Without a gateway key the host uses `MockModel`. A release binary refuses to start in mock mode.

## HTTP (Flue v1 paths)

| Method | Path | Result |
|---|---|---|
| `POST` | `/agents/:agent/:id` | `202 { streamUrl, offset, submissionId, uid }` |
| `GET` | `/agents/:agent/:id?view=history` | Snapshot `{ v:1, conversationId, offset, messages, settlements, incarnation }` |
| `GET` | `/agents/:agent/:id?view=updates&offset=-1&live=sse` | `event: data` records, `event: control` |
| `POST` | `/agents/:agent/:id/abort` | Abort the live submission and the queue |
| `GET`/`POST` | `/agents/:agent/:id/approvals[/:tool_call_id]` | List / answer a suspension |
| `POST` | `/ag-ui` | AG-UI `RUN_STARTED` + stream URL |
| `POST` | `/mcp` | MCP JSON-RPC 2.0 (`initialize`, `tools/list`, `tools/call`, `oah_dispatch`) |
| `GET`/`POST`/`DELETE` | `/schedules` | Cron book; fires `sched:{id}:{due}` signals |
| `POST` | `/hooks/slack|github|bearer/:agent/:id` | Signed webhooks to channel signals (64KiB max) |
| `GET` | `/health/live`, `/health/ready` | Process and store |

The host reads an optional `x-oah-principal` header and stores it on the submission. Auth is the app's job.

Offsets are `-1` at origin. Later offsets are `%016d_%016d`.

## Workspace

| Crate | Role |
|---|---|
| `oah-core` | Newtype ids, records, reducer, classifier |
| `oah-model` | Messages, `ModelClient`, `MockModel` |
| `oah-oag` | Gateway `/v1/messages` SSE client |
| `oah-loop` | Tool-batch agent loop (gates run before execute) |
| `oah-store` / `oah-store-memory` / `oah-store-sqlite` / `oah-store-postgres` | Ledger + append-only log |
| `oah-store-conformance` | Admit / claim / fence / suspend contracts |
| `oah-sandbox` | virtual + local (`OAH_HOSTED=1` refused) + box-control mock |
| `oah-skills` | `SKILL.md`, `skill:<name>:<sha16>`, `{{id}}` |
| `oah-mcp` | `mcp__<server>__<tool>`, allowlist, flatten, JSON-RPC door |
| `oah-schedules` | Claim-and-advance cron |
| `oah-channels` | Slack v0 HMAC, GitHub sha256, bearer CT-eq |
| `oah-otel` | Content capture off by default, redact, histograms |
| `oah-runtime` | `RenderCx`, gates, coordinator |
| `oah-http` | Protocol + demo console |
| `oah-host-native` | Tokio host |
| `oah-host-cloudflare` | Durable Object lease / fencing (native-tested) |
| `oah` | CLI |

Engineering rules from opengrok: `unsafe_code = forbid`. Clippy denies `unwrap`, `expect`, and `panic`. Ids are newtypes.

Pinned toolchain: Rust 1.88, edition 2021 (`rust-toolchain.toml`). The PRD's later target is Rust 1.95, edition 2024, matching the gateway.

## Still host-specific

A live `hexuria/box` cluster, `@flue/sdk` CI against Flue 2.0.6 fixtures, workers-rs wasm, and moving opengrok-server onto this host stay out of this repo (G6). Postgres two-replica chaos is out of scope. The adapter and conformance suite run when `OAH_POSTGRES__URL` is set:

```bash
export OAH_POSTGRES__URL=postgresql://ubuntu:oah@127.0.0.1:5432/oah
cargo test --workspace
```

Host-specific contracts:

- [docs/cloudflare.md](docs/cloudflare.md). Durable Object lease and fencing.
- [docs/box-control.md](docs/box-control.md). `hexuria/box` driver methods.

```bash
cargo clippy --workspace --all-targets -- -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic
```

## License

MIT. See `NOTICE` for Apache-2.0 attribution of the Flue specification this port follows. "Flue-compatible protocol" is descriptive use. `Flue` is not used in crate or product names.
