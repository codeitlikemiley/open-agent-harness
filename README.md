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
