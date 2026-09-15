#!/usr/bin/env bash
# One local check: unit tests plus a MockModel support-desk run.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo test --workspace

cargo run -p oah -- run support-desk \
  -m "Ticket 42: CSV export is broken" \
  --id "smoke-$(date -u +%Y%m%dT%H%M%SZ)" \
  --demo

echo "local-smoke: tests green, CLI settled completed"
