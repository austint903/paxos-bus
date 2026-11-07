#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PAXOSBUS_SINGLE_MACHINE=1
exec "$SCRIPT_DIR/run-paxosbus.sh" "$@"
