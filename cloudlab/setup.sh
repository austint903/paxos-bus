#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# CloudLab environment setup for PaxosBus — the "Dockerfile, as a script".
#
# Turns a barebone Ubuntu node into one that can run paxosbus-replica /
# paxosbus-client. Installs build deps + Go (mirrors Dockerfile.swiftpaxos:
# golang:1.22 + iputils-ping + ca-certificates) and builds the two static Go
# binaries. Idempotent and safe to re-run.
#
# Runs in two situations, both fine:
#   1. Automatically on boot, as the profile's Execute service (as root).
#   2. Manually / by run-cloudlab.sh over SSH (as your user; uses sudo).
#
# Why /local and not /proj for the binaries: the boot service runs as root, and
# /proj is root-squashed NFS (root can't write it), so build output goes to
# /local (always writable, fast local disk, world-executable). Durable RESULTS
# are collected back to your laptop by run-cloudlab.sh, so nothing important is
# lost when the node is released. /home and /usr are wiped on release too, but
# this script re-runs on every boot, so that's a non-issue.
#
# Usage: setup.sh [-f]      (-f forces a clean rebuild of the binaries)
# ─────────────────────────────────────────────────────────────────────────────

FORCE=0
[[ "${1:-}" == "-f" ]] && FORCE=1

GO_VERSION="${GO_VERSION:-1.22.5}"
REPO_URL="${REPO_URL:-https://github.com/austint903/paxos-bus.git}"
SRC="${SRC:-/local/paxos-bus}"          # repo clone (rebuilt each boot)
WORK="${WORK:-/local/paxosbus-cloudlab}" # build output + marker
BIN="$WORK/bin"

SUDO=""
[[ "$(id -u)" -ne 0 ]] && SUDO="sudo"

log() { echo "[setup] $*"; }

# ── Packages ─────────────────────────────────────────────────────────────────
# iputils-ping is REQUIRED (replicas/clients ping each other); netcat-openbsd +
# autossh back run-cloudlab.sh's reachability probe and SSH-tunnel fallback.
log "installing packages"
export DEBIAN_FRONTEND=noninteractive
$SUDO apt-get update -y
$SUDO apt-get install -y --no-install-recommends \
    build-essential git curl ca-certificates \
    iputils-ping netcat-openbsd autossh

# ── Go ───────────────────────────────────────────────────────────────────────
# Map uname -m -> Go's release arch. The clusters are mixed: Wisconsin/Clemson
# are x86_64, Utah is aarch64. Hardcoding amd64 lands an x86 Go on the ARM node,
# where every `go` call dies with "cannot execute binary file: Exec format error".
case "$(uname -m)" in
    x86_64)        GOARCH_TAR=amd64 ;;
    aarch64|arm64) GOARCH_TAR=arm64 ;;
    *) echo "[setup] unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac
if [[ ! -x /usr/local/go/bin/go ]] || ! /usr/local/go/bin/go version | grep -q "go${GO_VERSION}"; then
    log "installing Go ${GO_VERSION} (${GOARCH_TAR})"
    $SUDO rm -rf /usr/local/go
    curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${GOARCH_TAR}.tar.gz" \
        | $SUDO tar -C /usr/local -xz
fi
GO=/usr/local/go/bin/go
$GO version

# ── Source ───────────────────────────────────────────────────────────────────
log "fetching source ($REPO_URL)"
if [[ ! -d "$SRC/.git" ]]; then
    $SUDO git clone "$REPO_URL" "$SRC"
else
    $SUDO git -C "$SRC" pull --ff-only || true
fi

# ── Build ────────────────────────────────────────────────────────────────────
if [[ $FORCE -eq 1 ]]; then
    $SUDO rm -rf "$WORK"
fi
$SUDO mkdir -p "$BIN"
log "building paxosbus-replica + paxosbus-client -> $BIN"
$SUDO env PATH="/usr/local/go/bin:$PATH" GOCACHE=/local/.gocache CGO_ENABLED=0 \
    "$GO" build -C "$SRC" -o "$BIN/paxosbus-replica" ./paxosbus/cmd/paxosbus-replica
$SUDO env PATH="/usr/local/go/bin:$PATH" GOCACHE=/local/.gocache CGO_ENABLED=0 \
    "$GO" build -C "$SRC" -o "$BIN/paxosbus-client"  ./paxosbus/cmd/paxosbus-client

# Root swiftpaxos binary (paxos/epaxos master+server+client, one binary — used
# by run-swiftpaxos-cloudlab.sh; mirrors Dockerfile.swiftpaxos's build).
log "building swiftpaxos -> $BIN"
$SUDO env PATH="/usr/local/go/bin:$PATH" GOCACHE=/local/.gocache CGO_ENABLED=0 \
    "$GO" build -C "$SRC" -o "$BIN/swiftpaxos" .

# World-readable/executable so your user can run them even though root built them.
$SUDO chmod -R a+rX "$WORK"
$SUDO touch "$WORK/setup.done"

log "done. binaries:"
ls -l "$BIN"
echo "[setup] READY — run-cloudlab.sh can launch from $BIN"
