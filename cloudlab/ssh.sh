#!/usr/bin/env bash
set -euo pipefail

# Convenience SSH into one of the reserved CloudLab nodes by role.
#   cloudlab/ssh.sh client            # open a shell on the Utah (client) node
#   cloudlab/ssh.sh r0                # Wisconsin (leader)
#   cloudlab/ssh.sh r1 'tail -f /tmp/paxosbus.log'   # run a command remotely
#
# Roles: client|utah, r0|wisc, r1|clemson, r2|mass.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODES_FILE="${NODES_FILE:-$DIR/nodes.env}"
[[ -f "$NODES_FILE" ]] || { echo "missing $NODES_FILE (copy nodes.env.example)"; exit 1; }
# shellcheck disable=SC1090
source "$NODES_FILE"

case "${1:-}" in
    client|utah)    H=$CLIENT_HOST ;;
    r0|wisc)        H=$REPLICA0_HOST ;;
    r1|clemson)     H=$REPLICA1_HOST ;;
    r2|mass)        H=$REPLICA2_HOST ;;
    *) echo "usage: $0 <client|r0|r1|r2> [remote command...]"; exit 1 ;;
esac
[[ -n "$H" ]] || { echo "host for '$1' is empty in $NODES_FILE"; exit 1; }
shift

exec ssh -o StrictHostKeyChecking=accept-new "$SSH_USER@$H" "$@"
