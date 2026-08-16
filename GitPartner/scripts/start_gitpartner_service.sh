#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="${GITPARTNER_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd -P)}"

NODE_ID=""
ENROLL_REQUESTED=0
previous=""
for argument in "$@"; do
  if [ "$previous" = "--node" ] || [ "$previous" = "--node-id" ]; then
    NODE_ID="$argument"
    previous=""
    continue
  fi
  case "$argument" in
    --node|--node-id) previous="$argument" ;;
    --enroll|--enroll-only) ENROLL_REQUESTED=1 ;;
  esac
done

if [ -n "$NODE_ID" ] && [ "$ENROLL_REQUESTED" != "1" ]; then
  if [ -f "${ROOT}/configs/nodes/${NODE_ID}.json" ]; then
    exec bash "${SCRIPT_DIR}/start_gitpartner_node.sh" "$@"
  fi
  PYTHON_BIN="${GITPARTNER_PYTHON:-}"
  if [ -z "$PYTHON_BIN" ]; then
    if command -v python3 >/dev/null 2>&1; then
      PYTHON_BIN="python3"
    else
      PYTHON_BIN="python"
    fi
  fi
  export PYTHONPATH="${ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"
  MANIFEST="${ROOT}/configs/node_launchers.json"
  if [ ! -f "$MANIFEST" ]; then
    echo "GITPARTNER_NODE_LAUNCH_ERROR registered launcher manifest is missing: $MANIFEST" >&2
    exit 2
  fi
  "$PYTHON_BIN" -c \
    'from pathlib import Path; from limited_remote_partner.endpoint.node_launcher import has_registered_launcher; import sys; raise SystemExit(0 if has_registered_launcher(Path(sys.argv[1]), sys.argv[2]) else 1)' \
    "$MANIFEST" "$NODE_ID" || {
      echo "GITPARTNER_NODE_LAUNCH_ERROR node is not present in the registered launcher manifest: $NODE_ID" >&2
      exit 2
    }
  exec "$PYTHON_BIN" -m limited_remote_partner.endpoint.node_launcher \
    --manifest "$MANIFEST" "$@"
fi

if [ "${1:-}" = "--legacy-a-server" ] || [ "${1:-}" = "--a-server" ]; then
  shift
  exec bash "${SCRIPT_DIR}/start_a_server_relay.sh" "$@"
fi

for argument in "$@"; do
  case "$argument" in
    --enroll|--enroll-only|--node|--node-id|--config|--non-interactive|\
    --no-bootstrap-channels|--publish-mode|--remote-root|--engine-root|\
    --stop|--restart|--force-restart)
      exec bash "${SCRIPT_DIR}/start_gitpartner_node.sh" "$@"
      ;;
  esac
done

shopt -s nullglob
node_configs=("${ROOT}"/configs/nodes/*.json)
shopt -u nullglob
if [ "${#node_configs[@]}" -gt 0 ]; then
  exec bash "${SCRIPT_DIR}/start_gitpartner_node.sh" "$@"
fi

# Preserve already-provisioned A-side installations. A fresh clone has no
# runtime marker and therefore follows the generic node path, which reports
# NODE_NOT_ENROLLED instead of silently assuming an identity.
if [ -n "${GITPARTNER_CONFIG:-}" ] || \
   [ -f "${ROOT}/.partner_state/effective-server.json" ] || \
   [ -f "${ROOT}/work/git-partner-server-supervisor.pid" ]; then
  exec bash "${SCRIPT_DIR}/start_a_server_relay.sh" "$@"
fi

exec bash "${SCRIPT_DIR}/start_gitpartner_node.sh" "$@"
