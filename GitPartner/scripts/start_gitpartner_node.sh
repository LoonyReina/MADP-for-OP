#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash scripts/start_gitpartner_node.sh [--node-id ID] [--config PATH]
      [--foreground | --follow | --status | --stop | --force-restart]
      [--tail-lines N] [--follow-interval-seconds SECONDS]
  bash scripts/start_gitpartner_node.sh --enroll [--node-id ID]
      [--role client|server|local] [--non-interactive]
      [--transport-mode direct|relay] [--gateway-id ID] [--server-ssh HOST]
      [--no-bootstrap-channels] [--enroll-only]

The default action replaces and restarts only the selected enrolled node
service. A fresh clone fails closed with GITPARTNER_NODE_NOT_ENROLLED and
prints the enrollment command; it never guesses a node identity.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT="${GITPARTNER_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd -P)}"
ACTION="start"
ENROLL=0
ENROLL_ONLY=0
INTERACTIVE=1
BOOTSTRAP=1
NODE_ID=""
CONFIG=""
ROLE=""
REMOTE_ROOT=""
ENGINE_ROOT="test_engine_demo"
PUBLISH_MODE="auto"
IMPORT_LOGIN_NETWORK_ENV=0
TRANSPORT_MODE="direct"
GATEWAY_ID=""
SERVER_SSH=""
TAIL_LINES=""
FOLLOW_INTERVAL_SECONDS=""
FORCE_RESTART=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --enroll) ENROLL=1 ;;
    --enroll-only) ENROLL=1; ENROLL_ONLY=1 ;;
    --non-interactive) INTERACTIVE=0 ;;
    --no-bootstrap-channels) BOOTSTRAP=0 ;;
    --foreground) ACTION="foreground" ;;
    --follow) ACTION="follow" ;;
    --status) ACTION="status" ;;
    --stop) ACTION="stop" ;;
    --restart|--background) ACTION="start" ;;
    --force-restart) FORCE_RESTART=1 ;;
    --node-id|--node)
      [ "$#" -ge 2 ] || { echo "$1 requires a value" >&2; exit 2; }
      NODE_ID="$2"
      shift
      ;;
    --config)
      [ "$#" -ge 2 ] || { echo "--config requires a value" >&2; exit 2; }
      CONFIG="$2"
      shift
      ;;
    --role)
      [ "$#" -ge 2 ] || { echo "--role requires a value" >&2; exit 2; }
      ROLE="$2"
      shift
      ;;
    --remote-root)
      [ "$#" -ge 2 ] || { echo "--remote-root requires a value" >&2; exit 2; }
      REMOTE_ROOT="$2"
      shift
      ;;
    --engine-root)
      [ "$#" -ge 2 ] || { echo "--engine-root requires a value" >&2; exit 2; }
      ENGINE_ROOT="$2"
      shift
      ;;
    --publish-mode)
      [ "$#" -ge 2 ] || { echo "--publish-mode requires a value" >&2; exit 2; }
      PUBLISH_MODE="$2"
      shift
      ;;
    --import-login-network-env) IMPORT_LOGIN_NETWORK_ENV=1 ;;
    --transport-mode)
      [ "$#" -ge 2 ] || { echo "--transport-mode requires a value" >&2; exit 2; }
      TRANSPORT_MODE="$2"
      shift
      ;;
    --gateway-id)
      [ "$#" -ge 2 ] || { echo "--gateway-id requires a value" >&2; exit 2; }
      GATEWAY_ID="$2"
      shift
      ;;
    --server-ssh)
      [ "$#" -ge 2 ] || { echo "--server-ssh requires a value" >&2; exit 2; }
      SERVER_SSH="$2"
      shift
      ;;
    --tail-lines)
      [ "$#" -ge 2 ] || { echo "--tail-lines requires a value" >&2; exit 2; }
      TAIL_LINES="$2"
      shift
      ;;
    --follow-interval-seconds)
      [ "$#" -ge 2 ] || {
        echo "--follow-interval-seconds requires a value" >&2
        exit 2
      }
      FOLLOW_INTERVAL_SECONDS="$2"
      shift
      ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

cd "$ROOT"
if [ -f .venv/bin/activate ]; then
  # shellcheck disable=SC1091
  . .venv/bin/activate
fi
PYTHON_SOURCE="${GITPARTNER_RUNTIME_SOURCE:-${ROOT}/src}"
if [ -n "${GITPARTNER_RUNTIME_SOURCE:-}" ]; then
  case "$PYTHON_SOURCE" in
    /*) ;;
    *)
      echo "GITPARTNER_RUNTIME_SOURCE must be an absolute path" >&2
      exit 2
      ;;
  esac
  if [ ! -f "${PYTHON_SOURCE}/limited_remote_partner/endpoint/node_service.py" ]; then
    echo "GITPARTNER_RUNTIME_SOURCE is not a GitPartner runtime: ${PYTHON_SOURCE}" >&2
    exit 2
  fi
fi
PYTHONPATH_SEPARATOR=":"
case "$(uname -s 2>/dev/null || true)" in
  MINGW*|MSYS*|CYGWIN*)
    if command -v cygpath >/dev/null 2>&1; then
      PYTHON_SOURCE="$(cygpath -w "$PYTHON_SOURCE")"
    fi
    PYTHONPATH_SEPARATOR=";"
    ;;
esac
if [ -n "${PYTHONPATH:-}" ]; then
  export PYTHONPATH="${PYTHON_SOURCE}${PYTHONPATH_SEPARATOR}${PYTHONPATH}"
else
  export PYTHONPATH="${PYTHON_SOURCE}"
fi
export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"
export GIT_TERMINAL_PROMPT=0
unset GIT_ASKPASS SSH_ASKPASS

if [ "$ACTION" = "start" ] || [ "$ACTION" = "foreground" ]; then
  if [ ! -s "${ROOT}/api.txt" ]; then
    echo "GITPARTNER_REPO_TOKEN_MISSING path=${ROOT}/api.txt" >&2
    echo "place the Gitee token in repository-root api.txt and chmod 600 it" >&2
    exit 8
  fi
fi

PYTHON_BIN="${GITPARTNER_PYTHON:-}"
if [ -z "$PYTHON_BIN" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "GITPARTNER_PYTHON_NOT_FOUND install Python 3 or set GITPARTNER_PYTHON" >&2
    exit 127
  fi
fi

if [ -z "$CONFIG" ] && [ -n "${GITPARTNER_RUNTIME_CONFIG:-}" ]; then
  CONFIG="${GITPARTNER_RUNTIME_CONFIG}"
fi

if [ -z "$CONFIG" ] && [ -n "$NODE_ID" ] && [ -f "configs/nodes/${NODE_ID}.json" ]; then
  CONFIG="configs/nodes/${NODE_ID}.json"
fi

if [ "$ACTION" = "start" ] || [ "$ACTION" = "foreground" ]; then
  network_args=()
  if [ -n "$CONFIG" ]; then
    network_args+=(--config "$CONFIG")
  fi
  if [ "$IMPORT_LOGIN_NETWORK_ENV" = "1" ]; then
    network_args+=(--enable-login-shell-import)
  fi
  NETWORK_EXPORTS="$("$PYTHON_BIN" -m limited_remote_partner.core.login_environment shell-exports "${network_args[@]}")"
  if [ -n "$NETWORK_EXPORTS" ]; then
    eval "$NETWORK_EXPORTS"
  fi
  "$PYTHON_BIN" -m limited_remote_partner.core.login_environment report "${network_args[@]}"
fi

if [ "$ENROLL" = "1" ]; then
  if [ -z "$NODE_ID" ] && [ -z "$CONFIG" ]; then
    shopt -s nullglob
    enrolled_configs=("${ROOT}"/configs/nodes/*.json)
    shopt -u nullglob
    if [ "${#enrolled_configs[@]}" -eq 1 ]; then
      CONFIG="${enrolled_configs[0]#"${ROOT}/"}"
      NODE_ID="$(basename "${enrolled_configs[0]}" .json)"
      echo "GITPARTNER_ENROLL_RESUME node_id=${NODE_ID} config=${CONFIG}"
    fi
  fi
  if [ -z "$NODE_ID" ]; then
    if [ "$INTERACTIVE" != "1" ] || [ ! -t 0 ]; then
      echo "GITPARTNER_NODE_ID_REQUIRED pass --node-id for non-interactive enrollment" >&2
      exit 4
    fi
    read -r -p "Node ID: " NODE_ID
  fi
  NODE_ID="$(printf '%s' "$NODE_ID" | tr '[:upper:]' '[:lower:]')"
  [ -n "$NODE_ID" ] || { echo "GITPARTNER_NODE_ID_REQUIRED" >&2; exit 4; }
  if [ -z "$CONFIG" ]; then
    CONFIG="configs/nodes/${NODE_ID}.json"
  fi
  if [ -f "$CONFIG" ]; then
    echo "GITPARTNER_ENROLL_CHECKPOINT_FOUND node_id=${NODE_ID} config=${CONFIG}"
    "$PYTHON_BIN" -m limited_remote_partner.cli.endpoint_cli resume-node \
      --config "$CONFIG" \
      --repo-dir "$ROOT"
  else
    init_args=(
      --node "$NODE_ID"
      --base-config configs/partner.json
      --output "$CONFIG"
      --repo-dir "$ROOT"
      --remote-root "$REMOTE_ROOT"
      --engine-root "$ENGINE_ROOT"
      --publish-mode "$PUBLISH_MODE"
      --transport-mode "$TRANSPORT_MODE"
    )
    if [ "$IMPORT_LOGIN_NETWORK_ENV" = "1" ]; then
      init_args+=(--import-login-network-env)
    fi
    if [ -n "$ROLE" ]; then
      init_args+=(--role "$ROLE")
    fi
    if [ -n "$GATEWAY_ID" ]; then
      init_args+=(--gateway-id "$GATEWAY_ID")
    fi
    if [ -n "$SERVER_SSH" ]; then
      init_args+=(--server-ssh "$SERVER_SSH")
    fi
    if [ "$INTERACTIVE" = "1" ] && [ -t 0 ]; then
      init_args+=(--interactive)
    fi
    if [ "$BOOTSTRAP" != "1" ]; then
      init_args+=(--no-bootstrap-channels)
    fi
    "$PYTHON_BIN" -m limited_remote_partner.cli.endpoint_cli init-node "${init_args[@]}"
  fi
  if [ "$ENROLL_ONLY" = "1" ]; then
    exit 0
  fi
fi

service_args=("$ACTION" --repo-dir "$ROOT")
if [ -n "$CONFIG" ]; then
  service_args+=(--config "$CONFIG")
fi
if [ -n "$NODE_ID" ]; then
  service_args+=(--node "$NODE_ID")
fi
if [ -n "$ROLE" ]; then
  service_args+=(--role "$ROLE")
fi
if [ -n "$TAIL_LINES" ]; then
  service_args+=(--tail-lines "$TAIL_LINES")
fi
if [ -n "$FOLLOW_INTERVAL_SECONDS" ]; then
  service_args+=(--follow-interval-seconds "$FOLLOW_INTERVAL_SECONDS")
fi
if [ "$FORCE_RESTART" = "1" ]; then
  service_args+=(--force-restart)
fi
exec "$PYTHON_BIN" -m limited_remote_partner.endpoint.node_service "${service_args[@]}"
