#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash scripts/install_gitpartner_endpoint_service.sh \
    --endpoint-id ID \
    --gp-root /workspace/AscendOP/ascend-git-partner \
    --runtime-source /workspace/AscendOP/runtime/gitpartner/generations/GEN/src \
    --protocol-source /workspace/AscendOP/runtime/protocol/generations/GEN/src \
    --config /workspace/AscendOP/ascend-git-partner/configs/endpoints/ID.json \
    [--cann-env-script /usr/local/Ascend/cann-9.0.0/set_env.sh] \
    [--git-timeout-seconds 60] \
    [--service-manager auto|systemd|resident] \
    [--enable-now]

Installs one endpoint's immutable runtime binding and shared systemd unit.
The GP repository token must already exist at GP_ROOT/api.txt.
EOF
}

ENDPOINT_ID=""
GP_ROOT=""
RUNTIME_SOURCE=""
PROTOCOL_SOURCE=""
CONFIG=""
CANN_ENV_SCRIPT=""
GIT_TIMEOUT_SECONDS="60"
SERVICE_MANAGER="auto"
ENABLE_NOW=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --endpoint-id) ENDPOINT_ID="${2:-}"; shift ;;
    --gp-root) GP_ROOT="${2:-}"; shift ;;
    --runtime-source) RUNTIME_SOURCE="${2:-}"; shift ;;
    --protocol-source) PROTOCOL_SOURCE="${2:-}"; shift ;;
    --config) CONFIG="${2:-}"; shift ;;
    --cann-env-script) CANN_ENV_SCRIPT="${2:-}"; shift ;;
    --git-timeout-seconds) GIT_TIMEOUT_SECONDS="${2:-}"; shift ;;
    --service-manager) SERVICE_MANAGER="${2:-}"; shift ;;
    --enable-now) ENABLE_NOW=1 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

if ! [[ "$ENDPOINT_ID" =~ ^[a-z0-9][a-z0-9._-]*$ ]]; then
  echo "GITPARTNER_INSTALL_INVALID_ENDPOINT_ID value=${ENDPOINT_ID:-unset}" >&2
  exit 2
fi
if [[ "$SERVICE_MANAGER" != "auto" && "$SERVICE_MANAGER" != "systemd" && "$SERVICE_MANAGER" != "resident" ]]; then
  echo "GITPARTNER_INSTALL_INVALID_SERVICE_MANAGER value=${SERVICE_MANAGER}" >&2
  exit 2
fi
if ! [[ "$GIT_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || [ "$GIT_TIMEOUT_SECONDS" -lt 15 ] || [ "$GIT_TIMEOUT_SECONDS" -gt 120 ]; then
  echo "GITPARTNER_INSTALL_INVALID_GIT_TIMEOUT_SECONDS value=${GIT_TIMEOUT_SECONDS}" >&2
  exit 2
fi

require_absolute_dir() {
  local label="$1"
  local value="$2"
  if [ -z "$value" ] || [ "${value#/}" = "$value" ] || [ ! -d "$value" ]; then
    echo "GITPARTNER_INSTALL_INVALID_${label} value=${value:-unset}" >&2
    exit 2
  fi
}

require_absolute_file() {
  local label="$1"
  local value="$2"
  if [ -z "$value" ] || [ "${value#/}" = "$value" ] || [ ! -f "$value" ]; then
    echo "GITPARTNER_INSTALL_INVALID_${label} value=${value:-unset}" >&2
    exit 2
  fi
}

require_safe_environment_value() {
  local label="$1"
  local value="$2"
  if [[ "$value" == *$'\n'* ]] || [[ "$value" == *$'\r'* ]] || [[ "$value" == *\"* ]]; then
    echo "GITPARTNER_INSTALL_UNSAFE_${label}" >&2
    exit 2
  fi
}

require_absolute_dir GP_ROOT "$GP_ROOT"
require_absolute_dir RUNTIME_SOURCE "$RUNTIME_SOURCE"
require_absolute_dir PROTOCOL_SOURCE "$PROTOCOL_SOURCE"
require_absolute_file CONFIG "$CONFIG"
require_absolute_file TOKEN "${GP_ROOT}/api.txt"
require_absolute_file WRAPPER "${GP_ROOT}/scripts/run_gitpartner_endpoint_service.sh"
require_absolute_file UNIT "${GP_ROOT}/services/ascendop-gp-endpoint@.service"
if [ -n "$CANN_ENV_SCRIPT" ]; then
  require_absolute_file CANN_ENV_SCRIPT "$CANN_ENV_SCRIPT"
fi
for pair in \
  "GP_ROOT=${GP_ROOT}" \
  "RUNTIME_SOURCE=${RUNTIME_SOURCE}" \
  "PROTOCOL_SOURCE=${PROTOCOL_SOURCE}" \
  "CONFIG=${CONFIG}" \
  "CANN_ENV_SCRIPT=${CANN_ENV_SCRIPT}" \
  "GIT_TIMEOUT_SECONDS=${GIT_TIMEOUT_SECONDS}"; do
  require_safe_environment_value "${pair%%=*}" "${pair#*=}"
done

install -d -m 0755 /etc/ascendop/endpoints
ENV_FILE="/etc/ascendop/endpoints/${ENDPOINT_ID}.env"
TEMP_FILE="$(mktemp)"
trap 'rm -f "$TEMP_FILE"' EXIT
printf '%s\n' \
  "ASCENDOP_GP_ROOT=\"${GP_ROOT}\"" \
  "GITPARTNER_RUNTIME_SOURCE=\"${RUNTIME_SOURCE}\"" \
  "GITPARTNER_PROTOCOL_SOURCE=\"${PROTOCOL_SOURCE}\"" \
  "GITPARTNER_RUNTIME_CONFIG=\"${CONFIG}\"" \
  "ASCENDOP_CANN_ENV_SCRIPT=\"${CANN_ENV_SCRIPT}\"" \
  "GITPARTNER_GIT_TIMEOUT_SECONDS=\"${GIT_TIMEOUT_SECONDS}\"" \
  > "$TEMP_FILE"
install -m 0600 "$TEMP_FILE" "$ENV_FILE"

GITPARTNER_GIT_TIMEOUT_SECONDS="${GIT_TIMEOUT_SECONDS}" \
  PYTHONPATH="${RUNTIME_SOURCE}:${PROTOCOL_SOURCE}${PYTHONPATH:+:${PYTHONPATH}}" \
  python3 -m limited_remote_partner.cli.enroll_materialized \
  --config "$CONFIG" \
  --repo-dir "$GP_ROOT"

if [ "$SERVICE_MANAGER" = "auto" ]; then
  if [ -d /run/systemd/system ] && [ "$(cat /proc/1/comm 2>/dev/null || true)" = "systemd" ]; then
    SERVICE_MANAGER="systemd"
  else
    SERVICE_MANAGER="resident"
  fi
fi

if [ "$SERVICE_MANAGER" = "systemd" ]; then
  install -m 0644 \
    "${GP_ROOT}/services/ascendop-gp-endpoint@.service" \
    /etc/systemd/system/ascendop-gp-endpoint@.service
  systemctl daemon-reload
  if [ "$ENABLE_NOW" = "1" ]; then
    systemctl enable --now "ascendop-gp-endpoint@${ENDPOINT_ID}.service"
  fi
elif [ "$ENABLE_NOW" = "1" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
  bash "${GP_ROOT}/scripts/run_gitpartner_endpoint_service.sh" --background
fi
echo "GITPARTNER_ENDPOINT_SERVICE_INSTALLED endpoint_id=${ENDPOINT_ID} env=${ENV_FILE} service_manager=${SERVICE_MANAGER} enabled_now=${ENABLE_NOW}"
