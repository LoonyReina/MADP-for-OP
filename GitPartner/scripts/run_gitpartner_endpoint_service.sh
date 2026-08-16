#!/usr/bin/env bash
set -euo pipefail

require_absolute_dir() {
  local name="$1"
  local value="${!name:-}"
  if [ -z "$value" ] || [ "${value#/}" = "$value" ] || [ ! -d "$value" ]; then
    echo "GITPARTNER_SERVICE_INVALID_${name} value=${value:-unset}" >&2
    exit 2
  fi
}

require_absolute_file() {
  local name="$1"
  local value="${!name:-}"
  if [ -z "$value" ] || [ "${value#/}" = "$value" ] || [ ! -f "$value" ]; then
    echo "GITPARTNER_SERVICE_INVALID_${name} value=${value:-unset}" >&2
    exit 2
  fi
}

require_absolute_dir ASCENDOP_GP_ROOT
require_absolute_dir GITPARTNER_RUNTIME_SOURCE
require_absolute_dir GITPARTNER_PROTOCOL_SOURCE
require_absolute_file GITPARTNER_RUNTIME_CONFIG

if [ -n "${ASCENDOP_CANN_ENV_SCRIPT:-}" ]; then
  require_absolute_file ASCENDOP_CANN_ENV_SCRIPT
  set +u
  # shellcheck disable=SC1090
  . "${ASCENDOP_CANN_ENV_SCRIPT}"
  set -u
fi

export PYTHONPATH="${GITPARTNER_RUNTIME_SOURCE}:${GITPARTNER_PROTOCOL_SOURCE}${PYTHONPATH:+:${PYTHONPATH}}"
export GITPARTNER_ROOT="${ASCENDOP_GP_ROOT}"
export GITPARTNER_GIT_TIMEOUT_SECONDS="${GITPARTNER_GIT_TIMEOUT_SECONDS:-60}"
export PYTHONUNBUFFERED=1

cd "${ASCENDOP_GP_ROOT}"
LAUNCHER_ARGS=("$@")
if [ "${#LAUNCHER_ARGS[@]}" -eq 0 ]; then
  LAUNCHER_ARGS=(--foreground)
fi
exec /usr/bin/env bash scripts/start_gitpartner_node.sh \
  "${LAUNCHER_ARGS[@]}" \
  --config "${GITPARTNER_RUNTIME_CONFIG}"
