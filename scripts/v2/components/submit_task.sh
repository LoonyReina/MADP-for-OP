#!/usr/bin/env bash
# submit_task.sh — writer 提交一个新 task 到 logs/{Op}/task.md
#
# Writer 必填：--op --type --version
# 自动：task_id (单调每天序号), ready_ts (now)
# 默认: priority=0, status=pending, deploy_verified=no
#
# Schema 见 docs/contracts/task_md.md
#
# Usage:
#   bash scripts/v2/components/submit_task.sh \
#     --op DemoOp --type correctness --version 28 \
#     --vendor demo_v28 --cases "1..8" \
#     --notes "V28 fix int8"
#
# Returns task_id on stdout.
set -euo pipefail

OP=""
TYPE=""
VERSION=""
VENDOR=""
CASES=""
MSPROF_FLAGS=""
PRIORITY="0"
DEPLOY_VERIFIED="no"
NOTES=""

while [ $# -gt 0 ]; do
  case "$1" in
    --op)               OP="$2"; shift 2;;
    --type)             TYPE="$2"; shift 2;;
    --version)          VERSION="$2"; shift 2;;
    --vendor)           VENDOR="$2"; shift 2;;
    --cases)            CASES="$2"; shift 2;;
    --msprof-flags)     MSPROF_FLAGS="$2"; shift 2;;
    --priority)         PRIORITY="$2"; shift 2;;
    --deploy-verified)  DEPLOY_VERIFIED="yes"; shift;;
    --notes)            NOTES="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

[ -z "$OP" ] && { echo "ERROR: --op required" >&2; exit 2; }
[ -z "$TYPE" ] && { echo "ERROR: --type required" >&2; exit 2; }
[ -z "$VERSION" ] && { echo "ERROR: --version required" >&2; exit 2; }

case "$TYPE" in
  correctness|perf-time|perf-bin|perf-bin-dual|build-only|install-only|test-only) ;;
  *) echo "ERROR: bad type $TYPE; see docs/contracts/task_md.md" >&2; exit 2;;
esac

TASK_FILE="logs/$OP/task.md"
mkdir -p "$(dirname "$TASK_FILE")"
[ -f "$TASK_FILE" ] || touch "$TASK_FILE"

# C3 fix: portable mkdir-based lock (flock unavailable on git-bash Windows).
# Shared lock with set_task_field.sh: prevents concurrent append vs awk-rewrite race.
# S1 fix: id allocation inside lock so 2 simultaneous submits don't collide.
LOCK="${TASK_FILE}.lockd"
WAITED=0
while ! mkdir "$LOCK" 2>/dev/null; do
  sleep 1
  WAITED=$((WAITED+1))
  if [ "$WAITED" -ge 30 ]; then
    echo "ERROR: lock timeout on $LOCK (held by $(cat $LOCK/holder 2>/dev/null))" >&2
    exit 3
  fi
done
echo "$$ submit_task $(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$LOCK/holder"
trap "rm -rf '$LOCK' 2>/dev/null" EXIT

DATE=$(date -u +%Y%m%d)
PREFIX="T${DATE}-"
LAST_ID=$(grep -oE "^task_id: ${PREFIX}[0-9]+$" "$TASK_FILE" 2>/dev/null | tail -1 | grep -oE '[0-9]+$' || echo "")
if [ -z "$LAST_ID" ]; then
  N=1
else
  N=$((10#$LAST_ID + 1))
fi
TASK_ID=$(printf "%s%03d" "$PREFIX" "$N")

READY_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

cat >>"$TASK_FILE" <<EOF

## task ${TASK_ID}: ${TYPE} ${OP} v${VERSION}
task_id: ${TASK_ID}
type: ${TYPE}
status: pending
priority: ${PRIORITY}
op: ${OP}
version: ${VERSION}
vendor: ${VENDOR}
cases: "${CASES}"
msprof_flags: "${MSPROF_FLAGS}"
deploy_verified: ${DEPLOY_VERIFIED}
ready_ts: ${READY_TS}
claimed_ts:
started_ts:
completed_ts:
result:
result_summary:
artifacts:
error_log:
notes: |
  ${NOTES}

---
EOF

rm -rf "$LOCK" 2>/dev/null
trap - EXIT
echo "$TASK_ID"
