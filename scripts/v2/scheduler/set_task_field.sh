#!/usr/bin/env bash
# set_task_field.sh — sed 改 task.md 里指定 task_id block 的某个字段
#
# Usage: bash set_task_field.sh <task.md> <task_id> <field> <value>
#
# C2 fix (2026-05-02): 看到下一个 task heading (`^## task ` or another `^task_id:` line) 立刻退出 block,
# 防止跨 block 误改。
# C3 fix: flock 整个 read+rewrite，跟 submit_task 共享同一锁，防 append race。
# Whitelist (S3): 只允许改 status/claimed_ts/started_ts/completed_ts/result/result_summary/artifacts/error_log
set -euo pipefail

FILE="${1:?usage: set_task_field.sh <file> <task_id> <field> <value>}"
TASK_ID="${2:?need task_id}"
FIELD="${3:?need field}"
VALUE="${4:?need value}"

# Whitelist: scheduler can only mutate these (writer-owned fields blocked)
case "$FIELD" in
  status|claimed_ts|started_ts|completed_ts|result|result_summary|artifacts|error_log) ;;
  *) echo "ERROR: $FIELD not in scheduler-mutable whitelist" >&2; exit 2;;
esac

# Portable mkdir-based lock (works on POSIX + git-bash Windows; flock unavailable on git-bash)
LOCK="${FILE}.lockd"
WAITED=0
while ! mkdir "$LOCK" 2>/dev/null; do
  sleep 1
  WAITED=$((WAITED+1))
  if [ "$WAITED" -ge 30 ]; then
    echo "ERROR: lock timeout on $LOCK (held by $(cat $LOCK/holder 2>/dev/null))" >&2
    exit 3
  fi
done
echo "$$ set_task_field $(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$LOCK/holder"
trap "rm -rf '$LOCK' 2>/dev/null" EXIT

awk -v tid="$TASK_ID" -v field="$FIELD" -v value="$VALUE" '
  BEGIN { in_block=0; replaced=0 }
  # New task heading or a different task_id closes current block
  /^## task / {
    if (in_block) in_block=0
  }
  /^task_id:/ {
    line=$0; sub(/^task_id:[ \t]*/, "", line)
    if (line == tid) {
      in_block=1
    } else if (in_block) {
      # Different task_id encountered while supposedly inside our block — close
      in_block=0
    }
  }
  # `---` separator closes block
  in_block && /^---$/ {
    in_block=0
  }
  # R2 fix: 严格匹配 field 后必须紧跟冒号 + (行尾|空白)，防止 result 撞 result_summary
  in_block && $0 ~ ("^" field ":($|[ \t])") && !replaced {
    print field ": " value
    replaced=1
    next
  }
  { print }
' "$FILE" > "$FILE.tmp"

mv "$FILE.tmp" "$FILE"
rm -rf "$LOCK" 2>/dev/null
trap - EXIT

# The private prototype directly killed remote processes here. The public
# reconstruction exposes a narrow hook instead; endpoint credentials and
# process-selection policy stay outside the scheduler.
if [ "$FIELD" = "status" ] && [ "$VALUE" = "aborted" ] && [ -n "${ASCENDOP_ABORT_HOOK:-}" ]; then
  "$ASCENDOP_ABORT_HOOK" "$FILE" "$TASK_ID"
fi
