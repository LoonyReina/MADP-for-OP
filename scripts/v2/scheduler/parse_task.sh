#!/usr/bin/env bash
# parse_task.sh — 提取 task.md 里指定 task_id 的 block 字段
#
# C1 fix (2026-05-02): 进入 `notes: |` block 后用缩进规则识别——
# notes 体直到下一个 left-aligned non-empty line（`---` 或 next 字段）才结束。
# 这样 notes 里写 `## task ` 子标题也不会误判 block 结束。
#
# Usage: bash parse_task.sh <file> <task_id> [--field=NAME]
#   未指定 --field 输出整个 block；指定 --field=X 只输出该字段值

set -euo pipefail

FILE="${1:?usage: parse_task.sh <task.md> <task_id> [--field=NAME]}"
TASK_ID="${2:?need task_id}"
FIELD=""
[[ "${3:-}" == --field=* ]] && FIELD="${3#--field=}"

awk -v tid="$TASK_ID" -v field="$FIELD" '
  function is_block_terminator(line) {
    # `---` 或下一个 ## task heading 或下一个 task_id 都关闭 block
    return (line == "---" || line ~ /^## task / || line ~ /^task_id:/)
  }
  BEGIN { in_block=0; in_notes=0 }
  {
    line = $0
    if (in_block && in_notes) {
      # in notes body: check if line is unindented (i.e. column 0 starts a key:value)
      # YAML pipe-block: any line indented with whitespace is part of notes; non-indented = block end
      if (line ~ /^[a-zA-Z_]+:/ || line == "---" || line ~ /^## task /) {
        in_notes = 0
        # fall through to handle this line normally
      } else {
        # still inside notes body — not a structural line
        if (field == "" || field == "notes") print line
        next
      }
    }
    if (in_block) {
      if (is_block_terminator(line) && line != "task_id: " tid) {
        # tid line of OTHER task or `---` or new heading
        if (line ~ /^task_id:/) {
          tline = line; sub(/^task_id:[ \t]*/, "", tline)
          if (tline != tid) { in_block = 0 }
        } else {
          in_block = 0
        }
      }
    } else {
      if (line ~ /^task_id:/) {
        tline = line; sub(/^task_id:[ \t]*/, "", tline)
        if (tline == tid) { in_block = 1 }
      }
    }
    if (!in_block) next
    # in_block: emit / extract field
    if (line ~ /^notes:[ \t]*\|?[ \t]*$/) {
      in_notes = 1
      if (field == "") print line
      next
    }
    if (field != "" && line ~ "^" field ":") {
      sub("^" field ":[ \t]*", "", line)
      gsub(/^"|"$/, "", line)
      print line
      exit
    }
    if (field == "") print line
  }
' "$FILE"
