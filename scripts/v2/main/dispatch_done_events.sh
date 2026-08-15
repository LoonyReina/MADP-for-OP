#!/usr/bin/env bash
# dispatch_done_events.sh — main wakeup utility, scan _history.jsonl for new
# task completions since last dispatch and emit per-writer message drafts.
#
# Push design (vs writer self-poll): main is already wakeup-driven, runs this
# at each tick, and uses SendMessage to push to writer. Writer stays idle until
# notified.
#
# State: logs/_dispatched_cursor (single line, "<lineno>")
#
# Output (stdout): one JSON-ish record per writer to notify, fields:
#   {"op": "...", "task_id": "...", "status": "done|failed|...", "summary": "...",
#    "next_hint": "..."}
# Main caller parses these and SendMessage to writer named lowercase op.
#
# Usage:
#   bash scripts/v2/main/dispatch_done_events.sh        # print new events
#   bash scripts/v2/main/dispatch_done_events.sh --reset # reset cursor

set -uo pipefail

HIST=logs/_history.jsonl
CURSOR=logs/_dispatched_cursor
[ ! -f "$HIST" ] && { echo "no history yet"; exit 0; }

if [ "${1:-}" = "--reset" ]; then
  rm -f "$CURSOR"
  echo "cursor reset"
  exit 0
fi

last=$(cat "$CURSOR" 2>/dev/null || echo 0)
total=$(wc -l <"$HIST" | tr -d ' ')
total=${total:-0}

if [ "$total" -le "$last" ]; then
  exit 0
fi

# Scan new lines [last+1, total]
tail -n +"$((last + 1))" "$HIST" | while IFS= read -r line; do
  [ -z "$line" ] && continue
  # crude jq-less parse (we wrote these lines, format known)
  op=$(echo "$line" | grep -oE '"op":"[^"]+"' | head -1 | sed 's/"op":"\(.*\)"/\1/')
  tid=$(echo "$line" | grep -oE '"task_id":"[^"]+"' | head -1 | sed 's/"task_id":"\(.*\)"/\1/')
  status=$(echo "$line" | grep -oE '"status":"[^"]+"' | head -1 | sed 's/"status":"\(.*\)"/\1/')
  summary=$(echo "$line" | grep -oE '"summary":"[^"]+"' | head -1 | sed 's/"summary":"\(.*\)"/\1/')

  [ -z "$op" ] || [ -z "$tid" ] && continue

  # Look up task type to compute next_hint
  task_md="logs/$op/task.md"
  if [ -f "$task_md" ]; then
    type=$(awk -v t="$tid" '
      /^## task / { intask=0 }
      $0 ~ "^## task " t ":" { intask=1 }
      intask && /^type:/ { sub(/^type:[ \t]*/, ""); print; exit }
    ' "$task_md")
  fi

  # Heuristic next_hint by (status, type)
  hint=""
  case "$status:$type" in
    done:correctness)  hint="rule13: submit perf-time next" ;;
    done:perf-time)    hint="rule13/14: check case 6/7/8 timing; if all >=100us submit perf-bin --cases=6,7,8; else grow size + resubmit" ;;
    done:perf-bin)     hint="rule13: read timeline/heatmap/roofline -> design V_(n+1)" ;;
    failed:*)          hint="diagnose-first (rule 8): inspect run.log, dump real vs golden before code change" ;;
    timeout:*)         hint="task hit 1800s timeout; investigate hang or split case scope" ;;
  esac

  printf '{"op":"%s","task_id":"%s","status":"%s","type":"%s","summary":"%s","next_hint":"%s"}\n' \
    "$op" "$tid" "$status" "${type:-?}" "$summary" "$hint"
done

echo "$total" >"$CURSOR"
