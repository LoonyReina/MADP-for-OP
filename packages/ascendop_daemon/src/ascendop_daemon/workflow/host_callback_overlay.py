from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from ascendop_daemon.workflow.operator_job_errors import EngineJobBuildError
from ascendop_daemon.workflow.operator_job_files import tree_digest


HOST_CALLBACK_ATTRIBUTION_FLAG = "host-callback-attribution"
HOST_CALLBACK_OVERLAY_MARKER = "ASCENDOP_HOST_CALLBACK_OVERLAY_V1"
HOST_CALLBACK_OVERLAY_AUDIT = "ASCENDOP_HOST_CALLBACK_OVERLAY.json"

_TILING_REGISTRATION = re.compile(
    r"IMPL_OP_OPTILING\s*\([^)]*\)\s*\.Tiling\s*\(\s*([A-Za-z_]\w*)\s*\)",
    re.MULTILINE,
)

_HOST_CALLBACK_HELPER = r'''
// ASCENDOP_HOST_CALLBACK_OVERLAY_V1
#include <chrono>
#include <cstdlib>
#include <exception>
#include <execinfo.h>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <new>
#include <sstream>
#include <string>

static inline std::mutex &AscendopHostCallbackMutex() {
  static std::mutex callback_mutex;
  return callback_mutex;
}

static inline std::string AscendopHostCallbackJsonEscape(const char *value) {
  std::ostringstream escaped;
  for (const unsigned char character : std::string(value == nullptr ? "" : value)) {
    switch (character) {
      case '\\': escaped << "\\\\"; break;
      case '"': escaped << "\\\""; break;
      case '\n': escaped << "\\n"; break;
      case '\r': escaped << "\\r"; break;
      case '\t': escaped << "\\t"; break;
      default:
        if (character < 0x20) {
          escaped << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                  << static_cast<unsigned int>(character) << std::dec;
        } else {
          escaped << character;
        }
    }
  }
  return escaped.str();
}

static inline void AscendopHostCallbackTrace(
    const char *phase, const char *callback, const char *status = "",
    const char *exception_type = "", const char *error = "") {
  const char *path = std::getenv("ASCENDOP_RUNTIME_BOUNDARY_TRACE_FILE");
  if (path == nullptr || path[0] == '\0') {
    return;
  }
  const auto monotonic_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
  void *frames[64];
  const bool capture_frames = exception_type != nullptr && exception_type[0] != '\0';
  const int frame_count = capture_frames ? backtrace(frames, 64) : 0;
  char **symbols = frame_count > 0 ? backtrace_symbols(frames, frame_count) : nullptr;
  std::lock_guard<std::mutex> guard(AscendopHostCallbackMutex());
  std::ofstream output(path, std::ios::app);
  output << "{\"schema\":\"ascendop.host-callback-event.v1\","
         << "\"phase\":\"" << AscendopHostCallbackJsonEscape(phase) << "\","
         << "\"callback\":\"" << AscendopHostCallbackJsonEscape(callback) << "\","
         << "\"status\":\"" << AscendopHostCallbackJsonEscape(status) << "\","
         << "\"exception_type\":\""
         << AscendopHostCallbackJsonEscape(exception_type) << "\","
         << "\"error\":\"" << AscendopHostCallbackJsonEscape(error) << "\","
         << "\"monotonic_ns\":" << monotonic_ns << ",\"native_frames\":[";
  for (int index = 0; index < frame_count; ++index) {
    if (index != 0) {
      output << ",";
    }
    output << "\"" << AscendopHostCallbackJsonEscape(
        symbols == nullptr ? "" : symbols[index]) << "\"";
  }
  output << "]}" << std::endl;
  std::free(symbols);
}
'''.strip()


def apply_host_callback_overlay(
    source_snapshot: Path,
    *,
    audit_copy: Path | None = None,
) -> dict[str, Any]:
    """Instrument registered tiling callbacks in an isolated source snapshot."""

    source_snapshot = source_snapshot.resolve()
    op_host = source_snapshot / "op_host"
    if not op_host.is_dir():
        raise EngineJobBuildError(
            f"host callback source directory is missing: {op_host}"
        )

    patched: list[dict[str, Any]] = []
    for path in sorted(op_host.glob("*_tiling.cpp")):
        before = path.read_text(encoding="utf-8", errors="strict")
        callbacks = tuple(dict.fromkeys(_TILING_REGISTRATION.findall(before)))
        if not callbacks:
            continue
        after = before
        if HOST_CALLBACK_OVERLAY_MARKER not in after:
            after = _HOST_CALLBACK_HELPER + "\n\n" + after
        for callback in callbacks:
            after = _instrument_callback(after, callback)
        path.write_text(after, encoding="utf-8")
        patched.append(
            {
                "path": path.relative_to(source_snapshot).as_posix(),
                "callbacks": list(callbacks),
                "adapter": "tiling-callback-boundary-v1",
            }
        )

    if not patched:
        raise EngineJobBuildError(
            "host-callback-attribution found no registered tiling callback"
        )

    audit = {
        "schema": "ascendop.host-callback-overlay.v1",
        "flag": HOST_CALLBACK_ATTRIBUTION_FLAG,
        "status": "implemented",
        "patched": patched,
        "source_snapshot_sha256": tree_digest(source_snapshot),
    }
    payload = json.dumps(audit, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    (source_snapshot / HOST_CALLBACK_OVERLAY_AUDIT).write_text(
        payload, encoding="utf-8"
    )
    if audit_copy is not None:
        audit_copy.parent.mkdir(parents=True, exist_ok=True)
        audit_copy.write_text(payload, encoding="utf-8")
    return audit


def _instrument_callback(text: str, callback: str) -> str:
    definition = re.search(
        rf"\b(?:static\s+)?ge::graphStatus\s+{re.escape(callback)}\s*\([^)]*\)\s*\{{",
        text,
        re.MULTILINE,
    )
    if definition is None:
        raise EngineJobBuildError(
            f"registered tiling callback definition is missing: {callback}"
        )
    opening = text.find("{", definition.start())
    closing = _matching_brace(text, opening)
    body = text[opening + 1 : closing]
    if f'AscendopHostCallbackTrace("enter", "{callback}")' in body:
        return text

    def instrument_return(match: re.Match[str]) -> str:
        expression = " ".join(match.group(1).split())
        escaped = expression.replace("\\", "\\\\").replace('"', '\\"')
        return (
            '{ AscendopHostCallbackTrace("return", "'
            + callback
            + '", "'
            + escaped
            + '"); return '
            + match.group(1).strip()
            + "; }"
        )

    instrumented = re.sub(r"\breturn\s+([^;{}]+);", instrument_return, body)
    wrapped = (
        "\n  AscendopHostCallbackTrace(\"enter\", \""
        + callback
        + "\");\n  try {"
        + instrumented
        + "\n  } catch (const std::bad_alloc &ascendop_exception) {\n"
        + "    AscendopHostCallbackTrace(\"exception\", \""
        + callback
        + "\", \"\", \"std::bad_alloc\", ascendop_exception.what());\n"
        + "    throw;\n"
        + "  } catch (const std::exception &ascendop_exception) {\n"
        + "    AscendopHostCallbackTrace(\"exception\", \""
        + callback
        + "\", \"\", \"std::exception\", ascendop_exception.what());\n"
        + "    throw;\n"
        + "  } catch (...) {\n"
        + "    AscendopHostCallbackTrace(\"exception\", \""
        + callback
        + "\", \"\", \"unknown\", \"non-std exception\");\n"
        + "    throw;\n"
        + "  }\n"
    )
    return text[: opening + 1] + wrapped + text[closing:]


def _matching_brace(text: str, opening: int) -> int:
    depth = 0
    index = opening
    state = "code"
    while index < len(text):
        current = text[index]
        following = text[index + 1] if index + 1 < len(text) else ""
        if state == "code":
            if current == '"':
                state = "string"
            elif current == "'":
                state = "character"
            elif current == "/" and following == "/":
                state = "line-comment"
                index += 1
            elif current == "/" and following == "*":
                state = "block-comment"
                index += 1
            elif current == "{":
                depth += 1
            elif current == "}":
                depth -= 1
                if depth == 0:
                    return index
        elif state in {"string", "character"}:
            if current == "\\":
                index += 1
            elif (state == "string" and current == '"') or (
                state == "character" and current == "'"
            ):
                state = "code"
        elif state == "line-comment" and current == "\n":
            state = "code"
        elif state == "block-comment" and current == "*" and following == "/":
            state = "code"
            index += 1
        index += 1
    raise EngineJobBuildError("registered tiling callback body is unbalanced")


__all__ = [
    "HOST_CALLBACK_ATTRIBUTION_FLAG",
    "HOST_CALLBACK_OVERLAY_AUDIT",
    "HOST_CALLBACK_OVERLAY_MARKER",
    "apply_host_callback_overlay",
]
