from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ascendop_daemon.workflow.operator_job_errors import EngineJobBuildError
from ascendop_daemon.workflow.operator_job_files import tree_digest


TRACE_FLAG = "runtime-boundary-trace"
NATIVE_WORKSPACE_QUERY_ATTRIBUTION_FLAG = "native-workspace-query-attribution"
TRACE_MARKER = "ASCENDOP_RUNTIME_BOUNDARY_TRACE_OVERLAY_V2"

_TRACE_HELPER = r"""
// ASCENDOP_RUNTIME_BOUNDARY_TRACE_OVERLAY_V2
#include <chrono>
#include <cstdlib>
#include <execinfo.h>
#include <fstream>
#include <iomanip>
#include <limits>
#include <mutex>
#include <new>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

static inline std::mutex &AscendopRuntimeBoundaryMutex() {
  static std::mutex trace_mutex;
  return trace_mutex;
}

static inline std::string AscendopRuntimeJsonEscape(const char *value) {
  std::ostringstream escaped;
  for (const unsigned char character : std::string(value == nullptr ? "" : value)) {
    switch (character) {
      case '\\': escaped << "\\\\"; break;
      case '"': escaped << "\\\""; break;
      case '\b': escaped << "\\b"; break;
      case '\f': escaped << "\\f"; break;
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

static inline void AscendopRuntimeProcessMemory(
    unsigned long long &virtual_bytes, unsigned long long &resident_bytes) {
  virtual_bytes = 0;
  resident_bytes = 0;
  unsigned long long virtual_pages = 0;
  unsigned long long resident_pages = 0;
  std::ifstream input("/proc/self/statm");
  if (!(input >> virtual_pages >> resident_pages)) {
    return;
  }
  const long page_size = sysconf(_SC_PAGESIZE);
  if (page_size <= 0) {
    return;
  }
  virtual_bytes = virtual_pages * static_cast<unsigned long long>(page_size);
  resident_bytes = resident_pages * static_cast<unsigned long long>(page_size);
}

static inline void AscendopRuntimeBoundaryTrace(
    const char *phase, unsigned long long value = 0, long long ordinal = -1) {
  const char *path = std::getenv("ASCENDOP_RUNTIME_BOUNDARY_TRACE_FILE");
  if (path == nullptr || path[0] == '\0') {
    return;
  }
  const auto monotonic_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
  std::lock_guard<std::mutex> guard(AscendopRuntimeBoundaryMutex());
  std::ofstream output(path, std::ios::app);
  output << "{\"schema\":\"ascendop.runtime-boundary-event.v1\","
         << "\"phase\":\"" << phase << "\","
         << "\"monotonic_ns\":" << monotonic_ns << ","
         << "\"value\":" << value << ","
         << "\"ordinal\":" << ordinal << "}" << std::endl;
}

static inline void AscendopRuntimeWorkspaceQueryAttribution(
    const char *phase, const char *operation, unsigned long long value = 0,
    const char *exception_type = "", const char *error = "") {
  const char *path = std::getenv("ASCENDOP_RUNTIME_BOUNDARY_TRACE_FILE");
  if (path == nullptr || path[0] == '\0') {
    return;
  }
  const auto monotonic_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
  unsigned long long virtual_bytes = 0;
  unsigned long long resident_bytes = 0;
  AscendopRuntimeProcessMemory(virtual_bytes, resident_bytes);
  void *frames[64];
  const bool capture_frames = exception_type != nullptr && exception_type[0] != '\0';
  const int frame_count = capture_frames ? backtrace(frames, 64) : 0;
  char **symbols = frame_count > 0 ? backtrace_symbols(frames, frame_count) : nullptr;
  std::lock_guard<std::mutex> guard(AscendopRuntimeBoundaryMutex());
  std::ofstream output(path, std::ios::app);
  output << "{\"schema\":\"ascendop.native-workspace-query-event.v1\","
         << "\"phase\":\"" << AscendopRuntimeJsonEscape(phase) << "\","
         << "\"operation\":\"" << AscendopRuntimeJsonEscape(operation) << "\","
         << "\"monotonic_ns\":" << monotonic_ns << ","
         << "\"value\":" << value << ","
         << "\"exception_type\":\""
         << AscendopRuntimeJsonEscape(exception_type) << "\","
         << "\"error\":\"" << AscendopRuntimeJsonEscape(error) << "\","
         << "\"allocation_request_bytes\":null,"
         << "\"process_virtual_bytes\":" << virtual_bytes << ","
         << "\"process_resident_bytes\":" << resident_bytes << ","
         << "\"native_frames\":[";
  for (int index = 0; index < frame_count; ++index) {
    if (index != 0) {
      output << ",";
    }
    output << "\"" << AscendopRuntimeJsonEscape(
        symbols == nullptr ? "" : symbols[index]) << "\"";
  }
  output << "]}" << std::endl;
  std::free(symbols);
}

static inline unsigned long long AscendopRuntimeTensorBytes(
    const at::Tensor &tensor) {
  if (!tensor.defined() || tensor.numel() <= 0) {
    return 0;
  }
  const auto elements = static_cast<unsigned long long>(tensor.numel());
  const auto itemsize = static_cast<unsigned long long>(tensor.itemsize());
  const auto limit = std::numeric_limits<unsigned long long>::max();
  return itemsize != 0 && elements > limit / itemsize
             ? limit
             : elements * itemsize;
}

static inline unsigned long long AscendopRuntimeShapeElements(
    const std::vector<int64_t> &shape) {
  unsigned long long elements = 1;
  const auto limit = std::numeric_limits<unsigned long long>::max();
  for (size_t axis = 0; axis < shape.size(); ++axis) {
    const auto dimension = shape[axis];
    AscendopRuntimeBoundaryTrace(
        "output-dimension", dimension < 0 ? limit : dimension, axis);
    if (dimension < 0) {
      return limit;
    }
    const auto extent = static_cast<unsigned long long>(dimension);
    if (extent != 0 && elements > limit / extent) {
      return limit;
    }
    elements *= extent;
  }
  return elements;
}

static inline unsigned long long AscendopRuntimeShapeBytes(
    unsigned long long elements, unsigned long long itemsize) {
  const auto limit = std::numeric_limits<unsigned long long>::max();
  return itemsize != 0 && elements > limit / itemsize
             ? limit
             : elements * itemsize;
}
""".strip()


def apply_runtime_boundary_overlay(
    task_case: Path, *, native_attribution: bool = False
) -> dict[str, Any]:
    """Instrument a copied diagnostic task package without touching its source snapshot."""

    task_case = task_case.resolve()
    if not task_case.is_dir():
        raise EngineJobBuildError(
            f"runtime boundary task package is missing: {task_case}"
        )
    candidates = [
        task_case / "common" / "pytorch_npu_helper.hpp",
        task_case / "extension" / "custom_op.cpp",
    ]
    patched: list[dict[str, str]] = []
    aclnn_helper_traced = False
    for path in candidates:
        if not path.is_file():
            continue
        before = path.read_text(encoding="utf-8")
        if TRACE_MARKER in before:
            after = before
            adapter = "already-instrumented"
            aclnn_helper_traced = aclnn_helper_traced or path.name == (
                "pytorch_npu_helper.hpp"
            )
        elif path.name == "pytorch_npu_helper.hpp" and "#define EXEC_NPU_CMD" in before:
            after = _instrument_aclnn_helper(before)
            adapter = "aclnn-workspace-executor-v1"
            aclnn_helper_traced = True
        elif (
            path.name == "custom_op.cpp"
            and aclnn_helper_traced
            and "EXEC_NPU_CMD(" in before
            and "at::empty(" in before
        ):
            after = _instrument_aclnn_wrapper(before)
            adapter = "aclnn-wrapper-allocation-v1"
        elif path.name == "custom_op.cpp" and "OpCommand" in before:
            after = _instrument_opcommand_wrapper(before)
            adapter = "opcommand-run-v1"
        else:
            continue
        if after != before:
            path.write_text(after, encoding="utf-8")
        patched.append(
            {
                "path": path.relative_to(task_case).as_posix(),
                "adapter": adapter,
            }
        )
    if not patched:
        raise EngineJobBuildError(
            "runtime-boundary-trace found no supported ACLNN or OpCommand wrapper"
        )
    audit = {
        "schema": "ascendop.runtime-boundary-overlay.v2",
        "flags": [
            TRACE_FLAG,
            *([NATIVE_WORKSPACE_QUERY_ATTRIBUTION_FLAG] if native_attribution else []),
        ],
        "patched": patched,
        "task_case_sha256": tree_digest(task_case),
    }
    (task_case / "ASCENDOP_RUNTIME_BOUNDARY_OVERLAY.json").write_text(
        json.dumps(audit, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return audit


def _instrument_aclnn_helper(text: str) -> str:
    text = text.replace(
        "#define EXEC_NPU_CMD", _TRACE_HELPER + "\n\n#define EXEC_NPU_CMD", 1
    )
    lines = text.splitlines()
    _instrument_native_workspace_query(lines)
    _macro_boundary(
        lines,
        "auto workspace_tensor =",
        'AscendopRuntimeBoundaryTrace("workspace-allocation-enter", workspace_size);',
        "",
    )
    _macro_boundary(
        lines,
        "workspace_addr = const_cast<void *>(workspace_tensor.storage().data());",
        "",
        'AscendopRuntimeBoundaryTrace("workspace-allocation-return", workspace_size);',
    )
    _macro_boundary(
        lines,
        "auto api_ret =",
        'AscendopRuntimeBoundaryTrace("executor-call-enter", workspace_size);',
        "",
    )
    _macro_boundary(
        lines,
        "opApiFunc(workspace_addr, workspace_size, executor, acl_stream);",
        "",
        'AscendopRuntimeBoundaryTrace("executor-call-return", workspace_size);',
    )
    _macro_boundary(
        lines,
        "cmd.Run();",
        'AscendopRuntimeBoundaryTrace("opcommand-run-enter", workspace_size);',
        'AscendopRuntimeBoundaryTrace("opcommand-run-return", workspace_size);',
    )
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


def _instrument_native_workspace_query(lines: list[str]) -> None:
    marker = "auto workspace_status = call(getWorkspaceSizeFunc, converted_params);"
    index = next((i for i, line in enumerate(lines) if marker in line), -1)
    if index < 0:
        raise EngineJobBuildError(
            f"runtime boundary ACLNN overlay marker is missing: {marker}"
        )
    indent = lines[index][: len(lines[index]) - len(lines[index].lstrip())]
    statements = [
        'AscendopRuntimeBoundaryTrace("workspace-query-enter");',
        'AscendopRuntimeWorkspaceQueryAttribution("workspace-query-native-enter", #aclnn_api);',
        "decltype(call(getWorkspaceSizeFunc, converted_params)) workspace_status{};",
        "try {",
        "  workspace_status = call(getWorkspaceSizeFunc, converted_params);",
        "} catch (const std::bad_alloc &ascendop_exception) {",
        '  AscendopRuntimeWorkspaceQueryAttribution("workspace-query-native-exception", #aclnn_api, 0, "std::bad_alloc", ascendop_exception.what());',
        "  throw;",
        "} catch (const std::exception &ascendop_exception) {",
        '  AscendopRuntimeWorkspaceQueryAttribution("workspace-query-native-exception", #aclnn_api, 0, "std::exception", ascendop_exception.what());',
        "  throw;",
        "} catch (...) {",
        '  AscendopRuntimeWorkspaceQueryAttribution("workspace-query-native-exception", #aclnn_api, 0, "unknown", "non-std exception");',
        "  throw;",
        "}",
        'AscendopRuntimeBoundaryTrace("workspace-query-return", workspace_size);',
        'AscendopRuntimeWorkspaceQueryAttribution("workspace-query-native-return", #aclnn_api, workspace_size);',
    ]
    lines[index : index + 1] = [f"{indent}{statement:<72}\\" for statement in statements]


def _instrument_aclnn_wrapper(text: str) -> str:
    lines = text.splitlines()
    allocation = next(
        (i for i, line in enumerate(lines) if "= at::empty(" in line), -1
    )
    if allocation < 0:
        raise EngineJobBuildError(
            "runtime boundary ACLNN wrapper output allocation marker is missing"
        )
    indent = lines[allocation][: len(lines[allocation]) - len(lines[allocation].lstrip())]
    metadata = [
        f'{indent}for (int64_t axis = 0; axis < input.dim(); ++axis) {{',
        f'{indent}  AscendopRuntimeBoundaryTrace("input-dimension", input.size(axis), axis);',
        f"{indent}}}",
        f'{indent}AscendopRuntimeBoundaryTrace("input-bytes", AscendopRuntimeTensorBytes(input));',
        f'{indent}for (int64_t axis = 0; axis < weight.dim(); ++axis) {{',
        f'{indent}  AscendopRuntimeBoundaryTrace("weight-dimension", weight.size(axis), axis);',
        f"{indent}}}",
        f'{indent}AscendopRuntimeBoundaryTrace("weight-bytes", AscendopRuntimeTensorBytes(weight));',
        f'{indent}AscendopRuntimeBoundaryTrace("bias-bytes", bias.has_value() ? AscendopRuntimeTensorBytes(bias.value()) : 0);',
        f"{indent}const auto ascendop_output_elements = AscendopRuntimeShapeElements(output_shape);",
        f'{indent}AscendopRuntimeBoundaryTrace("output-elements", ascendop_output_elements);',
        f"{indent}const auto ascendop_output_bytes = AscendopRuntimeShapeBytes(",
        f"{indent}    ascendop_output_elements, input.itemsize());",
        f'{indent}AscendopRuntimeBoundaryTrace("output-allocation-enter", ascendop_output_bytes);',
    ]
    lines[allocation:allocation] = metadata
    allocation += len(metadata)
    lines.insert(
        allocation + 1,
        f'{indent}AscendopRuntimeBoundaryTrace("output-allocation-return", AscendopRuntimeTensorBytes(output));',
    )

    loop = next(
        (
            i
            for i, line in enumerate(lines)
            if "for (int64_t round = 0; round < profile_rounds; ++round)" in line
        ),
        -1,
    )
    call = next((i for i, line in enumerate(lines) if "EXEC_NPU_CMD(" in line), -1)
    if loop < 0 or call < 0 or call <= loop:
        raise EngineJobBuildError(
            "runtime boundary ACLNN wrapper round markers are missing"
        )
    loop_indent = lines[loop][: len(lines[loop]) - len(lines[loop].lstrip())] + "  "
    lines.insert(
        loop + 1,
        f'{loop_indent}AscendopRuntimeBoundaryTrace("round-enter", round, case_num);',
    )
    call += 1
    lines.insert(
        call + 1,
        f'{loop_indent}AscendopRuntimeBoundaryTrace("round-return", round, case_num);',
    )
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


def _macro_boundary(lines: list[str], marker: str, before: str, after: str) -> None:
    index = next((i for i, line in enumerate(lines) if marker in line), -1)
    if index < 0:
        raise EngineJobBuildError(
            f"runtime boundary ACLNN overlay marker is missing: {marker}"
        )
    indent = lines[index][: len(lines[index]) - len(lines[index].lstrip())]
    if before:
        lines.insert(index, f"{indent}{before:<72}\\")
        index += 1
    if after:
        lines.insert(index + 1, f"{indent}{after:<72}\\")


def _instrument_opcommand_wrapper(text: str) -> str:
    include_end = text.find("\n\n")
    if include_end < 0:
        raise EngineJobBuildError("runtime boundary OpCommand include block is invalid")
    text = text[:include_end] + "\n\n" + _TRACE_HELPER + text[include_end:]
    lines = text.splitlines()
    empty_indexes = [i for i, line in enumerate(lines) if "= at::empty(" in line]
    for index in reversed(empty_indexes):
        indent = lines[index][: len(lines[index]) - len(lines[index].lstrip())]
        lines.insert(
            index, f'{indent}AscendopRuntimeBoundaryTrace("output-allocation-enter");'
        )
        lines.insert(
            index + 2,
            f'{indent}AscendopRuntimeBoundaryTrace("output-allocation-return");',
        )
    run_indexes = [i for i, line in enumerate(lines) if line.strip() == ".Run();"]
    direct_run_indexes = [
        i for i, line in enumerate(lines) if line.strip() == "cmd.Run();"
    ]
    inline_run_indexes = [
        i
        for i, line in enumerate(lines)
        if ".Run();" in line
        and i not in run_indexes
        and i not in direct_run_indexes
    ]
    for index in reversed(run_indexes):
        previous = index - 1
        while previous >= 0 and not lines[previous].strip():
            previous -= 1
        if previous < 0:
            raise EngineJobBuildError("runtime boundary OpCommand chain is invalid")
        lines[previous] = lines[previous].rstrip() + ";"
        command_indent = " " * 8
        for search in range(index - 1, -1, -1):
            if "cmd.Name(" in lines[search]:
                command_indent = lines[search][
                    : len(lines[search]) - len(lines[search].lstrip())
                ]
                break
        lines[index : index + 1] = [
            f'{command_indent}AscendopRuntimeBoundaryTrace("opcommand-run-enter");',
            f"{command_indent}cmd.Run();",
            f'{command_indent}AscendopRuntimeBoundaryTrace("opcommand-run-return");',
        ]
    for index in reversed(direct_run_indexes):
        if any(abs(index - other) <= 2 for other in run_indexes):
            continue
        indent = lines[index][: len(lines[index]) - len(lines[index].lstrip())]
        lines.insert(
            index, f'{indent}AscendopRuntimeBoundaryTrace("opcommand-run-enter");'
        )
        lines.insert(
            index + 2, f'{indent}AscendopRuntimeBoundaryTrace("opcommand-run-return");'
        )
    for index in reversed(inline_run_indexes):
        indent = lines[index][: len(lines[index]) - len(lines[index].lstrip())]
        lines.insert(
            index, f'{indent}AscendopRuntimeBoundaryTrace("opcommand-run-enter");'
        )
        lines.insert(
            index + 2, f'{indent}AscendopRuntimeBoundaryTrace("opcommand-run-return");'
        )
    if not run_indexes and not direct_run_indexes and not inline_run_indexes:
        raise EngineJobBuildError("runtime boundary OpCommand Run marker is missing")
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


__all__ = [
    "NATIVE_WORKSPACE_QUERY_ATTRIBUTION_FLAG",
    "TRACE_FLAG",
    "apply_runtime_boundary_overlay",
]
