from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any


PROTOCOL_VERSION = "cannjudge-cann90-official-template-v1"
SUPPORTED_PROFILE = "cannjudge-cann90"

ROOT_CMAKE_TEMPLATE = """cmake_minimum_required(VERSION 3.16.0)
project({op_snake}_op_prj)
find_package(ASC REQUIRED)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(ARCH32_COMPUTE_UNITS ascend910b ascend910_93)
set(ARCH35_COMPUTE_UNITS ascend950)

if(NOT DEFINED ASCEND_COMPUTE_UNIT OR ASCEND_COMPUTE_UNIT STREQUAL "")
    set(ASCEND_COMPUTE_UNIT ${{ARCH32_COMPUTE_UNITS}} ${{ARCH35_COMPUTE_UNITS}})
endif()
set(package_name {op_snake}_custom)

 npu_op_package(${{package_name}}
    TYPE RUN
    CONFIG
        INSTALL_PATH ${{CMAKE_BINARY_DIR}}
)

if(EXISTS "${{CMAKE_CURRENT_SOURCE_DIR}}/op_host")
    add_subdirectory(op_host)
endif()

if(EXISTS "${{CMAKE_CURRENT_SOURCE_DIR}}/op_kernel")
    add_subdirectory(op_kernel)
endif()

message(WARNING "cmake 'make' does NOT build kernel binary by default. Use: bash build.sh --soc=<soc>")
"""

HOST_CMAKE = """file(GLOB host_ops_def_srcs
    ${CMAKE_CURRENT_SOURCE_DIR}/*def.cpp
)

file(GLOB host_ops_infershape_srcs
    ${CMAKE_CURRENT_SOURCE_DIR}/*_infershape.cpp
)

set(host_ops_tiling_srcs)
file(GLOB TILING_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*tiling.cpp)
list(APPEND host_ops_tiling_srcs ${TILING_FILES})

set(host_ops_srcs
    ${host_ops_def_srcs}
    ${host_ops_infershape_srcs}
    ${host_ops_tiling_srcs}
)

npu_op_code_gen(
    SRC ${host_ops_srcs}
    PACKAGE ${package_name}
    OUT_DIR ${ASCEND_AUTOGEN_PATH}
    COMPILE_OPTIONS
        -I$ENV{ASCEND_HOME_PATH}/aarch64-linux/include
        -I$ENV{ASCEND_HOME_PATH}/aarch64-linux/asc/include/tiling
        -I$ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc
        -I$ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc/op_common
        -I$ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc/base
        -I$ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc/exe_graph
        -I$ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc/graph
)

npu_op_library(cust_optiling TILING
    ${host_ops_srcs}
)

target_include_directories(cust_optiling PRIVATE
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/include
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/asc/include/tiling
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc/op_common
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc/base
)

set(op_api_dir ${CMAKE_CURRENT_SOURCE_DIR}/../op_api)
if(EXISTS ${op_api_dir} AND IS_DIRECTORY ${op_api_dir})
    file(GLOB op_api_srcs ${op_api_dir}/*.cpp)
else()
    file(GLOB op_api_srcs "${CMAKE_BINARY_DIR}/autogen/aclnn_*.cpp")
endif()

npu_op_library(cust_opapi ACLNN
    ${op_api_srcs}
)

target_include_directories(cust_opapi PRIVATE
    ${op_api_dir}
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/include
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/include/aclnn
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/asc/include
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc/op_common
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc/base
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc/aicpu
)

target_compile_options(cust_opapi PRIVATE -UACLNN_WITH_BINARY)

file(GLOB proto_src ${ASCEND_AUTOGEN_PATH}/op_proto.cc)
set_source_files_properties(${proto_src} PROPERTIES GENERATED TRUE)

npu_op_library(cust_op_proto GRAPH
    ${host_ops_srcs}
    ${proto_src}
)

target_include_directories(cust_op_proto PRIVATE
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/include
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/asc/include/tiling
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc/op_common
    $ENV{ASCEND_HOME_PATH}/aarch64-linux/pkg_inc/base
)

npu_op_package_add(${package_name}
    LIBRARY
        cust_optiling
        cust_op_proto
        cust_opapi
)
"""

KERNEL_CMAKE = """file(GLOB_RECURSE ALL_KERNEL_FILES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.cpp)

npu_op_kernel_sources(ascendc_kernels
    OP_TYPE OP
    KERNEL_DIR .
    KERNEL_FILE ${ALL_KERNEL_FILES}
)

npu_op_kernel_library(ascendc_kernels
    SRC_BASE ${CMAKE_CURRENT_SOURCE_DIR}
    TILING_LIBRARY cust_optiling
)

npu_op_package_add(${package_name}
    LIBRARY
        ascendc_kernels
)
"""

BUILD_SH_TEMPLATE = """#!/bin/bash
set -e

export BASE_PATH=$(
  cd "$(dirname $0)"
  pwd
)
export BUILD_PATH="${BASE_PATH}/build"
export BUILD_OUT_PATH="${BASE_PATH}/build_out"

CORE_NUMS=$(cat /proc/cpuinfo | grep "processor" | wc -l)
if [ ${CORE_NUMS} -gt 8 ]; then
  CORE_NUMS=8
fi

usage() {
  echo "Build script for {op_snake} operator"
  echo "Usage: bash build.sh [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  -h, --help              Print this help message"
  echo "  -j[n]                   Compile thread nums, default is ${CORE_NUMS}, eg: -j8"
  echo "  --make_clean            Clean build artifacts"
  echo "  -u, --ut                Run UT (Unit Tests)"
  echo "  -e, --example           Run examples (requires NPU)"
  echo ""
  echo "Examples:"
  echo "  bash build.sh           # Build with default soc (ascend910b)"
  echo "  bash build.sh -j8       # Build with 8 threads"
  echo "  bash build.sh --make_clean"
  echo "  bash build.sh -u        # Run UT tests"
  echo "  bash build.sh -e        # Run aclnn example (requires NPU)"
}

clean_build() {
  if [ -d "${BUILD_PATH}" ]; then
    echo "Cleaning build directory..."
    rm -rf ${BUILD_PATH}/*
  fi
}

clean_build_out() {
  if [ -d "${BUILD_OUT_PATH}" ]; then
    echo "Cleaning build_out directory..."
    rm -rf ${BUILD_OUT_PATH}/*
  fi
}

THREAD_NUM=${CORE_NUMS}
COMPUTE_UNIT="ascend910b"
ENABLE_CLEAN=FALSE
RUN_UT=FALSE
RUN_EXAMPLE=FALSE

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    -j*)
      THREAD_NUM="${1:2}"
      if [ -z "$THREAD_NUM" ]; then
        THREAD_NUM=${CORE_NUMS}
      fi
      shift
      ;;
    -u|--ut)
      RUN_UT=true
      shift
      ;;
    -e|--example)
      RUN_EXAMPLE=true
      shift
      ;;
    --make_clean)
      ENABLE_CLEAN=TRUE
      shift
      ;;
    -*)
      echo "[ERROR] Invalid option: $1"
      usage
      exit 1
      ;;
    *)
      echo "[ERROR] Unexpected argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [ "$ENABLE_CLEAN" = "TRUE" ]; then
  clean_build
  clean_build_out
  exit 0
fi

if [ "$RUN_UT" = true ]; then
  echo "[INFO] Running UT tests..."
  cd "${BASE_PATH}/tests/ut"
  ./run.sh
  UT_RESULT=$?
  if [ $UT_RESULT -ne 0 ]; then
    echo "[ERROR] UT tests failed"
    exit 1
  fi
  echo "[INFO] UT tests passed!"
  exit 0
fi

CMAKE_ARGS="-DASCEND_COMPUTE_UNIT=$COMPUTE_UNIT"

if [ ! -d "${BUILD_PATH}" ]; then
  mkdir -p "${BUILD_PATH}"
fi

[ -f "${BUILD_PATH}/CMakeCache.txt" ] && rm -f ${BUILD_PATH}/CMakeCache.txt

echo "----------------------------------------------------------------"
echo "[INFO] Configuring project..."
echo "[INFO] CMAKE_ARGS: ${CMAKE_ARGS}"
cd "${BUILD_PATH}" && cmake ${CMAKE_ARGS} ..

echo "----------------------------------------------------------------"
echo "[INFO] Building project with ${THREAD_NUM} threads..."
cmake --build . --target all binary package install -- -j ${THREAD_NUM}

KERNEL_O=$(find ${BUILD_PATH}/op_kernel/ascendc_kernels/binary/${COMPUTE_UNIT} -name "*.o" 2>/dev/null | head -1)
if [ -z "$KERNEL_O" ]; then
    echo "[ERROR] Kernel binary not found"
    exit 1
fi

PKG_PATH=$(ls "${BUILD_PATH}"/custom_opp_*.run 2>/dev/null | head -n 1)
if [ -z "$PKG_PATH" ] || [ ! -f "$PKG_PATH" ] || [ ! -s "$PKG_PATH" ]; then
    echo "[ERROR] Package not found or empty"
    exit 1
fi

echo "----------------------------------------------------------------"
echo "[INFO] Build completed successfully!"
echo "[INFO] Kernel binary: ${KERNEL_O}"
echo "[INFO] Package: ${PKG_PATH}"

if [ "$RUN_EXAMPLE" = true ]; then
  echo "----------------------------------------------------------------"
  echo "[INFO] Running examples..."
  cd "${BASE_PATH}/examples"
  ./run.sh
  EXAMPLE_RESULT=$?
  cd - > /dev/null
  if [ $EXAMPLE_RESULT -ne 0 ]; then
    echo "[ERROR] Example execution failed"
    exit 1
  fi
  echo "[INFO] Example completed successfully!"
fi
"""


class OfficialTemplateError(RuntimeError):
    pass


def op_snake_name(op: str) -> str:
    value = re.sub(r"(?<!^)(?=[A-Z])", "_", op).lower()
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise OfficialTemplateError(f"invalid operator name: {op}")
    return value


def rendered_scaffold(op: str) -> dict[str, str]:
    op_snake = op_snake_name(op)
    return {
        "CMakeLists.txt": ROOT_CMAKE_TEMPLATE.format(op_snake=op_snake),
        "op_host/CMakeLists.txt": HOST_CMAKE,
        "op_kernel/CMakeLists.txt": KERNEL_CMAKE,
        "build.sh": BUILD_SH_TEMPLATE.replace("{op_snake}", op_snake),
    }


def materialize_official_workspace(
    *,
    source: Path,
    output: Path,
    op: str,
    template_root: Path | None = None,
) -> dict[str, Any]:
    source = source.resolve()
    output = output.resolve()
    if not source.is_dir():
        raise OfficialTemplateError(f"source workspace is missing: {source}")
    if output == source or source in output.parents:
        raise OfficialTemplateError("output must be outside the source workspace")
    resolved_template_root = template_root.resolve() if template_root else None
    if resolved_template_root is not None:
        validate_template_root(resolved_template_root, op)

    op_snake = op_snake_name(op)
    mapped = mapped_operator_files(source, op_snake)
    if output.exists():
        shutil.rmtree(output)
    if resolved_template_root is not None:
        copy_template_tree(resolved_template_root, output)
    else:
        (output / "op_host").mkdir(parents=True)
        (output / "op_kernel").mkdir(parents=True)
    for relative, text in rendered_scaffold(op).items():
        write_text(output / relative, text)
    for relative, text in mapped.items():
        write_text(output / relative, text)
    op_api = source / "op_api"
    if op_api.is_dir():
        shutil.copytree(op_api, output / "op_api")
    os.chmod(output / "build.sh", 0o755)

    manifest = {
        "protocol_version": PROTOCOL_VERSION,
        "profile": SUPPORTED_PROFILE,
        "operator": op,
        "operator_snake": op_snake,
        "source_sha256": tree_digest(source),
        "materialized_payload_sha256": tree_digest(output),
        "files": {
            relative: file_sha256(output / relative)
            for relative in sorted((*rendered_scaffold(op), *mapped))
        },
        "mapping": (
            "official-seven-file-pass-through"
            if official_layout_present(source, op_snake)
            else "legacy-msopgen-seven-file-fusion"
        ),
        "template_assets": (
            {
                "source_sha256": tree_digest(resolved_template_root),
                "file_count": sum(
                    1 for path in resolved_template_root.rglob("*") if path.is_file()
                ),
                "includes_examples": (resolved_template_root / "examples").is_dir(),
                "includes_unit_tests": (resolved_template_root / "tests" / "ut").is_dir(),
            }
            if resolved_template_root is not None
            else {
                "source_sha256": "",
                "file_count": 0,
                "includes_examples": False,
                "includes_unit_tests": False,
            }
        ),
    }
    write_text(
        output / "OFFICIAL_TEMPLATE_SYNC.json",
        json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    manifest["workspace_sha256"] = tree_digest(output)
    return manifest


def synchronize_in_place(
    source: Path,
    op: str,
    *,
    template_root: Path | None = None,
) -> dict[str, Any]:
    source = source.resolve()
    parent = source.parent
    temp = Path(tempfile.mkdtemp(prefix=f".{source.name}.official.", dir=str(parent)))
    backup = parent / f".{source.name}.authoring-backup"
    try:
        manifest = materialize_official_workspace(
            source=source,
            output=temp,
            op=op,
            template_root=template_root,
        )
        if backup.exists():
            shutil.rmtree(backup)
        os.replace(source, backup)
        os.replace(temp, source)
        shutil.rmtree(backup)
        return manifest
    finally:
        if temp.exists():
            shutil.rmtree(temp, ignore_errors=True)


def official_layout_present(source: Path, op_snake: str) -> bool:
    expected = (
        source / "op_host" / f"{op_snake}_def.cpp",
        source / "op_host" / f"{op_snake}_infershape.cpp",
        source / "op_host" / f"{op_snake}_tiling.cpp",
        source / "op_kernel" / f"{op_snake}.cpp",
        source / "op_kernel" / f"{op_snake}.h",
        source / "op_kernel" / f"{op_snake}_tiling_data.h",
        source / "op_kernel" / f"{op_snake}_tiling_key.h",
    )
    return all(path.is_file() for path in expected)


def mapped_operator_files(source: Path, op_snake: str) -> dict[str, str]:
    if official_layout_present(source, op_snake):
        relatives = (
            f"op_host/{op_snake}_def.cpp",
            f"op_host/{op_snake}_infershape.cpp",
            f"op_host/{op_snake}_tiling.cpp",
            f"op_kernel/{op_snake}.cpp",
            f"op_kernel/{op_snake}.h",
            f"op_kernel/{op_snake}_tiling_data.h",
            f"op_kernel/{op_snake}_tiling_key.h",
        )
        return {
            relative: (source / relative).read_text(encoding="utf-8")
            for relative in relatives
        }

    host_source = source / "op_host" / f"{op_snake}.cpp"
    kernel_source = source / "op_kernel" / f"{op_snake}.cpp"
    tiling_source = source / "op_kernel" / f"{op_snake}_tiling.h"
    for path in (host_source, kernel_source, tiling_source):
        if not path.is_file():
            raise OfficialTemplateError(
                f"legacy msopgen source required for official mapping is missing: {path}"
            )
    legacy_include = f"{op_snake}_tiling.h"
    official_include = f"{op_snake}_tiling_data.h"
    return {
        f"op_host/{op_snake}_def.cpp": host_source.read_text(
            encoding="utf-8"
        ).replace(legacy_include, official_include),
        f"op_host/{op_snake}_infershape.cpp": (
            f"// {op_snake}: infer-shape registration is contained in "
            "the merged host translation unit above.\n"
        ),
        f"op_host/{op_snake}_tiling.cpp": (
            f"// {op_snake}: tiling registration is contained in "
            "the merged host translation unit above.\n"
        ),
        f"op_kernel/{op_snake}.cpp": kernel_source.read_text(
            encoding="utf-8"
        ).replace(legacy_include, official_include),
        f"op_kernel/{op_snake}.h": (
            "#pragma once\n"
            "// The submitted kernel implementation is self-contained in the cpp file.\n"
        ),
        f"op_kernel/{op_snake}_tiling_data.h": tiling_source.read_text(
            encoding="utf-8"
        ),
        f"op_kernel/{op_snake}_tiling_key.h": (
            "#pragma once\n"
            "// The merged host implementation selects its tiling key directly.\n"
        ),
    }


def validate_template_root(template_root: Path, op: str) -> None:
    if not template_root.is_dir():
        raise OfficialTemplateError(
            f"official template workspace is missing: {template_root}"
        )
    rendered = rendered_scaffold(op)
    for relative, expected in rendered.items():
        path = template_root / relative
        if not path.is_file():
            raise OfficialTemplateError(
                f"official template file is missing: {path}"
            )
        actual = path.read_text(encoding="utf-8").replace("\r\n", "\n")
        if actual != expected:
            raise OfficialTemplateError(
                f"official template file drifted from the pinned profile: {path}"
            )
    for relative in ("examples", "tests/ut"):
        path = template_root / relative
        if not path.is_dir():
            raise OfficialTemplateError(
                f"official template asset directory is missing: {path}"
            )


def copy_template_tree(source: Path, output: Path) -> None:
    for path in sorted(source.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise OfficialTemplateError(
                f"official template workspace contains symlink: {path}"
            )
        relative = path.relative_to(source)
        target = output / relative
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif path.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.replace("\r\n", "\n"), encoding="utf-8", newline="\n")


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise OfficialTemplateError(f"workspace contains symlink: {path}")
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        if path.is_file():
            digest.update(path.read_bytes().replace(b"\r\n", b"\n"))
            digest.update(b"\0")
    return digest.hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize an AscendOP source tree in the official CANNJudge CANN 9.0 template"
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--template-root",
        type=Path,
        help="downloaded official project/code root whose examples and UT assets are preserved",
    )
    parser.add_argument("--op", required=True)
    parser.add_argument("--in-place", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.in_place == (args.output is not None):
        print(
            "choose exactly one of --in-place or --output",
            file=sys.stderr,
        )
        return 2
    try:
        result = (
            synchronize_in_place(
                args.source,
                args.op,
                template_root=args.template_root,
            )
            if args.in_place
            else materialize_official_workspace(
                source=args.source,
                output=args.output,
                op=args.op,
                template_root=args.template_root,
            )
        )
    except (OfficialTemplateError, OSError, ValueError) as exc:
        print(f"official template sync failed: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    else:
        print(result["workspace_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
