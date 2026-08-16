from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import importlib.util
import inspect
import json
import os
import pickle
import shlex
import shutil
import sys
import tempfile
import time
import types
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from limited_remote_partner.engine.test_engine import atomic_write_json, utc_now
from limited_remote_partner.resources.wheel_cache import file_lock


class CaseCacheError(RuntimeError):
    pass


class CaseCacheMiss(CaseCacheError):
    pass


PROTOCOL_VERSION = "engine-case-cache-v1"
FUSION_START = "# BEGIN ASCENDOP ATTACK CASE FUSION"
FUSION_END = "# END ASCENDOP ATTACK CASE FUSION"
PRECOMPUTED_GOLDEN_OPS = {
    "Copysign",
    "Fmin",
    "FractionalMaxPool3D",
    "Hypot",
    "Logcumsumexp",
}


def parse_case_range(value: str) -> list[int]:
    raw = str(value or "").strip()
    if ".." in raw:
        left, right = raw.split("..", 1)
        start = int(left)
        finish = int(right)
        values = list(range(start, finish + 1))
    else:
        values = [int(item) for item in raw.replace(",", " ").split()]
    if not values or any(item <= 0 for item in values):
        raise CaseCacheError(f"invalid case range: {value}")
    if len(values) != len(set(values)):
        raise CaseCacheError(f"duplicate case id: {value}")
    return values


def prepare_case_cache(
    *,
    task_case: Path,
    cache_root: Path,
    op: str,
    case_range: str,
    output: Path,
    env_output: Path,
    require_hit: bool = False,
) -> dict[str, Any]:
    started = time.monotonic()
    task_case = task_case.resolve()
    cache_root = cache_root.resolve()
    output = output.resolve()
    env_output = env_output.resolve()
    test_op = task_case / "test_op.py"
    if not test_op.is_file():
        raise CaseCacheError(f"test_op.py is missing: {test_op}")
    if not str(op or "").strip():
        raise CaseCacheError("case cache operator is empty")
    case_ids = parse_case_range(case_range)
    identity = cache_identity(test_op, op=op, case_ids=case_ids)
    key = cache_key(identity)
    cache_root.mkdir(parents=True, exist_ok=True)
    entry = cache_root / key
    manifest = valid_cache_entry(entry, key=key, op=op, case_ids=case_ids)
    cache_hit = manifest is not None
    lock_wait_seconds = 0.0
    population_seconds = 0.0
    if manifest is None and require_hit:
        receipt = {
            "protocol_version": PROTOCOL_VERSION,
            "state": "missing",
            "cache_key": key,
            "cache_hit": False,
            "cache_entry": str(entry),
            "operator": op,
            "case_ids": case_ids,
            "case_count": len(case_ids),
            "identity": identity,
            "timing_seconds": {
                "lock_wait": 0.0,
                "population": 0.0,
                "total": round(time.monotonic() - started, 6),
            },
            "finished_at": utc_now(),
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(output, receipt)
        raise CaseCacheMiss(f"required case cache entry is missing: {key}")
    if manifest is None:
        lock_started = time.monotonic()
        with file_lock(cache_root / f"{key}.lock"):
            lock_wait_seconds = time.monotonic() - lock_started
            manifest = valid_cache_entry(entry, key=key, op=op, case_ids=case_ids)
            if manifest is None:
                population_started = time.monotonic()
                manifest = populate_cache_entry(
                    task_case=task_case,
                    cache_root=cache_root,
                    entry=entry,
                    key=key,
                    identity=identity,
                    op=op,
                    case_ids=case_ids,
                )
                population_seconds = time.monotonic() - population_started
            else:
                cache_hit = True
    assert manifest is not None
    receipt = {
        "protocol_version": PROTOCOL_VERSION,
        "state": "ready",
        "cache_key": key,
        "cache_hit": cache_hit,
        "cache_entry": str(entry),
        "operator": op,
        "adapter_mode": case_cache_adapter_mode(op),
        "case_ids": case_ids,
        "case_count": len(case_ids),
        "payload_bytes": sum(int(item["size_bytes"]) for item in manifest["cases"]),
        "identity": identity,
        "timing_seconds": {
            "lock_wait": round(lock_wait_seconds, 6),
            "population": round(population_seconds, 6),
            "total": round(time.monotonic() - started, 6),
        },
        "finished_at": utc_now(),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, receipt)
    env_output.parent.mkdir(parents=True, exist_ok=True)
    env_output.write_text(
        f"export ASCENDOP_CASE_CACHE_ENTRY={shlex.quote(str(entry))}\n",
        encoding="utf-8",
    )
    return receipt


def cache_identity(test_op: Path, *, op: str, case_ids: list[int]) -> dict[str, Any]:
    return {
        "protocol_version": PROTOCOL_VERSION,
        "operator": op,
        "adapter_mode": case_cache_adapter_mode(op),
        "case_ids": list(case_ids),
        "test_op_sha256": file_sha256(test_op),
        "golden_generator_sha256": golden_generator_sha256(op),
        "python": sys.version.split()[0],
        "torch": package_version("torch"),
        "torch_npu": package_version("torch-npu", "torch_npu"),
    }


def package_version(*names: str) -> str:
    for name in names:
        try:
            return importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            continue
    return "missing"


def cache_key(identity: dict[str, Any]) -> str:
    payload = json.dumps(
        identity, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def populate_cache_entry(
    *,
    task_case: Path,
    cache_root: Path,
    entry: Path,
    key: str,
    identity: dict[str, Any],
    op: str,
    case_ids: list[int],
) -> dict[str, Any]:
    module = load_eager_test_module(task_case / "test_op.py")
    meta = getattr(module, "_ascendop_attack_case_meta", {})
    if not isinstance(meta, dict) or str(meta.get("op") or "") != op:
        raise CaseCacheError(
            f"attack fusion operator mismatch: expected={op} observed={meta!r}"
        )
    case_data = getattr(module, "case_data", None)
    if not isinstance(case_data, dict):
        raise CaseCacheError("test_op.py does not expose case_data")
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{key}.", dir=str(cache_root)))
    cases: list[dict[str, Any]] = []
    try:
        for case in case_ids:
            name = f"case{case}"
            if name not in case_data:
                raise CaseCacheError(f"test_op.py is missing {name}")
            payload = {
                "case": case,
                "case_data": case_data[name],
                "golden": compute_golden(op, case_data[name]),
            }
            path = temp_dir / f"{name}.pkl"
            with path.open("wb") as handle:
                pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
            cases.append(
                {
                    "case": case,
                    "file": path.name,
                    "size_bytes": path.stat().st_size,
                    "sha256": file_sha256(path),
                }
            )
        manifest = {
            "protocol_version": PROTOCOL_VERSION,
            "state": "complete",
            "cache_key": key,
            "operator": op,
            "adapter_mode": case_cache_adapter_mode(op),
            "case_ids": list(case_ids),
            "identity": identity,
            "cases": cases,
            "created_at": utc_now(),
        }
        atomic_write_json(temp_dir / "manifest.json", manifest)
        if entry.exists():
            shutil.rmtree(entry)
        os.replace(temp_dir, entry)
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
    result = valid_cache_entry(entry, key=key, op=op, case_ids=case_ids)
    if result is None:
        raise CaseCacheError("published case cache failed validation")
    return result


def _preload_torch_runtime() -> None:
    # Several generated test modules import custom_ops_lib before torch. Load
    # the runtime libraries first so extension dependencies such as libc10.so
    # are already available regardless of template import order.
    try:
        importlib.import_module("torch")
        importlib.import_module("torch_npu")
    except (ImportError, RuntimeError) as exc:
        raise CaseCacheError(f"cannot preload torch runtime: {exc}") from exc


def load_eager_test_module(path: Path) -> Any:
    _preload_torch_runtime()
    module_name = f"ascendop_case_cache_prepare_{os.getpid()}_{time.time_ns()}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise CaseCacheError(f"cannot load test module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def compute_golden(op: str, record: dict[str, Any]) -> Any:
    # Every fused test module can use the immutable case-spec cache. Operators
    # with an explicit adapter additionally precompute and proxy their golden.
    if op not in PRECOMPUTED_GOLDEN_OPS:
        return None
    import torch

    def tensor(value: Any) -> Any:
        if isinstance(value, torch.Tensor):
            return value.cpu()
        try:
            return torch.from_numpy(value)
        except TypeError:
            return torch.as_tensor(value)

    if op in {"Copysign", "Fmin", "Hypot"}:
        left = tensor(record["input"])
        right = tensor(record["other"])
        function = {
            "Copysign": torch.copysign,
            "Fmin": torch.fmin,
            "Hypot": torch.hypot,
        }[op]
        return function(left, right)
    if op == "FractionalMaxPool3D":
        input_x = tensor(record["input"])
        random_samples = tensor(record["_random_samples"])
        kwargs: dict[str, Any] = {
            "return_indices": bool(record["return_indices"]),
            "_random_samples": random_samples,
        }
        if record.get("output_ratio"):
            kwargs["output_ratio"] = record["output_ratio"]
        else:
            kwargs["output_size"] = record["output_size"]
        module = torch.nn.FractionalMaxPool3d(record["kernel_size"], **kwargs)
        return module(input_x)
    if op == "Logcumsumexp":
        input_x = tensor(record["input"])
        dim = record["dim"]
        if isinstance(dim, torch.Tensor):
            dim = int(dim.item())
        return torch.logcumsumexp(input_x, int(dim))
    raise CaseCacheError(f"case cache adapter is missing for operator: {op}")


def case_cache_adapter_mode(op: str) -> str:
    return (
        "precomputed-golden"
        if op in PRECOMPUTED_GOLDEN_OPS
        else "immutable-case-spec"
    )


_GOLDEN_GENERATOR = compute_golden


def golden_generator_sha256(op: str) -> str:
    payload = json.dumps(
        {
            "operator": op,
            "source": inspect.getsource(_GOLDEN_GENERATOR),
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def valid_cache_entry(
    entry: Path, *, key: str, op: str, case_ids: list[int]
) -> dict[str, Any] | None:
    manifest_path = entry / "manifest.json"
    if not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(manifest, dict):
        return None
    if (
        manifest.get("protocol_version") != PROTOCOL_VERSION
        or manifest.get("state") != "complete"
        or manifest.get("cache_key") != key
        or manifest.get("operator") != op
        or manifest.get("case_ids") != case_ids
    ):
        return None
    raw_cases = manifest.get("cases")
    if not isinstance(raw_cases, list) or len(raw_cases) != len(case_ids):
        return None
    for expected_case, raw in zip(case_ids, raw_cases):
        if not isinstance(raw, dict) or int(raw.get("case", 0) or 0) != expected_case:
            return None
        path = entry / str(raw.get("file") or "")
        if not path.is_file() or path.stat().st_size != int(raw.get("size_bytes", -1)):
            return None
    return manifest


def load_cache_manifest(entry: Path) -> dict[str, Any]:
    entry = entry.resolve()
    path = entry / "manifest.json"
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CaseCacheError(f"invalid case cache manifest: {path}") from exc
    if not isinstance(manifest, dict) or manifest.get("protocol_version") != PROTOCOL_VERSION:
        raise CaseCacheError(f"unsupported case cache manifest: {path}")
    return manifest


def load_case_payloads(entry: Path) -> dict[int, dict[str, Any]]:
    entry = entry.resolve()
    manifest = load_cache_manifest(entry)
    result: dict[int, dict[str, Any]] = {}
    for raw in manifest.get("cases", []):
        case, payload = load_case_payload(entry, raw)
        result[case] = payload
    expected = [int(item) for item in manifest.get("case_ids", [])]
    if sorted(result) != sorted(expected):
        raise CaseCacheError(
            f"case cache payload set mismatch: expected={expected} observed={sorted(result)}"
        )
    return result


def load_case_payload(entry: Path, raw: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    case = int(raw.get("case", 0) or 0)
    path = (entry / str(raw.get("file") or "")).resolve()
    if entry not in path.parents or not path.is_file():
        raise CaseCacheError(f"case cache payload escapes entry: {path}")
    if path.stat().st_size != int(raw.get("size_bytes", -1)):
        raise CaseCacheError(f"case cache payload size changed: {path}")
    with path.open("rb") as handle:
        payload = pickle.load(handle)
    if not isinstance(payload, dict) or int(payload.get("case", 0) or 0) != case:
        raise CaseCacheError(f"invalid case cache payload: {path}")
    return case, payload


class RuntimeCaseStore:
    """Keep at most one persisted case and golden resident in a batch process."""

    def __init__(self, entry: Path) -> None:
        self.entry = entry.resolve()
        self.manifest = load_cache_manifest(self.entry)
        raw_cases = self.manifest.get("cases", [])
        if not isinstance(raw_cases, list):
            raise CaseCacheError("case cache manifest cases must be a list")
        self._records: dict[int, dict[str, Any]] = {}
        for raw in raw_cases:
            if not isinstance(raw, dict):
                raise CaseCacheError("case cache manifest contains an invalid case")
            case = int(raw.get("case", 0) or 0)
            if case <= 0 or case in self._records:
                raise CaseCacheError(f"invalid cached case id: {case}")
            self._records[case] = raw
        expected = [int(item) for item in self.manifest.get("case_ids", [])]
        if list(self._records) != expected:
            raise CaseCacheError(
                "case cache manifest order mismatch: "
                f"expected={expected} observed={list(self._records)}"
            )
        self._active_case: int | None = None
        self._active_payload: dict[str, Any] | None = None
        self.case_data: Mapping[str, Any] = _RuntimeCaseData(self)

    @property
    def operator(self) -> str:
        return str(self.manifest.get("operator") or "")

    @property
    def case_ids(self) -> list[int]:
        return list(self._records)

    def activate(self, case: int) -> None:
        if case not in self._records:
            raise CaseCacheError(f"case cache is missing active case: {case}")
        if case != self._active_case:
            self._active_case = case
            self._active_payload = None

    def payload(self, case: int | None = None) -> dict[str, Any]:
        selected = self._active_case if case is None else int(case)
        if selected not in self._records:
            raise CaseCacheError(f"active case is not set: {selected}")
        if selected != self._active_case:
            self.activate(selected)
        if self._active_payload is None:
            observed_case, payload = load_case_payload(
                self.entry, self._records[selected]
            )
            if observed_case != selected:
                raise CaseCacheError(
                    f"case cache selected {selected} but loaded {observed_case}"
                )
            self._active_payload = payload
        return self._active_payload

    def golden(self) -> Any:
        return self.payload()["golden"]


class _RuntimeCaseData(Mapping[str, Any]):
    def __init__(self, store: RuntimeCaseStore) -> None:
        self._store = store

    def __getitem__(self, key: str) -> Any:
        raw = str(key)
        if not raw.startswith("case") or not raw[4:].isdigit():
            raise KeyError(key)
        case = int(raw[4:])
        if case not in self._store.case_ids:
            raise KeyError(key)
        return self._store.payload(case)["case_data"]

    def __iter__(self) -> Iterator[str]:
        return iter(f"case{case}" for case in self._store.case_ids)

    def __len__(self) -> int:
        return len(self._store.case_ids)


def transform_fusion_source(source: str) -> str:
    start = source.find(FUSION_START)
    finish = source.find(FUSION_END)
    if start < 0 or finish < start:
        raise CaseCacheError("test_op.py is missing generated attack fusion markers")
    block = source[start:finish]
    assignments = block.find("_ascendop_attack_cases = {}")
    update = block.find("case_data.update(_ascendop_attack_cases)")
    if assignments < 0 or update < assignments:
        raise CaseCacheError("attack fusion assignment boundary changed")
    update_end = update + len("case_data.update(_ascendop_attack_cases)")
    replacement = "\n".join(
        [
            "from limited_remote_partner.resources.case_cache import load_runtime_case_store as _ascendop_load_runtime_case_store",
            "_ascendop_runtime_case_store = _ascendop_load_runtime_case_store()",
            "_ascendop_attack_cases = _ascendop_runtime_case_store.case_data",
            "case_data = _ascendop_runtime_case_store.case_data",
        ]
    )
    transformed_block = block[:assignments] + replacement + block[update_end:]
    return source[:start] + transformed_block + source[finish:]


def load_runtime_case_store() -> RuntimeCaseStore:
    return RuntimeCaseStore(runtime_cache_entry())


def load_cached_test_module(path: Path) -> Any:
    _preload_torch_runtime()
    source = transform_fusion_source(path.read_text(encoding="utf-8"))
    module_name = f"ascendop_cached_test_op_{os.getpid()}_{time.time_ns()}"
    module = types.ModuleType(module_name)
    module.__file__ = str(path)
    module.__package__ = ""
    sys.modules[module_name] = module
    try:
        exec(compile(source, str(path), "exec"), module.__dict__)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    install_golden_proxy(module, runtime_cache_entry())
    return module


def runtime_cache_entry() -> Path:
    raw = os.environ.get("ASCENDOP_CASE_CACHE_ENTRY", "").strip()
    if not raw:
        raise CaseCacheError("ASCENDOP_CASE_CACHE_ENTRY is not set")
    return Path(raw).resolve()


def install_golden_proxy(module: Any, entry: Path) -> None:
    store = getattr(module, "_ascendop_runtime_case_store", None)
    if not isinstance(store, RuntimeCaseStore) or store.entry != entry.resolve():
        store = RuntimeCaseStore(entry)
    module._ascendop_case_cache_store = store
    module._ascendop_real_torch = module.torch
    module.torch = _TorchProxy(module.torch, module, store.operator)


def set_active_case(module: Any, case: int) -> None:
    runtime_store(module).activate(case)


def active_golden(module: Any) -> Any:
    return runtime_store(module).golden()


def runtime_store(module: Any) -> RuntimeCaseStore:
    store = getattr(module, "_ascendop_case_cache_store", None)
    if not isinstance(store, RuntimeCaseStore):
        raise CaseCacheError("test module has no runtime case cache store")
    return store


class _TorchProxy:
    def __init__(self, torch_module: Any, test_module: Any, op: str) -> None:
        self._torch = torch_module
        self._test_module = test_module
        self._op = op

    def __getattr__(self, name: str) -> Any:
        function_name = {
            "Copysign": "copysign",
            "Fmin": "fmin",
            "Hypot": "hypot",
            "Logcumsumexp": "logcumsumexp",
        }.get(self._op)
        if function_name == name:
            return lambda *_args, **_kwargs: active_golden(self._test_module)
        if self._op == "FractionalMaxPool3D" and name == "nn":
            return _NNProxy(self._torch.nn, self._test_module)
        return getattr(self._torch, name)


class _NNProxy:
    def __init__(self, nn_module: Any, test_module: Any) -> None:
        self._nn = nn_module
        self._test_module = test_module

    def __getattr__(self, name: str) -> Any:
        if name == "FractionalMaxPool3d":
            return lambda *_args, **_kwargs: _CachedFractionalMaxPool3d(
                self._test_module
            )
        return getattr(self._nn, name)


class _CachedFractionalMaxPool3d:
    def __init__(self, test_module: Any) -> None:
        self._test_module = test_module

    def __call__(self, _input: Any) -> Any:
        return active_golden(self._test_module)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare or inspect an immutable AscendOP case/golden cache"
    )
    subparsers = parser.add_subparsers(dest="action", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--task-case", type=Path, required=True)
    prepare.add_argument("--cache-root", type=Path, required=True)
    prepare.add_argument("--op", required=True)
    prepare.add_argument("--case-range", required=True)
    prepare.add_argument("--output", type=Path, required=True)
    prepare.add_argument("--env-output", type=Path, required=True)
    prepare.add_argument(
        "--require-hit",
        action="store_true",
        help="fail without populating when the immutable cache entry is absent",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = prepare_case_cache(
            task_case=args.task_case,
            cache_root=args.cache_root,
            op=args.op,
            case_range=args.case_range,
            output=args.output,
            env_output=args.env_output,
            require_hit=args.require_hit,
        )
    except CaseCacheMiss as exc:
        print(f"case cache miss: {exc}", file=sys.stderr)
        return 3
    except CaseCacheError as exc:
        print(f"case cache failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
