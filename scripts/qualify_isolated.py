"""Qualify only the public checkout using a dedicated MADP virtual environment.

No production commands, provider requests, endpoint requests or model calls are
performed. Reuse an existing environment with --venv; otherwise create one.
The output directory must be new. Test/build logs remain in that directory.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ("ascendop_protocol", "ascendop_control", "ascendop_agent_runner", "ascendop_daemon", "ascendop_test_gateway")


def isolated_environment(work: Path, venv: Path | None = None) -> dict[str, str]:
    environment = {key: value for key, value in os.environ.items()
                   if key.upper() in {"SYSTEMROOT", "WINDIR", "COMSPEC", "SYSTEMDRIVE", "PATHEXT"}}
    scripts = (venv or work / "venv") / ("Scripts" if os.name == "nt" else "bin")
    system_bin = str(Path(os.environ.get("SystemRoot", "C:/Windows")) / "System32") if os.name == "nt" else "/usr/bin:/bin"
    environment.update(PATH=os.pathsep.join((str(scripts), str(Path(sys.executable).parent), system_bin)),
        TEMP=str(work / "tmp"), TMP=str(work / "tmp"), TMPDIR=str(work / "tmp"),
        PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1", PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
        PIP_CONFIG_FILE=os.devnull, PIP_NO_CACHE_DIR="1", PIP_DISABLE_PIP_VERSION_CHECK="1",
        PYTHONUTF8="1")
    return environment


def stage_public(work: Path) -> Path:
    checkout = work / "checkout"
    checkout.mkdir()
    listing = subprocess.run(["git", "-C", str(ROOT), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                             capture_output=True, check=True).stdout.decode("utf-8").split("\0")
    for name in listing:
        if not name:
            continue
        source = ROOT / name
        destination = checkout / name
        if source.is_symlink() or ROOT not in source.resolve().parents or checkout not in destination.resolve().parents:
            raise ValueError("public export path is not a regular in-repository file")
        if not source.is_file():
            raise ValueError("tracked public input is missing")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    return checkout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--venv", type=Path, help="reuse a dedicated MADP venv (its MADP wheels will be updated)")
    parser.add_argument("--skip-tooling", action="store_true", help="test dependencies are already installed")
    args = parser.parse_args()
    work = args.work_root.resolve()
    if work.exists() or work in ROOT.parents or work == ROOT:
        raise ValueError("use a new dedicated output directory")
    venv = args.venv.resolve() if args.venv else work / "venv"
    if args.venv:
        config = (venv / "pyvenv.cfg").read_text(encoding="utf-8").lower()
        if "include-system-site-packages = false" not in config:
            raise ValueError("reuse requires a venv without system site packages")
    work.mkdir(parents=True)
    (work / "tmp").mkdir()
    checkout = stage_public(work)
    environment = isolated_environment(work, venv)
    results = []

    def run(label: str, command: list[str], cwd: Path = checkout, timeout: int = 600,
            extra_environment: dict[str, str] | None = None) -> int:
        with (work / (label + ".log")).open("w", encoding="utf-8") as log:
            try:
                process = subprocess.run(command, cwd=cwd, env={**environment, **(extra_environment or {})}, stdout=log,
                                         stderr=subprocess.STDOUT, timeout=timeout)
                code = process.returncode
            except subprocess.TimeoutExpired:
                code = 124
        results.append({"check": label, "exit_code": code})
        print(json.dumps(results[-1]), flush=True)
        return code

    code = 0 if args.venv else run("venv", [sys.executable, "-I", "-m", "venv", str(venv)])
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if not code and not args.skip_tooling:
        code = run("tooling", [str(python), "-I", "-m", "pip", "install", "--index-url", "https://pypi.org/simple",
                                "-r", str(checkout / "requirements-test.txt")])
    if not code:
        run("scan", [str(python), "-I", "scripts/publication_scan.py"])
        run("closure", [str(python), "-I", "scripts/check_import_closure.py"])
        run("source-tests", [str(python), "-I", "-m", "pytest", "-q", "--confcutdir=.",
            "--basetemp=" + str(work / "test-source"), "--junitxml=" + str(work / "source-tests.xml")],
            extra_environment={"PYTHONPATH": os.pathsep.join(str(checkout / "packages" / p / "src") for p in PACKAGES)})
        for package in PACKAGES:
            run("wheel-" + package, [str(python), "-I", "-m", "build", "--wheel", "--no-isolation",
                "--outdir", str(work / "wheels"), str(checkout / "packages" / package)])
        if all(row["exit_code"] == 0 for row in results if row["check"].startswith("wheel-")):
            code = run("install", [str(python), "-I", "-m", "pip", "install", "--no-index", "--force-reinstall", "--no-deps",
                "--find-links", str(work / "wheels"), *[str(path) for path in sorted((work / "wheels").glob("*.whl"))]])
            if not code:
                run("wheel-tests", [str(python), "-I", "-m", "pytest", "-q", "--confcutdir=.", "-o", "pythonpath=",
                    "--basetemp=" + str(work / "test-wheel"), "--junitxml=" + str(work / "wheel-tests.xml")])
                run("origins", [str(python), "-I", "-c",
                    "import importlib,pathlib,sys,json; names=" + repr(list(PACKAGES)) + "; "
                    "origins={n:str(pathlib.Path(importlib.import_module(n).__file__).resolve()) for n in names}; "
                    "assert all(pathlib.Path(sys.prefix).resolve() in pathlib.Path(p).parents for p in origins.values()); "
                    "print(json.dumps(origins))"], cwd=work)
                run("dependencies", [str(python), "-I", "-m", "pip", "check"], cwd=work)
                for entry in ("ascendop-control-api", "ascendop-agent-runner"):
                    executable = python.parent / (entry + (".exe" if os.name == "nt" else ""))
                    run("entry-" + entry, [str(executable), "--help"], cwd=work, timeout=30)
                run("synthetic-demo", [str(python), "-I", str(checkout / "scripts/demo_unified_core.py"),
                    "--root", str(work / "demo"), "--operators", "3"], cwd=work)
    report = {"schema": "madp.isolated-qualification.v1", "state": "passed" if all(r["exit_code"] == 0 for r in results) else "failed",
        "production_access": "none", "environment": "reused-dedicated-venv" if args.venv else "fresh-venv",
        "checks": results}
    (work / "REPORT.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report))
    return 0 if report["state"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
