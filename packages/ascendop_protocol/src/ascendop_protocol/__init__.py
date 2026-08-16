"""Shared, dependency-free AscendOP protocol contracts.

Wire V3 is the only eager export because it is the Engine runtime boundary.
Legacy top-level attribute access remains available through lazy domain imports.
"""

from importlib import import_module as _import_module

from .wire_v3 import *  # noqa: F401,F403


_LAZY_EXPORT_MODULES = (
    "management",
    "agent",
    "workflow",
    "reference_knowledge",
    "competition",
)


def __getattr__(name: str) -> object:
    for module_name in _LAZY_EXPORT_MODULES:
        module = _import_module(f"{__name__}.{module_name}")
        if hasattr(module, name):
            value = getattr(module, name)
            globals()[name] = value
            return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
