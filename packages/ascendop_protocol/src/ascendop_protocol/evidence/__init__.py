"""Flow V5 registered evidence-operation contracts.

Imports stay lazy so the actor catalog can bind to the evidence registry without
creating an actor/evidence package import cycle.
"""

from importlib import import_module as _import_module


_EXPORT_MODULES = ("registry", "contracts")


def __getattr__(name: str) -> object:
    for module_name in _EXPORT_MODULES:
        module = _import_module(f"{__name__}.{module_name}")
        if hasattr(module, name):
            value = getattr(module, name)
            globals()[name] = value
            return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
