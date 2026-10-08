import importlib.util
from pathlib import Path


_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "interleave_markov_demo.py"
_SPEC = importlib.util.spec_from_file_location("interleave_markov_demo", _SCRIPT)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
INTERLEAVED, SEQUENTIAL, run = _MODULE.INTERLEAVED, _MODULE.SEQUENTIAL, _MODULE.run


def test_transition_rows_are_stochastic():
    for matrix in (SEQUENTIAL, INTERLEAVED):
        for row in matrix.values():
            assert abs(sum(row.values()) - 1.0) < 1e-12


def test_interleave_reduces_local_optimum_mass_in_toy_model():
    sequential = run(SEQUENTIAL)
    interleaved = run(INTERLEAVED)
    assert interleaved["L"] < sequential["L"]
    assert interleaved["E"] > sequential["E"]
