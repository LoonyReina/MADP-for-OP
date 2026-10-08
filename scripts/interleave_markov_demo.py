"""Print an illustrative comparison of sequential and interleaved research.

This is a model of research-policy transitions, not a benchmark and not a claim
about any model, accelerator or evaluator. It has no dependencies outside the
Python standard library so that it can run in the public checkout.
"""
from __future__ import annotations

from typing import Mapping, Sequence


STATES = ("B", "N", "D", "L", "E")


def step(distribution: Mapping[str, float], matrix: Mapping[str, Mapping[str, float]]) -> dict[str, float]:
    next_distribution = {state: 0.0 for state in STATES}
    for source, mass in distribution.items():
        for target, probability in matrix[source].items():
            next_distribution[target] += mass * probability
    return next_distribution


def run(matrix: Mapping[str, Mapping[str, float]], transitions: int = 12) -> dict[str, float]:
    distribution = {state: 0.0 for state in STATES}
    distribution["B"] = 1.0
    for _ in range(transitions):
        distribution = step(distribution, matrix)
    return distribution


SEQUENTIAL = {
    "B": {"B": 0.25, "N": 0.75},
    "N": {"N": 0.15, "L": 0.70, "E": 0.15},
    "D": {"B": 0.60, "D": 0.20, "E": 0.20},
    "L": {"L": 0.90, "D": 0.05, "E": 0.05},
    "E": {"B": 0.20, "E": 0.80},
}

INTERLEAVED = {
    "B": {"B": 0.25, "N": 0.55, "D": 0.20},
    "N": {"N": 0.15, "L": 0.45, "D": 0.25, "E": 0.15},
    "D": {"B": 0.70, "D": 0.10, "E": 0.20},
    "L": {"L": 0.55, "D": 0.35, "E": 0.10},
    "E": {"B": 0.20, "E": 0.80},
}


def main() -> None:
    for name, matrix in (("sequential", SEQUENTIAL), ("interleaved", INTERLEAVED)):
        result = run(matrix)
        print(f"{name}: reached_E={result['E']:.4f} local_optimum_L={result['L']:.4f}")


if __name__ == "__main__":
    main()

