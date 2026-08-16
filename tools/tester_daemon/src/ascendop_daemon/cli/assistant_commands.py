from __future__ import annotations

import argparse
from pathlib import Path

from ascendop_daemon.automation.assistant_trigger import (
    AssistantTriggerEngine,
    load_trigger_rules,
)
from ascendop_daemon.cli.common import (
    add_runtime_paths,
    application,
    print_json,
    read_json_object,
)


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("assistant", help="Assistant action outbox")
    actions = parser.add_subparsers(dest="assistant_action", required=True)

    evaluate = actions.add_parser("evaluate")
    add_runtime_paths(evaluate)
    evaluate.add_argument("--rules", type=Path, required=True)
    evaluate.add_argument("--metrics", type=Path, required=True)
    evaluate.add_argument("--operator", required=True)
    evaluate.add_argument("--candidate-digest", required=True)
    evaluate.add_argument("--origin-workspace", required=True)
    evaluate.add_argument("--evidence", action="append", default=[])
    evaluate.set_defaults(handler=evaluate_rules)

    claim = actions.add_parser("claim")
    add_runtime_paths(claim)
    claim.add_argument("--target-id", required=True)
    claim.add_argument("--consumer-id", required=True)
    claim.add_argument("--max-items", type=int, default=1)
    claim.set_defaults(handler=claim_actions)

    receipt = actions.add_parser("receipt")
    add_runtime_paths(receipt)
    receipt.add_argument("--receipt", type=Path, required=True)
    receipt.add_argument("--consumer-id", required=True)
    receipt.add_argument("--claim-token", required=True)
    receipt.set_defaults(handler=record_receipt)


def evaluate_rules(args: argparse.Namespace) -> int:
    app = application(args)
    app.initialize()
    actions = AssistantTriggerEngine(app.database).evaluate(
        load_trigger_rules(args.rules),
        read_json_object(args.metrics),
        operator=args.operator,
        candidate_digest=args.candidate_digest,
        workspace=args.origin_workspace,
        evidence=args.evidence,
    )
    return print_json({"actions": actions})


def claim_actions(args: argparse.Namespace) -> int:
    app = application(args)
    app.database.initialize()
    return print_json(
        {
            "actions": app.database.claim_assistant_actions(
                args.target_id,
                consumer_id=args.consumer_id,
                max_items=max(1, args.max_items),
            )
        }
    )


def record_receipt(args: argparse.Namespace) -> int:
    app = application(args)
    app.database.initialize()
    return print_json(
        app.database.record_assistant_action_receipt(
            read_json_object(args.receipt),
            consumer_id=args.consumer_id,
            claim_token=args.claim_token,
        )
    )
