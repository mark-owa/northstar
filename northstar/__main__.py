"""Run `python -m northstar demo` or `python -m northstar --help`."""

import argparse
import json
import sys
import tempfile
from collections import Counter
from pathlib import Path

from . import (
    PersistenceError, RecoveryError, RecoveryRequired, ValidationError, cancel_incident,
    exclusive_store, find_by_shipment, find_by_tag, get_incident, ingest_json,
    initialize_store, load_store, next_eligible_candidate, process_next,
    resolve_failed_incident, resolve_recovery_order, save_checkpoint, top_unresolved,
)


def _print(value):
    print(json.dumps(value, indent=2, ensure_ascii=True, default=lambda x: sorted(x) if isinstance(x, set) else str(x)))


def demo():
    with tempfile.TemporaryDirectory(prefix="northstar_demo_") as directory:
        state = initialize_store(directory)
        events = [
            {"event_id": "EV-1", "incident_id": "INC-1", "shipment_id": "SHIP-1", "type": "damaged",
             "priority": 4, "region": "APAC", "tags": [" Fragile ", "fragile", "electronics"]},
            {"event_id": "EV-1"},
            42,
            {"event_id": "EV-2", "incident_id": "INC-2", "shipment_id": "SHIP-1", "type": "delayed",
             "priority": True, "region": "EU", "tags": []},
            {"event_id": "EV-2", "incident_id": "INC-2", "shipment_id": "SHIP-1", "type": "delayed",
             "priority": 5, "region": "EU", "tags": ["urgent"]},
        ]
        result = ingest_json(state, json.dumps(events))
        _print({"step": "import", "counts": result["counts"]})
        _print({"step": "before_checkpoint", "candidate": next_eligible_candidate(state, "normal")})
        save_checkpoint(state)
        _print({"step": "first_normal_attempt", "result": process_next(state, "normal", outcome="failed")})
        _print({"step": "retry", "result": process_next(state, "retry", outcome="resolved")})
        _print({"step": "report", "unresolved": top_unresolved(state, 3),
                "shipment_matches": find_by_shipment(state, "SHIP-1")})
        save_checkpoint(state)
        recovered = load_store(directory)
        _print({"step": "restart", "incidents": recovered["incidents_by_id"],
                "next_normal": next_eligible_candidate(recovered, "normal")})


def parser():
    root = argparse.ArgumentParser(description="Northstar incident engine (local, one worker, simulated processing)")
    root.add_argument("--store", default="northstar_data", help="state directory; place this option before the command")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("demo", help="run an isolated end-to-end example without touching your store")
    commands.add_parser("init", help="initialize a NEW store; existing files are never overwritten")
    commands.add_parser("status", help="show counts, checkpoint state, and unresolved recovery ordering")
    commands.add_parser("checkpoint", help="save a complete checkpoint and compact covered journal records")
    ingest = commands.add_parser("import", help="import creation events from a JSON file")
    ingest.add_argument("file")
    show = commands.add_parser("show", help="retrieve an incident")
    show.add_argument("incident_id")
    find = commands.add_parser("find", help="query an index")
    field = find.add_mutually_exclusive_group(required=True)
    field.add_argument("--shipment")
    field.add_argument("--tag")
    report = commands.add_parser("report", help="highest-priority unresolved incident IDs")
    report.add_argument("k", type=int)
    candidate = commands.add_parser("next", help="inspect a candidate; does not process it")
    candidate.add_argument("lane", choices=["normal", "retry"])
    process = commands.add_parser("process", help="simulate one attempt in an explicitly selected lane")
    process.add_argument("lane", choices=["normal", "retry"])
    process.add_argument("--outcome", choices=["resolved", "failed"], required=True)
    process.add_argument("--reason", default="simulated processing failure")
    cancel = commands.add_parser("cancel", help="cancel a queued or failed incident")
    cancel.add_argument("incident_id")
    cancel.add_argument("--reason")
    resolve = commands.add_parser("resolve", help="manually resolve a failed incident")
    resolve.add_argument("incident_id")
    resolve.add_argument("--reason", required=True)
    order = commands.add_parser("recover-order", help="supply ALL eligible failed IDs in bottom-to-top retry order")
    order.add_argument("incident_ids", nargs="+")
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    if args.command == "demo":
        demo()
        return 0
    try:
        with exclusive_store(args.store):
            if args.command == "init":
                state = initialize_store(args.store)
                _print({"outcome": "initialized", "directory": str(Path(args.store).resolve())})
                return 0
            state = load_store(args.store)
            command = args.command
            if command == "import":
                result = ingest_json(state, Path(args.file).read_text(encoding="utf-8"))
            elif command == "status":
                result = {"incidents": len(state["incidents_by_id"]),
                          "statuses": dict(Counter(r["status"] for r in state["incidents_by_id"].values())),
                          "successful_event_ids": len(state["successful_event_ids"]),
                          "audit_entries": len(state["audit_trail"]),
                          "checkpoint_id": state["checkpoint_id"],
                          "pending_recovery_order": sorted(state["_runtime"]["pending_recovery"])}
            elif command == "show":
                result = get_incident(state, args.incident_id)
            elif command == "find":
                result = find_by_tag(state, args.tag) if args.tag is not None else find_by_shipment(state, args.shipment)
            elif command == "report":
                result = top_unresolved(state, args.k)
            elif command == "next":
                result = next_eligible_candidate(state, args.lane)
            elif command == "process":
                result = process_next(state, args.lane, outcome=args.outcome, reason=args.reason)
            elif command == "cancel":
                result = cancel_incident(state, args.incident_id, reason=args.reason)
            elif command == "resolve":
                result = resolve_failed_incident(state, args.incident_id, args.reason)
            elif command == "recover-order":
                result = resolve_recovery_order(state, args.incident_ids)
            else:
                result = {"outcome": "checkpoint_requested"}
            saved = save_checkpoint(state)  # Clean shutdown, including recovery changes.
            _print(result)
            if not saved["journal_compacted"]:
                print("Checkpoint committed; journal cleanup will be retried later.", file=sys.stderr)
            return 1 if isinstance(result, dict) and result.get("outcome") == "rejected" else 0
    except (PersistenceError, RecoveryError, RecoveryRequired, ValidationError, OSError) as exc:
        print(f"Northstar: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
