"""Reproducible synthetic workload; no external services or third-party packages.

Example: python benchmarks/benchmark.py --incidents 500000 --events 2000000
         --with-persistence --output docs/benchmark_results.json
"""
import argparse
import gc
import json
import platform
import sys
import tempfile
import time
from collections import deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from northstar import (
    create_incident, find_by_shipment, find_by_tag, initialize_store,
    load_store, new_state, top_unresolved,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--incidents", type=int, default=10000)
    parser.add_argument("--events", type=int, default=40000)
    parser.add_argument("--lookups", type=int, default=100000)
    parser.add_argument("--queue-operations", type=int, default=100000)
    parser.add_argument("--with-persistence", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not (1 <= args.incidents <= args.events) or min(args.lookups, args.queue_operations) < 1:
        parser.error("require 1 <= incidents <= events and positive operation counts")
    metrics = {"python": sys.version.split()[0], "platform": platform.platform(),
               "workload": vars(args) | {"output": str(args.output) if args.output else None},
               "notes": ["Synthetic creation events followed by duplicate arrivals; every arrival is audited.",
                         "Duplicate arrivals do not add successful event IDs.",
                         "Fixture ingestion bypasses periodic checkpoints to isolate individual costs.",
                         "Queue timing rotates a copy of the scheduling deque; it performs no business processing.",
                         "Single run on this machine; timings are observations, not service guarantees."]}

    def measure(label, operation):
        start = time.perf_counter()
        value = operation()
        seconds = time.perf_counter() - start
        metrics[label + "_seconds"] = round(seconds, 6)
        print(json.dumps({"step": label, "seconds": round(seconds, 6)}), flush=True)
        return value

    state = new_state()
    groups = min(10000, args.incidents)

    def build():
        for i in range(args.incidents):
            result = create_incident(state, {
                "event_id": f"EV-{i}", "incident_id": f"INC-{i:07d}",
                "shipment_id": f"SHIP-{i % groups}", "type": "damaged", "region": "APAC",
                "priority": i % 5 + 1, "tags": [f"tag-{i % groups}", "fragile"]})
            if result["outcome"] != "accepted":
                raise AssertionError(result)

    measure("create_incidents", build)

    def duplicates():
        for i in range(args.events - args.incidents):
            result = create_incident(state, {"event_id": f"EV-{i % args.incidents}"})
            if result["outcome"] != "duplicate":
                raise AssertionError(result)

    measure("duplicate_arrivals_with_audit", duplicates)

    def id_lookups():
        total = 0
        for i in range(args.lookups):
            total += state["incidents_by_id"][f"INC-{i % args.incidents:07d}"]["priority"]
        return total

    def tag_lookups():
        return sum(len(find_by_tag(state, f"tag-{i % groups}")) for i in range(args.lookups))

    def shipment_lookups():
        return sum(len(find_by_shipment(state, f"SHIP-{i % groups}")) for i in range(args.lookups))

    measure("id_lookups", id_lookups)
    metrics["tag_results_returned"] = measure("tag_lookups_with_result_copy", tag_lookups)
    metrics["shipment_results_returned"] = measure("shipment_lookups_with_result_copy", shipment_lookups)

    def rotate():
        queue = deque(state["normal_queue"])
        start = time.perf_counter()
        for _ in range(args.queue_operations):
            queue.append(queue.popleft())
        return round(time.perf_counter() - start, 6)

    metrics["deque_pop_append_pairs_seconds"] = rotate()
    metrics["report_top_20"] = measure("report_top_20", lambda: top_unresolved(state, 20))
    metrics["state_counts"] = {"incidents": len(state["incidents_by_id"]),
                               "successful_event_ids": len(state["successful_event_ids"]),
                               "audit_entries": len(state["audit_trail"]),
                               "normal_tickets": len(state["normal_queue"])}
    if args.with_persistence:
        with tempfile.TemporaryDirectory(prefix="northstar_benchmark_") as directory:
            measure("complete_checkpoint", lambda: initialize_store(directory, state))
            metrics["checkpoint_bytes"] = (Path(directory) / "state.json").stat().st_size
            del state
            gc.collect()
            restored = measure("validated_restore_and_index_rebuild", lambda: load_store(directory))
            assert len(restored["incidents_by_id"]) == args.incidents
            assert len(restored["successful_event_ids"]) == args.incidents
            assert len(restored["audit_trail"]) == args.events
            assert len(restored["normal_queue"]) == args.incidents
            assert top_unresolved(restored, 20) == metrics["report_top_20"]
            metrics["restore_count_and_report_checks"] = "passed"
            del restored
            gc.collect()
    try:
        import resource
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        metrics["process_peak_rss_bytes"] = rss if sys.platform == "darwin" else rss * 1024
    except ImportError:
        metrics["process_peak_rss_bytes"] = None
    output = json.dumps(metrics, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
    print(output, flush=True)


if __name__ == "__main__":
    main()

