# Northstar incident engine

Northstar is a Python logistics incident-processing case study focused on reliable state transitions, deduplication, retry scheduling, auditability, and crash recovery.

It accepts incident-creation events, validates them before commit, keeps authoritative records plus derived indexes, schedules normal work FIFO and retries LIFO, and persists recovery state through checkpoints plus an authorization journal. Processing is simulated; it does not contact carriers or modify real shipments.

**Repository history:** Northstar was developed before this GitHub repository was created. The public history starts with the later repository import rather than the original local development timeline.

## Why it exists

The project explores a common backend problem: accepting events is easy; keeping state correct across duplicates, failures, retries, cancellation, persistence, and restart is harder.

Key invariants include:

- an event ID is recorded as successful only after its incident creation commits;
- authoritative incident state drives eligibility, not stale queue entries;
- creation updates the record, indexes, queue, ledger, and audit trail as one logical mutation;
- interrupted processing consumes the durable authorization that was already committed;
- malformed snapshots or journal records stop recovery instead of silently guessing.

## Architecture

| Area | Main code | Responsibility |
| --- | --- | --- |
| Intake and validation | `northstar/core.py` | Parse events, validate fields, deduplicate, commit state |
| Scheduling | `northstar/core.py` | FIFO normal queue, LIFO retry stack, stale-entry checks |
| Processing | `northstar/engine.py` | Authorize attempts, apply outcomes, handle retry budget |
| Persistence | `northstar/storage.py` | Checkpoints, authorization journal, locking, recovery |
| CLI | `northstar/__main__.py` | Demo and persistent-store commands |
| Evidence | `tests/` | Regression and crash/recovery behavior |

For the detailed state model and recovery rules, see [Architecture](docs/ARCHITECTURE.md).

## Run it

Requires Python 3.10+ and has no third-party runtime dependencies.

```bash
python -m northstar demo
python -m unittest discover -s tests -v
```

The isolated demo accepts valid incidents, rejects malformed rows, detects a duplicate, checkpoints creation, fails one processing attempt, resolves its retry, generates a report, and reloads the saved state.

For a persistent local store:

```bash
python -m northstar --store my_northstar init
python -m northstar --store my_northstar import examples/events.json
python -m northstar --store my_northstar status
python -m northstar --store my_northstar process normal --outcome failed --reason "Carrier system unavailable"
python -m northstar --store my_northstar process retry --outcome resolved
python -m northstar --store my_northstar report 3
```

The example import returns **3 accepted, 1 duplicate, and 3 rejected**. A malformed event does not reserve its event ID, so a corrected occurrence can later be accepted.

## Verification

The suite covers validation, rollback, indexes, ordering, state transitions, checkpoint failures, corrupt recovery data, replay, process locking, CLI behavior, and subprocess termination around durability boundaries.

A recorded synthetic benchmark used **500,000 accepted incidents plus 1,500,000 duplicate arrivals**. The run measured a roughly **543 MB checkpoint** and roughly **2.5 GB peak process memory**, which also exposes the design's main scaling limit: full snapshots and an ever-growing audit history are expensive.

See [Validation](docs/VALIDATION.md) and the raw [benchmark results](docs/benchmark_results.json) for the exact evidence and caveats.

## Scope

Northstar is a local one-worker prototype, not a carrier integration or production database.

- processing outcomes are simulated;
- checkpointed state after the last completed snapshot can be replayed after a crash;
- authorization journal entries have stronger durability than ordinary post-checkpoint state;
- no external exactly-once guarantee is claimed;
- full snapshots and retained audit history are intentionally simple and become expensive at large scale;
- Windows locking support exists but the recorded validation run was on Linux/Python 3.12.14.

## Docs

- [Architecture and recovery model](docs/ARCHITECTURE.md)
- [Validation evidence](docs/VALIDATION.md)
- [Benchmark measurements](docs/benchmark_results.json)
