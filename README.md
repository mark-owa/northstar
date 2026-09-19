# Northstar incident engine

Northstar is a runnable Python logistics incident-processing case study. It imports incident creation events, validates and audits each outcome, maintains incident records and indexes, schedules FIFO work and LIFO retries, simulates processing, and recovers from checkpoints and a durable authorization journal.

This is a local prototype for one worker. Processing is simulated: you choose `resolved` or `failed`. It does not contact carriers or change real shipments. Python 3.10 or newer is required; there are no third-party runtime dependencies.

**Repository history:** Northstar was developed before this GitHub repository was
created. It was published here later, so the public commit history begins with the
repository import rather than the project's original development timeline.

## Start in Visual Studio Code

1. Extract the ZIP and open the `northstar_project` folder in VS Code.
2. Open **Terminal → New Terminal** in that folder.
3. Run the isolated demonstration:

```bash
python -m northstar demo
```

If your Windows installation uses the Python launcher, substitute `py` for `python`. On some macOS/Linux installations, use `python3`.

The demo accepts two incidents, identifies a duplicate, rejects malformed rows, checks creation persistence, fails one attempt, resolves its retry, produces a report, and reloads the saved state. It uses a temporary directory and deletes that demonstration data on exit.

Run the acceptance suite:

```bash
python -m unittest discover -s tests -v
```

## Use a persistent local store

Run these commands from the project folder. `--store` goes before the command. Use a new directory for `init`; it refuses to overwrite an existing store.

```bash
python -m northstar --store my_northstar init
python -m northstar --store my_northstar import examples/events.json
python -m northstar --store my_northstar status
python -m northstar --store my_northstar next normal
python -m northstar --store my_northstar process normal --outcome failed --reason "Carrier system unavailable"
python -m northstar --store my_northstar next retry
python -m northstar --store my_northstar process retry --outcome resolved
python -m northstar --store my_northstar show INC-9001
python -m northstar --store my_northstar find --shipment SHIP-7711
python -m northstar --store my_northstar find --tag " FRAGILE "
python -m northstar --store my_northstar report 3
```

The example import returns **3 accepted, 1 duplicate, 3 rejected**. Its invalid `EV-10032` does not reserve the ID; the corrected occurrence is accepted. The original `INC-9001` survives a later conflicting creation.

Other commands:

```bash
python -m northstar --store my_northstar cancel INC-9002 --reason "Duplicate operational case"
python -m northstar --store my_northstar resolve INC-9003 --reason "Operations verified delivery"
python -m northstar --store my_northstar checkpoint
python -m northstar --help
```

Manual resolution is accepted only for an incident currently `failed`. The illustrated `resolve` command therefore rejects `INC-9003` if it is still queued. Cancellation is allowed from `queued` or `failed`; both cancelled and resolved are terminal.

Each CLI command loads the store and checkpoints on clean exit, including query commands that may have performed recovery. This makes it convenient for learning and small demonstrations. At large volumes, use the Python API in a long-lived process so each query does not load and save the entire store.

Exit codes: `0` means the command completed; `1` means a whole operation was rejected; `2` means input, storage, or recovery prevented completion. A completed batch may contain rejected rows and still exit `0`; inspect its counts and ordered results.

## Follow the code from intake to eligible candidate

| Responsibility | File and entry point | Question it answers |
| --- | --- | --- |
| Parse the batch | `northstar/core.py`: `parse_event_batch` | Is this JSON text containing an array? |
| Validate one event | `validate_event_envelope`, `validate_creation_fields` | Can this event be interpreted and accepted under our rules? |
| Accept atomically | `create_incident` | Do the record, indexes, queue, ledger, and audit agree? |
| Find eligible work | `next_eligible_candidate`, `assess_candidate` | Does the authoritative record still authorize this ticket? |
| Prove creation is saved | `creation_is_checkpointed` | Can recovery find this incident if an authorization survives? |
| Authorize and simulate | `northstar/engine.py`: `process_next` | Has an attempt slot been durably consumed before work begins? |
| Save and recover | `northstar/storage.py`: `save_checkpoint`, `load_store` | What remains true after the process disappears? |
| Exercise the contract | `tests/` | What observable evidence supports the design? |

Read `create_incident` first, then its tests in `tests/test_intake.py`. Follow one accepted row, one rejected row, and one duplicate. Predict which structures change before running anything. The persistence layer combines concepts beyond the initial Python lessons; `docs/ARCHITECTURE.md` explains its boundaries separately.

## Python API

Here is a small session you can save in a new script in the project folder. It assumes a new `api_store` directory; subsequent sessions should use `load_store` instead of `initialize_store`.

```python
from pathlib import Path
from northstar import (
    exclusive_store, initialize_store, ingest_json,
    next_eligible_candidate, process_next, save_checkpoint,
)

with exclusive_store("api_store"):
    state = initialize_store("api_store")
    text = Path("examples/events.json").read_text(encoding="utf-8")
    print(ingest_json(state, text)["counts"])

    # Acceptance in memory and durable creation are separate facts.
    print(next_eligible_candidate(state, "normal"))
    save_checkpoint(state)

    print(process_next(state, "normal", outcome="failed", reason="Temporary outage"))
    print(process_next(state, "retry", outcome="resolved"))
    save_checkpoint(state)
```

Keep `exclusive_store` for the full session and serialize calls within that session. The core deliberately exposes dictionaries for study; use the public functions to change them. Directly editing state, mutating it from callbacks, sharing it between threads, or running two independent state copies against one directory is outside the supported API contract.

`get_incident` returns a deep copy. `find_by_tag` and `find_by_shipment` return copied sets. Reading these query results cannot change authoritative records or indexes.

`process_next` requires an explicit lane (`normal` or `retry`) and outcome (`failed` or `resolved`). The current design does not impose fairness between lanes. The engine preserves each lane's order and lets the caller decide which lane to service.

| Candidate result | Meaning and next action |
| --- | --- |
| `eligible` | A current ticket is available. Selection alone consumes no authorization. |
| `waiting_checkpoint` | Its creation has not been saved. Checkpoint successfully and select again. |
| `empty` | No eligible ticket remains in this lane. |
| `recovery_order_required` | Recovery lacks enough information to rank interrupted failures. Supply an operator-approved retry order. |

## Persistence and recovery

The store contains `state.json` (a complete checkpoint) and `authorizations.jsonl` (uncompacted authorization history). Keep these files together. The CLI holds an operating-system file lock while it owns the store; the OS releases ownership when that process exits.

The ordinary checkpoint trigger is **1,000 accepted events or 5 minutes**, whichever is observed first. `ingest_json` checks after each row. A long-lived application must also call `maybe_checkpoint(state)` between other operations or on its timer, then `save_checkpoint(state)` at clean shutdown. There is no background thread. A delayed or failed checkpoint extends the possible replay window.

Before processing, an incident's creation must be in a completed checkpoint. Every attempt then appends one small authorization record and flushes/fsyncs it. A full checkpoint is not required before every attempt.

An authorization committed before a crash consumes its slot even if no work actually began. Three committed authorizations exhaust the automatic budget. Complete surviving journal records are conservatively treated as consumed, including a record whose caller never received confirmation. An incomplete final append can be trimmed; a malformed complete record stops recovery.

If an authorization or later processing operation is interrupted, the live state is marked as requiring recovery. Further writes are blocked. Release the session and reopen using `load_store`; do not catch that error and keep processing with the old state.

An interrupted attempt becomes `failed` without resetting its count. If only one retry is eligible, its order is unambiguous. If several entries cannot be ranked from saved evidence, retry processing pauses for an explicit order. For example:

```bash
python -m northstar --store my_northstar status
python -m northstar --store my_northstar recover-order INC-9300 INC-9400
```

The IDs must list **every eligible failed incident exactly once, from bottom to top**. The final ID will be retried first. Preserve the relative order of entries that already have known scheduling sequences. `status` lists incidents with unknown order; the report and incident queries help identify other eligible failed incidents. Cancellation or manual resolution can also remove work from consideration.

Bad authoritative snapshots or complete corrupt journal records stop startup with a clear error and preserve the files. Retain a copy of the entire affected store for investigation. Restore a known-good checkpoint and compatible journal together if available. Do not delete the journal to make startup succeed: doing so can erase consumed attempt evidence. If no valid copy exists, explicit reconciliation is required; the engine cannot reconstruct facts that are absent from all surviving files.

The supported durability boundary is an **application process crash on local storage**. Ordinary incident changes, successful event IDs, cancellations, outcomes, and detailed audit entries after the last completed checkpoint may be lost. Durable attempt authorizations have the stronger journal guarantee. Actual carrier actions, external exactly-once behavior, physical disk loss, arbitrary file tampering, and host power-loss durability are not claimed. This implementation does not fsync directory metadata or implement database-grade storage recovery.

## Validation and measured limits

The included suite has **52 tests**, with additional parameterized cases. It covers validation, rollback, indexes, ordering, transitions, reporting, checkpoint failures, corrupt recovery data, repeated replay, process locking, CLI use, and actual subprocess termination at journal/checkpoint boundaries.

A synthetic run on Python 3.12.14/Linux used **500,000 accepted incident creations plus 1,500,000 duplicate arrivals**, retaining **2,000,000 audit entries**. There were 500,000 successful event IDs: rejected and duplicate arrivals do not create new successful IDs.

| Observation | Measured result |
| --- | ---: |
| Create 500,000 incidents in memory | 9.03 s |
| Identify and audit 1,500,000 duplicate arrivals | 5.87 s |
| 100,000 ID lookups | 0.061 s |
| 100,000 tag lookups, copying 5,000,000 total matching IDs | 0.234 s |
| Top 20 unresolved report | 0.395 s |
| One complete checkpoint | 14.83 s |
| Validate, restore, and rebuild indexes | 21.96 s |
| Serialized checkpoint size | 543,000,357 bytes |
| Peak process memory across the benchmark | 2,508,320,768 bytes |

These are observations from one synthetic run, not enterprise capacity guarantees. Fixture ingestion bypassed periodic saves to isolate costs. The run did not measure sustained worker throughput with per-attempt fsync or repeated full checkpoints. The complete audit history and full snapshots consume substantial memory, storage, and pause time; retention and incremental persistence would be separate future requirements.

Reproduce a small run or the target run:

```bash
python benchmarks/benchmark.py --with-persistence
python benchmarks/benchmark.py --incidents 500000 --events 2000000 --with-persistence
```

The large run creates a temporary snapshot and deletes it afterward. Allow several gigabytes of RAM and enough free disk space. See `docs/benchmark_results.json` for the exact results and `docs/VALIDATION.md` for the acceptance evidence.

## Deliberate implementation boundaries

- JSON duplicate object-member names and nonstandard constants such as `NaN` are rejected, avoiding ambiguous input. Ordinary repeated tags are valid and are normalized/deduplicated.
- A journal record is limited to **1 MiB**. If an exceptionally large incident ID makes an authorization exceed that limit, processing is rejected before any journal write or work. Intake itself does not impose this additional ID-length limit.
- Full batch parsing and reports use temporary memory. The report scans and sorts a separate list; it does not maintain a continuously updated priority index.
- This version accepts JSON **creation** events. Cancellation, manual resolution, and simulated worker outcomes use explicit Python/CLI operations; no additional inbound event schema was invented.
- The tests ran on Linux/Python 3.12.14. A Windows locking path is included, but was not executed in this environment.

