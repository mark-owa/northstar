# Acceptance evidence

Validation run: 2026-09-13, Linux, Python 3.12.14. Command:

```bash
python -m unittest discover -s tests -v
```

**52 tests passed.** Several methods contain multiple subcases; the count is test methods, not every input permutation. Tests use temporary stores and simulated processing. They do not make external carrier requests.

| Requirement or risk | Evidence |
| --- | --- |
| Reject malformed batches before accepted-state changes | Invalid syntax, wrong root, non-text input, ambiguous JSON members, and nonstandard constants |
| Continue after invalid rows | Valid–invalid–valid batch; required fields, IDs, exact priorities, tags |
| Corrected rejected ID remains usable | Invalid occurrence, corrected occurrence, accepted duplicate |
| Duplicate checks precede other creation validation | Minimal duplicate containing only an already accepted event ID |
| Preserve existing incident on conflicting creation | Compare every accepted structure before/after rejection |
| Atomic accepted creation | Inject exceptions after record, index bucket/membership, queue, ledger, and success-audit mutations |
| Preserve shared memberships during rollback | Existing shipment/tag memberships survive; new buckets are removed |
| FIFO/LIFO semantics | Acceptance order independent of numeric IDs; later failure retried first |
| Stale entries do not consume attempts | Unknown/cancelled entries, old failed-attempt number, wrong sequence, malformed tickets |
| Three durable authorizations maximum | Exhaustion, repeated recovery, actual process death after third authorization |
| Legal transitions | Cancellation restrictions, terminal states, manual-resolution reason, rollback after operator failure |
| Reporting does not change processing order | Exact report results; accepted-state structures unchanged |
| Complete checkpoint and restore | Full incident fields, event ledger, audit timestamps, FIFO order, retry order, indexes |
| Failed temporary save preserves old checkpoint | Fail after partial write, after fsync, and before replacement |
| Ambiguous commit blocks unsafe continuation | Exception after replacement requires reopen |
| Cleanup failure is safe | Retained journal entries do not revive a checkpointed resolution |
| Corrupt authority stops recovery | Invalid status, attempts, priority, normalization, sequences, ledger, allocator, watermark, shape |
| Derived ticket repair | Missing tickets and unknown/cancelled tickets rebuilt from independent incident metadata |
| Uncertain authorization append | Before write, after flushed write, after fsync; no worker start; live state requires recovery |
| Journal prefix handling | Incomplete final tail trimmed; invalid complete/interior data preserved and rejected |
| Journal semantics | Unknown incident, wrong store, gaps, repeated IDs, skipped attempts, invalid count/timestamp |
| Explicit recovery ordering | Several interrupted failures; known order preserved; cancellation cannot let a stale ticket block repair |
| Checkpoint trigger | 1,000 accepted records; 5-minute threshold; ordinary intake checkpoint integration |
| One process owns a store | Competing CLI process rejected; process death releases OS lock |
| CLI usability | Import, failed normal attempt, successful retry, incident query, normalized tag lookup |

## Real process interruption tests

`tests/crash_worker.py` calls `os._exit(77)` from controlled hooks. This bypasses exception handling and context-manager cleanup, exercising recovery from actual process termination.

Boundaries include a flushed journal write, fsynced authorization, authorization before work, work before an outcome checkpoint, partial checkpoint, checkpoint before replacement, completed replacement before publication, and journal cleanup. A third authorization crash test checks specifically that recovery cannot authorize a fourth attempt.

These tests establish behavior for the implemented single-writer process-crash model. They do not emulate host power loss, faulty disks, distributed workers, adversarial valid-looking changes to files, or external business effects. The Windows file-lock branch has not been executed here.

## Scale experiment

`benchmarks/benchmark.py` generated 500,000 valid creations and 1,500,000 duplicate arrivals. It measured lookup/result-copy costs, a temporary report, a complete checkpoint, and validated restore. Counts and report results matched after restart. Raw results are in `benchmark_results.json`.

The 2 million arrivals produced 2 million audit entries and 500,000 distinct successful event IDs. The fixture bypassed periodic saves while being assembled. Therefore the measurements do not imply that production ingestion with repeated full checkpoints, actual processing, or per-attempt fsync achieves the same throughput.

One defect found during review was repaired: a cancelled retry could retain a stale ticket and incorrectly veto recovery ordering. The repair derives surviving known order from authoritative incident records. A dedicated regression test covers that case.

