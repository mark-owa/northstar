"""JSON checkpoints and a bounded JSONL authorization journal.

The supported failure model is an application-process crash on local storage,
with one writer. Filesystem/hardware loss and arbitrary external edits are not
an exactly-once guarantee. A doubtful append fences the live state until reopen.
"""

import json
import os
import tempfile
import time
import uuid
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

from .core import (
    INCIDENT_FIELDS, MAX_AUTHORIZATIONS, STATUSES, ValidationError, _allocate_sequence,
    _ensure_writable, _hit, new_state, record_audit, strict_json_loads, usable_id,
    validate_creation_fields,
)

MAX_JOURNAL_LINE_BYTES = 1_048_576
CHECKPOINT_EVENTS = 1000
CHECKPOINT_SECONDS = 300


class PersistenceError(RuntimeError):
    pass


class RecoveryError(RuntimeError):
    pass


@contextmanager
def exclusive_store(directory):
    """Cross-process CLI lock; the OS releases it if the process exits/crashes."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".northstar.lock").open("a+b") as stream:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise PersistenceError("Another process owns this Northstar directory.") from exc
        try:
            yield directory
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _directory(state):
    directory = state["_runtime"]["directory"]
    if directory is None:
        raise PersistenceError("Use initialize_store() or load_store() to bind persistent state.")
    return Path(directory)


def _atomic_text(path, chunks, fault=None, prefix="checkpoint"):
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            chunks = iter(chunks)
            stream.write(next(chunks, ""))
            _hit(fault, prefix + "_partial")
            stream.writelines(chunks)
            stream.flush()
            os.fsync(stream.fileno())
            _hit(fault, prefix + "_synced")
        _hit(fault, prefix + "_before_replace")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _snapshot(state, covered_id):
    return {
        "version": 1,
        "store_id": state["store_id"],
        "checkpoint_id": state["checkpoint_id"] + 1,
        "authorization_watermark": covered_id,
        "next_sequence": state["next_sequence"],
        "incidents_by_id": state["incidents_by_id"],
        "successful_event_ids": list(state["successful_event_ids"]),
        "normal_queue": list(state["normal_queue"]),
        "retry_stack": list(state["retry_stack"]),
        "audit_trail": state["audit_trail"],
    }


def save_checkpoint(state, *, fault=None):
    _ensure_writable(state)
    directory = _directory(state)
    runtime = state["_runtime"]
    # This records the fully applied prefix, not a speculative allocation counter.
    covered_id = runtime["applied_authorization_id"]
    payload = _snapshot(state, covered_id)
    creation_ids = set(state["incidents_by_id"])
    encoder = json.JSONEncoder(ensure_ascii=True, allow_nan=False, separators=(",", ":"))
    try:
        _atomic_text(directory / "state.json", encoder.iterencode(payload), fault)
    except Exception as exc:
        raise PersistenceError(f"checkpoint did not commit: {exc}") from exc
    try:
        _hit(fault, "checkpoint_replaced")
    except Exception as exc:
        runtime["recovery_required"] = True
        raise PersistenceError("Checkpoint replaced, but confirmation was interrupted; reopen store.") from exc
    state["checkpoint_id"] = payload["checkpoint_id"]
    state["authorization_watermark"] = covered_id
    runtime["checkpointed_creation_ids"] = creation_ids
    runtime["accepted_since_checkpoint"] = 0
    runtime["last_checkpoint_time"] = time.monotonic()
    # Serialized ownership guarantees that all journal entries are now covered.
    # Failure here leaves a valid checkpoint, with covered entries possibly retained.
    try:
        _hit(fault, "checkpoint_committed")
        _atomic_text(directory / "authorizations.jsonl", (), fault, "journal_cleanup")
    except Exception as exc:
        record_audit(state, "journal_cleanup", "failed", reason=str(exc))
        return {"checkpoint_id": state["checkpoint_id"], "journal_compacted": False,
                "warning": str(exc)}
    return {"checkpoint_id": state["checkpoint_id"], "journal_compacted": True}


def checkpoint_due(state, *, now=None):
    runtime = state["_runtime"]
    now = time.monotonic() if now is None else now
    return (runtime["accepted_since_checkpoint"] >= CHECKPOINT_EVENTS or
            now - runtime["last_checkpoint_time"] >= CHECKPOINT_SECONDS)


def maybe_checkpoint(state):
    return save_checkpoint(state) if checkpoint_due(state) else None


def initialize_store(directory, state=None):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / "state.json").exists() or (directory / "authorizations.jsonl").exists():
        raise PersistenceError("Store files already exist. Reopen them; initialization never overwrites.")
    state = new_state() if state is None else state
    if state["checkpoint_id"] != 0 or state["_runtime"]["directory"] is not None:
        raise PersistenceError("Only fresh in-memory state can initialize a store.")
    state["_runtime"]["directory"] = str(directory)
    with (directory / "authorizations.jsonl").open("xb") as stream:
        stream.flush()
        os.fsync(stream.fileno())
    save_checkpoint(state)
    return state


def append_authorization(state, record, *, fault=None):
    """Return only after fsync. Any doubtful write requires recovery, not a retry."""
    _ensure_writable(state)
    encoded = (json.dumps(record, ensure_ascii=True, allow_nan=False,
                          separators=(",", ":")) + "\n").encode("utf-8")
    if len(encoded) > MAX_JOURNAL_LINE_BYTES:
        raise PersistenceError("Authorization exceeds the documented 1 MiB journal-record limit.")
    path = _directory(state) / "authorizations.jsonl"
    try:
        # r+b avoids silently recreating a journal somebody removed externally.
        with path.open("r+b") as stream:
            stream.seek(0, os.SEEK_END)
            _hit(fault, "journal_before_write")
            if stream.write(encoded) != len(encoded):
                raise OSError("short journal write")
            stream.flush()
            _hit(fault, "journal_written")
            os.fsync(stream.fileno())
            _hit(fault, "journal_synced")
    except Exception as exc:
        state["_runtime"]["recovery_required"] = True
        raise PersistenceError("Authorization write is uncertain; no work started. Reopen the store.") from exc


def _integer(value, minimum=0):
    return type(value) is int and value >= minimum


def _timestamp(value):
    try:
        return isinstance(value, str) and datetime.fromisoformat(
            value.replace("Z", "+00:00")).utcoffset() == timedelta(0)
    except ValueError:
        return False


def _require(condition, message):
    if not condition:
        raise RecoveryError(message)


def validate_snapshot(data):
    required = {"version", "store_id", "checkpoint_id", "authorization_watermark", "next_sequence",
                "incidents_by_id", "successful_event_ids", "normal_queue", "retry_stack", "audit_trail"}
    _require(isinstance(data, dict) and set(data) == required, "snapshot fields/shape are invalid")
    _require(type(data["version"]) is int and data["version"] == 1, "unsupported snapshot version")
    try:
        _require(str(uuid.UUID(data["store_id"])) == data["store_id"], "invalid store_id")
    except (ValueError, TypeError, AttributeError) as exc:
        raise RecoveryError("invalid store_id") from exc
    for key, minimum in (("checkpoint_id", 1), ("authorization_watermark", 0), ("next_sequence", 1)):
        _require(_integer(data[key], minimum), f"invalid {key}")
    records, ledger = data["incidents_by_id"], data["successful_event_ids"]
    _require(isinstance(records, dict), "incidents_by_id must be an object")
    _require(isinstance(ledger, list) and all(usable_id(x) for x in ledger), "invalid event ledger")
    ledger_set = set(ledger)
    _require(len(ledger_set) == len(ledger), "duplicate entries in event ledger")
    sequences, creation_events, last_auth_ids = set(), set(), set()
    total_authorizations = 0
    for identifier, incident in records.items():
        _require(usable_id(identifier) and isinstance(incident, dict) and
                 set(incident) == INCIDENT_FIELDS, "invalid incident shape/fields")
        _require(identifier == incident["incident_id"], "incident key/id mismatch")
        event = {key: incident[key] for key in ("incident_id", "shipment_id", "type", "priority", "region", "tags")}
        event["event_id"] = incident["creation_event_id"]
        try:
            _require(validate_creation_fields(event) == event, "incident fields are not normalized")
        except ValidationError as exc:
            raise RecoveryError(f"invalid incident {identifier}: {exc}") from exc
        _require(event["event_id"] in ledger_set and event["event_id"] not in creation_events,
                 "creation is absent from ledger or its event ID is reused")
        creation_events.add(event["event_id"])
        status, attempts = incident["status"], incident["attempts"]
        _require(isinstance(status, str) and status in STATUSES, "invalid status")
        _require(_integer(attempts) and attempts <= MAX_AUTHORIZATIONS, "invalid authorization count")
        total_authorizations += attempts
        _require(type(incident["recovery_pending"]) is bool, "invalid recovery flag")
        sequence = incident["queue_sequence"]
        _require(_integer(sequence, 1) and sequence not in sequences, "missing/duplicate queue sequence")
        sequences.add(sequence)
        retry, failure, auth = (incident["retry_sequence"], incident["latest_failure_attempt"],
                                incident["last_authorization_id"])
        _require(incident["failure_reason"] is None or isinstance(incident["failure_reason"], str),
                 "invalid failure reason")
        _require(failure is None or (_integer(failure, 1) and failure <= attempts), "invalid latest failed attempt")
        if attempts == 0:
            _require(status in {"queued", "cancelled"} and auth is None and failure is None,
                     "status or history inconsistent with zero authorizations")
        else:
            _require(status != "queued" and _integer(auth, attempts) and auth <= data["authorization_watermark"]
                     and auth not in last_auth_ids, "invalid incident authorization evidence")
            last_auth_ids.add(auth)
        if status == "failed":
            _require(failure == attempts and attempts > 0 and isinstance(incident["failure_reason"], str)
                     and bool(incident["failure_reason"]), "invalid failed incident")
        if status == "failed" and attempts < MAX_AUTHORIZATIONS:
            if incident["recovery_pending"]:
                _require(retry is None, "pending recovery cannot claim known retry order")
            else:
                _require(_integer(retry, 1) and retry not in sequences, "missing/duplicate retry sequence")
                sequences.add(retry)
        else:
            _require(retry is None and not incident["recovery_pending"], "invalid scheduling metadata for status")
        if status == "processing":
            _require(failure is None or failure < attempts, "processing attempt is already marked failed")
    _require(not sequences or data["next_sequence"] > max(sequences), "sequence allocator would reuse a value")
    _require(total_authorizations == data["authorization_watermark"],
             "checkpoint watermark does not match the incorporated authorization counts")
    _require(not last_auth_ids or max(last_auth_ids) == data["authorization_watermark"],
             "checkpoint lacks the last covered authorization")
    for key in ("normal_queue", "retry_stack", "audit_trail"):
        _require(isinstance(data[key], list), f"{key} must be an array")
    for entry in data["audit_trail"]:
        _require(isinstance(entry, dict) and set(entry) == {
            "timestamp", "action", "outcome", "event_id", "incident_id", "reason", "position"}, "invalid audit entry")
        _require(_timestamp(entry["timestamp"]), "invalid audit timestamp")
        for key in ("action", "outcome"):
            _require(isinstance(entry[key], str) and bool(entry[key]), "invalid audit action/outcome")
        for key in ("event_id", "incident_id"):
            _require(entry[key] is None or usable_id(entry[key]), "invalid audit identifier")
        _require(entry["reason"] is None or isinstance(entry["reason"], str), "invalid audit reason")
        _require(entry["position"] is None or _integer(entry["position"]), "invalid audit position")
    return ledger_set


def _rebuild(state):
    shipment, tags, normal, retry, pending = {}, {}, [], [], set()
    for identifier, incident in state["incidents_by_id"].items():
        shipment.setdefault(incident["shipment_id"], set()).add(identifier)
        for tag in incident["tags"]:
            tags.setdefault(tag, set()).add(identifier)
        if incident["status"] == "queued":
            normal.append((identifier, incident["queue_sequence"]))
        if incident["status"] == "failed" and incident["attempts"] < MAX_AUTHORIZATIONS:
            if incident["recovery_pending"]:
                pending.add(identifier)
            else:
                retry.append((identifier, incident["latest_failure_attempt"], incident["retry_sequence"]))
    normal.sort(key=lambda x: x[1])
    retry.sort(key=lambda x: x[2])
    state.update(shipment_index=shipment, tag_index=tags, normal_queue=deque(normal), retry_stack=retry)
    state["_runtime"]["pending_recovery"] = pending


def _replay_journal(state, path):
    """Validate the complete prefix before allowing any tail repair or state publication."""
    _require(path.exists(), "authorization journal is missing; cannot prove attempt history")
    covered, previous, applied = state["authorization_watermark"], 0, state["authorization_watermark"]
    good_offset, tail_size = 0, 0
    with path.open("rb") as stream:
        while True:
            raw = stream.readline(MAX_JOURNAL_LINE_BYTES + 1)
            if not raw:
                break
            _require(len(raw) <= MAX_JOURNAL_LINE_BYTES, "journal record exceeds size limit")
            if not raw.endswith(b"\n"):
                tail_size = len(raw)
                break
            try:
                record = strict_json_loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError, RecursionError) as exc:
                raise RecoveryError("invalid complete journal record; file preserved") from exc
            _require(isinstance(record, dict) and set(record) == {
                "store_id", "authorization_id", "incident_id", "attempt", "timestamp"}, "invalid journal schema")
            number, attempt = record["authorization_id"], record["attempt"]
            _require(record["store_id"] == state["store_id"], "journal belongs to another store")
            _require(_integer(number, 1) and number > previous, "journal IDs are not strictly increasing")
            _require(_integer(attempt, 1) and attempt <= MAX_AUTHORIZATIONS and
                     _timestamp(record["timestamp"]), "invalid journal attempt/timestamp")
            identifier = record["incident_id"]
            _require(usable_id(identifier) and identifier in state["incidents_by_id"], "unknown journal incident")
            previous = number
            if number > covered:
                _require(number == applied + 1, "authorization journal has an uncovered gap")
                incident = state["incidents_by_id"][identifier]
                _require(incident["status"] not in {"cancelled", "resolved"}, "journal would revive terminal incident")
                _require(attempt == incident["attempts"] + 1, "journal would repeat/skip an incident attempt")
                incident.update(attempts=attempt, status="processing", last_authorization_id=number,
                                retry_sequence=None, recovery_pending=False)
                applied = number
                record_audit(state, "authorization_replay", "recovered", incident_id=identifier,
                             reason=f"authorization {number}; attempt {attempt}")
            good_offset = stream.tell()
    state["_runtime"]["applied_authorization_id"] = applied
    return good_offset, tail_size


def load_store(directory):
    """Build and validate a separate state; errors never overwrite the saved checkpoint."""
    directory = Path(directory).resolve()
    try:
        data = strict_json_loads((directory / "state.json").read_text(encoding="utf-8"))
        ledger = validate_snapshot(data)
        state = new_state()
        state.update(data)
        state["successful_event_ids"] = ledger
        runtime = state["_runtime"]
        runtime["directory"] = str(directory)
        runtime["checkpointed_creation_ids"] = set(state["incidents_by_id"])
        runtime["applied_authorization_id"] = state["authorization_watermark"]
        saved_normal, saved_retry = data["normal_queue"], data["retry_stack"]
        _rebuild(state)
        if (saved_normal != [list(x) for x in state["normal_queue"]] or
                saved_retry != [list(x) for x in state["retry_stack"]]):
            record_audit(state, "scheduling_reconciliation", "recovered",
                         reason="derived tickets reconstructed from authoritative scheduling metadata")
        offset, tail_size = _replay_journal(state, directory / "authorizations.jsonl")
        for identifier, incident in state["incidents_by_id"].items():
            if incident["status"] == "processing":
                incident.update(status="failed", latest_failure_attempt=incident["attempts"],
                                failure_reason="interrupted authorization; outcome was not checkpointed",
                                retry_sequence=None, recovery_pending=incident["attempts"] < MAX_AUTHORIZATIONS)
                record_audit(state, "interrupted_attempt", "recovered", incident_id=identifier,
                             reason=f"authorization budget consumed: {incident['attempts']}/3")
        _rebuild(state)
        # A single eligible retry has no relative order to invent.
        if len(runtime["pending_recovery"]) == 1 and not state["retry_stack"]:
            identifier = next(iter(runtime["pending_recovery"]))
            incident = state["incidents_by_id"][identifier]
            incident.update(recovery_pending=False, retry_sequence=_allocate_sequence(state))
            record_audit(state, "recovery_order", "recovered", incident_id=identifier,
                         reason="only one eligible retry; ordering is unambiguous")
            _rebuild(state)
        # Revalidate all recovered authoritative semantics against the applied prefix.
        candidate = _snapshot(state, runtime["applied_authorization_id"])
        validate_snapshot(candidate)
        if tail_size:
            with (directory / "authorizations.jsonl").open("r+b") as stream:
                stream.truncate(offset)
                stream.flush()
                os.fsync(stream.fileno())
            record_audit(state, "journal_tail_repair", "recovered",
                         reason=f"removed {tail_size} bytes of an interrupted final append")
        return state
    except RecoveryError:
        raise
    except (OSError, ValueError, TypeError, KeyError, RecursionError) as exc:
        raise RecoveryError(f"Cannot recover Northstar safely: {exc}. Saved checkpoint preserved.") from exc


def resolve_recovery_order(state, incident_ids):
    """Operator supplies every eligible failed ID, bottom-to-top; retain known relative order."""
    _ensure_writable(state)
    if not state["_runtime"]["pending_recovery"]:
        raise ValidationError("there is no unresolved recovery ordering")
    if not isinstance(incident_ids, list) or not all(usable_id(x) for x in incident_ids):
        raise ValidationError("provide incident IDs in an ordered list")
    eligible = {key for key, r in state["incidents_by_id"].items()
                if r["status"] == "failed" and r["attempts"] < MAX_AUTHORIZATIONS}
    if len(incident_ids) != len(set(incident_ids)) or set(incident_ids) != eligible:
        raise ValidationError("order must contain every eligible failed incident exactly once")
    # Tickets may still contain a recently cancelled/resolved incident. Obtain
    # the surviving known order from authority so stale tickets cannot veto repair.
    known = [identifier for _, identifier in sorted(
        (state["incidents_by_id"][identifier]["retry_sequence"], identifier)
        for identifier in eligible
        if not state["incidents_by_id"][identifier]["recovery_pending"])]
    known_set = set(known)
    if [x for x in incident_ids if x in known_set] != known:
        raise ValidationError("order must preserve the existing retries' known relative order")
    from .core import _put, _rollback
    undo = []
    try:
        for identifier in incident_ids:
            _put(state["incidents_by_id"][identifier], "retry_sequence", _allocate_sequence(state), undo)
            _put(state["incidents_by_id"][identifier], "recovery_pending", False, undo)
        record_audit(state, "recovery_order", "accepted",
                     reason="operator supplied bottom-to-top order: " + ", ".join(incident_ids), undo=undo)
        _rebuild(state)
    except Exception:
        _rollback(undo)
        _rebuild(state)
        raise
    return {"outcome": "accepted", "retry_order_bottom_to_top": incident_ids[:]}
