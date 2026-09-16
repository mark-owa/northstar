"""Northstar's in-memory rules. All state mutations are single-worker operations."""

import copy
import json
import time
import uuid
from collections import deque
from datetime import datetime, timezone

MAX_AUTHORIZATIONS = 3
STATUSES = {"queued", "processing", "failed", "resolved", "cancelled"}
ALLOWED_TRANSITIONS = {
    "queued": {"processing", "cancelled"},
    "processing": {"resolved", "failed"},
    "failed": {"processing", "resolved", "cancelled"},
    "resolved": set(),
    "cancelled": set(),
}
EVENT_FIELDS = ("event_id", "incident_id", "shipment_id", "type", "priority", "region", "tags")
INCIDENT_FIELDS = set(EVENT_FIELDS) - {"event_id"} | {
    "creation_event_id", "status", "attempts", "queue_sequence", "retry_sequence",
    "latest_failure_attempt", "failure_reason", "last_authorization_id", "recovery_pending",
}


class ValidationError(ValueError):
    """An input violates the documented contract."""


class RecoveryRequired(RuntimeError):
    """Stop writes and reopen the store before doing more work."""


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _hit(fault, stage):
    # Optional test hook. Production callers normally omit it.
    if fault is not None:
        fault(stage)


def _ensure_writable(state):
    if state["_runtime"]["recovery_required"]:
        raise RecoveryRequired("Persistence/processing was interrupted; reopen the store before writing.")


def new_state():
    return {
        "version": 1,
        "store_id": str(uuid.uuid4()),
        "checkpoint_id": 0,
        "authorization_watermark": 0,
        "next_sequence": 1,
        "incidents_by_id": {},
        "successful_event_ids": set(),
        "shipment_index": {},
        "tag_index": {},
        "normal_queue": deque(),
        "retry_stack": [],
        "audit_trail": [],
        "_runtime": {
            "directory": None,
            "checkpointed_creation_ids": set(),
            "applied_authorization_id": 0,
            "accepted_since_checkpoint": 0,
            "last_checkpoint_time": time.monotonic(),
            "recovery_required": False,
            "pending_recovery": set(),
        },
    }


def usable_id(value):
    return isinstance(value, str) and bool(value) and value == value.strip()


def _id(value, name):
    if not usable_id(value):
        raise ValidationError(f"{name} must be a nonempty string without surrounding whitespace")
    return value


def _nonblank(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a nonblank string")
    return value.strip()


def strict_json_loads(text):
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError(f"ambiguous JSON: duplicate member {key!r}")
            result[key] = value
        return result

    def constant(value):
        raise ValidationError(f"nonstandard JSON number: {value}")

    return json.loads(text, object_pairs_hook=object_pairs, parse_constant=constant)


def parse_event_batch(raw_json):
    """Parse the envelope only. Bad rows belong to per-row validation."""
    if not isinstance(raw_json, str):
        raise ValidationError("batch input must be JSON text")
    try:
        rows = strict_json_loads(raw_json)
    except (ValueError, RecursionError) as exc:
        raise ValidationError(f"invalid JSON: {exc}") from exc
    if not isinstance(rows, list):
        raise ValidationError("JSON root must be an array")
    return rows


def validate_event_envelope(event):
    if not isinstance(event, dict):
        raise ValidationError("event must be an object")
    return _id(event.get("event_id"), "event_id")


def validate_creation_fields(event):
    """Return fresh normalized data; never erase an invalid tag during normalization."""
    validate_event_envelope(event)
    for key in EVENT_FIELDS:
        if key not in event:
            raise ValidationError(f"missing field: {key}")
    clean = {key: _id(event[key], key) for key in EVENT_FIELDS[:3]}
    for key in ("type", "region"):
        clean[key] = _nonblank(event[key], key)
    if type(event["priority"]) is not int or not 1 <= event["priority"] <= 5:
        raise ValidationError("priority must be an integer from 1 to 5; booleans are invalid")
    clean["priority"] = event["priority"]
    if not isinstance(event["tags"], list):
        raise ValidationError("tags must be a list")
    tags, seen = [], set()
    for tag in event["tags"]:
        normalized = _nonblank(tag, "each tag").lower()
        if normalized not in seen:
            seen.add(normalized)
            tags.append(normalized)
    clean["tags"] = tags
    return clean


_MISSING = object()


def _put(mapping, key, value, undo):
    undo.append(("put", mapping, key, mapping.get(key, _MISSING)))
    mapping[key] = value


def _append(sequence, value, undo):
    undo.append(("append", sequence, len(sequence)))
    sequence.append(value)


def _set_add(values, value, undo):
    if value not in values:
        undo.append(("add", values, value))
        values.add(value)


def _rollback(undo):
    # Every undo entry exists before its mutation. No full-state copy or queue scan.
    for entry in reversed(undo):
        kind, target, key = entry[:3]
        if kind == "put":
            if entry[3] is _MISSING:
                target.pop(key, None)
            else:
                target[key] = entry[3]
        elif kind == "append":
            while len(target) > key:
                target.pop()
        else:
            target.discard(key)


def record_audit(state, action, outcome, *, event_id=None, incident_id=None,
                 reason=None, position=None, undo=None):
    entry = {
        "timestamp": utc_now(), "action": action, "outcome": outcome,
        "event_id": event_id if usable_id(event_id) else None,
        "incident_id": incident_id if usable_id(incident_id) else None,
        "reason": reason, "position": position,
    }
    if undo is None:
        state["audit_trail"].append(entry)
    else:
        _append(state["audit_trail"], entry, undo)
    return entry


def _allocate_sequence(state):
    sequence = state["next_sequence"]
    state["next_sequence"] += 1
    return sequence  # Gaps are permitted; allocated numbers are not reused in this state.


def _add_membership(index, key, incident_id, undo, fault, label):
    if key not in index:
        _put(index, key, set(), undo)
        _hit(fault, f"bucket:{label}:{key}")
    _set_add(index[key], incident_id, undo)
    _hit(fault, f"membership:{label}:{key}")


def create_incident(state, event, *, position=None, fault=None):
    """Accept one creation, identify a duplicate, or reject without partial creation."""
    _ensure_writable(state)
    event_id = incident_id = None
    if isinstance(event, dict):
        event_id, incident_id = event.get("event_id"), event.get("incident_id")
    try:
        event_id = validate_event_envelope(event)
        if event_id in state["successful_event_ids"]:
            record_audit(state, "create", "duplicate", event_id=event_id,
                         incident_id=incident_id, reason="event already accepted", position=position)
            return {"outcome": "duplicate", "event_id": event_id, "reason": "event already accepted"}
        clean = validate_creation_fields(event)
        incident_id = clean["incident_id"]
        if incident_id in state["incidents_by_id"]:
            raise ValidationError("incident_id already exists; creation cannot update it")
    except ValidationError as exc:
        record_audit(state, "create", "rejected", event_id=event_id,
                     incident_id=incident_id, reason=str(exc), position=position)
        return {"outcome": "rejected", "reason": str(exc)}

    sequence = _allocate_sequence(state)
    incident = {key: value for key, value in clean.items() if key != "event_id"}
    incident.update(creation_event_id=event_id, status="queued", attempts=0,
                    queue_sequence=sequence, retry_sequence=None, latest_failure_attempt=None,
                    failure_reason=None, last_authorization_id=None, recovery_pending=False)
    undo = []
    try:
        _put(state["incidents_by_id"], incident_id, incident, undo)
        _hit(fault, "incident")
        _add_membership(state["shipment_index"], incident["shipment_id"], incident_id,
                        undo, fault, "shipment")
        for tag in incident["tags"]:
            _add_membership(state["tag_index"], tag, incident_id, undo, fault, "tag")
        _append(state["normal_queue"], (incident_id, sequence), undo)
        _hit(fault, "queue")
        _set_add(state["successful_event_ids"], event_id, undo)
        _hit(fault, "ledger")
        record_audit(state, "create", "accepted", event_id=event_id,
                     incident_id=incident_id, position=position, undo=undo)
        _hit(fault, "creation_audit")
    except Exception as exc:
        _rollback(undo)
        record_audit(state, "create", "failed", event_id=event_id, incident_id=incident_id,
                     reason=f"creation rolled back: {exc}", position=position)
        return {"outcome": "rejected", "reason": f"creation rolled back: {exc}"}
    state["_runtime"]["accepted_since_checkpoint"] += 1
    return {"outcome": "accepted", "event_id": event_id, "incident_id": incident_id}


def import_events(state, raw_json, *, after_row=None, fault=None):
    _ensure_writable(state)
    counts = {"accepted": 0, "duplicate": 0, "rejected": 0}
    try:
        rows = parse_event_batch(raw_json)
    except ValidationError as exc:
        record_audit(state, "import_batch", "rejected", reason=str(exc))
        return {"outcome": "rejected", "counts": counts, "results": [], "error": str(exc)}
    results = []
    for position, row in enumerate(rows):
        result = create_incident(state, row, position=position, fault=fault)
        result["position"] = position
        counts[result["outcome"]] += 1
        results.append(result)
        if after_row is not None:
            after_row(state)
    return {"outcome": "completed", "counts": counts, "results": results, "error": None}


def can_transition(current, target):
    return target in ALLOWED_TRANSITIONS.get(current, set())


def _operator_transition(state, incident_id, target, reason=None, fault=None):
    _ensure_writable(state)
    incident = state["incidents_by_id"].get(incident_id) if usable_id(incident_id) else None
    error = None
    if incident is None:
        error = "unknown incident"
    elif not can_transition(incident["status"], target):
        error = f"illegal transition: {incident['status']} -> {target}"
    elif target == "resolved" and (not isinstance(reason, str) or not reason.strip()):
        error = "manual resolution requires a nonblank reason"
    elif reason is not None and not isinstance(reason, str):
        error = "reason must be a string"
    if error:
        record_audit(state, "operator_transition", "rejected", incident_id=incident_id, reason=error)
        return {"outcome": "rejected", "reason": error}
    undo = []
    try:
        _put(incident, "status", target, undo)
        _put(incident, "retry_sequence", None, undo)
        _put(incident, "recovery_pending", False, undo)
        _hit(fault, "operator_record")
        record_audit(state, "operator_transition", target, incident_id=incident_id,
                     reason=reason.strip() if reason else None, undo=undo)
        _hit(fault, "operator_audit")
    except Exception as exc:
        _rollback(undo)
        record_audit(state, "operator_transition", "failed", incident_id=incident_id, reason=str(exc))
        return {"outcome": "rejected", "reason": str(exc)}
    state["_runtime"]["pending_recovery"].discard(incident_id)
    return {"outcome": "accepted", "incident_id": incident_id, "status": target}


def cancel_incident(state, incident_id, *, reason=None, fault=None):
    return _operator_transition(state, incident_id, "cancelled", reason, fault)


def resolve_failed_incident(state, incident_id, reason, *, fault=None):
    # can_transition alone would also allow a worker's processing -> resolved;
    # operator resolution is deliberately limited to failed incidents.
    incident = state["incidents_by_id"].get(incident_id) if usable_id(incident_id) else None
    if incident is not None and incident["status"] != "failed":
        _ensure_writable(state)
        record_audit(state, "manual_resolution", "rejected", incident_id=incident_id,
                     reason="manual resolution requires failed status")
        return {"outcome": "rejected", "reason": "manual resolution requires failed status"}
    return _operator_transition(state, incident_id, "resolved", reason, fault)


def creation_is_checkpointed(state, incident_id):
    return incident_id in state["_runtime"]["checkpointed_creation_ids"]


def assess_candidate(state, lane, ticket):
    if lane not in ("normal", "retry"):
        raise ValidationError("lane must be normal or retry")
    size = 2 if lane == "normal" else 3
    if not isinstance(ticket, (tuple, list)) or len(ticket) != size or not usable_id(ticket[0]):
        return {"outcome": "stale", "reason": "malformed ticket"}
    if any(type(number) is not int or number < 1 for number in ticket[1:]):
        return {"outcome": "stale", "reason": "malformed ticket metadata"}
    incident = state["incidents_by_id"].get(ticket[0])
    if incident is None:
        return {"outcome": "stale", "reason": "unknown incident"}
    expected = "queued" if lane == "normal" else "failed"
    if incident["status"] != expected:
        return {"outcome": "stale", "reason": f"status is {incident['status']}"}
    if lane == "normal" and ticket[1] != incident["queue_sequence"]:
        return {"outcome": "stale", "reason": "queue sequence mismatch"}
    if lane == "retry" and (incident["recovery_pending"] or
            ticket[1] != incident["latest_failure_attempt"] or ticket[1] != incident["attempts"] or
            ticket[2] != incident["retry_sequence"]):
        return {"outcome": "stale", "reason": "retry attempt/sequence mismatch"}
    if incident["attempts"] >= MAX_AUTHORIZATIONS:
        return {"outcome": "stale", "reason": "authorization budget exhausted"}
    if not creation_is_checkpointed(state, ticket[0]):
        return {"outcome": "waiting_checkpoint", "reason": "creation is not checkpointed"}
    return {"outcome": "eligible", "incident_id": ticket[0], "ticket": tuple(ticket), "lane": lane}


def next_eligible_candidate(state, lane):
    _ensure_writable(state)
    if lane not in ("normal", "retry"):
        raise ValidationError("lane must be normal or retry")
    if lane == "retry" and state["_runtime"]["pending_recovery"]:
        return {"outcome": "recovery_order_required",
                "incident_ids": sorted(state["_runtime"]["pending_recovery"])}
    queue = state["normal_queue"] if lane == "normal" else state["retry_stack"]
    while queue:
        ticket = queue[0] if lane == "normal" else queue[-1]
        result = assess_candidate(state, lane, ticket)
        if result["outcome"] != "stale":
            return result  # Keep valid/waiting tickets until authorization succeeds.
        if lane == "normal":
            queue.popleft()
        else:
            queue.pop()
        identifier = ticket[0] if isinstance(ticket, (tuple, list)) and ticket else None
        record_audit(state, "candidate", "stale", incident_id=identifier, reason=result["reason"])
    return {"outcome": "empty", "lane": lane}


def get_incident(state, incident_id):
    incident = state["incidents_by_id"].get(incident_id)
    return copy.deepcopy(incident) if incident is not None else None


def find_by_shipment(state, shipment_id):
    return set(state["shipment_index"].get(shipment_id, ()))


def find_by_tag(state, tag):
    return set(state["tag_index"].get(_nonblank(tag, "tag").lower(), ()))


def top_unresolved(state, k):
    # API contract: k is a nonnegative integer, with 0 meaning no results.
    if type(k) is not int or k < 0:
        raise ValidationError("k must be a nonnegative integer")
    if k == 0:
        return []
    incidents = [r for r in state["incidents_by_id"].values()
                 if r["status"] in {"queued", "processing", "failed"}]
    incidents.sort(key=lambda r: (-r["priority"], r["incident_id"]))
    return [r["incident_id"] for r in incidents[:k]]
