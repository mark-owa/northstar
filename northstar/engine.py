"""Connect candidate selection to durable permission and simulated processing."""

from .core import (
    MAX_AUTHORIZATIONS, RecoveryRequired, ValidationError, _allocate_sequence, _append,
    _ensure_writable, _hit, _put, _rollback, assess_candidate, import_events,
    next_eligible_candidate, record_audit, utc_now,
)
from .storage import append_authorization, maybe_checkpoint


def ingest_json(state, raw_json):
    """Application intake with the agreed periodic checkpoint trigger after each row."""
    return import_events(state, raw_json, after_row=maybe_checkpoint)


def _finish_attempt(state, identifier, outcome, reason, fault):
    incident, undo = state["incidents_by_id"][identifier], []
    try:
        _put(incident, "status", outcome, undo)
        _put(incident, "failure_reason", reason if outcome == "failed" else None, undo)
        if outcome == "failed":
            _put(incident, "latest_failure_attempt", incident["attempts"], undo)
            if incident["attempts"] < MAX_AUTHORIZATIONS:
                sequence = _allocate_sequence(state)
                _put(incident, "retry_sequence", sequence, undo)
                _append(state["retry_stack"], (identifier, incident["attempts"], sequence), undo)
                _hit(fault, "retry_enqueued")
        _hit(fault, "outcome_record")
        record_audit(state, "processing_finished", outcome, incident_id=identifier,
                     reason=reason if outcome == "failed" else None, undo=undo)
        _hit(fault, "outcome_audit")
    except Exception:
        _rollback(undo)
        raise


def process_next(state, lane, *, outcome, reason="simulated processing failure", fault=None):
    """Process one eligible candidate in the selected lane. No external side effects.

    An eligible ticket stays scheduled until durable authorization succeeds.
    Failed/uncertain persistence raises and fences the state until load_store().
    """
    _ensure_writable(state)
    if outcome not in ("resolved", "failed"):
        raise ValidationError("simulated outcome must be resolved or failed")
    if outcome == "failed" and (not isinstance(reason, str) or not reason.strip()):
        raise ValidationError("failure requires a nonblank reason")
    runtime = state["_runtime"]
    if runtime.get("active_incident_id") is not None:
        raise RecoveryRequired("A processing operation is already active; one worker is supported.")
    candidate = next_eligible_candidate(state, lane)
    if candidate["outcome"] != "eligible":
        return candidate
    # A candidate is not a permanent permission token. Recheck at the boundary.
    checked = assess_candidate(state, lane, candidate["ticket"])
    if checked["outcome"] != "eligible":
        return checked
    identifier = candidate["incident_id"]
    incident = state["incidents_by_id"][identifier]
    number = runtime["applied_authorization_id"] + 1
    attempt = incident["attempts"] + 1
    record = {"store_id": state["store_id"], "authorization_id": number,
              "incident_id": identifier, "attempt": attempt, "timestamp": utc_now()}
    append_authorization(state, record, fault=fault)
    try:
        runtime["applied_authorization_id"] = number
        runtime["active_incident_id"] = identifier
        incident.update(attempts=attempt, status="processing", last_authorization_id=number,
                        retry_sequence=None, recovery_pending=False, failure_reason=None)
        if lane == "normal":
            state["normal_queue"].popleft()
        else:
            state["retry_stack"].pop()
        record_audit(state, "processing_authorized", "authorized", incident_id=identifier,
                     reason=f"authorization {number}; attempt {attempt}")
        _hit(fault, "after_authorization")
        # The simulated worker begins here, after durable permission, exactly once per call.
        record_audit(state, "processing_started", "started", incident_id=identifier)
        _hit(fault, "after_processing_started")
        _finish_attempt(state, identifier, outcome, reason.strip() if isinstance(reason, str) else reason, fault)
        runtime["active_incident_id"] = None
    except Exception as exc:
        runtime["recovery_required"] = True
        record_audit(state, "processing_interrupted", "failed", incident_id=identifier, reason=str(exc))
        raise RecoveryRequired("Authorization is consumed; reopen to reconcile the interrupted operation.") from exc
    return {"outcome": outcome, "incident_id": identifier, "attempt": attempt,
            "authorization_id": number}
