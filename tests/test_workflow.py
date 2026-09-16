import copy
import json
from unittest.mock import patch

from northstar import (
    RecoveryRequired, ValidationError, cancel_incident, can_transition, create_incident,
    ingest_json, load_store, next_eligible_candidate, process_next,
    resolve_failed_incident, resolve_recovery_order, save_checkpoint, top_unresolved,
)
from support import StoreCase, accepted_state, event, fail_at


class WorkflowTests(StoreCase):
    def test_creation_must_be_checkpointed_and_waiting_ticket_is_retained(self):
        self.add(90, 1, checkpoint=False)
        before = list(self.state["normal_queue"])
        self.assertEqual(process_next(self.state, "normal", outcome="resolved")["outcome"], "waiting_checkpoint")
        self.assertEqual(list(self.state["normal_queue"]), before)
        self.assertEqual(self.state["incidents_by_id"]["INC-90"]["attempts"], 0)
        save_checkpoint(self.state)
        self.assertEqual(next_eligible_candidate(self.state, "normal")["incident_id"], "INC-90")

    def test_fifo_uses_acceptance_order_not_id_sort(self):
        self.add(90, 1, 5)
        actual = [process_next(self.state, "normal", outcome="resolved")["incident_id"] for _ in range(3)]
        self.assertEqual(actual, ["INC-90", "INC-1", "INC-5"])
        self.assertEqual(next_eligible_candidate(self.state, "normal")["outcome"], "empty")

    def test_lifo_and_cancelled_ticket_consumes_no_attempt(self):
        self.add(1, 2)
        process_next(self.state, "normal", outcome="failed")
        process_next(self.state, "normal", outcome="failed")
        self.assertEqual(next_eligible_candidate(self.state, "retry")["incident_id"], "INC-2")
        cancel_incident(self.state, "INC-2")
        self.assertEqual(process_next(self.state, "retry", outcome="resolved")["incident_id"], "INC-1")
        self.assertEqual(self.state["incidents_by_id"]["INC-2"]["attempts"], 1)
        self.assertEqual(self.state["incidents_by_id"]["INC-2"]["status"], "cancelled")

    def test_stale_retry_attempt_and_sequence_cannot_authorize(self):
        self.add(1)
        process_next(self.state, "normal", outcome="failed")
        old = self.state["retry_stack"][-1]
        process_next(self.state, "retry", outcome="failed")
        self.state["retry_stack"].append(old)
        self.state["retry_stack"].append(("INC-1", 2, 999))
        candidate = next_eligible_candidate(self.state, "retry")
        self.assertEqual(candidate["ticket"][1], 2)
        self.assertEqual(self.state["incidents_by_id"]["INC-1"]["attempts"], 2)
        self.assertEqual(process_next(self.state, "retry", outcome="failed")["attempt"], 3)
        self.assertEqual(next_eligible_candidate(self.state, "retry")["outcome"], "empty")

    def test_unknown_malformed_and_cancelled_normal_tickets_are_skipped(self):
        self.add(1, 2)
        cancel_incident(self.state, "INC-1")
        for ticket in (("missing", 1), ("INC-2", True), None, "INC-2"):
            self.state["normal_queue"].appendleft(ticket)
        self.assertEqual(process_next(self.state, "normal", outcome="resolved")["incident_id"], "INC-2")
        self.assertEqual(self.state["incidents_by_id"]["INC-1"]["attempts"], 0)

    def test_empty_queues_do_not_write_authorizations(self):
        before = (self.directory / "authorizations.jsonl").read_bytes()
        for lane in ("normal", "retry"):
            self.assertEqual(process_next(self.state, lane, outcome="failed")["outcome"], "empty")
        self.assertEqual((self.directory / "authorizations.jsonl").read_bytes(), before)

    def test_three_total_authorizations_then_manual_review(self):
        self.add(1)
        for lane, count in (("normal", 1), ("retry", 2), ("retry", 3)):
            result = process_next(self.state, lane, outcome="failed", reason="no response")
            self.assertEqual(result["attempt"], count)
        self.assertEqual(process_next(self.state, "retry", outcome="resolved")["outcome"], "empty")
        self.assertEqual(top_unresolved(self.state, 10), ["INC-1"])
        self.assertEqual(self.state["incidents_by_id"]["INC-1"]["status"], "failed")

    def test_terminal_states_and_manual_resolution_reason(self):
        self.add(1, 2)
        self.assertEqual(resolve_failed_incident(self.state, "INC-1", "fixed")["outcome"], "rejected")
        process_next(self.state, "normal", outcome="failed")
        self.assertEqual(resolve_failed_incident(self.state, "INC-1", " ")["outcome"], "rejected")
        self.assertEqual(resolve_failed_incident(self.state, "INC-1", "operator verified delivery")["outcome"], "accepted")
        self.assertEqual(cancel_incident(self.state, "INC-1")["outcome"], "rejected")
        cancel_incident(self.state, "INC-2")
        self.assertEqual(cancel_incident(self.state, "INC-2")["outcome"], "rejected")
        self.assertEqual(cancel_incident(self.state, "unknown")["outcome"], "rejected")
        self.assertEqual(next_eligible_candidate(self.state, "retry")["outcome"], "empty")
        self.assertFalse(can_transition("processing", "cancelled"))
        self.assertFalse(can_transition("cancelled", "processing"))
        self.assertFalse(can_transition("resolved", "processing"))

    def test_operator_change_rollback_preserves_retry(self):
        self.add(1)
        process_next(self.state, "normal", outcome="failed")
        for stage in ("operator_record", "operator_audit"):
            before = accepted_state(self.state)
            self.assertEqual(cancel_incident(self.state, "INC-1", fault=fail_at(stage))["outcome"], "rejected")
            self.assertEqual(accepted_state(self.state), before)

    def test_report_order_and_operational_queues_unchanged(self):
        for number, priority in ((4, 5), (3, 5), (1, 4), (2, 5), (0, 5), (5, 4)):
            create_incident(self.state, event(number, priority=priority))
        save_checkpoint(self.state)
        process_next(self.state, "normal", outcome="resolved")  # INC-4
        process_next(self.state, "normal", outcome="failed")    # INC-3
        cancel_incident(self.state, "INC-0")
        before = accepted_state(self.state)
        self.assertEqual(top_unresolved(self.state, 3), ["INC-2", "INC-3", "INC-1"])
        self.assertEqual(top_unresolved(self.state, 0), [])
        self.assertEqual(top_unresolved(self.state, 99), ["INC-2", "INC-3", "INC-1", "INC-5"])
        self.assertEqual(accepted_state(self.state), before)
        for invalid in (True, -1, 1.5, "3"):
            with self.assertRaises(ValidationError):
                top_unresolved(self.state, invalid)

    def test_attempt_does_not_take_full_snapshot(self):
        self.add(1)
        before = (self.directory / "state.json").read_bytes()
        with patch("northstar.storage.save_checkpoint", side_effect=AssertionError("unexpected full save")):
            process_next(self.state, "normal", outcome="failed")
            process_next(self.state, "retry", outcome="resolved")
        self.assertEqual((self.directory / "state.json").read_bytes(), before)
        self.assertEqual(len((self.directory / "authorizations.jsonl").read_text().splitlines()), 2)

    def test_outcome_exception_consumes_authorization_and_requires_recovery(self):
        self.add(1)
        with self.assertRaises(RecoveryRequired):
            process_next(self.state, "normal", outcome="failed", fault=fail_at("outcome_audit"))
        with self.assertRaises(RecoveryRequired):
            create_incident(self.state, event(2))
        recovered = load_store(self.directory)
        self.assertEqual(recovered["incidents_by_id"]["INC-1"]["attempts"], 1)
        self.assertEqual(recovered["incidents_by_id"]["INC-1"]["status"], "failed")

    def test_missing_recovery_order_requires_an_explicit_operator_decision(self):
        self.add(1, 2)
        process_next(self.state, "normal", outcome="failed")
        process_next(self.state, "normal", outcome="failed")
        recovered = load_store(self.directory)
        self.assertEqual(next_eligible_candidate(recovered, "retry")["outcome"], "recovery_order_required")
        with self.assertRaises(ValidationError):
            resolve_recovery_order(recovered, ["INC-1"])
        resolve_recovery_order(recovered, ["INC-2", "INC-1"])
        self.assertEqual(next_eligible_candidate(recovered, "retry")["incident_id"], "INC-1")
        save_checkpoint(recovered)
        again = load_store(self.directory)
        self.assertEqual(next_eligible_candidate(again, "retry")["incident_id"], "INC-1")

    def test_recovery_order_must_preserve_known_relative_order(self):
        self.add(1, 2, 3)
        process_next(self.state, "normal", outcome="failed")
        process_next(self.state, "normal", outcome="failed")
        save_checkpoint(self.state)  # INC-1 then INC-2 order is durable.
        process_next(self.state, "normal", outcome="failed")
        recovered = load_store(self.directory)
        with self.assertRaises(ValidationError):
            resolve_recovery_order(recovered, ["INC-2", "INC-1", "INC-3"])
        resolve_recovery_order(recovered, ["INC-1", "INC-3", "INC-2"])
        self.assertEqual(next_eligible_candidate(recovered, "retry")["incident_id"], "INC-2")

    def test_ingest_checks_1000_event_checkpoint_trigger(self):
        result = ingest_json(self.state, json.dumps([event(i) for i in range(1001)]))
        self.assertEqual(result["counts"]["accepted"], 1001)
        self.assertEqual(len(self.state["_runtime"]["checkpointed_creation_ids"]), 1000)
        self.assertEqual(self.state["_runtime"]["accepted_since_checkpoint"], 1)
        self.assertEqual(len(load_store(self.directory)["incidents_by_id"]), 1000)

    def test_cancelling_a_known_retry_cannot_block_recovery_order_repair(self):
        self.add(1, 2, 3)
        process_next(self.state, "normal", outcome="failed")
        process_next(self.state, "normal", outcome="failed")
        save_checkpoint(self.state)
        process_next(self.state, "normal", outcome="failed")
        recovered = load_store(self.directory)
        cancel_incident(recovered, "INC-2")  # Its old ticket is deliberately still present.
        resolve_recovery_order(recovered, ["INC-1", "INC-3"])
        self.assertEqual(next_eligible_candidate(recovered, "retry")["incident_id"], "INC-3")
        self.assertEqual(recovered["incidents_by_id"]["INC-2"]["status"], "cancelled")
