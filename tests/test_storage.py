import copy
import json

from northstar import (
    PersistenceError, RecoveryError, RecoveryRequired, cancel_incident, checkpoint_due,
    create_incident, initialize_store, load_store, next_eligible_candidate, process_next,
    save_checkpoint,
)
from northstar.core import utc_now
from support import StoreCase, accepted_state, event, fail_at


class StorageTests(StoreCase):
    def test_round_trip_preserves_authority_ledger_audit_and_order(self):
        self.add(9, 1, 7, 2)
        process_next(self.state, "normal", outcome="failed")
        process_next(self.state, "normal", outcome="failed")
        save_checkpoint(self.state)
        restored = load_store(self.directory)
        self.assertEqual(accepted_state(restored), accepted_state(self.state))
        self.assertEqual(restored["audit_trail"], self.state["audit_trail"])
        self.assertEqual(next_eligible_candidate(restored, "normal")["incident_id"], "INC-7")
        self.assertEqual(next_eligible_candidate(restored, "retry")["incident_id"], "INC-1")

    def test_initialize_never_overwrites_and_missing_store_is_not_silently_empty(self):
        before = (self.directory / "state.json").read_bytes()
        with self.assertRaises(PersistenceError):
            initialize_store(self.directory)
        self.assertEqual((self.directory / "state.json").read_bytes(), before)
        with self.assertRaises(RecoveryError):
            load_store(self.directory / "missing")
        (self.directory / "authorizations.jsonl").unlink()
        with self.assertRaises(RecoveryError):
            load_store(self.directory)

    def test_failed_checkpoint_keeps_old_bytes_and_does_not_claim_creation_coverage(self):
        self.add(1)
        before = (self.directory / "state.json").read_bytes()
        create_incident(self.state, event(2))
        for stage in ("checkpoint_partial", "checkpoint_synced", "checkpoint_before_replace"):
            with self.subTest(stage=stage):
                with self.assertRaises(PersistenceError):
                    save_checkpoint(self.state, fault=fail_at(stage))
                self.assertEqual((self.directory / "state.json").read_bytes(), before)
                self.assertNotIn("INC-2", self.state["_runtime"]["checkpointed_creation_ids"])
                self.assertEqual(set(load_store(self.directory)["incidents_by_id"]), {"INC-1"})

    def test_checkpoint_confirmation_exception_requires_reopen(self):
        self.add(1, checkpoint=False)
        with self.assertRaises(PersistenceError):
            save_checkpoint(self.state, fault=fail_at("checkpoint_replaced"))
        with self.assertRaises(RecoveryRequired):
            create_incident(self.state, event(2))
        self.assertIn("INC-1", load_store(self.directory)["incidents_by_id"])

    def test_cleanup_failure_never_replays_a_checkpointed_resolution_as_failed(self):
        self.add(1)
        process_next(self.state, "normal", outcome="resolved")
        saved = save_checkpoint(self.state, fault=fail_at("journal_cleanup_before_replace"))
        self.assertFalse(saved["journal_compacted"])
        self.assertTrue((self.directory / "authorizations.jsonl").read_bytes())
        for _ in range(3):
            restored = load_store(self.directory)
            self.assertEqual(restored["incidents_by_id"]["INC-1"]["status"], "resolved")
            self.assertEqual(restored["incidents_by_id"]["INC-1"]["attempts"], 1)
        save_checkpoint(restored)
        self.assertEqual((self.directory / "authorizations.jsonl").read_bytes(), b"")

    def test_partial_and_semantically_invalid_snapshot_preserves_files(self):
        self.add(1)
        original = self.snapshot()
        bad = []
        for field, value in (("status", "mystery"), ("attempts", -1), ("attempts", True),
                             ("priority", True), ("region", "  APAC"), ("tags", ["Fragile"]),
                             ("queue_sequence", None), ("recovery_pending", "yes"),
                             ("creation_event_id", "UNKNOWN"), ("incident_id", "INC-2")):
            data = copy.deepcopy(original)
            data["incidents_by_id"]["INC-1"][field] = value
            bad.append((field, json.dumps(data)))
        for key, value in (("authorization_watermark", 1), ("next_sequence", 1),
                           ("version", True), ("successful_event_ids", ["EV-1", "EV-1"]),
                           ("normal_queue", {})):
            data = copy.deepcopy(original)
            data[key] = value
            bad.append((key, json.dumps(data)))
        bad.extend([("truncated", '{"version":1'), ("wrong shape", "[]")])
        for label, text in bad:
            with self.subTest(label=label):
                (self.directory / "state.json").write_text(text)
                before = (self.directory / "state.json").read_bytes()
                with self.assertRaises(RecoveryError):
                    load_store(self.directory)
                self.assertEqual((self.directory / "state.json").read_bytes(), before)
                self.assertEqual(self.state["incidents_by_id"]["INC-1"]["status"], "queued")

    def test_missing_and_corrupted_derived_tickets_rebuild_in_authoritative_order(self):
        self.add(9, 1, 7, 2)
        process_next(self.state, "normal", outcome="failed")
        process_next(self.state, "normal", outcome="failed")
        cancel_incident(self.state, "INC-2")
        save_checkpoint(self.state)
        data = self.snapshot()
        data["normal_queue"] = [["INC-2", 4], ["unknown", 99], None]
        data["retry_stack"] = [["INC-9", 99, 999]]
        self.write_snapshot(data)
        restored = load_store(self.directory)
        self.assertEqual([ticket[0] for ticket in restored["normal_queue"]], ["INC-7"])
        self.assertEqual([ticket[0] for ticket in restored["retry_stack"]], ["INC-9", "INC-1"])
        self.assertEqual(restored["tag_index"]["fragile"], {"INC-9", "INC-1", "INC-7", "INC-2"})

    def test_authoritative_missing_or_duplicate_order_is_rejected(self):
        self.add(1, 2)
        base = self.snapshot()
        for value in (None, 1):
            data = copy.deepcopy(base)
            data["incidents_by_id"]["INC-2"]["queue_sequence"] = value
            self.write_snapshot(data)
            with self.assertRaises(RecoveryError):
                load_store(self.directory)

    def test_uncertain_append_fences_state_and_complete_line_consumes_budget(self):
        self.add(1)
        for stage, expected in (("journal_before_write", 0), ("journal_written", 1), ("journal_synced", 1)):
            with self.subTest(stage=stage):
                state = load_store(self.directory)
                with self.assertRaises(PersistenceError):
                    process_next(state, "normal", outcome="resolved", fault=fail_at(stage))
                self.assertEqual(state["incidents_by_id"]["INC-1"]["attempts"], 0)
                self.assertFalse(any(a["action"] == "processing_started" for a in state["audit_trail"]))
                with self.assertRaises(RecoveryRequired):
                    save_checkpoint(state)
                restored = load_store(self.directory)
                self.assertEqual(restored["incidents_by_id"]["INC-1"]["attempts"], expected)
                # Restore the same pristine checkpoint for the next independent fault.
                (self.directory / "authorizations.jsonl").write_bytes(b"")

    def test_third_authorization_is_never_forgotten_across_repeated_recovery(self):
        self.add(1)
        process_next(self.state, "normal", outcome="failed")
        process_next(self.state, "retry", outcome="failed")
        save_checkpoint(self.state)
        with self.assertRaises(RecoveryRequired):
            process_next(self.state, "retry", outcome="resolved", fault=fail_at("after_authorization"))
        for _ in range(3):
            restored = load_store(self.directory)
            incident = restored["incidents_by_id"]["INC-1"]
            self.assertEqual((incident["attempts"], incident["status"]), (3, "failed"))
            self.assertEqual(next_eligible_candidate(restored, "retry")["outcome"], "empty")

    def test_authorizations_for_multiple_incidents_are_all_replayed(self):
        self.add(1, 2)
        process_next(self.state, "normal", outcome="failed")
        process_next(self.state, "normal", outcome="failed")
        process_next(self.state, "retry", outcome="failed")
        process_next(self.state, "retry", outcome="failed")
        process_next(self.state, "retry", outcome="failed")
        process_next(self.state, "retry", outcome="failed")
        for _ in range(2):
            restored = load_store(self.directory)
            self.assertEqual([restored["incidents_by_id"][f"INC-{i}"]["attempts"] for i in (1, 2)], [3, 3])
            self.assertEqual(next_eligible_candidate(restored, "retry")["outcome"], "empty")

    def test_incomplete_final_append_is_trimmed_after_retaining_complete_prefix(self):
        self.add(1, 2)
        process_next(self.state, "normal", outcome="failed")
        path = self.directory / "authorizations.jsonl"
        prefix = path.read_bytes()
        with path.open("ab") as stream:
            stream.write(b'{"authorization_id":2,"incident_id":"INC-')
        restored = load_store(self.directory)
        self.assertEqual(path.read_bytes(), prefix)
        self.assertEqual(restored["incidents_by_id"]["INC-1"]["attempts"], 1)
        self.assertEqual(restored["incidents_by_id"]["INC-2"]["attempts"], 0)
        process_next(restored, "normal", outcome="resolved")
        self.assertEqual(len(path.read_text().splitlines()), 2)
        self.assertEqual(load_store(self.directory)["incidents_by_id"]["INC-2"]["attempts"], 1)

    def test_invalid_complete_journal_record_is_never_silently_trimmed(self):
        self.add(1)
        process_next(self.state, "normal", outcome="failed")
        path = self.directory / "authorizations.jsonl"
        original = path.read_bytes()
        for invalid in (b'{"broken":}\n', b'{}\n', b'\xff\n', b'\n'):
            with self.subTest(invalid=invalid):
                path.write_bytes(original + invalid + b'{"partial"')
                before = path.read_bytes()
                with self.assertRaises(RecoveryError):
                    load_store(self.directory)
                self.assertEqual(path.read_bytes(), before)

    def test_journal_semantic_corruption_rejected(self):
        self.add(1)
        base = {"store_id": self.state["store_id"], "authorization_id": 1,
                "incident_id": "INC-1", "attempt": 1, "timestamp": utc_now()}
        for field, value in (("store_id", "other"), ("authorization_id", 2),
                             ("incident_id", "unknown"), ("attempt", 4),
                             ("attempt", 2), ("attempt", True), ("timestamp", "yesterday")):
            with self.subTest(field=field, value=value):
                row = dict(base, **{field: value})
                path = self.directory / "authorizations.jsonl"
                path.write_text(json.dumps(row) + "\n")
                before = path.read_bytes()
                with self.assertRaises(RecoveryError):
                    load_store(self.directory)
                self.assertEqual(path.read_bytes(), before)
        (self.directory / "authorizations.jsonl").write_text((json.dumps(base) + "\n") * 2)
        with self.assertRaises(RecoveryError):
            load_store(self.directory)

    def test_checkpointed_duplicate_and_uncheckpointed_creation_replay(self):
        self.add(1)
        create_incident(self.state, event(2))
        restored = load_store(self.directory)
        self.assertEqual(create_incident(restored, event(1))["outcome"], "duplicate")
        self.assertEqual(create_incident(restored, event(2))["outcome"], "accepted")

    def test_checkpoint_trigger_uses_accepted_count_or_elapsed_time(self):
        runtime = self.state["_runtime"]
        start = runtime["last_checkpoint_time"]
        self.assertFalse(checkpoint_due(self.state, now=start + 299))
        self.assertTrue(checkpoint_due(self.state, now=start + 300))
        runtime["accepted_since_checkpoint"] = 999
        self.assertFalse(checkpoint_due(self.state, now=start))
        runtime["accepted_since_checkpoint"] = 1000
        self.assertTrue(checkpoint_due(self.state, now=start))

