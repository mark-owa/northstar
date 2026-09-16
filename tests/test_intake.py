import copy
import json
import unittest

from northstar import (
    ValidationError, create_incident, find_by_shipment, find_by_tag, get_incident,
    import_events, new_state, parse_event_batch, validate_creation_fields,
)
from support import accepted_state, event, fail_at


class IntakeTests(unittest.TestCase):
    def test_batch_must_be_json_text_with_array_root(self):
        for text in (None, [], "{}", "null", "42", '"hello"', '[{"x":1}',
                     '[{"event_id":"a","event_id":"b"}]', '[NaN]'):
            with self.subTest(text=text):
                state = new_state()
                create_incident(state, event())
                before = accepted_state(state)
                result = import_events(state, text)
                self.assertEqual(result["outcome"], "rejected")
                self.assertEqual(accepted_state(state), before)
                self.assertEqual(state["audit_trail"][-1]["action"], "import_batch")

    def test_empty_batch_and_parse_shape(self):
        state = new_state()
        self.assertEqual(import_events(state, "[]"), {
            "outcome": "completed", "counts": {"accepted": 0, "duplicate": 0, "rejected": 0},
            "results": [], "error": None})
        self.assertEqual(parse_event_batch('[{}, 7]'), [{}, 7])

    def test_missing_required_fields_and_malformed_rows(self):
        invalid = [None, [], 42, "event"]
        for key in event():
            row = event()
            del row[key]
            invalid.append(row)
        for row in invalid:
            with self.subTest(row=row):
                state = new_state()
                self.assertEqual(create_incident(state, row)["outcome"], "rejected")
                self.assertEqual(state["incidents_by_id"], {})
                self.assertEqual(state["successful_event_ids"], set())

    def test_identifiers_are_strings_case_sensitive_and_never_trimmed(self):
        for field in ("event_id", "incident_id", "shipment_id"):
            for invalid in (None, 1, True, [], {}, "", " ", " ID", "ID "):
                with self.subTest(field=field, invalid=invalid):
                    self.assertEqual(create_incident(new_state(), event(**{field: invalid}))["outcome"], "rejected")
        state = new_state()
        create_incident(state, event())
        self.assertEqual(create_incident(state, event(event_id="ev-1", incident_id="inc-1"))["outcome"], "accepted")

    def test_priority_exact_integer_range(self):
        for priority in (True, False, 0, 6, 4.0, "4", None, [], {}):
            with self.subTest(priority=priority):
                self.assertEqual(create_incident(new_state(), event(priority=priority))["outcome"], "rejected")
        for priority in range(1, 6):
            self.assertEqual(create_incident(new_state(), event(priority=priority))["outcome"], "accepted")

    def test_type_region_and_tags_validate_before_normalization(self):
        for field in ("type", "region"):
            for invalid in (None, 1, "", "  "):
                with self.subTest(field=field, invalid=invalid):
                    with self.assertRaises(ValidationError):
                        validate_creation_fields(event(**{field: invalid}))
        for tags in (None, "fragile", {}, ["fragile", ""], ["fragile", 1], [None], ["  "]):
            with self.subTest(tags=tags):
                self.assertEqual(create_incident(new_state(), event(tags=tags))["outcome"], "rejected")
        self.assertEqual(validate_creation_fields(event(tags=[]))["tags"], [])

    def test_normalization_preserves_source_and_ignores_extras(self):
        row = event(type=" Damaged ", region=" APAC ", tags=[" Fragile ", "fragile", "ELECTRONICS"], extra=42)
        original = copy.deepcopy(row)
        state = new_state()
        create_incident(state, row)
        self.assertEqual(row, original)
        record = get_incident(state, "INC-1")
        self.assertEqual((record["type"], record["region"], record["tags"]),
                         ("Damaged", "APAC", ["fragile", "electronics"]))
        self.assertNotIn("extra", record)
        row["tags"].append("new")
        self.assertNotIn("new", state["incidents_by_id"]["INC-1"]["tags"])

    def test_duplicate_event_is_checked_before_remaining_fields(self):
        state = new_state()
        create_incident(state, event())
        before = accepted_state(state)
        result = create_incident(state, {"event_id": "EV-1", "priority": False})
        self.assertEqual(result["outcome"], "duplicate")
        self.assertEqual(accepted_state(state), before)
        self.assertEqual(state["audit_trail"][-1]["outcome"], "duplicate")

    def test_invalid_then_corrected_event_id_remains_available(self):
        state = new_state()
        result = import_events(state, json.dumps([event(priority=0), event(), event()]))
        self.assertEqual(result["counts"], {"accepted": 1, "duplicate": 1, "rejected": 1})
        self.assertEqual([r["position"] for r in result["results"]], [0, 1, 2])
        self.assertEqual(state["successful_event_ids"], {"EV-1"})

    def test_bad_row_does_not_stop_later_rows(self):
        state = new_state()
        result = import_events(state, json.dumps([event(1), False, event(2)]))
        self.assertEqual(result["counts"], {"accepted": 2, "duplicate": 0, "rejected": 1})
        self.assertEqual(list(state["incidents_by_id"]), ["INC-1", "INC-2"])

    def test_duplicate_incident_rejection_preserves_every_accepted_structure(self):
        state = new_state()
        create_incident(state, event())
        before = accepted_state(state)
        result = create_incident(state, event(2, incident_id="INC-1", shipment_id="SHIP-2", tags=["new"]))
        self.assertEqual(result["outcome"], "rejected")
        self.assertEqual(accepted_state(state), before)
        self.assertNotIn("EV-2", state["successful_event_ids"])

    def test_creation_rolls_back_at_every_mutation_boundary(self):
        stages = ["incident", "membership:shipment:SHIP-1", "membership:tag:fragile",
                  "bucket:tag:new", "membership:tag:new", "queue", "ledger", "creation_audit"]
        for stage in stages:
            with self.subTest(stage=stage):
                state = new_state()
                create_incident(state, event())
                before, audit_before = accepted_state(state), copy.deepcopy(state["audit_trail"])
                result = create_incident(state, event(2, tags=["fragile", "new"]), fault=fail_at(stage))
                self.assertEqual(result["outcome"], "rejected")
                self.assertEqual(accepted_state(state), before)
                self.assertEqual(state["audit_trail"][:-1], audit_before)
                self.assertEqual(state["audit_trail"][-1]["outcome"], "failed")
                self.assertEqual(create_incident(state, event(2))["outcome"], "accepted")

    def test_new_shared_bucket_is_removed_on_failure(self):
        state = new_state()
        create_incident(state, event(), fault=fail_at("bucket:shipment:SHIP-1"))
        self.assertEqual(state["shipment_index"], {})
        self.assertEqual(state["tag_index"], {})
        self.assertEqual(state["incidents_by_id"], {})

    def test_audit_preserves_known_ids_and_position(self):
        state = new_state()
        import_events(state, json.dumps([event(priority=False)]))
        audit = state["audit_trail"][-1]
        self.assertEqual((audit["event_id"], audit["incident_id"], audit["position"]), ("EV-1", "INC-1", 0))
        self.assertTrue(audit["timestamp"].endswith("Z"))
        self.assertIn("priority", audit["reason"])

    def test_indexes_and_lookup_results_do_not_expose_mutable_internal_data(self):
        state = new_state()
        create_incident(state, event())
        create_incident(state, event(2, tags=["urgent"]))
        self.assertEqual(find_by_tag(state, " FRAGILE "), {"INC-1"})
        self.assertEqual(find_by_shipment(state, "SHIP-1"), {"INC-1", "INC-2"})
        find_by_shipment(state, "SHIP-1").clear()
        record = get_incident(state, "INC-1")
        record["tags"].clear()
        self.assertEqual(find_by_tag(state, "fragile"), {"INC-1"})
        self.assertEqual(get_incident(state, "INC-1")["tags"], ["fragile", "electronics"])
        self.assertIsNone(get_incident(state, "missing"))

