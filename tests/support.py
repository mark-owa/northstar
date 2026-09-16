import copy
import json
import tempfile
import unittest
from pathlib import Path

from northstar import create_incident, initialize_store, save_checkpoint


def event(number=1, **changes):
    result = {"event_id": f"EV-{number}", "incident_id": f"INC-{number}",
              "shipment_id": "SHIP-1", "type": "damaged", "priority": 4,
              "region": "APAC", "tags": ["fragile", "electronics"]}
    result.update(changes)
    return result


def fail_at(wanted):
    def fault(stage):
        if stage == wanted:
            raise RuntimeError(f"injected failure at {stage}")
    return fault


def accepted_state(state):
    return copy.deepcopy({key: state[key] for key in (
        "incidents_by_id", "shipment_index", "tag_index", "successful_event_ids",
        "normal_queue", "retry_stack")})


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="northstar_test_")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.state = initialize_store(self.directory)

    def add(self, *numbers, checkpoint=True):
        for number in numbers:
            self.assertEqual(create_incident(self.state, event(number))["outcome"], "accepted")
        if checkpoint:
            save_checkpoint(self.state)

    def snapshot(self):
        return json.loads((self.directory / "state.json").read_text())

    def write_snapshot(self, data):
        (self.directory / "state.json").write_text(json.dumps(data), encoding="utf-8")

