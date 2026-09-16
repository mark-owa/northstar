import json
import subprocess
import sys
from pathlib import Path

from northstar import exclusive_store, load_store, next_eligible_candidate, process_next, save_checkpoint
from support import StoreCase

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "tests" / "crash_worker.py"


class ProcessCrashTests(StoreCase):
    def crash(self, action, boundary):
        result = subprocess.run([sys.executable, str(WORKER), str(self.directory), action, boundary],
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 77, result.stdout + result.stderr)

    def test_actual_process_death_before_work_and_before_outcome_checkpoint(self):
        self.add(1)
        for boundary in ("journal_written", "journal_synced", "after_authorization",
                         "after_processing_started", "outcome_audit"):
            with self.subTest(boundary=boundary):
                self.crash("process", boundary)
                restored = load_store(self.directory)
                self.assertEqual(restored["incidents_by_id"]["INC-1"]["attempts"], 1)
                self.assertEqual(restored["incidents_by_id"]["INC-1"]["status"], "failed")
                # Independent run starts from the same checkpoint with no attempt.
                (self.directory / "authorizations.jsonl").write_bytes(b"")

    def test_actual_process_death_during_checkpoint_and_cleanup(self):
        self.add(1)
        original = (self.directory / "state.json").read_bytes()
        for boundary, expected in (("checkpoint_partial", "failed"),
                                   ("checkpoint_before_replace", "failed"),
                                   ("checkpoint_replaced", "resolved"),
                                   ("checkpoint_committed", "resolved"),
                                   ("journal_cleanup_partial", "resolved"),
                                   ("journal_cleanup_before_replace", "resolved")):
            with self.subTest(boundary=boundary):
                self.crash("checkpoint", boundary)
                restored = load_store(self.directory)
                self.assertEqual(restored["incidents_by_id"]["INC-1"]["status"], expected)
                self.assertEqual(restored["incidents_by_id"]["INC-1"]["attempts"], 1)
                (self.directory / "state.json").write_bytes(original)
                (self.directory / "authorizations.jsonl").write_bytes(b"")

    def test_actual_crash_after_third_authorization_cannot_start_fourth(self):
        self.add(1)
        process_next(self.state, "normal", outcome="failed")
        process_next(self.state, "retry", outcome="failed")
        save_checkpoint(self.state)
        self.crash("retry", "after_authorization")
        restored = load_store(self.directory)
        self.assertEqual(restored["incidents_by_id"]["INC-1"]["attempts"], 3)
        self.assertEqual(next_eligible_candidate(restored, "retry")["outcome"], "empty")

    def test_second_process_cannot_open_locked_store_and_crash_releases_lock(self):
        with exclusive_store(self.directory):
            result = subprocess.run([sys.executable, "-m", "northstar", "--store", str(self.directory), "status"],
                                    cwd=ROOT, capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 2)
            self.assertIn("Another process", result.stderr)
        self.crash("lock", "unused")
        with exclusive_store(self.directory):
            self.assertEqual(load_store(self.directory)["incidents_by_id"], {})

    def test_cli_import_process_query_and_restart(self):
        def run(*args):
            result = subprocess.run([sys.executable, "-m", "northstar", "--store", str(self.directory), *args],
                                    cwd=ROOT, capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return json.loads(result.stdout)
        result = run("import", str(ROOT / "examples" / "events.json"))
        self.assertEqual(result["counts"], {"accepted": 3, "duplicate": 1, "rejected": 3})
        self.assertEqual(run("process", "normal", "--outcome", "failed")["attempt"], 1)
        self.assertEqual(run("process", "retry", "--outcome", "resolved")["attempt"], 2)
        self.assertEqual(run("show", "INC-9001")["status"], "resolved")
        self.assertEqual(run("find", "--tag", " FRAGILE "), ["INC-9001"])

