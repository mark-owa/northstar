"""Test-only child process: os._exit simulates death without exception cleanup."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from northstar import exclusive_store, load_store, process_next, save_checkpoint

directory, action, boundary = sys.argv[1:]


def crash(stage):
    if stage == boundary:
        os._exit(77)


with exclusive_store(directory):
    state = load_store(directory)
    if action == "process":
        process_next(state, "normal", outcome="resolved", fault=crash)
    elif action == "retry":
        process_next(state, "retry", outcome="resolved", fault=crash)
    elif action == "checkpoint":
        process_next(state, "normal", outcome="resolved")
        save_checkpoint(state, fault=crash)
    elif action == "lock":
        os._exit(77)
raise SystemExit("Requested crash boundary was not reached")

