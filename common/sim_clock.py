"""Shared clock for simulated time across multiple producers/processes.

Run_both.py starts all producers at the same time and imports their modules
in the main process, meaning START_TIME is perfectly synchronized.
"""
import os
import time

START_TIME = time.time()
SIM_DAY_SECONDS = int(os.environ.get("SIM_DAY_SECONDS", 300))


def get_sim_day() -> int:
    """Returns the current simulated day (0-indexed)."""
    return int((time.time() - START_TIME) / SIM_DAY_SECONDS)
