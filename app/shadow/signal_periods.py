"""Private, versioned selection of Bollinger observation periods."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

PERIOD_MS = {"1m": 60_000, "5m": 300_000, "10m": 600_000,
             "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000}
DEFAULT_PERIODS = ("1m", "5m")


def valid_periods(value) -> tuple[str, ...]:
    if (not isinstance(value, list) or not 2 <= len(value) <= len(PERIOD_MS)
            or any(not isinstance(item, str) or item not in PERIOD_MS for item in value)
            or len(set(value)) != len(value)):
        raise ValueError("INVALID_SIGNAL_PERIODS")
    return tuple(sorted(value, key=PERIOD_MS.__getitem__))


def periods_path(chart_db: Path) -> Path:
    return Path(chart_db).parent / "signal_periods.json"


def read_periods(path: Path) -> tuple[str, ...]:
    path = Path(path)
    if not path.exists():
        return DEFAULT_PERIODS
    if path.is_symlink() or not path.is_file():
        raise ValueError("INVALID_SIGNAL_PERIODS")
    raw = json.loads(path.read_text())
    if not isinstance(raw, dict) or set(raw) != {"periods"}:
        raise ValueError("INVALID_SIGNAL_PERIODS")
    return valid_periods(raw["periods"])


def save_periods(path: Path, value) -> tuple[str, ...]:
    periods = valid_periods(value)
    path = Path(path)
    if path.is_symlink() or not path.parent.is_dir():
        raise ValueError("INVALID_SIGNAL_PERIODS")
    fd, temporary = tempfile.mkstemp(prefix=".signal-periods-", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as output:
            json.dump({"periods": list(periods)}, output)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return periods
