"""Run configuration. Behaviour lives in code; *where and how* it runs lives here.

Every value can be overridden with an environment variable so the same code
runs locally, in CI, or on a scheduler without edits. See config/.env.example.
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv(path: Path) -> None:
    """Load KEY=VALUE lines from a .env file into os.environ without overriding
    variables that are already set. No third-party dependency needed."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv(ROOT / ".env")


def _env(name: str, default):
    val = os.getenv(name)
    if val is None:
        return default
    if isinstance(default, bool):
        return val.lower() in {"1", "true", "yes"}
    if isinstance(default, int):
        return int(val)
    if isinstance(default, float):
        return float(val)
    return val


def _path(name: str, default: Path) -> Path:
    """Relative paths in .env are resolved against the project root, not the
    current working directory, so the pipeline behaves the same from any cwd."""
    p = Path(_env(name, default))
    return p if p.is_absolute() else (ROOT / p).resolve()


RAW_DIR = _path("TLC_RAW_DIR", ROOT / "data" / "raw")
PROCESSED_DIR = _path("TLC_PROCESSED_DIR", ROOT / "data" / "processed")
LOG_DIR = _path("TLC_LOG_DIR", ROOT / "logs")
DOCS_DIR = ROOT / "docs"

# Retrieval
MAX_RETRIES = _env("TLC_MAX_RETRIES", 4)
SOCRATA_PAGE_SIZE = _env("TLC_SOCRATA_PAGE_SIZE", 1000)

# Validation thresholds (business rules)
MAX_TRIP_MINUTES = _env("TLC_MAX_TRIP_MINUTES", 180)       
MAX_TRIP_MILES = _env("TLC_MAX_TRIP_MILES", 100.0)         
MAX_SPEED_MPH = _env("TLC_MAX_SPEED_MPH", 80.0)          
UNKNOWN_ZONE_IDS = (264, 265)                             
AIRPORT_ZONE_IDS = (1, 132, 138)                            

# Run-level gates
MIN_DATA_YIELD_PCT = _env("TLC_MIN_DATA_YIELD_PCT", 90.0)  
MAX_RECON_GAP_PCT = _env("TLC_MAX_RECON_GAP_PCT", 5.0)      
FAIL_ON_WARN = _env("TLC_FAIL_ON_WARN", False)             
