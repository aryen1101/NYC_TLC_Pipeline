"""Console + file logging shared by every pipeline stage.

Log lines are pipe-delimited key=value so the next engineer can grep by
stage, source, page, attempt, or status without reading code.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

_FMT = "%(asctime)s | %(levelname)-7s | %(name)-10s | %(message)s"
_CONSOLE_ATTACHED = False


def _ensure_console() -> None:
    """Attach exactly one console handler to the root logger."""
    global _CONSOLE_ATTACHED
    if _CONSOLE_ATTACHED:
        return
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter(_FMT))
    root.addHandler(sh)
    _CONSOLE_ATTACHED = True


def configure(log_dir: Path, run_id: str) -> Path:
    """Attach a per-run file handler in addition to the console."""
    _ensure_console()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"pipeline_{run_id}.log"
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter(_FMT))
    logging.getLogger().addHandler(fh)
    return log_path


def get_logger(name: str) -> logging.Logger:
    _ensure_console()
    return logging.getLogger(name)
