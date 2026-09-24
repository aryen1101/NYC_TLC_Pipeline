"""
One command:  python run_pipeline.py --months 2026-05 2026-06 2026-07

    EXTRACT -> VALIDATE -> TRANSFORM -> SAVE -> LOG      (per month, then combine)

Exit codes: 0 success, 1 validation / gate failure (no output published for the
failing month), 2 extraction failure, 3 unexpected error.

Flags:
  --skip-extract          reuse raw files already on disk (offline rerun)
  --chaos missing_column  drop a required column before validation to prove the gate
  --chaos api_down        point the API client at a dead host to prove retry + fail-fast
  --fail-on-warn          promote WARN gates (yield, reconciliation) to failures
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from pipeline import config, extract, transform, validate
from pipeline.logging_utils import configure, get_logger
from pipeline.save import save_combined, save_month


def _q(p: Path) -> str:
    return str(p).replace("\\", "/")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--months", nargs="+", required=True, help="YYYY-MM ... (each becomes one output partition)")
    ap.add_argument("--skip-extract", action="store_true")
    ap.add_argument("--chaos", choices=["missing_column", "api_down"])
    ap.add_argument("--fail-on-warn", action="store_true", default=config.FAIL_ON_WARN)
    args = ap.parse_args(argv)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = configure(config.LOG_DIR, run_id)
    log = get_logger("run")
    log.info("run start | run_id=%s | months=%s | skip_extract=%s | chaos=%s", run_id, args.months,
             args.skip_extract, args.chaos)
    t0 = time.time()
    summary = {"run_id": run_id, "months": {}, "status": "SUCCESS"}

    manifest_rows: dict[str, int] = {}
    try:
        if args.chaos == "api_down":
            extract.SOCRATA_BASE = "https://127.0.0.1:9/resource"
        if not args.skip_extract:
            objs = extract.extract_all(args.months, config.RAW_DIR)
            for o in objs:
                if o.source == "tlc_trip_records":
                    manifest_rows[Path(o.local_path).stem.split("_")[-1]] = o.rows
        else:
            mpath = config.RAW_DIR / "retrieval_manifest.json"
            if mpath.exists():
                for o in json.loads(mpath.read_text(encoding="utf-8"))["objects"]:
                    if o["source"] == "tlc_trip_records":
                        manifest_rows[Path(o["local_path"]).stem.split("_")[-1]] = o["rows"]
            log.info("extract skipped | using raw files on disk | manifest_months=%s", sorted(manifest_rows))
    except Exception as exc: 
        log.error("EXTRACT FAILED | %s | no output written", exc)
        summary["status"] = "EXTRACT_FAILED"
        _write_summary(summary, t0, log)
        return 2

    zone_csv = config.RAW_DIR / "reference" / "taxi_zone_lookup.csv"
    api_json = config.RAW_DIR / "api" / extract.DATASET_ZONE_COUNTS / f"{extract.DATASET_ZONE_COUNTS}_all.json"

    exit_code = 0
    for month in args.months:
        mt0 = time.time()
        parquet = config.RAW_DIR / "tlc_parquet" / f"yellow_tripdata_{month}.parquet"
        con = duckdb.connect()
        try:
            if not parquet.exists():
                raise FileNotFoundError(f"raw parquet missing for {month}: {parquet}")
            transform.load_dims(con, _q(zone_csv))
            drop = " exclude (tpep_dropoff_datetime)" if args.chaos == "missing_column" else ""
            con.sql(f"create or replace view raw_source as select *{drop} from read_parquet('{_q(parquet)}')")
            if drop:
                log.warning("CHAOS | dropped column tpep_dropoff_datetime from raw view for %s", month)

            drift = validate.schema_gate(con, "raw_source", "raw_trips")
            validate.build_validated(con, month)
            report = validate.validation_report(con, month, manifest_rows.get(month), drift)

            transform.build_fact_trip(con)
            transform.build_quarantine_summary(con)
            transform.build_reconciliation(con, month, _q(api_json))
            transform.build_metrics(con)
            gates = report["checks"] + transform.run_level_gates(con, month)

            warns = [g for g in gates if g["status"] == "WARN"]
            if warns and args.fail_on_warn:
                raise validate.ValidationError(f"{month}: WARN gates promoted to failure: {[w['check'] for w in warns]}")

            out_dir = save_month(con, month, config.PROCESSED_DIR, report, gates)
            summary["months"][month] = {"status": "PUBLISHED", "raw_rows": report["raw_rows"],
                                        "valid_rows": report["valid_rows"], "yield_pct": report["data_yield_pct"],
                                        "warnings": [w["check"] for w in warns], "partition": str(out_dir),
                                        "seconds": round(time.time() - mt0, 1)}
            log.info("month done | month=%s | status=PUBLISHED | warnings=%d | secs=%.1f",
                     month, len(warns), time.time() - mt0)
        except validate.ValidationError as exc:
            log.error("VALIDATION FAILED | month=%s | %s | no processed output written for this month", month, exc)
            summary["months"][month] = {"status": "VALIDATION_FAILED", "error": str(exc)}
            summary["status"] = "PARTIAL_FAILURE"
            exit_code = 1
        except Exception as exc: 
            log.exception("UNEXPECTED FAILURE | month=%s | %s", month, exc)
            summary["months"][month] = {"status": "ERROR", "error": str(exc)}
            summary["status"] = "PARTIAL_FAILURE"
            exit_code = 3
        finally:
            con.close()

    published = [m for m, s in summary["months"].items() if s["status"] == "PUBLISHED"]
    if published:
        try:
            all_months = sorted({p.name.split("=")[1] for p in config.PROCESSED_DIR.glob("month=*")})
            save_combined(config.PROCESSED_DIR, config.DOCS_DIR, all_months)
        except Exception as exc:  
            log.exception("COMBINE FAILED | %s", exc)
            exit_code = exit_code or 3

    _write_summary(summary, t0, log)
    log.info("run end | run_id=%s | status=%s | exit=%d | log=%s", run_id, summary["status"], exit_code, log_path)
    return exit_code


def _write_summary(summary: dict, t0: float, log) -> None:
    summary["seconds"] = round(time.time() - t0, 1)
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    p = config.LOG_DIR / f"run_summary_{summary['run_id']}.json"
    p.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    log.info("summary written | %s", p)


if __name__ == "__main__":
    sys.exit(main())
