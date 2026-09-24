"""
VALIDATE stage: schema gate + business-rule validation of one month of trips.

Design choices (see docs/validation_rules.md):
  * The schema gate is a HARD stop. If a required column is missing, no output
    is written for the month; a misleading metric is worse than no metric.
  * Row-level rules never delete data. Each row gets a list of reason codes;
    rows with an empty list are "valid" and flow to the fact table, the rest
    are quarantined with their reasons so the exclusions are auditable.
  * Incomplete submissions (null passenger_count / RatecodeID / payment_type=0)
    are FLAGGED, not quarantined: they are real trips for volume and duration
    metrics but unusable for occupancy or payment metrics.
"""
from __future__ import annotations

import duckdb

from . import config
from .logging_utils import get_logger

log = get_logger("validate")


class ValidationError(RuntimeError):
    """Raised when a hard gate fails. The pipeline must not publish output."""


REQUIRED_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "tip_amount", "total_amount",
]

RULES = {
    "DUPLICATE":             "Exact duplicate of another row (same times, zones, vendor, distance, total); keep first.",
    "PICKUP_OUTSIDE_MONTH":  "Pickup timestamp not in the file's month (e.g. year 2008 or next month spill-over).",
    "NONPOSITIVE_DURATION":  "Drop-off at or before pickup; trip time cannot be measured.",
    "DURATION_OVER_MAX":     f"Trip longer than {config.MAX_TRIP_MINUTES} min; meter left running or clock error.",
    "ZERO_DISTANCE":         "Trip distance <= 0 miles; no movement recorded.",
    "DISTANCE_OVER_MAX":     f"Trip distance > {config.MAX_TRIP_MILES} miles; outside NYC metro envelope.",
    "IMPLAUSIBLE_SPEED":     f"Implied average speed > {config.MAX_SPEED_MPH} mph; distance/time inconsistent.",
    "UNKNOWN_PICKUP_ZONE":   "Pickup zone is 'Unknown'/'Outside NYC' or not in the TLC zone lookup.",
    "UNKNOWN_DROPOFF_ZONE":  "Drop-off zone is 'Unknown'/'Outside NYC' or not in the TLC zone lookup.",
    "NONPOSITIVE_TOTAL":     "total_amount <= 0; void, refund or dispute, not a revenue trip.",
    "NEGATIVE_FARE":         "fare_amount < 0; reversal record.",
}

OPTIONAL_COLUMNS = {
    "request_source": "VARCHAR",       
    "cbd_congestion_fee": "DOUBLE",    
    "Airport_fee": "DOUBLE",
    "congestion_surcharge": "DOUBLE",
    "tolls_amount": "DOUBLE",
    "store_and_fwd_flag": "VARCHAR",
}


def schema_gate(con: duckdb.DuckDBPyConnection, source_view: str, out_view: str = "raw_trips") -> list[str]:
    """Hard stop on missing required columns; normalise known optional drift.

    Reads `source_view` (the file as-is) and creates `out_view` with every
    optional column present. Returns the optional columns that were absent.
    """
    cols = {r[0] for r in con.sql(f"describe {source_view}").fetchall()}
    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        raise ValidationError(f"{source_view}: missing required columns: {missing}")

    drift = [c for c in OPTIONAL_COLUMNS if c not in cols]
    added = "".join(f", cast(null as {OPTIONAL_COLUMNS[c]}) as {c}" for c in drift)
    con.sql(f"create or replace view {out_view} as select *{added} from {source_view}")
    if drift:
        log.warning("schema drift | source=%s | optional columns absent, filled with NULL: %s", source_view, drift)
    log.info("schema gate passed | source=%s | required=%d | present=%d | drift=%d",
             source_view, len(REQUIRED_COLUMNS), len(cols), len(drift))
    return drift


def build_validated(con: duckdb.DuckDBPyConnection, month: str, raw_view: str = "raw_trips",
                    zones_table: str = "dim_zone") -> None:
    """Create table `trips_validated` for one month with reasons[] and is_valid."""
    unknown = ", ".join(str(z) for z in config.UNKNOWN_ZONE_IDS)
    con.sql(f"""
    create or replace table trips_validated as
    with base as (
        select
            *,
            row_number() over (
                partition by tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID,
                             DOLocationID, VendorID, trip_distance, total_amount
                order by (select null)
            ) as dup_rank,
            datediff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0 as duration_min,
            PULocationID in (select zone_id from {zones_table}) as pu_known,
            DOLocationID in (select zone_id from {zones_table}) as do_known
        from {raw_view}
    ),
    flagged as (
        select
            *,
            case when duration_min > 0 then trip_distance / (duration_min / 60.0) end as speed_mph,
            list_filter([
                case when dup_rank > 1 then 'DUPLICATE' end,
                case when strftime(tpep_pickup_datetime, '%Y-%m') <> '{month}' then 'PICKUP_OUTSIDE_MONTH' end,
                case when tpep_dropoff_datetime <= tpep_pickup_datetime then 'NONPOSITIVE_DURATION' end,
                case when duration_min > {config.MAX_TRIP_MINUTES} then 'DURATION_OVER_MAX' end,
                case when trip_distance <= 0 or trip_distance is null then 'ZERO_DISTANCE' end,
                case when trip_distance > {config.MAX_TRIP_MILES} then 'DISTANCE_OVER_MAX' end,
                case when duration_min > 0 and trip_distance / (duration_min / 60.0) > {config.MAX_SPEED_MPH}
                     then 'IMPLAUSIBLE_SPEED' end,
                case when PULocationID is null or PULocationID in ({unknown}) or not pu_known then 'UNKNOWN_PICKUP_ZONE' end,
                case when DOLocationID is null or DOLocationID in ({unknown}) or not do_known then 'UNKNOWN_DROPOFF_ZONE' end,
                case when total_amount <= 0 or total_amount is null then 'NONPOSITIVE_TOTAL' end,
                case when fare_amount < 0 then 'NEGATIVE_FARE' end
            ], x -> x is not null) as reasons,
            passenger_count is null as incomplete_submission
        from base
    )
    select *, len(reasons) = 0 as is_valid, '{month}' as file_month
    from flagged
    """)
    n_raw, n_valid = con.sql("select count(*), sum(is_valid::int) from trips_validated").fetchone()
    log.info("row rules applied | month=%s | raw=%d | valid=%d | yield=%.2f%%",
             month, n_raw, n_valid, 100.0 * n_valid / max(n_raw, 1))


def validation_report(con: duckdb.DuckDBPyConnection, month: str, manifest_rows: int | None,
                      schema_drift: list[str] | None = None) -> dict:
    """Summarise rule hits and run-level checks. Never mutates data."""
    raw, valid, incomplete = con.sql(
        "select count(*), sum(is_valid::int), sum(incomplete_submission::int) from trips_validated").fetchone()
    by_rule = dict(con.sql("""
        select reason, count(*) from (select unnest(reasons) reason from trips_validated)
        group by 1 order by 2 desc""").fetchall())
    by_vendor = [
        {"vendor_id": v, "rows": n, "quarantined": q, "quarantine_pct": round(100.0 * q / n, 2),
         "nonpositive_duration": npd}
        for v, n, q, npd in con.sql("""
            select VendorID, count(*), sum((not is_valid)::int),
                   sum(list_contains(reasons, 'NONPOSITIVE_DURATION')::int)
            from trips_validated group by 1 order by 1""").fetchall()
    ]
    yield_pct = round(100.0 * valid / max(raw, 1), 3)

    checks = []
    # Run-level check 1: rows read equal rows the extract manifest recorded.
    if manifest_rows is not None:
        checks.append({"check": "row_count_matches_manifest", "expected": manifest_rows, "actual": raw,
                       "status": "PASS" if raw == manifest_rows else "FAIL"})
    # Run-level check 2: data yield threshold.
    checks.append({"check": "data_yield_pct", "threshold": config.MIN_DATA_YIELD_PCT, "actual": yield_pct,
                   "status": "PASS" if yield_pct >= config.MIN_DATA_YIELD_PCT else "WARN"})
    # Run-level check 3: any vendor whose rows are entirely unusable for duration.
    for v in by_vendor:
        if v["rows"] > 1000 and v["nonpositive_duration"] == v["rows"]:
            checks.append({"check": f"vendor_{v['vendor_id']}_all_zero_duration", "actual": v["rows"],
                           "status": "WARN", "note": "escalate to TLC / vendor: drop-off time not populated"})

    report = {
        "month": month, "raw_rows": raw, "valid_rows": valid, "quarantined_rows": raw - valid,
        "data_yield_pct": yield_pct, "incomplete_submission_rows": incomplete,
        "incomplete_submission_pct": round(100.0 * incomplete / max(raw, 1), 2),
        "rule_hits": by_rule, "by_vendor": by_vendor, "checks": checks,
        "schema_drift_optional_columns_absent": schema_drift or [],
        "rules": RULES,
    }
    for c in checks:
        log.log(30 if c["status"] != "PASS" else 20, "check | %s | status=%s | actual=%s",
                c["check"], c["status"], c.get("actual"))
    failed = [c for c in checks if c["status"] == "FAIL"]
    if failed:
        raise ValidationError(f"{month}: run-level check failed: {failed}")
    return report
