"""
TRANSFORM stage: reorganise validated rows around the trip workflow and
compute the monthly metrics linked to the project KPI.

Workflow model (docs/data_model.md):
    request  ->  pickup (time, zone)  ->  drop-off (time, zone)  ->  payment
    fact_trip holds one row per valid trip with both lifecycle events and the
    outcome fields; dims describe zone, vendor, payment type and rate code.

Metrics are computed at grain (month, pickup_borough) plus an 'ALL NYC' row.
"""
from __future__ import annotations

import duckdb

from . import config
from .logging_utils import get_logger

log = get_logger("transform")


VENDORS = {1: "Creative Mobile Technologies, LLC", 2: "Curb Mobility, LLC",
           6: "Myle Technologies Inc", 7: "Helix"}
PAYMENT_TYPES = {0: "Flex Fare trip", 1: "Credit card", 2: "Cash",
                 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"}
RATE_CODES = {1: "Standard rate", 2: "JFK", 3: "Newark", 4: "Nassau or Westchester",
              5: "Negotiated fare", 6: "Group ride", 99: "Null / unknown"}


def load_dims(con: duckdb.DuckDBPyConnection, zone_csv: str) -> None:
    con.sql(f"""
        create or replace table dim_zone as
        select LocationID as zone_id, Borough as borough, Zone as zone_name, service_zone,
               LocationID in ({", ".join(map(str, config.AIRPORT_ZONE_IDS))}) as is_airport
        from read_csv('{zone_csv}', header=true)
    """)
    for name, mapping, key in (("dim_vendor", VENDORS, "vendor_id"),
                               ("dim_payment_type", PAYMENT_TYPES, "payment_type_id"),
                               ("dim_rate_code", RATE_CODES, "rate_code_id")):
        rows = ", ".join(f"({k}, '{v}')" for k, v in mapping.items())
        con.sql(f"create or replace table {name} as select * from (values {rows}) t({key}, description)")
    log.info("dims loaded | zones=%d", con.sql("select count(*) from dim_zone").fetchone()[0])


def build_fact_trip(con: duckdb.DuckDBPyConnection) -> None:
    """One row per VALID trip, organised around the workflow."""
    con.sql("""
        create or replace table fact_trip as
        select
            md5(concat_ws('|', v.file_month, v.VendorID, v.tpep_pickup_datetime, v.tpep_dropoff_datetime,
                          v.PULocationID, v.DOLocationID, v.trip_distance, v.total_amount)) as trip_id,
            v.file_month                         as month,
            v.VendorID                           as vendor_id,
            v.request_source,
            v.incomplete_submission,
            -- event 1: pickup
            v.tpep_pickup_datetime               as pickup_ts,
            v.PULocationID                       as pickup_zone_id,
            pz.borough                           as pickup_borough,
            hour(v.tpep_pickup_datetime)         as pickup_hour,
            dayofweek(v.tpep_pickup_datetime)    as pickup_dow,
            -- event 2: drop-off
            v.tpep_dropoff_datetime              as dropoff_ts,
            v.DOLocationID                       as dropoff_zone_id,
            dz.borough                           as dropoff_borough,
            -- derived workflow measures
            v.duration_min,
            v.trip_distance                      as distance_mi,
            v.speed_mph,
            (pz.is_airport or dz.is_airport)     as is_airport_trip,
            -- outcome / payment
            v.passenger_count,
            v.RatecodeID                         as rate_code_id,
            v.payment_type                       as payment_type_id,
            v.fare_amount, v.tip_amount, v.tolls_amount, v.total_amount,
            v.congestion_surcharge, v.cbd_congestion_fee
        from trips_validated v
        left join dim_zone pz on pz.zone_id = v.PULocationID
        left join dim_zone dz on dz.zone_id = v.DOLocationID
        where v.is_valid
    """)
    log.info("fact_trip built | rows=%d", con.sql("select count(*) from fact_trip").fetchone()[0])


def build_quarantine_summary(con: duckdb.DuckDBPyConnection) -> None:
    con.sql("""
        create or replace table quarantine_summary as
        select file_month as month, VendorID as vendor_id, reason, count(*) as row_count
        from (select file_month, VendorID, unnest(reasons) as reason from trips_validated where not is_valid)
        group by all order by row_count desc
    """)


def build_reconciliation(con: duckdb.DuckDBPyConnection, month: str, api_json: str) -> None:
    """Compare our pickup counts with TLC's published monthly zone counts (API)."""
    con.sql(f"""
        create or replace table api_zone_counts as
        select strftime(cast(metric_month as date), '%Y-%m') as month,
               cast(locationid as int) as zone_id, borough, zone,
               cast(trip_count as bigint) as published_pickups
        from read_json('{api_json}')
        where pickup_dropoff = 'Pick-up' and industry = 'Yellow Taxi' and month = '{month}'
    """)
    con.sql("""
        create or replace table reconciliation_zones as
        with ours as (
            select file_month as month, PULocationID as zone_id,
                   count(*) as raw_pickups, sum(is_valid::int) as valid_pickups
            from trips_validated group by 1, 2)
        select coalesce(o.month, a.month) as month, coalesce(o.zone_id, a.zone_id) as zone_id,
               z.borough, z.zone_name,
               coalesce(o.raw_pickups, 0) as raw_pickups, coalesce(o.valid_pickups, 0) as valid_pickups,
               a.published_pickups,
               coalesce(o.raw_pickups, 0) - coalesce(a.published_pickups, 0) as raw_minus_published,
               case when a.published_pickups > 0
                    then round(100.0 * (coalesce(o.raw_pickups, 0) - a.published_pickups) / a.published_pickups, 2) end as gap_pct
        from ours o full outer join api_zone_counts a on a.zone_id = o.zone_id and a.month = o.month
        left join dim_zone z on z.zone_id = coalesce(o.zone_id, a.zone_id)
        order by abs(raw_minus_published) desc
    """)


def build_metrics(con: duckdb.DuckDBPyConnection) -> None:
    """3-5 operational metrics at (month, borough) grain + ALL NYC, linked to the KPI."""
    con.sql("""
        create or replace table metrics_monthly as
        with raw_by_b as (
            select file_month as month, coalesce(z.borough, 'Unknown') as borough, count(*) as raw_rows,
                   sum(is_valid::int) as valid_trips
            from trips_validated v left join dim_zone z on z.zone_id = v.PULocationID
            group by 1, 2
        ),
        raw_all as (select file_month as month, 'ALL NYC' as borough, count(*) raw_rows, sum(is_valid::int) valid_trips
                    from trips_validated group by 1),
        raw_union as (select * from raw_by_b union all select * from raw_all),
        fact_by_b as (
            select month, pickup_borough as borough,
                   median(duration_min) as median_duration_min,
                   quantile_cont(duration_min, 0.9) as p90_duration_min,
                   median(speed_mph) as median_speed_mph,
                   100.0 * avg(is_airport_trip::int) as airport_trip_share_pct,
                   median(total_amount / distance_mi) as median_revenue_per_mile,
                   100.0 * avg(incomplete_submission::int) as incomplete_submission_pct
            from fact_trip group by 1, 2
        ),
        fact_all as (
            select month, 'ALL NYC' as borough,
                   median(duration_min), quantile_cont(duration_min, 0.9), median(speed_mph),
                   100.0 * avg(is_airport_trip::int), median(total_amount / distance_mi),
                   100.0 * avg(incomplete_submission::int)
            from fact_trip group by 1
        ),
        fact_union as (select * from fact_by_b union all select * from fact_all),
        pub_by_b as (select month, borough, sum(published_pickups) published_pickups from api_zone_counts group by 1, 2),
        pub_all as (select month, 'ALL NYC' as borough, sum(published_pickups) from api_zone_counts group by 1),
        pub_union as (select * from pub_by_b union all select * from pub_all)
        select r.month, r.borough,
               r.raw_rows, r.valid_trips,
               round(100.0 * r.valid_trips / r.raw_rows, 2)            as data_yield_pct,
               round(f.median_duration_min, 2)                          as median_duration_min,
               round(f.p90_duration_min, 2)                             as p90_duration_min,
               round(f.median_speed_mph, 2)                             as median_speed_mph,
               round(f.airport_trip_share_pct, 2)                       as airport_trip_share_pct,
               round(f.median_revenue_per_mile, 2)                      as median_revenue_per_mile,
               round(f.incomplete_submission_pct, 2)                    as incomplete_submission_pct,
               p.published_pickups                                      as tlc_published_pickups,
               round(100.0 * (r.raw_rows - p.published_pickups) / p.published_pickups, 2) as recon_gap_pct
        from raw_union r
        left join fact_union f using (month, borough)
        left join pub_union p using (month, borough)
        order by r.month, case when r.borough = 'ALL NYC' then 0 else 1 end, r.borough
    """)
    log.info("metrics built | rows=%d", con.sql("select count(*) from metrics_monthly").fetchone()[0])


def run_level_gates(con: duckdb.DuckDBPyConnection, month: str) -> list[dict]:
    """Gates that need transformed output: reconciliation gap vs TLC published counts."""
    row = con.sql(f"select raw_rows, tlc_published_pickups, recon_gap_pct from metrics_monthly "
                  f"where month = '{month}' and borough = 'ALL NYC'").fetchone()
    checks = []
    if row is None or row[1] is None:
        checks.append({"check": "reconciliation_vs_tlc_published", "status": "WARN",
                       "note": "TLC has not published zone counts for this month yet; cannot reconcile"})
    else:
        gap = abs(row[2])
        checks.append({"check": "reconciliation_vs_tlc_published", "threshold_pct": config.MAX_RECON_GAP_PCT,
                       "raw_rows": row[0], "published": row[1], "gap_pct": row[2],
                       "status": "PASS" if gap <= config.MAX_RECON_GAP_PCT else "WARN"})
    for c in checks:
        log.log(30 if c["status"] != "PASS" else 20, "gate | %s | status=%s | %s", c["check"], c["status"],
                {k: v for k, v in c.items() if k not in ("check", "status")})
    return checks
