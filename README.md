# NYC TLC Pipeline

A monthly data pipeline for NYC yellow taxi trip records. It downloads the raw TLC
files, validates every row against explicit business rules, reorganises the data
around the trip lifecycle and produces trip volume and service-time metrics per
borough. Metrics are released only when two gates pass: at least 90% of rows survive
validation, and the trip count agrees with the figure TLC publishes on NYC Open Data
within 5%.

```
python run_pipeline.py --months 2026-05 2026-06 2026-07
```

## Why this exists

An operations planning team needs one number a month per borough: how many trips ran
and how long they took, so they can size driver supply. The raw TLC files make that
harder than it sounds:

- roughly 4 million rows a month, including zero-distance trips, a 318,129-mile trip
  and pickups dated 2008
- the file schema changes between months (a new column appeared in June 2026)
- one vendor never populates the drop-off time
- the row total does not match the count TLC itself publishes

So the pipeline is less about computing an average and more about deciding which rows
can be trusted, saying openly which ones were excluded and why, and making the whole
thing safe to rerun every month.

**Users**

- Operations planning: trips and median duration per borough they can plan on
- Policy analysis: duration and speed trends across months
- TLC and its vendors: which vendor's submissions are unusable, so it can be fixed

**Headline metric: trusted monthly trips.** Trips per month and borough that pass all
validation rules, published with median duration, released only when data yield is at
least 90% and the reconciliation gap against TLC's published count is within 5%.

**Decision it supports:** whether the month's figures can go to planning as they are,
or whether someone has to talk to TLC or a vendor first.

## Data sources

| Source | Type | Used for |
|---|---|---|
| `yellow_tripdata_YYYY-MM.parquet`, TLC CloudFront bucket | file download | one row per trip: times, zones, distance, fares |
| `taxi_zone_lookup.csv`, same bucket | file download | zone id to borough |
| NYC Open Data `c5iv-bn4s`, Pickups and Drop-offs by Taxi Zone and Industry | Socrata API, paginated JSON | TLC's own monthly pickup count per zone, the reconciliation target |
| NYC Open Data `v6kb-cqej`, TLC Industry Indicators | Socrata API | trips per day and average minutes per trip, as a sanity check |
| TLC yellow taxi data dictionary, PDF in `docs/reference/` | manual download | meaning of vendor, payment and rate codes |

All of these are linked from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page.

Retrieval completeness is checked, not assumed. A parquet file is accepted only when
its size on disk equals the server's `Content-Length`. For the API, the pipeline asks
the server for `count(*)` first and then verifies that the fetched pages add up to it.
Every run writes `data/raw/retrieval_manifest.json` with URL, bytes, SHA-256 and row
count per object. More in [docs/source_map.md](docs/source_map.md).

## Pipeline

```
run_pipeline.py
  extract.py        download parquet, CSV and API pages; retry on 429/5xx; keep raw copies; write manifest
  validate.py       schema gate (hard stop) -> row rules -> reasons list per row -> run-level checks
  transform.py      fact_trip, dimensions, quarantine summary, reconciliation vs TLC, metrics
  save.py           one folder per month, written to a temp dir then swapped in, so reruns replace
  logging_utils.py  console plus logs/pipeline_<run_id>.log
```

Exit codes: 0 success, 1 validation failed (nothing written for that month), 2 download
failed, 3 unexpected error.

Behaviour verified on purpose:

| Property | How | Result |
|---|---|---|
| safe to rerun | June run twice | 3,621,446 rows both times, folder replaced |
| missing column stops the month | `--chaos missing_column` | exit 1, "no processed output written" |
| API unreachable | `--chaos api_down` | 4 retries at 2, 4, 8, 16 s, exit 2, nothing written |
| schema drift | May file lacks `request_source` | filled with NULL, logged as WARNING |

## Quick start

```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy config\.env.example .env        # optional: change thresholds or paths
python run_pipeline.py --months 2026-05 2026-06 2026-07
```

The first run downloads about 200 MB. Use `--skip-extract` afterwards to work offline.

Outputs go to `data/processed/month=YYYY-MM/` (fact table, quarantine, metrics,
validation report, per-zone reconciliation), plus `data/processed/metrics_monthly.csv`
and `docs/evidence_table.md`. Parquet files are git-ignored; the small CSV and JSON
outputs are committed as evidence.

## Model and metrics

The raw file is organised the way vendors upload it. The model is organised around the
trip: pickup event, the ride, drop-off event, payment. Rows that fail a rule are not
deleted; they go to a quarantine table with their list of reasons.

Metrics at month x borough grain, plus an ALL NYC row:

| Metric | Formula | Purpose |
|---|---|---|
| valid_trips | rows with no failed rule | the headline metric |
| data_yield_pct | valid / raw | release gate 1 |
| median_duration_min, p90_duration_min | drop-off minus pickup | service time planners care about |
| median_speed_mph | distance / hours | congestion proxy |
| airport_trip_share_pct | trips touching EWR, JFK or LGA | explains long Queens trips |
| recon_gap_pct | (raw count - TLC published) / published | release gate 2 |

Diagrams in [docs/data_model.md](docs/data_model.md), rules in
[docs/validation_rules.md](docs/validation_rules.md).

## Results, May to July 2026

| month | raw rows | valid trips | yield % | median min | p90 min | median mph | airport % | TLC published | gap % |
|---|---|---|---|---|---|---|---|---|---|
| 2026-05 | 4,090,836 | 3,888,064 | 95.04 | 14.63 | 36.0 | 8.92 | 7.87 | 3,945,903 | 3.67 |
| 2026-06 | 3,837,248 | 3,621,446 | 94.38 | 14.23 | 32.9 | 9.10 | 8.17 | 3,817,295 | 0.52 |
| 2026-07 | 3,530,109 | 3,307,448 | 93.69 | 14.25 | 31.83 | 9.50 | 8.49 | 3,506,619 | 0.67 |

All gates passed. Each month raised the same warning: Vendor 7 has 100% zero-duration
rows. Full borough breakdown in [docs/evidence_table.md](docs/evidence_table.md).

## Data quality notes

**Known**

- Yield is 93.7% to 95%. The largest exclusions are zero-distance trips (about 3%),
  non-positive durations (about 1.3%) and unknown drop-off zones (about 0.6%).
- Vendor 7 (Helix, per the TLC dictionary) never fills in drop-off time: 42k to 52k rows
  a month, all zero duration. They count toward volume, never toward duration.
- About a quarter of rows are Flex Fare trips (payment_type 0) with no passenger count,
  rate code or store-and-forward flag. They are kept and flagged, see Design decisions.
- June and July match TLC's published counts within 0.7%, and about 60% of zones match
  exactly.

**Unknown**

- May 2026 has 144,933 more trips than TLC publishes (3.67%). The excess sits in Bronx
  and Brooklyn zones and belongs to Vendor 1, which has about 200k more rows in May than
  in June or July. The May file was published on 26 June; TLC's aggregate was updated on
  28 August. Which one is right is unresolved, so the pipeline records the gap rather than
  adjusting anything. May's Bronx and Brooklyn figures should not be used until TLC
  explains the difference.
- Why TLC re-published the June file on 17 September 2026. A SHA-256 is stored per run
  so any future change is visible.

**Assumptions**

- Timestamps are local NYC time, meter on and meter off.
- Duplicates are rows identical on times, zones, vendor, distance and total. With no
  trip id, that is the only safe test.
- Flex Fare rows are real trips: their times, zones, distance and total are populated
  and look normal.
- Thresholds of 180 minutes, 100 miles and 80 mph come from the observed distribution
  (the 99.9th percentile of duration is about 113 minutes). They live in config, not code.

**Limitations**

- No trip, driver or vehicle id, so no waiting time and no exact duplicate tracking.
- Industry Indicators lag the trip files; only May was available for yellow taxis.
- Reconciliation shows that two sources disagree, not which one is correct.

## Design decisions

**Keep the Flex Fare rows.** A quarter of the data has five fields blank at once, which
looks like corruption. But the fields that matter for volume and duration are fine, and
the TLC dictionary confirms payment_type 0 is a real trip type. Dropping them would
have made the tables look clean while under-counting every borough by 25% and putting
June 25% away from TLC's number instead of 0.5%. They are kept and flagged as
`incomplete_submission`, so occupancy or payment analysis can exclude them explicitly.

**Flag, do not fix.** No row is edited or silently dropped. Every raw row ends with a
`reasons` list, and quarantined rows keep all their original columns, so any exclusion
can be audited or a threshold changed and the month rerun.

**Record the May gap instead of resolving it.** Both the raw file and TLC's aggregate
are TLC products. Picking one would be a guess presented as a fact.

## Repository layout

```
run_pipeline.py             entry point
evidence_walkthrough.ipynb  notebook over the outputs: profiling, rule hits, reconciliation, final table
pipeline/                   extract, validate, transform, save, config, logging_utils
config/.env.example         settings that can be overridden
data/raw/                   manifest, API raw pages, zone lookup (parquet ignored)
data/processed/             month=YYYY-MM/ folders and metrics_monthly.csv
docs/                       source_map, data_model, validation_rules, evidence_table, reference/
logs/                       run summaries (JSON committed, .log ignored)
```
