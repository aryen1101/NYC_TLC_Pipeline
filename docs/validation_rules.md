# Validation rules

Rule number one: I do not delete or fix rows. Every raw row gets a `reasons` list. Empty
list means valid and the row goes to `fact_trip`. Anything else goes to the quarantine
file with all its original columns, so someone can check my exclusions or change a
threshold and rerun.

## Hard gate (the month stops, nothing is written)

| Check | What | Why hard |
|---|---|---|
| required columns | 12 columns must exist (vendor, both timestamps, passenger count, distance, rate code, both zone ids, payment type, fare, tip, total) | without them the metrics mean nothing. Tested with `--chaos missing_column` |
| row count | rows read must equal the rows recorded in the manifest at download time | catches a partial or swapped file |

## Row rules

| Code | Rule | Why this threshold |
|---|---|---|
| DUPLICATE | same pickup, dropoff, PU zone, DO zone, vendor, distance, total. Keep the first one | no trip id in the data, so full-row match is the only safe test. 13,545 hits, all in July |
| PICKUP_OUTSIDE_MONTH | pickup month is not the file's month | a few rows are dated 2008/2009, a few spill into the next month |
| NONPOSITIVE_DURATION | dropoff <= pickup | cannot measure trip time. Vendor 7 hits this on every row |
| DURATION_OVER_MAX | longer than 180 min | 99.9th percentile is about 113 min, anything much longer is a meter left on |
| ZERO_DISTANCE | distance <= 0 | about 3% of rows. No speed or revenue per mile possible |
| DISTANCE_OVER_MAX | more than 100 miles | max in the data was 318,129 miles |
| IMPLAUSIBLE_SPEED | distance / hours above 80 mph | distance and time do not agree with each other |
| UNKNOWN_PICKUP_ZONE / UNKNOWN_DROPOFF_ZONE | zone is null, 264 (Unknown), 265 (Outside NYC) or not in the lookup | no borough possible. TLC's published counts also skip 264, so this keeps the comparison fair |
| NONPOSITIVE_TOTAL | total_amount <= 0 | refund / void / dispute, not a passenger trip |
| NEGATIVE_FARE | fare_amount < 0 | reversal record |

Hits per month are in each `data/processed/month=*/validation_report.json`. For May:
ZERO_DISTANCE 113,031, NONPOSITIVE_DURATION 52,063, UNKNOWN_DROPOFF_ZONE 21,726,
NONPOSITIVE_TOTAL 15,545, NEGATIVE_FARE 14,231, UNKNOWN_PICKUP_ZONE 6,486,
DURATION_OVER_MAX 1,430, IMPLAUSIBLE_SPEED 1,020, DISTANCE_OVER_MAX 136,
PICKUP_OUTSIDE_MONTH 14.

## Flagged but kept

`incomplete_submission`: passenger_count is null. In practice this always comes together
with null RatecodeID, null store_and_fwd_flag, payment_type 0 and null
congestion_surcharge. The data dictionary says payment_type 0 is a "Flex Fare trip".

About 23% of rows in May, 26% in June, 27% in July. Times, zones, distance and total are
all present and look normal, so they are real trips. I keep them for volume and duration
and flag them so nobody uses them for passenger count or payment analysis by accident.
Dropping them would have removed a quarter of the month silently.

## Run-level checks (WARN by default, `--fail-on-warn` makes them fail)

| Check | Threshold | Why |
|---|---|---|
| data_yield_pct | at least 90% | below that the month is more exclusions than data |
| reconciliation vs TLC published | gap within 5% | our count should agree with the client's own number |
| vendor all zero duration | any vendor with more than 1000 rows and 100% non-positive durations | this is a "call the vendor" signal, not a data fix |

## Assumptions I am making

1. Timestamps are local NYC time, meter engaged and disengaged. No timezone conversion.
2. For duplicates I keep the first row in file order. Nothing in the data says which copy is better.
3. Vendor 6 is Myle Technologies, Vendor 7 is Helix, payment_type 0 is Flex Fare, all from the TLC dictionary dated 18 March 2025 (copy in docs/reference).
4. The thresholds are mine. They sit in config so the client can change them without a code change.
5. Some quarantined rows are probably real (a genuine 3.5 hour trip exists). I accept a small false-exclusion rate to keep the median and p90 trustworthy.
