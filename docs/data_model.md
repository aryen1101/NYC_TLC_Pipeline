# Data model

## The trip as a workflow

The raw file is one wide table per month, organised by how vendors upload it. I
reorganised it around what actually happens in a trip.

```mermaid
flowchart LR
    R[Request<br/>street hail or app<br/>request_source] --> P[Pickup<br/>meter on<br/>pickup_ts, pickup_zone]
    P --> T[Ride<br/>duration_min, distance_mi, speed_mph]
    T --> D[Drop-off<br/>meter off<br/>dropoff_ts, dropoff_zone]
    D --> O[Payment<br/>fare, tip, tolls, total<br/>payment_type, rate_code]
    P -. failed a rule .-> Q[Quarantine<br/>reasons list]
    D -. failed a rule .-> Q
```

Two events with timestamps (pickup, drop-off), the measures in between, and the
payment outcome. Rows that fail a rule are not thrown away, they move to the quarantine
state with their reasons.

## Tables

```mermaid
erDiagram
    FACT_TRIP {
        string trip_id PK "md5 of month + vendor + times + zones + distance + total"
        string month "partition, YYYY-MM"
        int    vendor_id FK
        string request_source
        bool   incomplete_submission
        ts     pickup_ts
        int    pickup_zone_id FK
        string pickup_borough
        int    pickup_hour
        ts     dropoff_ts
        int    dropoff_zone_id FK
        string dropoff_borough
        float  duration_min
        float  distance_mi
        float  speed_mph
        bool   is_airport_trip
        int    passenger_count
        int    rate_code_id FK
        int    payment_type_id FK
        float  fare_amount
        float  tip_amount
        float  total_amount
    }
    DIM_ZONE {
        int    zone_id PK
        string borough
        string zone_name
        bool   is_airport
    }
    DIM_VENDOR {
        int    vendor_id PK
        string description
    }
    DIM_PAYMENT_TYPE {
        int    payment_type_id PK
        string description
    }
    DIM_RATE_CODE {
        int    rate_code_id PK
        string description
    }
    QUARANTINE {
        string month
        int    vendor_id
        list   reasons
    }
    API_ZONE_COUNTS {
        string month PK
        int    zone_id PK
        int    published_pickups
    }
    METRICS_MONTHLY {
        string month PK
        string borough PK
        int    raw_rows
        int    valid_trips
        float  data_yield_pct
        float  median_duration_min
        float  p90_duration_min
        float  median_speed_mph
        float  airport_trip_share_pct
        int    tlc_published_pickups
        float  recon_gap_pct
    }

    DIM_ZONE ||--o{ FACT_TRIP : pickup_zone_id
    DIM_ZONE ||--o{ FACT_TRIP : dropoff_zone_id
    DIM_VENDOR ||--o{ FACT_TRIP : vendor_id
    DIM_PAYMENT_TYPE ||--o{ FACT_TRIP : payment_type_id
    DIM_RATE_CODE ||--o{ FACT_TRIP : rate_code_id
    DIM_ZONE ||--o{ API_ZONE_COUNTS : zone_id
    FACT_TRIP }o--|| METRICS_MONTHLY : "grouped by month, borough"
    API_ZONE_COUNTS }o--|| METRICS_MONTHLY : reconciled
```

| Table | One row is | Key | Note |
|---|---|---|---|
| fact_trip | one valid trip | trip_id (hash) | TLC gives no natural id |
| quarantine | one rejected raw row | none | all original columns plus reasons |
| dim_zone | one taxi zone | zone_id | 265 rows including 264 Unknown and 265 Outside NYC |
| dim_vendor, dim_payment_type, dim_rate_code | one code | the code | labels from the TLC dictionary |
| api_zone_counts | month x zone | (month, zone_id) | yellow pickups from Open Data |
| metrics_monthly | month x borough, plus ALL NYC | (month, borough) | the output table |

Why not just mirror the source table: the source has no idea of "valid", mixes
lifecycle and payment and surcharge fields, and has no borough. This model separates
trusted rows from rejected ones, puts borough on both ends of the trip, and carries
TLC's published count next to ours so the trust gate is computed rather than claimed.

## Metrics and how they hang off the KPI

```
KPI        trusted monthly trips  (valid count, released only if yield >= 90% and gap <= 5%)
  outcome  valid_trips, data_yield_pct         how much of the month I can stand behind
  workflow median_duration_min, p90_duration_min   service time, meter on to meter off
  workflow median_speed_mph                    congestion proxy
  mix      airport_trip_share_pct              long airport trips push duration up
  trust    recon_gap_pct                       do we agree with TLC's own number
```

| Metric | Formula | Grain | Why |
|---|---|---|---|
| valid_trips | count of rows with empty reasons | month x borough | the KPI itself |
| data_yield_pct | valid / raw x 100 | month x borough | release gate; a drop means a vendor problem |
| median_duration_min | median(dropoff - pickup) | month x borough | what customers experience, robust to outliers |
| p90_duration_min | 90th percentile of duration | month x borough | the slow tail planners staff for |
| median_speed_mph | median(distance / hours) | month x borough | comparable congestion signal across months |
| airport_trip_share_pct | trips touching EWR/JFK/LGA / valid | month x borough | explains Queens' 33 min median |
| recon_gap_pct | (raw - published) / published x 100 | month x borough | release gate |

Controllable by operations: honestly none of these directly, this is regulator data.
What is controllable is the escalation: Helix drop-off times and the Vendor 1 May
resubmission both move yield and the reconciliation gap.

Missing event that limits the model most: a request / dispatch timestamp (no waiting
time) and a trip id (no proper duplicate tracking).
