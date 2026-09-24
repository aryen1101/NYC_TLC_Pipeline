# Source map

## Business question to data

The planning team's question is "how many trips, how long, per borough, per month".
Broken down:

| Question | Information needed | Where it lives | Owner | One row means | How I get it |
|---|---|---|---|---|---|
| How many trips this month, where | pickup time, pickup zone | TLC trip parquet | NYC TLC, compiled from vendor (TPEP) uploads | one metered trip | HTTP download |
| How long did they take | pickup and drop-off time, distance | same file | same | same | same |
| Which borough / airport | zone id to borough | taxi_zone_lookup.csv | NYC TLC | one taxi zone (265) | HTTP download |
| Does our count match TLC's | monthly pickups per zone for yellow taxis | NYC Open Data `c5iv-bn4s` | TLC via Open Data | month x industry x zone x pickup/dropoff | Socrata API, pages of 1000 |
| What does TLC say average trip time is | trips per day, avg minutes per trip | NYC Open Data `v6kb-cqej` | TLC via Open Data | month x license class | Socrata API |
| What do the codes mean | vendor, payment, rate code labels | yellow taxi data dictionary PDF | NYC TLC | n/a | downloaded in a browser, copy in docs/reference |

## Which source I trust for what

- Trip times, distance, fare: only the parquet has them at trip level, so it is the
  source. The Industry Indicators average (17.5 min for May) is just a cross-check
  against my median (14.6 min). Mean above median is what you expect from a skewed
  distribution, so they agree.
- Trip count per month: disputed. The parquet is the raw upload, the Open Data table is
  what TLC publishes. They differ by 0.5% to 3.7%. I do not pick one, I show both.
- Zone to borough: the lookup CSV, nothing else has it.

## Gaps, meaning things no source can tell me

- No trip id, driver id or vehicle id. So I cannot properly detect duplicates or track
  a resubmitted trip. Full-row equality is the best I can do.
- No request or dispatch timestamp. Waiting time cannot be measured. The workflow starts
  at meter on.
- No changelog. TLC silently re-publishes files (June changed on 17 Sep 2026). I keep a
  SHA-256 per run so I at least notice.
- Industry Indicators lag by two to three months. Only May was there for yellow.
- Vendor 7 (Helix) never populates drop-off time, so its trips only count for volume.

## Notes on access

- The parquet links and the zone lookup are on
  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- TLC returns 403, not 404, for months that are not published yet.
- nyc.gov also returns 403 to scripted downloads of the PDF dictionary. Download it in
  a browser or send a browser User-Agent.
