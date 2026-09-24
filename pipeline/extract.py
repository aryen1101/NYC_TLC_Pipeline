"""
EXTRACT stage: retrieve raw inputs from the three NYC TLC source systems and
preserve them untouched, with a provenance manifest per run.

Retrieval modes:
  1. FILES  - monthly yellow-taxi trip parquet from TLC's CloudFront bucket
  2. API    - NYC Open Data (Socrata) aggregate datasets, paginated JSON
  3. FILES  - taxi zone reference CSV (dimension table)

Nothing in this module cleans or interprets data. It only retrieves, verifies
byte counts / row counts, and writes a manifest so completeness can be proven.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

import requests

from .logging_utils import get_logger

log = get_logger("extract")

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net"
SOCRATA_BASE = "https://data.cityofnewyork.us/resource"

DATASET_ZONE_COUNTS = "c5iv-bn4s"   # Pickups and Drop-offs by Taxi Zone and Industry (monthly)
DATASET_INDICATORS = "v6kb-cqej"    # TLC Industry Indicators (monthly, per license class)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass
class RetrievedObject:
    source: str
    mode: str                 # "file" | "api"
    url: str
    local_path: str
    bytes: int
    sha256: str
    expected_bytes: int | None
    rows: int | None
    retrieved_at: str
    status: str               # "downloaded" | "cached" | "failed"
    note: str = ""


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _get_with_retry(url: str, params: dict | None = None, max_retries: int = 4,
                    timeout: int = 60, stream: bool = False) -> requests.Response:
    """Retry only transient failures (429 / 5xx / network). Fail fast on 4xx."""
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, params=params, timeout=timeout, stream=stream)
        except requests.RequestException as exc:
            if attempt > max_retries:
                raise
            wait = 2 ** attempt
            log.warning("network error | url=%s | attempt=%d/%d | wait=%ss | err=%s",
                        url, attempt, max_retries, wait, exc)
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            return resp
        if resp.status_code in RETRYABLE_STATUS and attempt <= max_retries:
            wait = int(resp.headers.get("Retry-After", 2 ** attempt))
            log.warning("transient failure | url=%s | status=%s | attempt=%d/%d | wait=%ss",
                        url, resp.status_code, attempt, max_retries, wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()

# FILES: monthly trip parquet
def download_trip_parquet(month: str, out_dir: Path, force: bool = False) -> RetrievedObject:
    """month = 'YYYY-MM'. Downloads yellow_tripdata_<month>.parquet if not cached.

    Completeness check: Content-Length from a HEAD request must equal bytes on
    disk. A partially written file is never left behind (tmp + rename).
    """
    fname = f"yellow_tripdata_{month}.parquet"
    url = f"{TLC_BASE}/trip-data/{fname}"
    dest = out_dir / fname
    out_dir.mkdir(parents=True, exist_ok=True)

    head = requests.head(url, timeout=30)
    if head.status_code == 403:
        raise FileNotFoundError(f"{fname} not published yet (HTTP 403 from TLC)")
    head.raise_for_status()
    expected = int(head.headers.get("Content-Length", 0)) or None

    if dest.exists() and not force and expected and dest.stat().st_size == expected:
        log.info("parquet cached | month=%s | bytes=%d", month, expected)
        status = "cached"
    else:
        log.info("parquet download | month=%s | url=%s | expected_bytes=%s", month, url, expected)
        tmp = dest.with_suffix(".parquet.part")
        with _get_with_retry(url, stream=True, timeout=300) as r, tmp.open("wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
        if expected and tmp.stat().st_size != expected:
            tmp.unlink(missing_ok=True)
            raise IOError(f"{fname}: size mismatch {tmp.stat().st_size} != {expected}")
        tmp.replace(dest)
        status = "downloaded"

    import pyarrow.parquet as pq
    rows = pq.ParquetFile(dest).metadata.num_rows

    return RetrievedObject(
        source="tlc_trip_records", mode="file", url=url, local_path=str(dest),
        bytes=dest.stat().st_size, sha256=_sha256(dest), expected_bytes=expected,
        rows=rows, retrieved_at=datetime.now(timezone.utc).isoformat(), status=status,
    )


# FILES: zone reference
def download_zone_lookup(out_dir: Path) -> RetrievedObject:
    url = f"{TLC_BASE}/misc/taxi_zone_lookup.csv"
    dest = out_dir / "taxi_zone_lookup.csv"
    out_dir.mkdir(parents=True, exist_ok=True)
    r = _get_with_retry(url)
    dest.write_bytes(r.content)
    rows = sum(1 for _ in dest.open(encoding="utf-8")) - 1
    log.info("zone lookup | rows=%d | bytes=%d", rows, len(r.content))
    return RetrievedObject(
        source="tlc_zone_lookup", mode="file", url=url, local_path=str(dest),
        bytes=len(r.content), sha256=_sha256(dest), expected_bytes=None, rows=rows,
        retrieved_at=datetime.now(timezone.utc).isoformat(), status="downloaded",
    )


#  Socrata paginated JSON, raw pages preserved
def fetch_socrata(dataset_id: str, where: str, out_dir: Path, page_size: int = 1000,
                  order: str = ":id") -> RetrievedObject:
    """Page through a Socrata dataset with $limit/$offset.

    Completeness: pagination stops only when a page returns fewer than
    page_size rows. The total row count is then cross-checked against a
    server-side SELECT count(*) with the same filter, so a silently truncated
    pull is detected rather than assumed complete.
    Every raw page is written to disk before anything is parsed.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    url = f"{SOCRATA_BASE}/{dataset_id}.json"

    cnt_resp = _get_with_retry(url, params={"$select": "count(*) as n", "$where": where})
    expected_rows = int(cnt_resp.json()[0]["n"])

    all_rows: list[dict] = []
    page = 0
    while True:
        page += 1
        params = {"$where": where, "$limit": page_size,
                  "$offset": (page - 1) * page_size, "$order": order}
        resp = _get_with_retry(url, params=params)
        raw_path = out_dir / f"{dataset_id}_page_{page:03d}.json"
        raw_path.write_bytes(resp.content)         
        rows = resp.json()
        all_rows.extend(rows)
        log.info("socrata page | dataset=%s | page=%d | rows=%d", dataset_id, page, len(rows))
        if len(rows) < page_size:
            break

    if len(all_rows) != expected_rows:
        raise IOError(f"{dataset_id}: retrieved {len(all_rows)} rows, server reports {expected_rows}")

    combined = out_dir / f"{dataset_id}_all.json"
    combined.write_text(json.dumps(all_rows, indent=1), encoding="utf-8")
    log.info("socrata complete | dataset=%s | pages=%d | rows=%d (matches server count)",
             dataset_id, page, len(all_rows))
    return RetrievedObject(
        source=f"nyc_open_data:{dataset_id}", mode="api",
        url=f"{url}?$where={where}", local_path=str(combined),
        bytes=combined.stat().st_size, sha256=_sha256(combined), expected_bytes=None,
        rows=len(all_rows), retrieved_at=datetime.now(timezone.utc).isoformat(),
        status="downloaded", note=f"{page} pages, page_size={page_size}",
    )


# Orchestration
def extract_all(months: list[str], raw_dir: Path) -> list[RetrievedObject]:
    """Retrieve every input for the requested months and write a manifest."""
    objs: list[RetrievedObject] = []

    for m in months:
        objs.append(download_trip_parquet(m, raw_dir / "tlc_parquet"))

    objs.append(download_zone_lookup(raw_dir / "reference"))

    month_list = ", ".join(f"'{m}-01'" for m in months)
    objs.append(fetch_socrata(
        DATASET_ZONE_COUNTS,
        where=f"industry = 'Yellow Taxi' AND metric_month in ({month_list})",
        out_dir=raw_dir / "api" / DATASET_ZONE_COUNTS,
    ))

    ym_list = ", ".join(f"'{m}'" for m in months)
    objs.append(fetch_socrata(
        DATASET_INDICATORS,
        where=f"license_class = 'Yellow' AND month_year in ({ym_list})",
        out_dir=raw_dir / "api" / DATASET_INDICATORS,
    ))

    manifest = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "months": months,
        "objects": [asdict(o) for o in objs],
    }
    (raw_dir / "retrieval_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    log.info("manifest written | objects=%d | path=%s", len(objs), raw_dir / "retrieval_manifest.json")
    return objs
