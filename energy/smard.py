"""
Client for the SMARD chart data API.

SMARD serves each series in weekly chunks. An index file lists the start of
every chunk (milliseconds since epoch, UTC); each chunk holds
[timestamp, value] pairs, with null for hours not published yet.
"""
import json
import time
from pathlib import Path

import requests

from energy.config import SMARD_URL


def _get_json(url: str, session=None, retries: int = 3) -> dict:
    session = session or requests.Session()
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=30)
            if response.status_code < 500:
                response.raise_for_status()
                return response.json()
        except requests.ConnectionError:
            if attempt == retries - 1:
                raise
        time.sleep(2 ** attempt)
    response.raise_for_status()
    return response.json()


def chunk_starts(filter_id: int, region: str, session=None) -> list[int]:
    return _get_json(f"{SMARD_URL}/{filter_id}/{region}/index_hour.json", session)["timestamps"]


def chunks_for_interval(starts: list[int], start_ms: int, end_ms: int) -> list[int]:
    """Chunk start timestamps whose week overlaps [start_ms, end_ms)."""
    starts = sorted(starts)
    selected = []
    for i, chunk in enumerate(starts):
        chunk_end = starts[i + 1] if i + 1 < len(starts) else float("inf")
        if chunk < end_ms and chunk_end > start_ms:
            selected.append(chunk)
    return selected


def fetch_chunk(filter_id: int, region: str, chunk: int, session=None, raw_dir: str | None = None,
                memo: dict | None = None) -> list:
    """
    One weekly chunk, always downloaded so that revisions published by SMARD
    reach the database on a re-run. The response is saved to raw_dir, so every
    load can be traced back to its source file. `memo` avoids downloading the
    same chunk twice within one run (a weekly chunk covers seven days).
    """
    url = f"{SMARD_URL}/{filter_id}/{region}/{filter_id}_{region}_hour_{chunk}.json"
    if memo is not None and url in memo:
        return memo[url]
    data = _get_json(url, session)
    if raw_dir:
        path = Path(raw_dir) / f"{filter_id}_{region}_{chunk}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data))
    if memo is not None:
        memo[url] = data["series"]
    return data["series"]


def get_series(filter_id: int, region: str, start_ms: int, end_ms: int,
               session=None, raw_dir: str | None = None, memo: dict | None = None) -> dict[int, float | None]:
    """Hourly values in [start_ms, end_ms) as {timestamp_ms: value}."""
    starts = memo.get(("index", filter_id, region)) if memo is not None else None
    if starts is None:
        starts = chunk_starts(filter_id, region, session)
        if memo is not None:
            memo[("index", filter_id, region)] = starts
    values = {}
    for chunk in chunks_for_interval(starts, start_ms, end_ms):
        for timestamp, value in fetch_chunk(filter_id, region, chunk, session, raw_dir, memo):
            if start_ms <= timestamp < end_ms:
                values[timestamp] = value
    return values
