from __future__ import annotations

# Thin Supabase REST helpers. The app intentionally uses PostgREST directly
# instead of a client library so local scripts and Vercel routes share one small
# predictable request layer.
import json
from typing import Any

import requests

from hub.config import MATCH_HISTORY_UI_ROW_LIMIT, get_secret
from hub.http import HTTP


def sb_headers(prefer: str | None = None) -> dict[str, str]:
    key = get_secret("SUPABASE_KEY")
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    return headers


def sb_url(table: str) -> str:
    base_url = get_secret("SUPABASE_URL")
    return f"{base_url}/rest/v1/{table}"


def sb_select(table: str, params: dict[str, str] | None = None) -> list[dict]:
    response = HTTP.get(
        sb_url(table),
        headers=sb_headers("return=representation"),
        params=params or {"select": "*"},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def sb_select_all(table: str, params: dict[str, str] | None = None, page_size: int = 1000) -> list[dict]:
    params = dict(params or {})
    base_limit = max(1, min(page_size, 1000))
    offset = 0
    rows: list[dict] = []

    while True:
        page_params = dict(params)
        page_params["limit"] = str(base_limit)
        page_params["offset"] = str(offset)
        batch = sb_select(table, page_params)
        rows.extend(batch)
        if len(batch) < base_limit:
            break
        offset += base_limit

    return rows


def sb_upsert(table: str, records: list[dict], on_conflict: str) -> None:
    if not records:
        return
    params = {"on_conflict": on_conflict} if on_conflict else {}
    response = HTTP.post(
        sb_url(table),
        headers=sb_headers("resolution=merge-duplicates,return=minimal"),
        params=params,
        data=json.dumps(records),
        timeout=20,
    )
    response.raise_for_status()


def sb_insert(table: str, records: list[dict]) -> None:
    if not records:
        return
    response = HTTP.post(
        sb_url(table),
        headers=sb_headers("return=minimal"),
        data=json.dumps(records),
        timeout=20,
    )
    if not response.ok:
        raise RuntimeError(f"Supabase insert failed for {table}: {response.status_code} {response.text}")


def sb_select_bootstrap(table: str, params: dict[str, str] | None = None, timeout: int = 4) -> list[dict]:
    # Initial page load should never sit behind a slow Supabase edge response.
    # Save/sync routes still use sb_select so write-critical paths remain strict.
    response = requests.get(
        sb_url(table),
        headers=sb_headers("return=representation"),
        params=params or {},
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else []


def sb_select_bootstrap_paged(table: str, params: dict[str, str] | None = None, timeout: int = 5, limit: int | None = None, page_size: int = 1000) -> list[dict]:
    # PostgREST commonly caps one response at 1000 rows. Summary tables are
    # lightweight, so we page them without touching the heavy raw_match JSON.
    rows: list[dict[str, Any]] = []
    target = limit or MATCH_HISTORY_UI_ROW_LIMIT
    for start in range(0, target, page_size):
        end = min(start + page_size - 1, target - 1)
        response = requests.get(
            sb_url(table),
            headers={**sb_headers("return=representation"), "Range-Unit": "items", "Range": f"{start}-{end}"},
            params=params or {},
            timeout=timeout,
        )
        response.raise_for_status()
        batch = response.json()
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(row for row in batch if isinstance(row, dict))
        if len(batch) < page_size:
            break
    return rows


def sb_delete_player_rankings(player: str) -> None:
    response = HTTP.delete(
        sb_url("personal_rankings"),
        headers=sb_headers("return=minimal"),
        params={"player": f"eq.{player}"},
        timeout=20,
    )
    response.raise_for_status()
