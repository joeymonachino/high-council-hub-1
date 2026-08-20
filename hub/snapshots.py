from __future__ import annotations

# Local JSON snapshots are development/protection fallbacks. Supabase should be
# treated as the live source of truth, but these helpers keep the app usable
# during local work or a temporary Supabase read failure.
import json
from typing import Any

from hub.config import DATA_DIR


def load_json_snapshot(name: str) -> list[dict]:
    snapshot_path = DATA_DIR / name
    if not snapshot_path.exists():
        return []
    with snapshot_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def merge_snapshot_rows(primary_rows: list[dict], fallback_rows: list[dict], key_names: tuple[str, ...]) -> list[dict]:
    merged: list[dict] = []
    seen: set[str] = set()

    def row_key(row: dict) -> str:
        for key_name in key_names:
            value = row.get(key_name)
            if value is not None and str(value).strip():
                return str(value).strip().lower()
        return ""

    for row in primary_rows + fallback_rows:
        key = row_key(row)
        if not key or key in seen:
            continue
        seen.add(key)
        merged.append(row)

    return merged
