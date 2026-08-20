from __future__ import annotations

# Backfill lightweight match summary tables from the raw match-history archive.
import argparse
import json
import sys
from typing import Any
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import app  # noqa: E402


def load_raw_history(page_size: int) -> list[dict]:
    return app.sb_select_all(
        "smitesource_match_history",
        {
            "select": "record_key,player,profile_player_uuid,hirez_player_uuid,match_key,match_id,source,source_match_id,canonical_match_key,god_name,queue_type,won,party_size,party_label,team_id,started_at,synced_at,raw_match",
            "order": "started_at.desc",
        },
        page_size=page_size,
    )


def upsert_batch_resilient(batch: list[dict], batch_number: int) -> tuple[int, int, int]:
    try:
        player_count, item_count = app.upsert_match_summary_rows(batch)
        return player_count, item_count, 0
    except Exception as exc:  # noqa: BLE001
        if len(batch) <= 1:
            response = getattr(exc, "response", None)
            error_text = response.text if response is not None else str(exc)
            print(json.dumps({"batch": batch_number, "failedRecord": batch[0].get("record_key"), "error": error_text}))
            return 0, 0, 1
        midpoint = len(batch) // 2
        left = upsert_batch_resilient(batch[:midpoint], batch_number)
        right = upsert_batch_resilient(batch[midpoint:], batch_number)
        return left[0] + right[0], left[1] + right[1], left[2] + right[2]




def delete_stale_item_rows(valid_item_keys: set[str], dry_run: bool) -> dict[str, Any]:
    existing_rows = app.sb_select_all("match_item_summary", {"select": "item_key"}, page_size=1000)
    stale_keys = [str(row.get("item_key") or "") for row in existing_rows if row.get("item_key") and str(row.get("item_key")) not in valid_item_keys]
    if not dry_run:
        for start in range(0, len(stale_keys), 100):
            chunk = stale_keys[start:start + 100]
            response = app.HTTP.delete(
                app.sb_url("match_item_summary"),
                headers=app.sb_headers("return=minimal"),
                params={"item_key": f"in.({','.join(chunk)})"},
                timeout=30,
            )
            response.raise_for_status()
    return {"staleItemRows": len(stale_keys), "deletedStaleItemRows": 0 if dry_run else len(stale_keys)}
def backfill(page_size: int, batch_size: int, dry_run: bool, cleanup_items: bool = False) -> dict:
    rows = load_raw_history(page_size)
    total_player_rows = 0
    total_item_rows = 0
    failed_rows = 0
    valid_item_keys: set[str] = set()
    batches = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        player_rows, item_rows = app.summary_rows_for_history_records(batch)
        valid_item_keys.update(str(item.get("item_key") or "") for item in item_rows if item.get("item_key"))
        batches += 1
        if not dry_run:
            player_count, item_count, failed_count = upsert_batch_resilient(batch, batches)
        else:
            player_count, item_count, failed_count = len(player_rows), len(item_rows), 0
        total_player_rows += player_count
        total_item_rows += item_count
        failed_rows += failed_count
        print(json.dumps({"batch": batches, "rawRows": len(batch), "playerRows": player_count, "itemRows": item_count, "failedRows": failed_count, "dryRun": dry_run}))
    cleanup_result = delete_stale_item_rows(valid_item_keys, dry_run) if cleanup_items else {"staleItemRows": None, "deletedStaleItemRows": None}
    return {
        "ok": True,
        "dryRun": dry_run,
        "rawRows": len(rows),
        "playerRows": total_player_rows,
        "itemRows": total_item_rows,
        "batches": batches,
        "failedRows": failed_rows,
        **cleanup_result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill match_player_summary and match_item_summary from smitesource_match_history.raw_match.")
    parser.add_argument("--page-size", type=int, default=100, help="Supabase read page size for raw archive rows.")
    parser.add_argument("--batch-size", type=int, default=75, help="Rows per summary upsert batch.")
    parser.add_argument("--dry-run", action="store_true", help="Build summaries without writing them.")
    parser.add_argument("--cleanup-items", action="store_true", help="Delete match_item_summary rows no longer produced by the current extractor.")
    args = parser.parse_args()
    print(json.dumps(backfill(args.page_size, args.batch_size, args.dry_run, args.cleanup_items), indent=2))


if __name__ == "__main__":
    main()
