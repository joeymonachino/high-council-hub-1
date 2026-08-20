# Clean stale duplicate item metadata rows while preserving richer catalog entries.
import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import app  # noqa: E402


def item_key(name: str) -> str:
    # This key collapses punctuation/case differences such as Book Of Thoth vs Book of Thoth.
    return re.sub(r"[^a-z0-9]", "", str(name or "").lower())


def has_rich_metadata(row: dict[str, Any]) -> bool:
    # A row is considered worth keeping if it has art or real catalog details.
    return any(row.get(field) for field in ("image_url", "summary", "passive", "source_url")) or bool(row.get("stats"))


def row_score(row: dict[str, Any]) -> tuple[int, int, int]:
    # Higher score means the row is a better canonical representative for a duplicate group.
    return (
        1 if has_rich_metadata(row) else 0,
        int(row.get("sample_count") or 0),
        len(str(row.get("name") or "")),
    )


def delete_row(name: str) -> None:
    # Supabase handles escaping through requests params, so names with apostrophes stay safe.
    response = app.HTTP.delete(
        app.sb_url("item_metadata"),
        headers=app.sb_headers("return=minimal"),
        params={"name": f"eq.{name}"},
        timeout=20,
    )
    response.raise_for_status()


def cleanup_item_metadata(dry_run: bool = True) -> dict[str, Any]:
    response = app.HTTP.get(
        app.sb_url("item_metadata"),
        headers=app.sb_headers("return=representation"),
        params={"select": "name,image_url,summary,passive,source_url,stats,sample_count", "order": "name.asc"},
        timeout=90,
    )
    response.raise_for_status()
    rows = response.json()
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = item_key(str(row.get("name") or ""))
        if key:
            groups.setdefault(key, []).append(row)

    removals: list[dict[str, Any]] = []
    for key, group in groups.items():
        if len(group) < 2:
            continue
        best = sorted(group, key=row_score, reverse=True)[0]
        has_better_rich_copy = has_rich_metadata(best)
        for row in group:
            if row is best:
                continue
            # Keep rich-vs-rich duplicates for manual review; only drop the clearly stale shells.
            if has_better_rich_copy and not has_rich_metadata(row):
                removals.append({"name": row.get("name"), "kept": best.get("name"), "key": key})

    if not dry_run:
        for removal in removals:
            delete_row(str(removal["name"]))

    return {"ok": True, "dryRun": dry_run, "scanned": len(rows), "removed": 0 if dry_run else len(removals), "wouldRemove": len(removals), "removals": removals}


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove stale no-image duplicate rows from item_metadata.")
    parser.add_argument("--apply", action="store_true", help="Actually delete stale duplicates. Omit for a dry run.")
    args = parser.parse_args()
    print(json.dumps(cleanup_item_metadata(dry_run=not args.apply), indent=2))


if __name__ == "__main__":
    main()
