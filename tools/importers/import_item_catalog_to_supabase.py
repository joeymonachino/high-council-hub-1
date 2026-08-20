# Import curated item catalog metadata into Supabase.
import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.importers.fetch_item_metadata import item_slug, upsert_metadata_rows  # noqa: E402


def first_value(row: dict[str, Any], *keys: str) -> Any:
    # Catalog exports vary by source, so this normalizes common field spellings.
    for key in keys:
        if row.get(key) not in (None, "", [], {}):
            return row.get(key)
    return None


def list_value(value: Any) -> list[Any]:
    # Accept arrays or comma-delimited strings for hand-edited catalog files.
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return []


def normalize_catalog_row(row: dict[str, Any]) -> dict[str, Any]:
    name = str(first_value(row, "name", "displayName", "display_name", "item", "itemName") or "").strip()
    display_name = str(first_value(row, "displayName", "display_name", "name") or name).strip()
    return {
        "name": name or display_name,
        "displayName": display_name or name,
        "slug": str(first_value(row, "slug") or item_slug(name or display_name)),
        "source": str(first_value(row, "source") or "manual-catalog"),
        "sourceUrl": str(first_value(row, "sourceUrl", "source_url", "url") or ""),
        "imageUrl": str(first_value(row, "imageUrl", "image_url", "iconUrl", "icon_url") or ""),
        "summary": str(first_value(row, "summary", "description", "desc") or ""),
        "cost": first_value(row, "cost", "price", "gold"),
        "itemType": str(first_value(row, "itemType", "item_type", "type", "category") or ""),
        "tags": list_value(first_value(row, "tags", "tagList")),
        "stats": list_value(first_value(row, "stats", "statLines", "stat_lines")),
        "passive": str(first_value(row, "passive", "passiveText", "passive_text") or ""),
        "categoriesSeen": list_value(first_value(row, "categoriesSeen", "categories_seen", "categories")),
        "sampleCount": int(first_value(row, "sampleCount", "sample_count", "games", "uses") or 0),
    }


def load_catalog(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("items") or payload.get("data") or []
    if not isinstance(payload, list):
        raise RuntimeError("Catalog file must be a JSON list or an object with an items/data list.")
    rows = [normalize_catalog_row(row) for row in payload if isinstance(row, dict)]
    return [row for row in rows if row.get("name")]


def main() -> None:
    parser = argparse.ArgumentParser(description="Import item descriptions/stats/images into Supabase item_metadata.")
    parser.add_argument("--catalog-file", required=True, help="JSON file containing item metadata rows.")
    parser.add_argument("--upsert", action="store_true", help="Write rows to Supabase. Omit for dry-run validation.")
    args = parser.parse_args()

    rows = load_catalog(Path(args.catalog_file))
    if args.upsert:
        upsert_metadata_rows(rows)
    print(json.dumps({"ok": True, "catalogFile": args.catalog_file, "rows": len(rows), "upserted": bool(args.upsert), "sample": rows[:3]}, indent=2))


if __name__ == "__main__":
    main()
