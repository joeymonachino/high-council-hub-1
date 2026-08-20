# Export critical Supabase tables to local JSON backups.
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import app  # noqa: E402

DEFAULT_TABLES = [
    "gods_metadata",
    "council_ratings",
    "personal_rankings",
    "rating_history",
    "smitesource_match_history",
    "match_player_summary",
    "match_item_summary",
    "item_metadata",
]


def export_table(table: str, output_dir: Path, page_size: int) -> dict[str, Any]:
    rows = app.sb_select_all(table, {"select": "*"}, page_size=page_size)
    output_path = output_dir / f"{table}.json"
    output_path.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
    return {"table": table, "rows": len(rows), "file": str(output_path)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Back up critical High Council Hub Supabase tables to local JSON files.")
    parser.add_argument("--output-dir", default="data/supabase_backups", help="Folder where timestamped backup folders are written.")
    parser.add_argument("--table", action="append", help="Export only this table. Repeat for multiple tables.")
    parser.add_argument("--page-size", type=int, default=200, help="Rows per Supabase page. Lower is gentler for raw_match-heavy tables.")
    args = parser.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir) / stamp
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = args.table or DEFAULT_TABLES
    results = []
    for table in tables:
        print(f"Exporting {table}...")
        try:
            results.append(export_table(table, output_dir, args.page_size))
        except Exception as exc:  # noqa: BLE001
            results.append({"table": table, "rows": 0, "file": "", "ok": False, "error": str(exc)})

    manifest = {"createdAt": datetime.now(timezone.utc).isoformat(), "results": results}
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
