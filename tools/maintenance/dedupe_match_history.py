from __future__ import annotations

# This script finds cross-source duplicate match-history rows, especially older
# Tracker imports that overlap newer SmiteSource rows for the same player, god,
# queue, and start time. It dry-runs by default and only deletes with --apply.
import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

# This block lets the script import the Flask app helpers when it is run from
# either the repo root or the tools directory.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app  # noqa: E402


# This helper identifies rows that came from the older Tracker import path.
def is_tracker_row(row: dict[str, Any]) -> bool:
    record_key = str(row.get("record_key") or "").lower()
    match_key = str(row.get("match_key") or "").lower()
    raw_match = row.get("raw_match") if isinstance(row.get("raw_match"), dict) else {}
    source = str(raw_match.get("source") or raw_match.get("platform") or "").lower()
    return "tracker" in record_key or "tracker" in match_key or "tracker" in source


# This helper gives each duplicate candidate a keep priority. Lower is better:
# native SmiteSource rows beat Tracker rows, canonical display names beat alias
# labels, and richer rows beat sparse rows.
def keep_priority(row: dict[str, Any]) -> tuple[int, int, int, str]:
    raw_match = row.get("raw_match") if isinstance(row.get("raw_match"), dict) else {}
    tracker_penalty = 1 if is_tracker_row(row) else 0
    display_name = str(row.get("god_name") or raw_match.get("godName") or "")
    canonical_penalty = 1 if app.normalize_god_identity_key(display_name) == "themorrigan" and display_name != "The Morrigan" else 0
    richness = sum(1 for value in raw_match.values() if value not in (None, "", [], {}))
    return (tracker_penalty, canonical_penalty, -richness, str(row.get("record_key") or ""))


# This helper deletes one row by its stable record_key through Supabase REST.
def delete_record_key(record_key: str) -> None:
    response = app.HTTP.delete(
        app.sb_url("smitesource_match_history"),
        headers=app.sb_headers(),
        params={"record_key": f"eq.{record_key}"},
        timeout=20,
    )
    if not response.ok:
        raise RuntimeError(f"Delete failed for {record_key}: {response.status_code} {response.text}")


# This helper builds the duplicate report and optionally applies deletes.
def dedupe_match_history(player: str = "", god: str = "", apply: bool = False) -> dict[str, Any]:
    # This block narrows the Supabase read whenever possible. Pulling the full
    # match table is slower and can time out once the history gets large.
    if player:
        rows = app.load_stored_match_history(player)
    else:
        rows = app.load_all_stored_match_history()
    if god:
        target_god_key = app.normalize_god_identity_key(god)
        rows = [
            row for row in rows
            if app.normalize_god_identity_key(str(row.get("god_name") or (row.get("raw_match") or {}).get("godName") or "")) == target_god_key
        ]

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        signature = app.match_history_duplicate_signature(str(row.get("player") or ""), row)
        if signature.strip("|"):
            groups[signature].append(row)

    duplicate_groups = []
    delete_keys: list[str] = []
    for signature, group_rows in groups.items():
        if len(group_rows) < 2:
            continue
        ordered = sorted(group_rows, key=keep_priority)
        keep = ordered[0]
        delete = ordered[1:]
        delete_keys.extend(str(row.get("record_key") or "") for row in delete if row.get("record_key"))
        duplicate_groups.append(
            {
                "signature": signature,
                "keep": {
                    "record_key": keep.get("record_key"),
                    "match_key": keep.get("match_key"),
                    "source": "tracker" if is_tracker_row(keep) else "smitesource",
                },
                "delete": [
                    {
                        "record_key": row.get("record_key"),
                        "match_key": row.get("match_key"),
                        "source": "tracker" if is_tracker_row(row) else "smitesource",
                    }
                    for row in delete
                ],
            }
        )

    if apply:
        for record_key in delete_keys:
            delete_record_key(record_key)

    return {
        "ok": True,
        "dryRun": not apply,
        "scannedRows": len(rows),
        "duplicateGroups": len(duplicate_groups),
        "deleteCount": len(delete_keys),
        "groups": duplicate_groups[:50],
    }


# This block parses CLI args and prints a JSON report that is easy to paste back
# into chat or inspect before running with --apply.
def main() -> None:
    parser = argparse.ArgumentParser(description="Find and optionally remove cross-source duplicate match history rows.")
    parser.add_argument("--player", default="", help="Optional council player filter, e.g. Joey")
    parser.add_argument("--god", default="", help="Optional god filter, e.g. Vulcan")
    parser.add_argument("--apply", action="store_true", help="Actually delete duplicate rows. Omit for dry-run.")
    args = parser.parse_args()
    print(json.dumps(dedupe_match_history(args.player, args.god, bool(args.apply)), indent=2))


if __name__ == "__main__":
    main()
