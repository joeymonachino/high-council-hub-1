from __future__ import annotations

# This script imports browser-exported SmiteSource HAR files into the same
# Supabase match-history table used by the app's Rater Stats and Chemistry tabs.
import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import app  # noqa: E402


def existing_dedupe_sets(player: str) -> tuple[set[str], set[str], int]:
    return app.stored_match_history_dedupe_sets(player)

def dry_run_summary(har_payload: dict[str, Any]) -> dict[str, Any]:
    grouped_rows = app.extract_smitesource_matches_from_har(har_payload)
    results: list[dict[str, Any]] = []

    for player, rows in grouped_rows.items():
        player_uuid = app.smitesource_player_uuid(app.SMITESOURCE_PROFILE_LINKS.get(player, ""))
        existing_keys, existing_signatures, stored_count = existing_dedupe_sets(player)
        seen_keys: set[str] = set()
        seen_signatures: set[str] = set()
        missing = 0
        skipped_cross_source = 0

        for row in rows:
            record = app.normalize_smitesource_history_record(player, player_uuid, row)
            match_key = str(record.get("match_key") or "")
            signature = app.match_history_duplicate_signature(player, record)
            if not match_key or match_key in existing_keys or match_key in seen_keys:
                continue
            if signature in existing_signatures or signature in seen_signatures:
                skipped_cross_source += 1
                continue
            seen_keys.add(match_key)
            seen_signatures.add(signature)
            missing += 1

        results.append(
            {
                "player": player,
                "captured": len(rows),
                "missing": missing,
                "inserted": 0,
                "stored": stored_count,
                "skippedCrossSource": skipped_cross_source,
                "dryRun": True,
            }
        )
    return {
        "ok": True,
        "dryRun": True,
        "captured": sum(item["captured"] for item in results),
        "missing": sum(item["missing"] for item in results),
        "inserted": 0,
        "results": results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import a SmiteSource browser HAR into Supabase match history.")
    parser.add_argument("--har-file", required=True, help="Path to a Chrome/Edge HAR export from SmiteSource.")
    parser.add_argument("--dry-run", action="store_true", help="Report what would be inserted without writing to Supabase.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    har_path = Path(args.har_file)
    har_payload = json.loads(har_path.read_text(encoding="utf-8"))
    if not isinstance(har_payload, dict):
        raise RuntimeError("HAR file must contain a JSON object.")

    summary = dry_run_summary(har_payload) if args.dry_run else app.import_smitesource_har_payload(har_payload)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

