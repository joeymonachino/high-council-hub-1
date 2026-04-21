from __future__ import annotations

# This block imports the standard-library tools used for argument parsing, JSON
# reading, local secrets loading, and timestamp normalization.
import argparse
import json
import os
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# This block imports requests for Supabase REST access.
import requests


# This block defines the local project paths so the importer can reuse the same
# secrets setup as the Flask app.
BASE_DIR = Path(__file__).resolve().parents[1]
LOCAL_SECRETS_PATH = BASE_DIR / "secrets" / "app_secrets.toml"
LOCAL_SECRETS_PATH_TXT = BASE_DIR / "secrets" / "app_secrets.toml.txt"
SECRETS_PATH = LOCAL_SECRETS_PATH if LOCAL_SECRETS_PATH.exists() else LOCAL_SECRETS_PATH_TXT
SESSION = requests.Session()
SESSION.trust_env = False


# This helper loads the local TOML secrets file when present.
def load_local_secrets() -> dict[str, Any]:
    if not SECRETS_PATH.exists():
        return {}
    with SECRETS_PATH.open("rb") as handle:
        return tomllib.load(handle)


# This helper reads one config value from the environment first and then falls
# back to the local secrets file so local runs stay simple.
def get_secret(name: str, default: str = "") -> str:
    secrets = load_local_secrets()
    return os.environ.get(name, str(secrets.get(name, default)))


# This helper builds the shared Supabase headers used by the importer.
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


# This helper builds one Supabase REST URL from the configured project URL.
def sb_url(table: str) -> str:
    base_url = get_secret("SUPABASE_URL")
    return f"{base_url}/rest/v1/{table}"


# This helper pages through Supabase rows so the importer can compare against
# the full stored history instead of just one page.
def sb_select_all(table: str, params: dict[str, str] | None = None, page_size: int = 1000) -> list[dict[str, Any]]:
    params = dict(params or {})
    rows: list[dict[str, Any]] = []
    offset = 0

    while True:
        page_params = dict(params)
        page_params["limit"] = str(page_size)
        page_params["offset"] = str(offset)
        response = SESSION.get(
            sb_url(table),
            headers=sb_headers("return=representation"),
            params=page_params,
            timeout=30,
        )
        response.raise_for_status()
        batch = response.json()
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    return rows


# This helper upserts normalized rows into the existing stored-history table so
# the live app can reuse them immediately without code changes.
def sb_upsert(table: str, records: list[dict[str, Any]], on_conflict: str) -> None:
    if not records:
        return
    response = SESSION.post(
        sb_url(table),
        headers=sb_headers("resolution=merge-duplicates,return=minimal"),
        params={"on_conflict": on_conflict},
        data=json.dumps(records),
        timeout=60,
    )
    if not response.ok:
        raise RuntimeError(f"Supabase upsert failed for {table}: {response.status_code} {response.text}")


# This helper normalizes a timestamp into a minute-rounded UTC signature so we
# can compare Tracker rows against existing SmiteSource-backed history.
def normalize_timestamp(value: str) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        return parsed.replace(second=0, microsecond=0).isoformat()
    except ValueError:
        return value


# This helper builds a conservative dedupe signature shared across sources.
def match_signature(player: str, queue_type: str, started_at: str, god_name: str) -> str:
    return f"{player}|{queue_type.strip().lower()}|{normalize_timestamp(started_at)}|{god_name.strip().lower()}"


# This helper assigns a friendly party label from a counted party size.
def party_label(size: int) -> str:
    return {1: "Solo", 2: "Duo", 3: "Trio"}.get(size, f"Party {size}")


# This helper extracts a numeric stat value from one Tracker stat dictionary.
def tracker_stat_value(stats: dict[str, Any], key: str, default: int | float = 0) -> int | float:
    value = ((stats.get(key) or {}) if isinstance(stats, dict) else {}).get("value", default)
    return value if isinstance(value, (int, float)) else default


# This helper loads the exported Tracker JSON file produced by the HAR extractor.
def load_tracker_export(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    matches = payload.get("matches") if isinstance(payload, dict) else []
    return [match for match in matches if isinstance(match, dict)]


# This helper loads existing stored-history signatures for one player so the
# Tracker import can conservatively fill only missing history.
def existing_signatures_for_player(player: str) -> set[str]:
    rows = sb_select_all(
        "smitesource_match_history",
        {
            "select": "player,started_at,queue_type,god_name",
            "player": f"eq.{player}",
            "order": "started_at.desc",
        },
    )
    return {
        match_signature(
            str(row.get("player") or ""),
            str(row.get("queue_type") or ""),
            str(row.get("started_at") or ""),
            str(row.get("god_name") or ""),
        )
        for row in rows
        if row.get("started_at") and row.get("god_name")
    }


# This helper reshapes one exported Tracker match into the same stored-history
# row shape the app already uses for chemistry.
def normalize_tracker_match(player: str, tracker_id: str, match: dict[str, Any]) -> dict[str, Any] | None:
    attributes = match.get("attributes") if isinstance(match.get("attributes"), dict) else {}
    metadata = match.get("metadata") if isinstance(match.get("metadata"), dict) else {}
    segments = [segment for segment in (match.get("segments") or []) if isinstance(segment, dict)]
    if not segments:
        return None

    target_segment = next(
        (
            segment
            for segment in segments
            if str(((segment.get("attributes") or {}) if isinstance(segment.get("attributes"), dict) else {}).get("platformUserIdentifier") or "") == tracker_id
        ),
        None,
    )
    if not target_segment:
        return None

    team_buckets: dict[str, list[dict[str, Any]]] = {}
    for segment in segments:
        attributes = segment.get("attributes") if isinstance(segment.get("attributes"), dict) else {}
        segment_meta = segment.get("metadata") if isinstance(segment.get("metadata"), dict) else {}
        team_id = str(segment_meta.get("teamId") or "unknown")
        team_buckets.setdefault(team_id, []).append(
            {
                "hirezPlayerUuid": str(attributes.get("platformUserIdentifier") or ""),
                "displayName": str(segment_meta.get("platformUserHandle") or ""),
                "personDisplayName": str(segment_meta.get("platformUserHandle") or ""),
                "teamId": team_id,
                "godName": str(segment_meta.get("godName") or ""),
                "partyId": segment_meta.get("partyId"),
            }
        )

    ordered_team_ids = list(team_buckets.keys())
    team1 = team_buckets.get(ordered_team_ids[0], []) if ordered_team_ids else []
    team2 = team_buckets.get(ordered_team_ids[1], []) if len(ordered_team_ids) > 1 else []

    target_meta = target_segment.get("metadata") if isinstance(target_segment.get("metadata"), dict) else {}
    target_stats = target_segment.get("stats") if isinstance(target_segment.get("stats"), dict) else {}
    target_team_id = str(target_meta.get("teamId") or "unknown")
    target_party_id = target_meta.get("partyId")
    same_team_players = team_buckets.get(target_team_id, [])
    if target_party_id is None:
        resolved_party_size = 1
    else:
        resolved_party_size = max(1, sum(1 for row in same_team_players if row.get("partyId") == target_party_id))

    started_at = str(metadata.get("timestamp") or "")
    queue_type = str(metadata.get("gamemodeName") or match.get("gamemode") or "")
    god_name = str(target_meta.get("godName") or "")
    won = target_team_id == str(metadata.get("winningTeamId") or "")
    match_id = str(
        attributes.get("id")
        or match.get("id")
        or f"{started_at}|{queue_type}|{god_name}|{tracker_id}"
    )

    return {
        "record_key": f"{player}:tracker:{match_id}",
        "player": player,
        "profile_player_uuid": tracker_id,
        "hirez_player_uuid": tracker_id,
        "match_key": f"tracker:{match_id}:{tracker_id}",
        "match_id": match_id,
        "god_name": god_name,
        "queue_type": queue_type,
        "won": won,
        "party_size": resolved_party_size,
        "party_label": party_label(resolved_party_size),
        "team_id": None,
        "started_at": started_at or None,
        "raw_match": {
            "matchId": match_id,
            "queueType": queue_type,
            "gameMode": queue_type,
            "startTimestamp": started_at,
            "won": won,
            "teamId": target_team_id,
            "partyId": target_party_id,
            "partySize": resolved_party_size,
            "partyLabel": party_label(resolved_party_size),
            "hirezPlayerUuid": tracker_id,
            "displayName": str(target_meta.get("platformUserHandle") or player),
            "godName": god_name,
            "assignedRole": str(((target_meta.get("assignedRole") or {}) if isinstance(target_meta.get("assignedRole"), dict) else {}).get("name") or ""),
            "playedRole": str(((target_meta.get("playedRole") or {}) if isinstance(target_meta.get("playedRole"), dict) else {}).get("name") or ""),
            "kills": tracker_stat_value(target_stats, "kills", 0),
            "deaths": tracker_stat_value(target_stats, "deaths", 0),
            "assists": tracker_stat_value(target_stats, "assists", 0),
            "totalGoldEarned": tracker_stat_value(target_stats, "goldEarned", 0),
            "totalXp": tracker_stat_value(target_stats, "xpEarned", 0),
            "totalDamage": tracker_stat_value(target_stats, "damage", 0),
            "totalWardsPlaced": tracker_stat_value(target_stats, "wardsPlaced", 0),
            "playerDurationSeconds": tracker_stat_value(target_stats, "timePlayed", metadata.get("duration") or 0),
            "matchDurationSeconds": metadata.get("duration") or 0,
            "team1Players": team1,
            "team2Players": team2,
        },
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


# This helper runs the full import for one player and prints a compact summary.
def import_tracker_export(player: str, tracker_id: str, export_path: Path, dry_run: bool) -> dict[str, Any]:
    matches = load_tracker_export(export_path)
    normalized_rows = [row for row in (normalize_tracker_match(player, tracker_id, match) for match in matches) if row]
    deduped_rows: list[dict[str, Any]] = []
    seen_record_keys: set[str] = set()
    for row in normalized_rows:
        record_key = str(row.get("record_key") or "")
        if not record_key or record_key in seen_record_keys:
            continue
        seen_record_keys.add(record_key)
        deduped_rows.append(row)

    existing_signatures = existing_signatures_for_player(player)
    missing_rows = [
        row
        for row in deduped_rows
        if match_signature(
            player,
            str(row.get("queue_type") or ""),
            str(row.get("started_at") or ""),
            str(row.get("god_name") or ""),
        ) not in existing_signatures
    ]

    if missing_rows and not dry_run:
        sb_upsert("smitesource_match_history", missing_rows, "record_key")

    return {
        "player": player,
        "trackerId": tracker_id,
        "loaded": len(matches),
        "normalized": len(deduped_rows),
        "missing": len(missing_rows),
        "inserted": 0 if dry_run else len(missing_rows),
        "dryRun": dry_run,
    }


# This block parses command-line input so the importer can be run locally from
# PowerShell after extracting Tracker JSON from a HAR file.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import missing Tracker match history into Supabase.")
    parser.add_argument("--player", required=True, help="Council player label, for example Joey")
    parser.add_argument("--tracker-id", required=True, help="Tracker platform user identifier, for example a Steam ID")
    parser.add_argument("--export-file", required=True, help="Path to the exported Tracker JSON file")
    parser.add_argument("--dry-run", action="store_true", help="Compare only; do not write to Supabase")
    return parser.parse_args()


# This block runs the requested import and prints a JSON summary that is easy
# to paste back here if we need to debug the result.
def main() -> None:
    args = parse_args()
    summary = import_tracker_export(
        player=args.player,
        tracker_id=args.tracker_id,
        export_path=Path(args.export_file),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
