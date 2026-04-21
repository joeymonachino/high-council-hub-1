import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import app  # noqa: E402

SUPABASE_URL = app.get_secret("SUPABASE_URL")
SUPABASE_KEY = app.get_secret("SUPABASE_KEY")


def tracker_stat_value(stats: dict[str, Any], key: str, default: int = 0) -> int:
    if not isinstance(stats, dict):
        return default
    payload = stats.get(key) if isinstance(stats.get(key), dict) else {}
    value = payload.get("value", default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def party_label(size: int) -> str:
    if size <= 1:
        return "Solo"
    if size == 2:
        return "Duo"
    if size == 3:
        return "Trio"
    return f"Party {size}"


def normalize_team_id(value: Any) -> int | None:
    text_value = str(value or "").strip().lower()
    if not text_value:
        return None
    if text_value == "order":
        return 1
    if text_value == "chaos":
        return 2
    try:
        return int(text_value)
    except (TypeError, ValueError):
        return None


def canonical_signature(player: str, queue_type: str, started_at: str, god_name: str) -> str:
    return f"{player}|{app.normalize_queue_key(queue_type)}|{app.normalize_history_timestamp(started_at)}|{str(god_name or '').strip().lower()}"


def existing_signatures(player: str) -> set[str]:
    rows = app.sb_select_all(
        "smitesource_match_history",
        {
            "select": "player,queue_type,started_at,god_name",
            "player": f"eq.{player}",
            "order": "started_at.desc",
        },
    )
    return {
        canonical_signature(
            str(row.get("player") or ""),
            str(row.get("queue_type") or ""),
            str(row.get("started_at") or ""),
            str(row.get("god_name") or ""),
        )
        for row in rows
        if row.get("started_at") and row.get("god_name")
    }


def sb_upsert_verbose(table: str, rows: list[dict[str, Any]], on_conflict: str) -> None:
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    response = app.HTTP.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=headers,
        params={"on_conflict": on_conflict},
        json=rows,
        timeout=30,
    )
    if not response.ok:
        raise RuntimeError(f"Supabase upsert failed for {table}: {response.status_code} {response.text}")



def segment_matches_player(segment: dict[str, Any], player: str) -> bool:
    aliases = app.COUNCIL_PLAYER_ALIASES.get(player, {})
    names = {str(value).strip().lower() for value in (aliases.get("names") or []) if str(value).strip()}
    ids = {str(value).strip() for value in (aliases.get("ids") or []) if str(value).strip()}
    attrs = segment.get("attributes") if isinstance(segment.get("attributes"), dict) else {}
    meta = segment.get("metadata") if isinstance(segment.get("metadata"), dict) else {}
    identifier = str(attrs.get("platformUserIdentifier") or "").strip()
    handle = str(meta.get("platformUserHandle") or "").strip().lower()
    return (identifier and identifier in ids) or (handle and handle in names)


def normalize_segment_match(player: str, segment: dict[str, Any], match: dict[str, Any]) -> dict[str, Any] | None:
    attributes = match.get("attributes") if isinstance(match.get("attributes"), dict) else {}
    metadata = match.get("metadata") if isinstance(match.get("metadata"), dict) else {}
    segments = [item for item in (match.get("segments") or []) if isinstance(item, dict)]
    if not segments:
        return None

    target_segment = segment
    target_meta = target_segment.get("metadata") if isinstance(target_segment.get("metadata"), dict) else {}
    target_stats = target_segment.get("stats") if isinstance(target_segment.get("stats"), dict) else {}
    target_attrs = target_segment.get("attributes") if isinstance(target_segment.get("attributes"), dict) else {}
    target_identifier = str(target_attrs.get("platformUserIdentifier") or "").strip()
    target_team_id = str(target_meta.get("teamId") or "unknown")
    target_party_id = target_meta.get("partyId")

    team_buckets: dict[str, list[dict[str, Any]]] = {}
    for item in segments:
        item_attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
        item_meta = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        team_id = str(item_meta.get("teamId") or "unknown")
        team_buckets.setdefault(team_id, []).append(
            {
                "hirezPlayerUuid": str(item_attrs.get("platformUserIdentifier") or ""),
                "displayName": str(item_meta.get("platformUserHandle") or ""),
                "personDisplayName": str(item_meta.get("platformUserHandle") or ""),
                "teamId": team_id,
                "godName": str(item_meta.get("godName") or ""),
                "partyId": item_meta.get("partyId"),
            }
        )

    ordered_team_ids = list(team_buckets.keys())
    team1 = team_buckets.get(ordered_team_ids[0], []) if ordered_team_ids else []
    team2 = team_buckets.get(ordered_team_ids[1], []) if len(ordered_team_ids) > 1 else []
    same_team_players = team_buckets.get(target_team_id, [])
    if target_party_id is None:
        resolved_party_size = 1
    else:
        resolved_party_size = max(1, sum(1 for row in same_team_players if row.get("partyId") == target_party_id))

    started_at = str(metadata.get("timestamp") or "")
    queue_type = str(metadata.get("gamemodeName") or match.get("gamemode") or "")
    god_name = str(target_meta.get("godName") or "")
    won = target_team_id == str(metadata.get("winningTeamId") or "")
    match_id = str(attributes.get("id") or match.get("id") or f"{started_at}|{queue_type}|{god_name}|{target_identifier}")

    return {
        "record_key": f"{player}:tracker-fanout:{match_id}",
        "player": player,
        "profile_player_uuid": target_identifier,
        "hirez_player_uuid": target_identifier,
        "match_key": f"tracker-fanout:{match_id}:{target_identifier}",
        "match_id": match_id,
        "god_name": god_name,
        "queue_type": queue_type,
        "won": won,
        "party_size": resolved_party_size,
        "party_label": party_label(resolved_party_size),
        "team_id": normalize_team_id(target_team_id),
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
            "hirezPlayerUuid": target_identifier,
            "displayName": str(target_meta.get("platformUserHandle") or player),
            "godName": god_name,
            "assignedRole": str(((target_meta.get("assignedRole") or {}) if isinstance(target_meta.get("assignedRole"), dict) else {}).get("name") or ""),
            "playedRole": str(((target_meta.get("playedRole") or {}) if isinstance(target_meta.get("playedRole"), dict) else {}).get("name") or ""),
            "kills": tracker_stat_value(target_stats, "kills", 0),
            "deaths": tracker_stat_value(target_stats, "deaths", 0),
            "assists": tracker_stat_value(target_stats, "assists", 0),
            "playerDurationSeconds": tracker_stat_value(target_stats, "timePlayed", metadata.get("duration") or 0),
            "matchDurationSeconds": metadata.get("duration") or 0,
            "team1Players": team1,
            "team2Players": team2,
        },
        "synced_at": app.datetime.now(app.timezone.utc).isoformat(),
    }


def backfill(export_file: Path, targets: list[str], dry_run: bool) -> dict[str, Any]:
    data = json.loads(export_file.read_text(encoding="utf-8"))
    matches = [item for item in (data.get("matches") or []) if isinstance(item, dict)]
    summaries: list[dict[str, Any]] = []

    for player in targets:
        signatures = existing_signatures(player)
        rows: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        for match in matches:
            segments = [item for item in (match.get("segments") or []) if isinstance(item, dict)]
            for segment in segments:
                if not segment_matches_player(segment, player):
                    continue
                row = normalize_segment_match(player, segment, match)
                if not row:
                    continue
                signature = canonical_signature(
                    player,
                    str(row.get("queue_type") or ""),
                    str(row.get("started_at") or ""),
                    str(row.get("god_name") or ""),
                )
                record_key = str(row.get("record_key") or "")
                if signature in signatures or record_key in seen_keys:
                    continue
                seen_keys.add(record_key)
                rows.append(row)

        if rows and not dry_run:
            try:
                sb_upsert_verbose("smitesource_match_history", rows, "record_key")
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"Supabase fanout upsert failed for {player}: {exc}") from exc
        summaries.append({"player": player, "loaded": len(rows), "inserted": 0 if dry_run else len(rows), "dryRun": dry_run})

    return {"ok": True, "targets": targets, "results": summaries}


def main() -> None:
    parser = argparse.ArgumentParser(description="Fan out Joey Tracker export matches into council teammate rows.")
    parser.add_argument("--export-file", required=True)
    parser.add_argument("--players", nargs="*", default=["Darian", "Jami"])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    summary = backfill(Path(args.export_file), args.players, bool(args.dry_run))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
