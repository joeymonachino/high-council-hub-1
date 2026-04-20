from __future__ import annotations

# This block imports the standard-library tools used for file access, dates,
# lightweight caching, and configuration parsing.
import json
import math
import os
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import quote, urlparse

# This block imports the third-party modules used by the Flask backend and
# by the direct Supabase REST integration.
import requests
from flask import Flask, abort, jsonify, render_template, request, send_file


# This block defines project paths so the Flask app can run either as a
# self-contained app or as a local companion to the original Streamlit repo.
BASE_DIR = Path(__file__).resolve().parent
FALLBACK_SOURCE_REPO = Path(os.environ.get("SOURCE_REPO", r"C:\Users\joeym\Repos\high-council-hub-1"))
DATA_DIR = (BASE_DIR / "data") if (BASE_DIR / "data").exists() else (FALLBACK_SOURCE_REPO / "data")
ASSETS_DIR = (BASE_DIR / "assets") if (BASE_DIR / "assets").exists() else (FALLBACK_SOURCE_REPO / "assets")
GODS_ASSETS_DIR = ASSETS_DIR / "gods"
PANTHEONS_DIR = ASSETS_DIR / "pantheons"
LOCAL_SECRETS_PATH = BASE_DIR / "secrets" / "app_secrets.toml"
LOCAL_SECRETS_PATH_TXT = BASE_DIR / "secrets" / "app_secrets.toml.txt"
SECRETS_PATH = (
    LOCAL_SECRETS_PATH
    if LOCAL_SECRETS_PATH.exists()
    else (
        LOCAL_SECRETS_PATH_TXT
        if LOCAL_SECRETS_PATH_TXT.exists()
        else ((BASE_DIR / ".streamlit" / "secrets.toml") if (BASE_DIR / ".streamlit" / "secrets.toml").exists() else (FALLBACK_SOURCE_REPO / ".streamlit" / "secrets.toml"))
    )
)
LOCAL_ACTIVITY_LOG_PATH = BASE_DIR / "local_activity_log.json"
LOCAL_ACTIVITY_LOG_ENABLED = not bool(os.environ.get("VERCEL"))

# This block keeps the app's core configuration in one place so both the
# Python backend and the JavaScript frontend can share the same metadata.
PLAYERS = ["Joey", "Darian", "Jami", "Jamie", "Mike"]
PLAYER_ABBR = {
    "Joey": "Jo",
    "Darian": "Da",
    "Jami": "Ji",
    "Jamie": "Je",
    "Mike": "Mi",
}
COUNCIL_COLORS = {
    "Joey": "#d7a33d",
    "Darian": "#4c8dd8",
    "Jami": "#d46fa2",
    "Jamie": "#4ea885",
    "Mike": "#c44e5e",
}
TIER_THRESHOLDS = [
    (95, "SS"),
    (90, "S"),
    (80, "A"),
    (70, "B"),
    (60, "C"),
    (50, "D"),
    (1, "F"),
]
TIER_ORDER = ["SS", "S", "A", "B", "C", "D", "F", "U"]
TIER_COLORS = {
    "SS": "#efd28e",
    "S": "#d7aa58",
    "A": "#bcc4d8",
    "B": "#9fb5ce",
    "C": "#97bea2",
    "D": "#b09d82",
    "F": "#b06a58",
    "U": "#8d877d",
}
HOT_TAKE_THRESHOLD = 30
SMITESOURCE_PROFILE_LINKS = {
    "Joey": "https://smitesource.com/player/f29ca789-74f0-442f-937a-f72fcba045d3",
    "Darian": "https://smitesource.com/player/8005a240-cd89-4f14-bc40-db769319cb43",
    "Jami": "https://smitesource.com/player/8f5f48ca-10d1-4104-ab5d-bb80d4683313",
    "Jamie": "",
    "Mike": "https://smitesource.com/player/f09127e9-676e-498e-b09e-6e20924a91f5",
}
SMITESOURCE_RPC_BASE = "https://smitesource.com/rpc"
SMITESOURCE_CACHE_TTL_SECONDS = int(os.environ.get("SMITESOURCE_CACHE_TTL_SECONDS", "1800"))
SMITESOURCE_MATCH_PAGE_SIZE = int(os.environ.get("SMITESOURCE_MATCH_PAGE_SIZE", "20"))
SMITESOURCE_MATCH_SAMPLE_SIZE = int(os.environ.get("SMITESOURCE_MATCH_SAMPLE_SIZE", "200"))


# This block creates the Flask application object that owns the routes and
# template/static configuration for the whole custom app.
app = Flask(__name__)


# This block stores a tiny in-memory cache for asset lookup maps so the app
# doesn't rescan the same image folders on every request.
ASSET_INDEX_CACHE: dict[str, dict[str, Path]] = {}
SMITESOURCE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
RATER_STATS_CACHE: tuple[float, dict[str, dict[str, Any]]] | None = None


# This helper loads secrets from the old Streamlit file so the Flask version
# can keep using the same Supabase URL, API key, and player PINs.
def load_streamlit_secrets() -> dict[str, Any]:
    if not SECRETS_PATH.exists():
        return {}
    with SECRETS_PATH.open("rb") as handle:
        return tomllib.load(handle)


# This helper reads a config value from environment variables first and then
# falls back to the copied Streamlit secrets, which keeps local setup simple.
def get_secret(name: str, default: str = "") -> str:
    secrets = load_streamlit_secrets()
    return os.environ.get(name, secrets.get(name, default))


# This helper returns the shared Supabase REST headers used for all reads and
# writes so the request code stays consistent.
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


# This helper builds a REST endpoint URL for a Supabase table name.
def sb_url(table: str) -> str:
    base_url = get_secret("SUPABASE_URL")
    return f"{base_url}/rest/v1/{table}"


# This helper performs a generic Supabase GET query and returns JSON rows.
def sb_select(table: str, params: dict[str, str] | None = None) -> list[dict]:
    response = requests.get(
        sb_url(table),
        headers=sb_headers("return=representation"),
        params=params or {"select": "*"},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


# This helper pages through a Supabase table so we can read larger history sets
# without being limited to one REST page.
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


# This helper performs a generic Supabase UPSERT so we can preserve the same
# merge-duplicate behavior that the Streamlit app used.
def sb_upsert(table: str, records: list[dict], on_conflict: str) -> None:
    if not records:
        return
    params = {"on_conflict": on_conflict} if on_conflict else {}
    response = requests.post(
        sb_url(table),
        headers=sb_headers("resolution=merge-duplicates,return=minimal"),
        params=params,
        data=json.dumps(records),
        timeout=20,
    )
    response.raise_for_status()


# This helper inserts append-only history rows into Supabase.
def sb_insert(table: str, records: list[dict]) -> None:
    if not records:
        return
    response = requests.post(
        sb_url(table),
        headers=sb_headers("return=minimal"),
        data=json.dumps(records),
        timeout=20,
    )
    if not response.ok:
        raise RuntimeError(f"Supabase insert failed for {table}: {response.status_code} {response.text}")


# This helper deletes a player's old personal rankings before rewriting the
# current ordering, which avoids leaving stale rows behind.
def sb_delete_player_rankings(player: str) -> None:
    response = requests.delete(
        sb_url("personal_rankings"),
        headers=sb_headers("return=minimal"),
        params={"player": f"eq.{player}"},
        timeout=20,
    )
    response.raise_for_status()


# This helper loads a local JSON snapshot from the old repo, which gives the
# app a graceful read fallback if Supabase can't be reached during testing.
def load_json_snapshot(name: str) -> list[dict]:
    snapshot_path = DATA_DIR / name
    if not snapshot_path.exists():
        return []
    with snapshot_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


# This helper extracts the SmiteSource player UUID from a linked profile URL so
# the backend can call the site's RPC endpoints without hardcoding IDs twice.
def smitesource_player_uuid(profile_url: str) -> str:
    if not profile_url:
        return ""
    parsed = urlparse(profile_url)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 2 and parts[-2] == "player":
        return parts[-1]
    return ""


# This helper turns mixed SmiteSource values into short strings that are safe
# to show in the UI, even when the API returns nested objects.
def smitesource_summary(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        parts = [smitesource_summary(item) for item in value]
        return ", ".join(part for part in parts if part)
    if isinstance(value, dict):
        for key in ("summary", "label", "title", "description", "text", "value", "rankName", "tierName", "name"):
            text = smitesource_summary(value.get(key))
            if text:
                return text
        fragments: list[str] = []
        for key in ("tier", "division", "mmr", "points", "queueType", "mode"):
            text = smitesource_summary(value.get(key))
            if text:
                fragments.append(text)
        return " • ".join(fragments)
    return str(value)


# This helper safely rounds one numeric value from SmiteSource into a frontend-
# friendly float or integer and falls back to None when data is absent.
def smitesource_number(value: Any, digits: int = 0) -> int | float | None:
    if not isinstance(value, (int, float)):
        return None
    return round(float(value), digits) if digits else int(round(float(value)))


# This helper points a SmiteSource god row back at local art when the same god
# image already exists in the project assets.
def smitesource_god_image_url(god_name: str) -> str:
    if not god_name:
        return ""
    return f"/god-image/{quote(god_name)}" if resolve_god_image(god_name) else ""


# This helper performs one SmiteSource RPC POST in the same wrapped format the
# live site expects for overview and recent-match data.
def smitesource_post(endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(
        f"{SMITESOURCE_RPC_BASE}/{endpoint}",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "HighCouncilHub/1.0",
        },
        json={"json": payload},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if isinstance(data, dict) and isinstance(data.get("json"), dict):
        return data["json"]
    return data if isinstance(data, dict) else {}


# This helper builds a stable key for one SmiteSource match row so we can dedupe
# paginated responses and safely upsert them into Supabase.
def smitesource_match_key(row: dict[str, Any]) -> str:
    return str(
        row.get("matchId")
        or row.get("matchUuid")
        or f"{row.get('startTimestamp')}|{row.get('queueType') or row.get('gameMode')}|{row.get('godName')}|{row.get('hirezPlayerUuid')}"
    )


# This helper walks SmiteSource match pages for one linked player. When
# `target_count` is None, it keeps going until history runs out or until it hits
# already-synced keys from Supabase.
def fetch_smitesource_match_rows(player_uuid: str, target_count: int | None = None, stop_keys: set[str] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_match_keys: set[str] = set()
    stop_keys = stop_keys or set()
    page_size = max(1, SMITESOURCE_MATCH_PAGE_SIZE)
    max_pages = max(1, math.ceil(target_count / page_size)) if target_count else None
    page_number = 1

    while True:
        matches_payload = smitesource_post(
            "matches/getPlayerMatches",
            {
                "playerUuid": player_uuid,
                "mode": "all",
                "season": "0",
                "page": page_number,
                "pageSize": page_size,
                "includeTeamDetails": True,
            },
        )
        page_rows = [row for row in (matches_payload.get("matches") or []) if isinstance(row, dict)]
        if not page_rows:
            break

        should_stop = False
        for row in page_rows:
            match_key = smitesource_match_key(row)
            if match_key in seen_match_keys:
                continue
            if stop_keys and match_key in stop_keys:
                should_stop = True
                continue

            seen_match_keys.add(match_key)
            rows.append(row)
            if target_count and len(rows) >= target_count:
                should_stop = True
                break

        if should_stop or len(page_rows) < page_size:
            break
        if max_pages and page_number >= max_pages:
            break
        page_number += 1

    return rows


# This helper reshapes one SmiteSource match row into a Supabase-friendly record
# with both indexed fields and the original JSON payload preserved.
def normalize_smitesource_history_record(player: str, profile_player_uuid: str, row: dict[str, Any]) -> dict[str, Any]:
    match_key = smitesource_match_key(row)
    return {
        "record_key": f"{player}:{match_key}",
        "player": player,
        "profile_player_uuid": profile_player_uuid,
        "hirez_player_uuid": str(row.get("hirezPlayerUuid") or ""),
        "match_key": match_key,
        "match_id": str(row.get("matchId") or row.get("matchUuid") or ""),
        "god_name": str(row.get("godName") or ""),
        "queue_type": str(row.get("queueType") or row.get("gameMode") or ""),
        "won": bool(row.get("won")),
        "party_size": int(row.get("partySize") or 0) if str(row.get("partySize") or "").strip() else None,
        "party_label": str(row.get("partyLabel") or ""),
        "team_id": int(row.get("teamId") or 0) if str(row.get("teamId") or "").strip() else None,
        "started_at": row.get("startTimestamp") or None,
        "raw_match": row,
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


# This helper loads one player's stored SmiteSource history from Supabase. It
# powers full-history chemistry once the backfill has been run.
def load_stored_match_history(player: str) -> list[dict[str, Any]]:
    rows = sb_select_all(
        "smitesource_match_history",
        {
            "select": "record_key,player,profile_player_uuid,hirez_player_uuid,match_key,started_at,raw_match",
            "player": f"eq.{player}",
            "order": "started_at.desc",
        },
    )
    return [row for row in rows if isinstance(row, dict)]


# This helper backfills one player's full SmiteSource history into Supabase and
# returns a compact sync summary for the API route.
def sync_smitesource_history_for_player(player: str, profile_url: str) -> dict[str, Any]:
    player_uuid = smitesource_player_uuid(profile_url)
    if not profile_url or not player_uuid:
        return {"player": player, "linked": False, "inserted": 0, "stored": 0}

    try:
        stored_rows = load_stored_match_history(player)
    except Exception:
        stored_rows = []

    existing_match_keys = {
        str(row.get("match_key") or "")
        for row in stored_rows
        if str(row.get("match_key") or "")
    }
    fetched_rows = fetch_smitesource_match_rows(player_uuid, target_count=None, stop_keys=existing_match_keys)
    records = [normalize_smitesource_history_record(player, player_uuid, row) for row in fetched_rows]
    if records:
        sb_upsert("smitesource_match_history", records, "record_key")

    return {
        "player": player,
        "linked": True,
        "inserted": len(records),
        "stored": len(stored_rows) + len(records),
        "stoppedOnExisting": bool(existing_match_keys),
    }


# This helper normalizes one top-god row from SmiteSource so the frontend can
# render the same field names for every linked council member.
def normalize_smitesource_top_god(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": row.get("godName") or row.get("godSlug") or "",
        "title": row.get("godTitle") or "",
        "pantheon": row.get("godPantheon") or "",
        "role": row.get("godType") or "",
        "damageType": row.get("godPrimaryDamageType") or "",
        "gamesPlayed": smitesource_number(row.get("gamesPlayed")),
        "wins": smitesource_number(row.get("wins")),
        "winRate": smitesource_number((row.get("winRate") or 0) * 100, 1) if isinstance(row.get("winRate"), (int, float)) else None,
        "kdRatio": smitesource_number(row.get("kdRatio"), 2),
        "kdaRatio": smitesource_number(row.get("kdaRatio"), 2),
        "damagePerMin": smitesource_number(row.get("damagePerMin"), 0),
        "goldPerMin": smitesource_number(row.get("goldPerMin"), 0),
        "xpPerMin": smitesource_number(row.get("xpPerMin"), 0),
        "imageUrl": smitesource_god_image_url(str(row.get("godName") or "")),
    }


# This helper normalizes one role-performance row from SmiteSource into the
# compact structure used by the Stats tab.
def normalize_smitesource_role(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "role": row.get("role") or "Unknown",
        "gamesPlayed": smitesource_number(row.get("gamesPlayed")),
        "wins": smitesource_number(row.get("wins")),
        "winRate": smitesource_number((row.get("winRate") or 0) * 100, 1) if isinstance(row.get("winRate"), (int, float)) else None,
        "kdRatio": smitesource_number(row.get("kdRatio"), 2),
        "kdaRatio": smitesource_number(row.get("kdaRatio"), 2),
        "damagePerMin": smitesource_number(row.get("damagePerMin"), 0),
        "goldPerMin": smitesource_number(row.get("goldPerMin"), 0),
        "xpPerMin": smitesource_number(row.get("xpPerMin"), 0),
    }


# This helper converts one recent SmiteSource match into a smaller record that
# is easy for the frontend to display in compact dashboard rows.
def normalize_smitesource_match(row: dict[str, Any]) -> dict[str, Any]:
    duration_seconds = row.get("playerDurationSeconds") or row.get("matchDurationSeconds") or 0
    return {
        "matchId": row.get("matchId"),
        "godName": row.get("godName") or "",
        "role": row.get("playedRole") or row.get("assignedRole") or "Unknown",
        "queueType": row.get("queueType") or row.get("gameMode") or "",
        "won": bool(row.get("won")),
        "kills": smitesource_number(row.get("kills")),
        "deaths": smitesource_number(row.get("deaths")),
        "assists": smitesource_number(row.get("assists")),
        "damage": smitesource_number(row.get("totalDamage")),
        "gold": smitesource_number(row.get("totalGoldEarned")),
        "wards": smitesource_number(row.get("totalWardsPlaced")),
        "durationMinutes": smitesource_number(duration_seconds / 60, 0) if duration_seconds else None,
        "startedAt": row.get("startTimestamp") or "",
        "imageUrl": smitesource_god_image_url(str(row.get("godName") or "")),
    }


# This helper inspects a SmiteSource match row and returns the named council
# teammates who were on the same team as the current player.
def council_teammates_in_match(player: str, player_hirez_uuid: str, identity_map: dict[str, dict[str, str]], row: dict[str, Any]) -> list[str]:
    if not player_hirez_uuid:
        return []

    target_team_id = row.get("teamId")
    team_arrays = [row.get("team1Players") or [], row.get("team2Players") or []]
    teammates: list[str] = []

    for team in team_arrays:
        if not isinstance(team, list):
            continue
        for teammate in team:
            if not isinstance(teammate, dict):
                continue
            teammate_uuid = teammate.get("hirezPlayerUuid")
            teammate_team_id = teammate.get("teamId")
            teammate_display = str(teammate.get("displayName") or teammate.get("personDisplayName") or "").strip().lower()
            if teammate_team_id != target_team_id or teammate_uuid == player_hirez_uuid:
                continue
            for council_player, identity in identity_map.items():
                identity_uuid = identity.get("hirezPlayerUuid", "")
                identity_name = identity.get("displayName", "").strip().lower()
                if council_player != player and (
                    (identity_uuid and identity_uuid == teammate_uuid)
                    or (identity_name and identity_name == teammate_display)
                ) and council_player not in teammates:
                    teammates.append(council_player)

    teammates.sort(key=PLAYERS.index)
    return teammates


# This helper aggregates party, duo, trio, queue, and recent shared-session
# chemistry records from a larger recent match sample.
def build_council_chemistry(player: str, player_hirez_uuid: str, identity_map: dict[str, dict[str, str]], match_rows: list[dict[str, Any]]) -> dict[str, Any]:
    pair_records: dict[str, dict[str, Any]] = {}
    duo_only_records: dict[str, dict[str, Any]] = {}
    party_size_records: dict[str, dict[str, Any]] = {}
    queue_records: dict[str, dict[str, Any]] = {}
    shared_group_records: dict[str, dict[str, Any]] = {}
    duo_god_records: dict[str, dict[str, Any]] = {}
    group_god_records: dict[str, dict[str, Any]] = {}
    recent_sessions: list[dict[str, Any]] = []
    overall_wins = 0
    overall_losses = 0

    for row in match_rows:
        if not isinstance(row, dict):
            continue

        teammates = council_teammates_in_match(player, player_hirez_uuid, identity_map, row)
        if not teammates:
            continue

        won = bool(row.get("won"))
        if won:
            overall_wins += 1
        else:
            overall_losses += 1

        # This block builds per-council-member records for duo chemistry and
        # most-played-with summaries.
        for teammate in teammates:
            record = pair_records.setdefault(teammate, {"player": teammate, "games": 0, "wins": 0, "losses": 0})
            record["games"] += 1
            record["wins"] += 1 if won else 0
            record["losses"] += 0 if won else 1

        # This block separately tracks true duo sessions so "Best Duo" only
        # reflects games where exactly two council members queued together.
        if len(teammates) == 1:
            teammate = teammates[0]
            duo_record = duo_only_records.setdefault(teammate, {"player": teammate, "games": 0, "wins": 0, "losses": 0})
            duo_record["games"] += 1
            duo_record["wins"] += 1 if won else 0
            duo_record["losses"] += 0 if won else 1

        # This block groups shared matches by party size label so we can show
        # solo/duo/trio style council records.
        party_label = str(row.get("partyLabel") or f"Party {int(row.get('partySize') or 0)}").strip()
        party_record = party_size_records.setdefault(party_label, {"label": party_label, "games": 0, "wins": 0, "losses": 0})
        party_record["games"] += 1
        party_record["wins"] += 1 if won else 0
        party_record["losses"] += 0 if won else 1

        # This block tracks queue-specific chemistry so the UI can call out
        # council performance in Joust, Arena, and similar queues.
        queue_label = str(row.get("queueType") or row.get("gameMode") or "Unknown Queue").replace("_", " ").title()
        queue_record = queue_records.setdefault(queue_label, {"label": queue_label, "games": 0, "wins": 0, "losses": 0})
        queue_record["games"] += 1
        queue_record["wins"] += 1 if won else 0
        queue_record["losses"] += 0 if won else 1

        # This block keeps group-composition records like "Jami + Mike" so we
        # can surface favorite duos and trios from one player's perspective.
        group_key_members = sorted([player] + teammates)
        group_key = " + ".join(group_key_members)
        group_record = shared_group_records.setdefault(
            group_key,
            {"label": group_key, "members": group_key_members, "games": 0, "wins": 0, "losses": 0},
        )
        group_record["games"] += 1
        group_record["wins"] += 1 if won else 0
        group_record["losses"] += 0 if won else 1

        # This block looks for signature god pairings in true duo sessions,
        # which creates a fun "best god combo" receipt for the council.
        if len(teammates) == 1:
            teammate = teammates[0]
            god_name = str(row.get("godName") or "")
            teammate_god = ""
            teammate_display_name = ""
            for team in [row.get("team1Players") or [], row.get("team2Players") or []]:
                if not isinstance(team, list):
                    continue
                for teammate_row in team:
                    if not isinstance(teammate_row, dict):
                        continue
                    teammate_uuid = teammate_row.get("hirezPlayerUuid")
                    if teammate_uuid == identity_map.get(teammate, {}).get("hirezPlayerUuid"):
                        teammate_god = str(teammate_row.get("godName") or "")
                        teammate_display_name = str(teammate_row.get("displayName") or teammate_row.get("personDisplayName") or "")
                        break
                if teammate_god:
                    break

            combo_key = f"{teammate}|{god_name}|{teammate_god}"
            duo_combo = duo_god_records.setdefault(
                combo_key,
                {
                    "teammate": teammate,
                    "teammateDisplayName": teammate_display_name,
                    "playerGod": god_name,
                    "teammateGod": teammate_god,
                    "games": 0,
                    "wins": 0,
                    "losses": 0,
                },
            )
            duo_combo["games"] += 1
            duo_combo["wins"] += 1 if won else 0
            duo_combo["losses"] += 0 if won else 1

        # This block stores the most recent shared council sessions so the tab
        # can show concrete recent duo/trio examples instead of only aggregates.
        participant_gods: dict[str, str] = {player: str(row.get("godName") or "")}
        for teammate in teammates:
            teammate_god = ""
            for team in [row.get("team1Players") or [], row.get("team2Players") or []]:
                if not isinstance(team, list):
                    continue
                for teammate_row in team:
                    if not isinstance(teammate_row, dict):
                        continue
                    teammate_uuid = teammate_row.get("hirezPlayerUuid")
                    if teammate_uuid == identity_map.get(teammate, {}).get("hirezPlayerUuid"):
                        teammate_god = str(teammate_row.get("godName") or "")
                        break
                if teammate_god:
                    break
            participant_gods[teammate] = teammate_god

        # This block tracks the full shared god comp for any council session so
        # the frontend can surface true winning/losing duo and trio receipts
        # with clear "who played what" context.
        comp_members = sorted([player] + teammates)
        ordered_assignments = [(member, participant_gods.get(member, "")) for member in comp_members]
        if len(comp_members) >= 2 and all(god_name for _, god_name in ordered_assignments):
            god_label = " + ".join(god_name for _, god_name in ordered_assignments)
            comp_key = f"{'|'.join(comp_members)}|{god_label}"
            group_god_record = group_god_records.setdefault(
                comp_key,
                {
                    "label": god_label,
                    "members": comp_members,
                    "participantGods": {member: god_name for member, god_name in ordered_assignments},
                    "games": 0,
                    "wins": 0,
                    "losses": 0,
                },
            )
            group_god_record["games"] += 1
            group_god_record["wins"] += 1 if won else 0
            group_god_record["losses"] += 0 if won else 1

        recent_sessions.append(
            {
                "godName": row.get("godName") or "",
                "queueType": queue_label,
                "won": won,
                "participants": [player] + teammates,
                "participantGods": participant_gods,
                "partyLabel": party_label,
                "startedAt": row.get("startTimestamp") or "",
                "kda": f"{int(row.get('kills') or 0)}/{int(row.get('deaths') or 0)}/{int(row.get('assists') or 0)}",
            }
        )

    # This helper finishes one record with a display win rate.
    def finish_record(record: dict[str, Any]) -> dict[str, Any]:
        games = int(record.get("games") or 0)
        wins = int(record.get("wins") or 0)
        record["winRate"] = round((wins / games) * 100, 1) if games else 0.0
        return record

    pair_list = [finish_record(record) for record in pair_records.values()]
    pair_list.sort(key=lambda item: (-item["games"], -item["winRate"], item["player"]))

    duo_only_list = [finish_record(record) for record in duo_only_records.values()]
    duo_only_list.sort(key=lambda item: (-item["games"], -item["winRate"], item["player"]))

    party_list = [finish_record(record) for record in party_size_records.values()]
    party_list.sort(key=lambda item: (-item["games"], item["label"]))

    queue_list = [finish_record(record) for record in queue_records.values()]
    queue_list.sort(key=lambda item: (-item["games"], -item["winRate"], item["label"]))

    shared_groups = [finish_record(record) for record in shared_group_records.values()]
    shared_groups.sort(key=lambda item: (-item["games"], -item["winRate"], item["label"]))

    duo_combos = [finish_record(record) for record in duo_god_records.values() if record.get("playerGod") and record.get("teammateGod")]
    duo_combos.sort(key=lambda item: (-item["games"], -item["winRate"], item["teammate"], item["playerGod"]))

    group_god_list = [finish_record(record) for record in group_god_records.values() if len(record.get("members") or []) >= 2]
    group_god_list.sort(key=lambda item: (-item["games"], -item["winRate"], item["label"]))

    recent_sessions.sort(key=lambda item: item.get("startedAt", ""), reverse=True)

    most_played_with = pair_list[0] if pair_list else None
    best_duo = next((record for record in duo_only_list if record["games"] >= 2), duo_only_list[0] if duo_only_list else None)
    best_group = next((record for record in shared_groups if record["games"] >= 2), shared_groups[0] if shared_groups else None)

    return {
        "overall": finish_record({"games": overall_wins + overall_losses, "wins": overall_wins, "losses": overall_losses}),
        "pairRecords": pair_list,
        "duoOnlyRecords": duo_only_list,
        "partySizeRecords": party_list,
        "queueRecords": queue_list,
        "sharedGroups": shared_groups,
        "duoCombos": duo_combos[:5],
        "groupGodRecords": group_god_list,
        "recentSessions": recent_sessions[:10],
        "mostPlayedWith": most_played_with,
        "bestDuo": best_duo,
        "bestGroup": best_group,
    }


# This helper collapses the full SmiteSource overview and matches payloads into
# one predictable profile object for each linked rater.
def build_smitesource_profile(player: str, profile_url: str) -> dict[str, Any]:
    player_uuid = smitesource_player_uuid(profile_url)
    cached = SMITESOURCE_CACHE.get(player)
    if not profile_url or not player_uuid:
        return {
            "player": player,
            "linked": False,
            "available": False,
            "profileUrl": profile_url,
            "playerUuid": player_uuid,
            "displayName": player,
            "error": "",
            "metrics": {},
            "topGods": [],
            "topRoles": [],
            "recentMatches": [],
            "chemistry": {},
            "selfHirezPlayerUuid": "",
            "insights": {},
            "rankSummary": "",
            "peakRankSummary": "",
        }

    if cached and (time.time() - cached[0]) < SMITESOURCE_CACHE_TTL_SECONDS:
        return cached[1]

    try:
        # This block prefers durable Supabase-backed match history when it has
        # been synced already, which gives chemistry access to full history
        # while still allowing the app to fall back to live recent samples.
        stored_history_rows: list[dict[str, Any]] = []
        stored_raw_match_rows: list[dict[str, Any]] = []
        try:
            stored_history_rows = load_stored_match_history(player)
            stored_raw_match_rows = [
                row.get("raw_match")
                for row in stored_history_rows
                if isinstance(row.get("raw_match"), dict)
            ]
        except Exception:
            stored_history_rows = []
            stored_raw_match_rows = []

        # This block fetches the broader player snapshot used for summary
        # metrics, top gods, top roles, and rank context.
        overview = smitesource_post(
            "matches/getPlayerOverview",
            {"playerUuid": player_uuid, "mode": "all", "season": "0"},
        )

        raw_match_rows = (
            stored_raw_match_rows
            if stored_raw_match_rows
            else fetch_smitesource_match_rows(
                player_uuid,
                target_count=max(SMITESOURCE_MATCH_SAMPLE_SIZE, SMITESOURCE_MATCH_PAGE_SIZE),
            )
        )

        totals = overview.get("totals") if isinstance(overview.get("totals"), dict) else {}
        top_gods = [
            normalize_smitesource_top_god(row)
            for row in (overview.get("topGods") or [])
            if isinstance(row, dict)
        ][:5]
        top_roles = [
            normalize_smitesource_role(row)
            for row in (overview.get("topRoles") or [])
            if isinstance(row, dict)
        ][:4]
        recent_matches = [
            normalize_smitesource_match(row)
            for row in raw_match_rows
        ][:5]
        self_hirez_uuid = next((str(row.get("hirezPlayerUuid") or "") for row in raw_match_rows if row.get("hirezPlayerUuid")), "")

        profile = {
            "player": player,
            "linked": True,
            "available": bool(totals or top_gods or top_roles or recent_matches),
            "profileUrl": profile_url,
            "playerUuid": player_uuid,
            "displayName": overview.get("displayName") or player,
            "metrics": {
                "matches": smitesource_number(totals.get("totalMatches")),
                "wins": smitesource_number(totals.get("wins")),
                "losses": smitesource_number(totals.get("losses")),
                "winRate": smitesource_number((totals.get("winRate") or 0) * 100, 1) if isinstance(totals.get("winRate"), (int, float)) else None,
                "kdRatio": smitesource_number(totals.get("kdRatio"), 2),
                "kdaRatio": smitesource_number(totals.get("kdaRatio"), 2),
                "damagePerMin": smitesource_number(totals.get("damagePerMin"), 0),
                "goldPerMin": smitesource_number(totals.get("goldPerMin"), 0),
                "xpPerMin": smitesource_number(totals.get("xpPerMin"), 0),
                "wardsPerMatch": smitesource_number(totals.get("wardsPerMatch"), 1),
                "hoursPlayed": smitesource_number((totals.get("totalDurationSeconds") or 0) / 3600, 1) if totals.get("totalDurationSeconds") else None,
            },
            "topGods": top_gods,
            "topRoles": top_roles,
            "recentMatches": recent_matches,
            "chemistry": {},
            "selfHirezPlayerUuid": self_hirez_uuid,
            "_rawMatchRows": raw_match_rows,
            "insights": {
                "recentForm": smitesource_summary((overview.get("insights") or {}).get("recentForm")),
                "damageProfile": smitesource_summary((overview.get("insights") or {}).get("damageProfile")),
                "economyProfile": smitesource_summary((overview.get("insights") or {}).get("economyProfile")),
                "buildDna": smitesource_summary((overview.get("insights") or {}).get("buildDna")),
                "srMomentum": smitesource_summary((overview.get("insights") or {}).get("srMomentum")),
            },
            "rankSummary": smitesource_summary(overview.get("currentRank")),
            "peakRankSummary": smitesource_summary(overview.get("peakRank")),
            "error": "",
            "historySource": "supabase" if stored_raw_match_rows else "live-sample",
        }
    except Exception as exc:  # noqa: BLE001
        if cached and cached[1].get("available"):
            return cached[1]
        self_hirez_uuid = next((str(row.get("hirezPlayerUuid") or "") for row in stored_raw_match_rows if row.get("hirezPlayerUuid")), "")
        profile = {
            "player": player,
            "linked": True,
            "available": bool(stored_raw_match_rows),
            "profileUrl": profile_url,
            "playerUuid": player_uuid,
            "displayName": player,
            "error": str(exc),
            "metrics": {},
            "topGods": [],
            "topRoles": [],
            "recentMatches": [normalize_smitesource_match(row) for row in stored_raw_match_rows[:5]],
            "chemistry": {},
            "selfHirezPlayerUuid": self_hirez_uuid,
            "_rawMatchRows": stored_raw_match_rows,
            "insights": {},
            "rankSummary": "",
            "peakRankSummary": "",
            "historySource": "supabase" if stored_raw_match_rows else "unavailable",
        }

    SMITESOURCE_CACHE[player] = (time.time(), profile)
    return profile


# This helper loads the whole council's SmiteSource profiles in one pass so the
# frontend can populate the stats tab from a single API request.
def load_rater_stats() -> dict[str, dict[str, Any]]:
    global RATER_STATS_CACHE

    if RATER_STATS_CACHE and (time.time() - RATER_STATS_CACHE[0]) < SMITESOURCE_CACHE_TTL_SECONDS:
        return RATER_STATS_CACHE[1]

    profiles = {
        player: build_smitesource_profile(player, SMITESOURCE_PROFILE_LINKS.get(player, ""))
        for player in PLAYERS
    }

    identity_map = {
        player: {
            "displayName": str(profile.get("displayName") or ""),
            "hirezPlayerUuid": str(profile.get("selfHirezPlayerUuid") or ""),
        }
        for player, profile in profiles.items()
        if profile.get("linked")
    }

    for player, profile in profiles.items():
        raw_match_rows = profile.pop("_rawMatchRows", [])
        profile["chemistry"] = build_council_chemistry(
            player,
            str(profile.get("selfHirezPlayerUuid") or ""),
            identity_map,
            raw_match_rows if isinstance(raw_match_rows, list) else [],
        )
        profile.pop("selfHirezPlayerUuid", None)

    available_count = sum(1 for profile in profiles.values() if profile.get("available"))
    if available_count >= 2:
        RATER_STATS_CACHE = (time.time(), profiles)
        return profiles

    if RATER_STATS_CACHE:
        return RATER_STATS_CACHE[1]

    return profiles


# This helper loads the local activity fallback log so recent changes can still
# appear in the Activity tab when Supabase history reads are unavailable.
def load_local_activity_log() -> list[dict]:
    if not LOCAL_ACTIVITY_LOG_ENABLED:
        return []
    if not LOCAL_ACTIVITY_LOG_PATH.exists():
        return []
    try:
        with LOCAL_ACTIVITY_LOG_PATH.open("r", encoding="utf-8") as handle:
            rows = json.load(handle)
        return rows if isinstance(rows, list) else []
    except Exception:  # noqa: BLE001
        return []


# This helper appends recent change rows to the local fallback log and trims it
# so it stays small and fast to read.
def append_local_activity_log(records: list[dict]) -> None:
    if not LOCAL_ACTIVITY_LOG_ENABLED:
        return
    if not records:
        return

    try:
        existing = load_local_activity_log()
        combined = records + existing
        deduped: list[dict] = []
        seen: set[tuple] = set()

        for row in combined:
            key = (
                row.get("player"),
                row.get("god_name"),
                row.get("old_value"),
                row.get("new_value"),
                row.get("change_type", "rating"),
                row.get("changed_at"),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)

        deduped.sort(key=lambda row: row.get("changed_at", ""), reverse=True)
        with LOCAL_ACTIVITY_LOG_PATH.open("w", encoding="utf-8") as handle:
            json.dump(deduped[:400], handle, indent=2)
    except OSError:
        # This block intentionally ignores read-only filesystem errors in
        # serverless production, where Supabase is the only durable history store.
        return


# This helper merges Supabase history rows with the local fallback log so the
# activity feed can stay populated and deduplicated.
def merge_history_rows(remote_rows: list[dict], local_rows: list[dict]) -> list[dict]:
    combined = list(remote_rows) + list(local_rows)
    merged: list[dict] = []
    seen: set[tuple] = set()

    for row in combined:
        key = (
            row.get("player"),
            row.get("god_name"),
            row.get("old_value"),
            row.get("new_value"),
            row.get("change_type", "rating"),
            row.get("changed_at"),
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)

    merged.sort(key=lambda row: row.get("changed_at", ""), reverse=True)
    return merged


# This helper normalizes a god or pantheon name into several likely filename
# variants so image lookup can survive spacing/casing differences.
def normalize_name_variants(name: str) -> list[str]:
    compact = name.replace(" ", "")
    underscored = name.replace(" ", "_")
    dehyphenated = compact.replace("-", "")
    return [
        name,
        compact,
        underscored,
        dehyphenated,
        name.lower(),
        compact.lower(),
        underscored.lower(),
        dehyphenated.lower(),
    ]


# This helper scans an asset directory once and caches a lowercase filename
# map so later requests can resolve images quickly.
def build_asset_index(kind: str, directory: Path) -> dict[str, Path]:
    if kind in ASSET_INDEX_CACHE:
        return ASSET_INDEX_CACHE[kind]

    asset_map: dict[str, Path] = {}
    if directory.exists():
        for asset in directory.iterdir():
            if asset.suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp"}:
                continue
            asset_map[asset.stem.lower()] = asset

    ASSET_INDEX_CACHE[kind] = asset_map
    return asset_map


# This helper resolves a god image path by trying several possible stem names.
def resolve_god_image(name: str) -> Path | None:
    image_map = build_asset_index("gods", GODS_ASSETS_DIR)
    for candidate in normalize_name_variants(name):
        asset = image_map.get(candidate.lower())
        if asset:
            return asset
    return None


# This helper resolves a pantheon icon path using the same multi-variant logic.
def resolve_pantheon_image(name: str) -> Path | None:
    image_map = build_asset_index("pantheons", PANTHEONS_DIR)
    for candidate in normalize_name_variants(name):
        asset = image_map.get(candidate.lower())
        if asset:
            return asset
    return None


# This helper applies the original tier thresholds to a numeric rating so the
# backend can recompute tiers after a save.
def get_tier_from_rating(rating: int) -> str:
    for threshold, label in TIER_THRESHOLDS:
        if rating >= threshold:
            return label
    return "U"


# This helper validates a player's PIN against the configured secrets.
def check_pin(player: str, entered_pin: str) -> bool:
    expected = get_secret(f"PIN_{player.upper()}")
    return entered_pin.strip() == expected


# This helper protects the full-history sync route so production backfills are
# only triggered by someone who knows the dedicated sync secret.
def check_sync_key(entered_key: str) -> bool:
    expected = get_secret("RATER_STATS_SYNC_KEY")
    return bool(expected) and entered_key.strip() == expected


# This helper turns raw personal ranking rows into the nested dictionary shape
# used throughout the app and by the original Streamlit UI.
def build_all_rankings(rank_rows: list[dict]) -> dict[str, dict[str, int]]:
    rankings: dict[str, dict[str, int]] = {}
    for row in rank_rows:
        player = row.get("player")
        god_name = row.get("god_name")
        rank = row.get("rank")
        if player and god_name and rank:
            rankings.setdefault(player, {})[god_name] = int(rank)
    return rankings


# This helper converts per-player scores on a god into pill-friendly data for
# the custom frontend cards and lists.
def build_council_pills(row: dict) -> list[dict]:
    pills: list[dict] = []
    for player in PLAYERS:
        raw_score = row.get(player)
        has_rating = isinstance(raw_score, (int, float)) and raw_score > 0
        pills.append(
            {
                "player": player,
                "abbr": PLAYER_ABBR.get(player, player[:2]),
                "color": COUNCIL_COLORS.get(player, "#d7a33d"),
                "score": int(raw_score) if has_rating else None,
                "rank": build_all_rankings([]).get(player, {}).get(row.get("God", "")),
            }
        )
    return pills


# This helper calculates whether a god deserves the HOT TAKE badge by checking
# whether any rated score is far from the group's mean.
def is_hot_take(row: dict) -> bool:
    rated_scores = [
        int(row[player])
        for player in PLAYERS
        if isinstance(row.get(player), (int, float)) and row[player] > 0
    ]
    if len(rated_scores) < 2:
        return False
    average_score = mean(rated_scores)
    return any(abs(score - average_score) >= HOT_TAKE_THRESHOLD for score in rated_scores)


# This helper merges metadata, council ratings, and personal ranking info into
# one frontend-friendly list of god records.
def merge_catalog(
    meta_rows: list[dict],
    rating_rows: list[dict],
    ranking_rows: list[dict],
) -> list[dict]:
    rating_lookup = {row["god_name"] if "god_name" in row else row["God"]: row for row in rating_rows}
    all_rankings = build_all_rankings(ranking_rows)
    catalog: list[dict] = []

    for source_row in meta_rows:
        row = dict(source_row)

        # This block normalizes the metadata keys so the Flask frontend can use
        # the same readable labels as the original Streamlit app.
        normalized = {
            "God": row.get("god_name", row.get("God")),
            "Title": row.get("title", row.get("Title")),
            "Pantheon": row.get("pantheon", row.get("Pantheon")),
            "Role": row.get("role", row.get("Role")),
            "Class": row.get("class", row.get("Class")),
            "Attack Type": row.get("attack_type", row.get("Attack Type")),
            "Damage Type": row.get("damage_type", row.get("Damage Type")),
            "Tier": str(row.get("tier", row.get("Tier", "U"))).strip().upper() or "U",
            "Rank": int(row.get("rank", row.get("Rank", 0)) or 0),
            "Movement": int(row.get("movement", row.get("Movement", 0)) or 0),
        }

        # This block merges in each council member's saved score from the
        # council_ratings table if a row exists for this god.
        rating_source = rating_lookup.get(normalized["God"], {})
        for player in PLAYERS:
            raw_value = rating_source.get(player.lower(), rating_source.get(player))
            normalized[player] = int(raw_value) if isinstance(raw_value, (int, float)) and raw_value > 0 else None

        # This block recomputes the consensus rating from non-zero scores to
        # match the original app's logic.
        rated_values = [normalized[player] for player in PLAYERS if normalized[player]]
        normalized["Rating"] = int(mean(rated_values)) if rated_values else 0

        # This block adds derived display data that the frontend can render
        # directly without redoing path and badge logic on every view.
        normalized["TierColor"] = TIER_COLORS.get(normalized["Tier"], TIER_COLORS["U"])
        normalized["ImageUrl"] = f"/god-image/{quote(normalized['God'])}" if resolve_god_image(normalized["God"]) else ""
        normalized["PantheonImageUrl"] = (
            f"/pantheon-image/{quote(normalized['Pantheon'])}" if normalized["Pantheon"] and resolve_pantheon_image(normalized["Pantheon"]) else ""
        )
        normalized["HotTake"] = is_hot_take(normalized)
        normalized["CouncilPills"] = [
            {
                "player": player,
                "abbr": PLAYER_ABBR.get(player, player[:2]),
                "color": COUNCIL_COLORS.get(player, "#d7a33d"),
                "score": normalized[player],
                "rank": all_rankings.get(player, {}).get(normalized["God"]),
            }
            for player in PLAYERS
        ]

        catalog.append(normalized)

    # This block preserves the live ranking sort used by the original app so
    # all tabs begin from the same consensus ordering.
    catalog.sort(key=lambda item: (item["Rank"] == 0, item["Rank"], item["God"]))
    return catalog


# This helper loads all core app data from Supabase and falls back to the JSON
# snapshots when live reads fail, which keeps development smoother.
def load_app_state() -> dict[str, Any]:
    errors: list[str] = []
    local_history_rows = load_local_activity_log()

    try:
        meta_rows = sb_select("gods_metadata", {"select": "*"})
    except Exception as exc:  # noqa: BLE001
        meta_rows = load_json_snapshot("gods_metadata.json")
        errors.append(f"metadata fallback: {exc}")

    try:
        rating_rows = sb_select("council_ratings", {"select": "*"})
    except Exception as exc:  # noqa: BLE001
        rating_rows = load_json_snapshot("council_ratings.json")
        errors.append(f"ratings fallback: {exc}")

    try:
        ranking_rows = sb_select("personal_rankings", {"select": "*"})
    except Exception as exc:  # noqa: BLE001
        ranking_rows = []
        errors.append(f"personal rankings unavailable: {exc}")

    try:
        history_rows = sb_select(
            "rating_history",
            {"select": "*", "order": "changed_at.desc", "limit": "120"},
        )
    except Exception as exc:  # noqa: BLE001
        history_rows = []
        errors.append(f"history unavailable: {exc}")

    catalog = merge_catalog(meta_rows, rating_rows, ranking_rows)
    all_rankings = build_all_rankings(ranking_rows)
    merged_history = merge_history_rows(history_rows, local_history_rows)

    # This block computes the shared headline stats shown at the top of the app.
    total_gods = len(catalog)
    avg_rating = int(mean([god["Rating"] for god in catalog if god["Rating"] > 0])) if any(god["Rating"] > 0 for god in catalog) else 0
    ss_count = sum(1 for god in catalog if god["Tier"] == "SS")

    return {
        "catalog": catalog,
        "all_rankings": all_rankings,
        "recent_history": merged_history[:120],
        "errors": errors,
        "stats": {
            "total_gods": total_gods,
            "avg_rating": avg_rating,
            "ss_count": ss_count,
        },
    }


# This helper fetches one player's saved personal ranking map directly from
# Supabase so the save workflow can compare old vs. new orderings.
def load_player_rankings(player: str) -> dict[str, int]:
    try:
        rows = sb_select(
            "personal_rankings",
            {"select": "*", "player": f"eq.{player}"},
        )
    except Exception:  # noqa: BLE001
        return {}
    return {row["god_name"]: int(row["rank"]) for row in rows if row.get("rank")}


# This helper loads the current metadata and ratings tables in a shape that is
# convenient for save-time recomputation.
def load_current_tables() -> tuple[list[dict], list[dict]]:
    meta_rows = sb_select("gods_metadata", {"select": "*"})
    rating_rows = sb_select("council_ratings", {"select": "*"})
    return meta_rows, rating_rows


# This helper mirrors the Streamlit save logic: recompute consensus rating,
# tier, rank, and movement from the edited rating matrix.
def recompute_metadata(meta_rows: list[dict], rating_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    metadata_lookup = {row["god_name"]: dict(row) for row in meta_rows}
    normalized_ratings: list[dict] = []
    computed_rows: list[dict] = []

    for rating_row in rating_rows:
        god_name = rating_row["god_name"]
        normalized = {"god_name": god_name}
        scores: list[int] = []

        # This block normalizes each player's stored score to either an int or
        # None so downstream calculations behave consistently.
        for player in PLAYERS:
            raw_score = rating_row.get(player.lower(), rating_row.get(player))
            value = int(raw_score) if isinstance(raw_score, (int, float)) and raw_score > 0 else None
            normalized[player.lower()] = value
            if value is not None:
                scores.append(value)

        normalized_ratings.append(normalized)

        # This block rebuilds the merged row the same way the original app did
        # before it wrote updated metadata back to Supabase.
        base_meta = metadata_lookup.get(god_name, {})
        computed_rows.append(
            {
                "god_name": god_name,
                "title": base_meta.get("title"),
                "pantheon": base_meta.get("pantheon"),
                "role": base_meta.get("role"),
                "class": base_meta.get("class"),
                "attack_type": base_meta.get("attack_type"),
                "damage_type": base_meta.get("damage_type"),
                "old_rank": int(base_meta.get("rank", 9999) or 9999),
                "rating": int(mean(scores)) if scores else 0,
            }
        )

    # This block sorts by consensus rating to assign fresh ranks and movement.
    computed_rows.sort(key=lambda item: (-item["rating"], item["god_name"]))
    metadata_records: list[dict] = []
    for index, row in enumerate(computed_rows, start=1):
        old_rank = row["old_rank"] if row["old_rank"] != 9999 else index
        metadata_records.append(
            {
                "god_name": row["god_name"],
                "title": row.get("title"),
                "pantheon": row.get("pantheon"),
                "role": row.get("role"),
                "class": row.get("class"),
                "attack_type": row.get("attack_type"),
                "damage_type": row.get("damage_type"),
                "tier": get_tier_from_rating(row["rating"]),
                "rank": index,
                "movement": int(old_rank) - index,
            }
        )

    return metadata_records, normalized_ratings


# This helper applies a player's submitted score map to the live council
# ratings table without disturbing the other players' columns.
def apply_player_scores(
    rating_rows: list[dict],
    player: str,
    submitted_scores: dict[str, int],
) -> tuple[list[dict], list[dict]]:
    lower_player = player.lower()
    history_records: list[dict] = []
    updated_rows: list[dict] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for row in rating_rows:
        mutable_row = dict(row)
        god_name = mutable_row["god_name"]
        old_raw = mutable_row.get(lower_player)
        old_value = int(old_raw) if isinstance(old_raw, (int, float)) and old_raw > 0 else 0
        new_value = int(submitted_scores.get(god_name, 0) or 0)

        # This block records rating history entries only when the player's
        # score actually changed for a god.
        if old_value != new_value:
            history_records.append(
                {
                    "player": player,
                    "god_name": god_name,
                    "old_value": old_value,
                    "new_value": new_value,
                    "change_type": "rating",
                    "changed_at": now_iso,
                }
            )

        mutable_row[lower_player] = new_value if new_value > 0 else None
        updated_rows.append(mutable_row)

    return updated_rows, history_records


# This helper transforms a submitted ranking order into Supabase records.
def build_personal_ranking_records(player: str, submitted_order: list[str]) -> list[dict]:
    return [
        {"player": player, "god_name": god_name, "rank": index}
        for index, god_name in enumerate(submitted_order, start=1)
    ]


# This helper creates rank-history rows by diffing the old personal ranking map
# against the newly submitted manual order.
def build_rank_history(
    player: str,
    old_ranks: dict[str, int],
    submitted_order: list[str],
) -> list[dict]:
    rank_history: list[dict] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    new_ranks = {god_name: index for index, god_name in enumerate(submitted_order, start=1)}

    # This block records both moved ranks and removed ranks so the activity tab
    # can reflect manual ordering changes.
    affected_gods = set(old_ranks) | set(new_ranks)
    for god_name in affected_gods:
        old_rank = old_ranks.get(god_name)
        new_rank = new_ranks.get(god_name)
        if old_rank == new_rank:
            continue
        rank_history.append(
            {
                "player": player,
                "god_name": god_name,
                "old_value": old_rank,
                "new_value": new_rank,
                "change_type": "rank",
                "changed_at": now_iso,
            }
        )

    return rank_history


# This helper strips UI-only fields from history rows so the remote Supabase
# history insert matches the current table schema.
def build_remote_history_records(records: list[dict]) -> list[dict]:
    return [
        {
            "player": row.get("player"),
            "god_name": row.get("god_name"),
            "old_value": row.get("old_value"),
            "new_value": row.get("new_value"),
            "change_type": row.get("change_type", "rating"),
            "changed_at": row.get("changed_at"),
        }
        for row in records
    ]


# This route renders the single-page shell; the dynamic tab content is filled
# by the frontend JavaScript after it fetches `/api/bootstrap`.
@app.route("/")
def index():
    return render_template("index.html")


# This route returns the initial app payload needed to render every tab and the
# rank editor without additional page loads.
@app.route("/api/bootstrap")
def api_bootstrap():
    state = load_app_state()
    return jsonify(
        {
            "config": {
                "players": PLAYERS,
                "playerAbbr": PLAYER_ABBR,
                "councilColors": COUNCIL_COLORS,
                "tierColors": TIER_COLORS,
                "tierOrder": TIER_ORDER,
                "hotTakeThreshold": HOT_TAKE_THRESHOLD,
            },
            "stats": state["stats"],
            "gods": state["catalog"],
            "allRankings": state["all_rankings"],
            "recentHistory": state["recent_history"],
            "errors": state["errors"],
        }
    )


# This route returns the live SmiteSource-derived rater stats used by the
# dedicated profile tab, while keeping the fetch logic hidden server-side.
@app.route("/api/rater-stats")
def api_rater_stats():
    return jsonify({"profiles": load_rater_stats()})


# This route backfills full SmiteSource match history into Supabase so the
# Chemistry tab can run on durable all-time data instead of recent samples.
@app.post("/api/rater-stats/sync")
def api_rater_stats_sync():
    global RATER_STATS_CACHE

    payload = request.get_json(silent=True) or {}
    sync_key = str(payload.get("syncKey") or request.headers.get("X-Sync-Key") or "")
    player = str(payload.get("player") or "").strip()

    if not check_sync_key(sync_key):
        return jsonify({"ok": False, "message": "Unauthorized sync request."}), 401

    targets = [player] if player in PLAYERS else PLAYERS
    if player and player not in PLAYERS:
        return jsonify({"ok": False, "message": "Unknown player."}), 400

    try:
        results = [
            sync_smitesource_history_for_player(target, SMITESOURCE_PROFILE_LINKS.get(target, ""))
            for target in targets
        ]
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "message": str(exc)}), 500

    # This block clears the in-memory caches so the next stats request reflects
    # the freshly synced durable history immediately.
    RATER_STATS_CACHE = None
    for target in targets:
        SMITESOURCE_CACHE.pop(target, None)

    return jsonify(
        {
            "ok": True,
            "results": results,
            "players": targets,
            "historySource": "supabase",
        }
    )


# This route fetches a focused rating-history slice for the analytics chart so
# the browser doesn't have to download the entire history table every time.
@app.route("/api/history")
def api_history():
    god_name = request.args.get("god", "").strip()
    player = request.args.get("player", "").strip()
    limit = request.args.get("limit", "200")

    params = {"select": "*", "order": "changed_at.asc", "limit": limit}
    if god_name:
        params["god_name"] = f"eq.{god_name}"
    if player:
        params["player"] = f"eq.{player}"

    try:
        rows = sb_select("rating_history", params)
    except Exception as exc:  # noqa: BLE001
        local_rows = load_local_activity_log()
        filtered_rows = [
            row for row in local_rows
            if (not god_name or row.get("god_name") == god_name)
            and (not player or row.get("player") == player)
        ]
        filtered_rows.sort(key=lambda row: row.get("changed_at", ""))
        return jsonify({"rows": filtered_rows[: int(limit)], "error": str(exc)})

    return jsonify({"rows": rows})


# This route validates the chosen player's PIN so the frontend can unlock the
# rate-and-rank editor without exposing the secret on the client.
@app.post("/api/unlock")
def api_unlock():
    payload = request.get_json(force=True)
    player = payload.get("player", "")
    pin = payload.get("pin", "")

    if player not in PLAYERS:
        return jsonify({"ok": False, "message": "Unknown player."}), 400

    if check_pin(player, pin):
        return jsonify({"ok": True})

    return jsonify({"ok": False, "message": "Wrong PIN."}), 401


# This route persists the edited ratings and personal ranking order back to
# Supabase while keeping the metadata/rank calculations in Python.
@app.post("/api/save-rankings")
def api_save_rankings():
    payload = request.get_json(force=True)
    player = payload.get("player", "")
    submitted_scores = payload.get("ratings", {})
    submitted_order = payload.get("order", [])

    if player not in PLAYERS:
        return jsonify({"ok": False, "message": "Unknown player."}), 400

    # This block validates the payload shape before any writes happen.
    if not isinstance(submitted_scores, dict) or not isinstance(submitted_order, list):
        return jsonify({"ok": False, "message": "Invalid payload."}), 400

    history_warning = ""

    try:
        meta_rows, rating_rows = load_current_tables()
        old_rank_map = load_player_rankings(player)
        # This block applies the player's edits to the live council_ratings set
        # and collects per-god rating history rows.
        updated_rating_rows, rating_history = apply_player_scores(
            rating_rows,
            player,
            {god_name: int(value or 0) for god_name, value in submitted_scores.items()},
        )

        # This block recomputes metadata after the edited scores are applied.
        metadata_records, normalized_ratings = recompute_metadata(meta_rows, updated_rating_rows)

        # This block stores the player-specific manual ranking order and records
        # movement history for the activity feed.
        personal_ranking_records = build_personal_ranking_records(player, submitted_order)
        rank_history = build_rank_history(player, old_rank_map, submitted_order)
        history_records = rating_history + rank_history

        sb_upsert("gods_metadata", metadata_records, "god_name")
        sb_upsert("council_ratings", normalized_ratings, "god_name")
        sb_delete_player_rankings(player)
        sb_insert("personal_rankings", personal_ranking_records)
        append_local_activity_log(history_records)

        try:
            sb_insert("rating_history", build_remote_history_records(history_records))
        except Exception as exc:
            # This block keeps the save flow successful even if the remote
            # history table is temporarily unavailable or has an older schema.
            history_warning = str(exc)

    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "message": str(exc)}), 500

    return jsonify(
        {
            "ok": True,
            "message": f"Saved {player}'s ratings and ranking order.",
            "ratingChanges": len(rating_history),
            "rankChanges": len(rank_history),
            "historyWarning": history_warning,
        }
    )


# This route serves existing god art out of the original asset directory so the
# Flask port can reuse the same visuals without duplicating files.
@app.route("/god-image/<path:god_name>")
def god_image(god_name: str):
    asset = resolve_god_image(god_name)
    if not asset:
        abort(404)
    return send_file(asset)


# This route serves pantheon icons used in the rankings tab.
@app.route("/pantheon-image/<path:pantheon_name>")
def pantheon_image(pantheon_name: str):
    asset = resolve_pantheon_image(pantheon_name)
    if not asset:
        abort(404)
    return send_file(asset)


# This block runs the Flask development server locally when you launch the file
# directly with `python app.py`.
if __name__ == "__main__":
    host = os.environ.get("FLASK_HOST", "127.0.0.1")
    port = int(os.environ.get("FLASK_PORT", "5000"))
    app.run(host=host, port=port, debug=True)
