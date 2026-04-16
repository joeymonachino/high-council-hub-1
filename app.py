from __future__ import annotations

# This block imports the standard-library tools used for file access, dates,
# lightweight caching, and configuration parsing.
import json
import os
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import quote

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


# This block creates the Flask application object that owns the routes and
# template/static configuration for the whole custom app.
app = Flask(__name__)


# This block stores a tiny in-memory cache for asset lookup maps so the app
# doesn't rescan the same image folders on every request.
ASSET_INDEX_CACHE: dict[str, dict[str, Path]] = {}


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
    response.raise_for_status()


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


# This helper loads the local activity fallback log so recent changes can still
# appear in the Activity tab when Supabase history reads are unavailable.
def load_local_activity_log() -> list[dict]:
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
    if not records:
        return

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
        except Exception:
            # This block keeps the save flow successful even if the remote
            # history table is temporarily unavailable or has an older schema.
            pass

    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "message": str(exc)}), 500

    return jsonify(
        {
            "ok": True,
            "message": f"Saved {player}'s ratings and ranking order.",
            "ratingChanges": len(rating_history),
            "rankChanges": len(rank_history),
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
