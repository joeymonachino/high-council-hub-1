from __future__ import annotations

# This script locally syncs a small number of SmiteSource match pages into the
# Supabase history table without needing to manually import one HAR per page.
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import app  # noqa: E402

DEFAULT_PLAYERS = ["Joey", "Jami", "Darian", "Mike"]
SMITESOURCE_MATCHES_URL = "https://smitesource.com/rpc/matches/getPlayerMatches"


# This helper extracts headers from a saved browser HAR so local requests look
# like the successful browser call that produced the HAR.
def headers_from_seed_har(seed_har: Path | None) -> dict[str, str]:
    fallback = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://smitesource.com",
        "Referer": "https://smitesource.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
    }
    if not seed_har:
        return fallback

    har = json.loads(seed_har.read_text(encoding="utf-8"))
    entries = har.get("log", {}).get("entries", []) if isinstance(har, dict) else []
    for entry in entries:
        request_payload = entry.get("request") if isinstance(entry.get("request"), dict) else {}
        url = str(request_payload.get("url") or "")
        if "/rpc/matches/getPlayerMatches" not in url:
            continue

        headers: dict[str, str] = {}
        for header in request_payload.get("headers") or []:
            if not isinstance(header, dict):
                continue
            name = str(header.get("name") or "").strip()
            value = str(header.get("value") or "")
            lower_name = name.lower()
            if not name or lower_name in {"host", "content-length", ":authority", ":method", ":path", ":scheme"}:
                continue
            headers[name] = value

        headers.update({key: value for key, value in fallback.items() if key not in headers})
        return headers

    return fallback


# This helper reads the first matching HAR request so we can preserve options
# like mode, season, includeBreakdowns, and includeTeamDetails.
def request_template_from_seed_har(seed_har: Path | None) -> dict[str, Any]:
    template = {
        "mode": "all",
        "season": "0",
        "page": 1,
        "pageSize": 10,
        "includeBreakdowns": False,
        "includeTeamDetails": True,
    }
    if not seed_har:
        return template

    har = json.loads(seed_har.read_text(encoding="utf-8"))
    entries = har.get("log", {}).get("entries", []) if isinstance(har, dict) else []
    for entry in entries:
        request_payload = entry.get("request") if isinstance(entry.get("request"), dict) else {}
        url = str(request_payload.get("url") or "")
        if "/rpc/matches/getPlayerMatches" not in url:
            continue

        request_json = app.har_request_json(entry)
        request_body = request_json.get("json") if isinstance(request_json.get("json"), dict) else request_json
        if isinstance(request_body, dict):
            template.update({key: value for key, value in request_body.items() if key != "playerUuid"})
        return template

    return template


# This helper resolves CLI player labels into SmiteSource UUIDs from the app's
# existing profile-link map so there is one canonical council roster.
def player_uuid_map(players: list[str]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for player in players:
        profile_url = app.SMITESOURCE_PROFILE_LINKS.get(player, "")
        player_uuid = app.smitesource_player_uuid(profile_url)
        if player_uuid:
            resolved[player] = player_uuid
    return resolved


# This helper fetches one SmiteSource match page using the captured request
# headers and a GET data= payload that mirrors the current site behavior.
def fetch_match_page(
    *,
    player_uuid: str,
    page: int,
    page_size: int,
    headers: dict[str, str],
    template: dict[str, Any],
) -> list[dict[str, Any]]:
    payload = dict(template)
    payload.update({"playerUuid": player_uuid, "page": page, "pageSize": page_size})
    query = urlencode({"data": json.dumps({"json": payload}, separators=(",", ":"))})
    response = app.HTTP.get(f"{SMITESOURCE_MATCHES_URL}?{query}", headers=headers, timeout=30)
    response.raise_for_status()
    response_payload = response.json()
    return app.smitesource_matches_from_payload(response_payload if isinstance(response_payload, dict) else {})


# This helper compares fetched rows against stored Supabase keys and returns only
# the records that are new for that specific council player.
def missing_records_for_player(player: str, player_uuid: str, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    existing_keys = {
        str(row.get("match_key") or "")
        for row in app.load_stored_match_history(player)
        if str(row.get("match_key") or "")
    }
    records: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for row in rows:
        record = app.normalize_smitesource_history_record(player, player_uuid, row)
        match_key = str(record.get("match_key") or "")
        if not match_key or match_key in existing_keys or match_key in seen_keys:
            continue
        seen_keys.add(match_key)
        records.append(record)

    return records, len(existing_keys)


# This helper loops each requested player/page and optionally writes missing rows
# to Supabase. Dry-run mode performs every read/fetch/dedupe step except upsert.
def sync_pages(
    *,
    players: list[str],
    pages: int,
    page_size: int,
    seed_har: Path | None,
    dry_run: bool,
    delay_seconds: float,
) -> dict[str, Any]:
    headers = headers_from_seed_har(seed_har)
    template = request_template_from_seed_har(seed_har)
    resolved_players = player_uuid_map(players)
    results: list[dict[str, Any]] = []

    for player, player_uuid in resolved_players.items():
        fetched_rows: list[dict[str, Any]] = []
        page_summaries: list[dict[str, Any]] = []
        blocked = False
        error_message = ""
        for page_number in range(1, pages + 1):
            try:
                page_rows = fetch_match_page(
                    player_uuid=player_uuid,
                    page=page_number,
                    page_size=page_size,
                    headers=headers,
                    template=template,
                )
            except Exception as exc:  # noqa: BLE001
                response = getattr(exc, "response", None)
                status_code = getattr(response, "status_code", None)
                blocked = status_code == 403
                error_message = f"SmiteSource returned {status_code or 'an error'}: {exc}"
                page_summaries.append(
                    {
                        "page": page_number,
                        "captured": 0,
                        "error": error_message,
                        "blocked": blocked,
                    }
                )
                break

            fetched_rows.extend(page_rows)
            page_summaries.append({"page": page_number, "captured": len(page_rows), "blocked": False})
            if delay_seconds and page_number < pages:
                time.sleep(delay_seconds)

        records, stored_count = missing_records_for_player(player, player_uuid, fetched_rows)
        if records and not dry_run:
            app.sb_upsert("smitesource_match_history", records, "record_key")
            try:
                app.upsert_match_summary_rows(records)
            except Exception:
                pass

        results.append(
            {
                "player": player,
                "playerUuid": player_uuid,
                "pages": page_summaries,
                "captured": len(fetched_rows),
                "missing": len(records),
                "inserted": 0 if dry_run else len(records),
                "storedBefore": stored_count,
                "dryRun": dry_run,
                "blocked": blocked,
                "error": error_message,
            }
        )
        if delay_seconds:
            time.sleep(delay_seconds)

    skipped_players = [player for player in players if player not in resolved_players]
    return {
        "ok": True,
        "source": "smitesource-pages",
        "syncedAt": datetime.now(timezone.utc).isoformat(),
        "dryRun": dry_run,
        "players": list(resolved_players.keys()),
        "skippedPlayers": skipped_players,
        "pagesRequested": pages,
        "pageSize": page_size,
        "captured": sum(item["captured"] for item in results),
        "missing": sum(item["missing"] for item in results),
        "inserted": sum(item["inserted"] for item in results),
        "results": results,
    }


# This block parses local CLI options. Defaults are intentionally council-focused
# so the common command is only a page count plus optional seed HAR.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync recent SmiteSource match pages into Supabase.")
    parser.add_argument("--players", nargs="*", default=DEFAULT_PLAYERS, help="Council players to sync. Defaults to Joey Jami Darian Mike.")
    parser.add_argument("--pages", type=int, default=1, help="Number of 10-match pages to fetch per player.")
    parser.add_argument("--page-size", type=int, default=10, help="Matches per page. SmiteSource currently shows 10.")
    parser.add_argument("--seed-har", help="Optional HAR file with a successful SmiteSource getPlayerMatches request.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and dedupe without writing to Supabase.")
    parser.add_argument("--delay", type=float, default=0.6, help="Seconds to wait between requests.")
    return parser.parse_args()


# This block runs the sync and prints a JSON summary that is easy to paste back
# into chat if we need to debug a blocked request or unexpected count.
def main() -> None:
    args = parse_args()
    if args.pages < 1:
        raise RuntimeError("--pages must be at least 1.")
    if args.page_size < 1:
        raise RuntimeError("--page-size must be at least 1.")

    summary = sync_pages(
        players=args.players,
        pages=args.pages,
        page_size=args.page_size,
        seed_har=Path(args.seed_har) if args.seed_har else None,
        dry_run=bool(args.dry_run),
        delay_seconds=max(0.0, float(args.delay)),
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()


