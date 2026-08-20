from __future__ import annotations

# This block imports the standard-library tools used for argument parsing,
# JSON/HAR reading, filesystem output, and simple URL filtering.
import argparse
import base64
import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


# This helper checks whether one HAR entry points at the Tracker match-history
# endpoint we care about and applies optional filters like player/gamemode/season.
def is_target_entry(
    entry: dict[str, Any],
    *,
    player_id: str,
    gamemode: str,
    season: str,
) -> bool:
    request = entry.get("request") if isinstance(entry.get("request"), dict) else {}
    url = str(request.get("url") or "")
    if "api.tracker.gg/api/v2/smite2/standard/matches/steam/" not in url:
        return False
    if "/live" in url:
        return False

    parsed = urlparse(url)
    path_parts = [part for part in parsed.path.split("/") if part]
    if not path_parts:
        return False

    request_player_id = path_parts[-1]
    query = parse_qs(parsed.query)

    if player_id and request_player_id != player_id:
        return False
    if gamemode and query.get("gamemode", [""])[0] != gamemode:
        return False
    if season and query.get("season", [""])[0] != season:
        return False

    return True


# This helper extracts the response body from one HAR entry and decodes it when
# the browser stored the content as base64.
def extract_response_text(entry: dict[str, Any]) -> str:
    response = entry.get("response") if isinstance(entry.get("response"), dict) else {}
    content = response.get("content") if isinstance(response.get("content"), dict) else {}
    text = content.get("text")
    if not isinstance(text, str):
        return ""

    encoding = str(content.get("encoding") or "")
    if encoding == "base64":
        return base64.b64decode(text).decode("utf-8")
    return text


# This helper parses one Tracker response body into the page payload shape we
# expect and raises a readable error when the file does not contain JSON.
def parse_tracker_payload(raw_text: str, source_label: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{source_label} did not contain valid JSON.") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"{source_label} JSON was not an object.")

    return payload


# This helper normalizes one page payload into a compact summary the importer
# can use later, while preserving the full match objects.
def normalize_page(
    payload: dict[str, Any],
    *,
    source_url: str,
) -> dict[str, Any]:
    root = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    matches = root.get("matches") or root.get("data") or root.get("items") or []
    matches = [match for match in matches if isinstance(match, dict)]
    metadata = root.get("metadata") if isinstance(root.get("metadata"), dict) else {}
    requesting = root.get("requestingPlayerAttributes") if isinstance(root.get("requestingPlayerAttributes"), dict) else {}

    return {
        "sourceUrl": source_url,
        "next": metadata.get("next"),
        "paginationType": root.get("paginationType") or payload.get("paginationType") or "",
        "requestingPlayerAttributes": requesting,
        "matchCount": len(matches),
        "matches": matches,
    }


# This helper walks the HAR and returns all matching Tracker page payloads.
def extract_pages_from_har(
    har_path: Path,
    *,
    player_id: str,
    gamemode: str,
    season: str,
) -> list[dict[str, Any]]:
    with har_path.open("r", encoding="utf-8") as handle:
        har = json.load(handle)

    log = har.get("log") if isinstance(har.get("log"), dict) else {}
    entries = log.get("entries") if isinstance(log.get("entries"), list) else []

    pages: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        if not is_target_entry(entry, player_id=player_id, gamemode=gamemode, season=season):
            continue

        request = entry.get("request") if isinstance(entry.get("request"), dict) else {}
        url = str(request.get("url") or "")
        raw_text = extract_response_text(entry)
        if not raw_text:
            continue

        payload = parse_tracker_payload(raw_text, f"HAR entry #{index}")
        pages.append(normalize_page(payload, source_url=url))

    return pages


# This helper dedupes match objects across pages by Tracker match id and emits a
# single combined export payload for later import.
def build_export_payload(pages: list[dict[str, Any]]) -> dict[str, Any]:
    seen_match_ids: set[str] = set()
    combined_matches: list[dict[str, Any]] = []

    for page in pages:
        for match in page.get("matches", []):
            match_id = str(match.get("id") or "")
            if match_id and match_id in seen_match_ids:
                continue
            if match_id:
                seen_match_ids.add(match_id)
            combined_matches.append(match)

    return {
        "pages": [
            {
                "sourceUrl": page.get("sourceUrl"),
                "next": page.get("next"),
                "paginationType": page.get("paginationType"),
                "requestingPlayerAttributes": page.get("requestingPlayerAttributes"),
                "matchCount": page.get("matchCount"),
            }
            for page in pages
        ],
        "totalPages": len(pages),
        "totalMatches": len(combined_matches),
        "matches": combined_matches,
    }


# This block parses command-line arguments so the extractor can be run from
# PowerShell after exporting one HAR from Chrome or Edge.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract Tracker.gg Smite 2 match JSON from a browser HAR file."
    )
    parser.add_argument("--har-file", required=True, help="Path to the exported HAR file.")
    parser.add_argument("--player-id", required=True, help="Tracker platform user identifier, for example a Steam ID.")
    parser.add_argument("--gamemode", default="joust", help="Tracker gamemode filter, default: joust.")
    parser.add_argument("--season", default="3", help="Tracker season filter, default: 3.")
    parser.add_argument(
        "--output",
        default="tracker_matches_export.json",
        help="Output JSON path. Defaults to tracker_matches_export.json in the current directory.",
    )
    return parser.parse_args()


# This block runs the HAR extraction and writes one combined JSON file that can
# be fed into a later importer step.
def main() -> None:
    args = parse_args()
    har_path = Path(args.har_file)
    output_path = Path(args.output)

    pages = extract_pages_from_har(
        har_path,
        player_id=args.player_id,
        gamemode=args.gamemode,
        season=args.season,
    )
    if not pages:
        raise RuntimeError("No matching Tracker match-history responses were found in the HAR file.")

    export_payload = build_export_payload(pages)
    output_path.write_text(json.dumps(export_payload, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "ok": True,
                "harFile": str(har_path),
                "output": str(output_path),
                "totalPages": export_payload["totalPages"],
                "totalMatches": export_payload["totalMatches"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
