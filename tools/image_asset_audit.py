from __future__ import annotations

# This script audits the local god-art folder against the metadata roster so we
# can find missing files, tiny placeholder files, and gods relying on remote art.
import argparse
import json
import re
from pathlib import Path
from urllib.parse import quote

# This block defines project paths relative to the script location so the tool
# works from the repo root, the tools folder, or a scheduled/manual shell.
ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data" / "gods_metadata.json"
GODS_DIR = ROOT / "assets" / "gods"
REPORT_PATH = ROOT / "tools" / "image_asset_report.md"
MIN_REAL_IMAGE_BYTES = 512

# This map mirrors the app's known remote fallbacks. These are intentionally
# curated instead of scraped live so the production app stays fast and stable.
REMOTE_FALLBACKS = {
    "morgan le fay": "https://arewesmite2yet.com/images/gods/smite1/card/morgan_le_fay.png",
    "morganlefay": "https://arewesmite2yet.com/images/gods/smite1/card/morgan_le_fay.png",
    "daji": "https://arewesmite2yet.com/images/gods/smite1/card/da_ji.png",
    "da ji": "https://arewesmite2yet.com/images/gods/smite1/card/da_ji.png",
    "bake kujira": "https://arewesmite2yet.com/images/gods/smite1/card/bake_kujira.png",
    "bakekujira": "https://arewesmite2yet.com/images/gods/smite1/card/bake_kujira.png",
    "chang e": "https://arewesmite2yet.com/images/gods/smite1/thumb/chang_e.png",
    "change": "https://arewesmite2yet.com/images/gods/smite1/thumb/chang_e.png",
    "chang'e": "https://arewesmite2yet.com/images/gods/smite1/thumb/chang_e.png",
}

# This map covers display-name-to-file-name quirks that would otherwise create
# false positives in the audit.
ALIASES = {
    "morgan le fay": ["MorganleFay", "MorganLeFay", "Morgan le Fay"],
    "morganlefay": ["MorganleFay", "MorganLeFay"],
    "daji": ["DaJi", "Da Ji", "Daji"],
    "da ji": ["DaJi", "Da Ji", "Daji"],
    "bake kujira": ["BakeKujira", "Bake Kujira"],
    "bakekujira": ["BakeKujira"],
    "chang e": ["ChangE", "Chang'e", "Change"],
    "change": ["ChangE", "Chang'e", "Change"],
    "chang'e": ["ChangE", "Chang'e", "Change"],
}


def normalize_key(name: str) -> str:
    """Return the loose matching key used by the Flask app."""
    return re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).strip()


def name_variants(name: str) -> list[str]:
    """Build candidate filename stems for a god display name."""
    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", name or "").strip()
    compact = cleaned.replace(" ", "")
    underscored = cleaned.replace(" ", "_")
    hyphenated = cleaned.replace(" ", "-")
    no_the = re.sub(r"^the\s+", "", cleaned, flags=re.IGNORECASE)
    variants = [
        name,
        cleaned,
        compact,
        underscored,
        hyphenated,
        no_the,
        no_the.replace(" ", ""),
        no_the.replace(" ", "_"),
        no_the.replace(" ", "-"),
    ]
    return list(dict.fromkeys([variant for value in variants for variant in (value, value.lower()) if variant]))


def build_asset_index() -> dict[str, Path]:
    """Index local god assets by lowercase stem."""
    assets: dict[str, Path] = {}
    if not GODS_DIR.exists():
        return assets
    for asset in GODS_DIR.iterdir():
        if asset.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}:
            assets[asset.stem.lower()] = asset
    return assets


def resolve_asset(god_name: str, assets: dict[str, Path]) -> Path | None:
    """Resolve the local asset even if it is a placeholder; the report labels it."""
    candidates = name_variants(god_name)
    alias_key = normalize_key(god_name)
    compact_alias_key = alias_key.replace(" ", "")
    candidates.extend(ALIASES.get(alias_key, []))
    candidates.extend(ALIASES.get(compact_alias_key, []))
    for candidate in candidates:
        for variant in name_variants(candidate):
            asset = assets.get(variant.lower())
            if asset:
                return asset
    return None


def fallback_url(god_name: str) -> str:
    """Return a curated remote fallback if one exists."""
    alias_key = normalize_key(god_name)
    compact_alias_key = alias_key.replace(" ", "")
    return REMOTE_FALLBACKS.get(alias_key) or REMOTE_FALLBACKS.get(compact_alias_key, "")


def load_gods() -> list[dict]:
    """Load metadata rows from the repo snapshot."""
    with DATA_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def audit_assets() -> dict:
    """Build the full audit result used by both JSON and markdown output."""
    assets = build_asset_index()
    rows = []
    for god in load_gods():
        god_name = god.get("God") or god.get("god_name") or ""
        asset = resolve_asset(god_name, assets)
        remote = fallback_url(god_name)
        size = asset.stat().st_size if asset else 0
        if asset and size <= MIN_REAL_IMAGE_BYTES:
            status = "placeholder"
        elif asset:
            status = "local"
        elif remote:
            status = "remote-fallback"
        else:
            status = "missing"
        rows.append({
            "god": god_name,
            "pantheon": god.get("Pantheon") or god.get("pantheon") or "",
            "status": status,
            "asset": str(asset.relative_to(ROOT)) if asset else "",
            "bytes": size,
            "remote": remote,
            "expected_route": f"/god-image/{quote(god_name)}",
        })

    grouped = {status: [row for row in rows if row["status"] == status] for status in ["local", "placeholder", "remote-fallback", "missing"]}
    return {
        "total": len(rows),
        "counts": {key: len(value) for key, value in grouped.items()},
        "rows": rows,
        "grouped": grouped,
    }


def write_markdown_report(result: dict, path: Path = REPORT_PATH) -> None:
    """Write a compact markdown report that is easy to read in VS Code."""
    lines = [
        "# Image Asset Audit",
        "",
        f"Total gods: {result['total']}",
        "",
        "| Status | Count |",
        "| --- | ---: |",
    ]
    for status, count in result["counts"].items():
        lines.append(f"| {status} | {count} |")

    for status in ["placeholder", "remote-fallback", "missing"]:
        rows = result["grouped"].get(status, [])
        lines.extend(["", f"## {status.title()}", "", "| God | Pantheon | Asset | Bytes | Remote |", "| --- | --- | --- | ---: | --- |"])
        if not rows:
            lines.append("| None |  |  |  |  |")
        for row in rows:
            lines.append(f"| {row['god']} | {row['pantheon']} | {row['asset']} | {row['bytes']} | {row['remote']} |")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    """CLI entry point for manual audits."""
    parser = argparse.ArgumentParser(description="Audit High Council god image assets.")
    parser.add_argument("--json", action="store_true", help="Print full JSON instead of the summary.")
    parser.add_argument("--write-report", action="store_true", help=f"Write {REPORT_PATH.relative_to(ROOT)}.")
    args = parser.parse_args()

    result = audit_assets()
    if args.write_report:
        write_markdown_report(result)
    if args.json:
        print(json.dumps({k: v for k, v in result.items() if k != "grouped"}, indent=2))
    else:
        print(json.dumps({"total": result["total"], "counts": result["counts"], "report": str(REPORT_PATH.relative_to(ROOT)) if args.write_report else ""}, indent=2))


if __name__ == "__main__":
    main()
