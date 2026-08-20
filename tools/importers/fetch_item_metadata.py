from __future__ import annotations

# Build a local item metadata snapshot for the Items tab. The app never scrapes
# at runtime; this script is a manual refresh tool for data/item_metadata.json.
import argparse
import html
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import app  # noqa: E402

BASE_URL = "https://smitebrain.com/items"
TRACKER_ITEM_IMAGE_BASE = "https://trackercdn.com/cdn/tracker.gg/smite2/images/items"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

TRACKER_IMAGE_SLUG_ALIASES = {
    "Titan's Bane": "titans-bane",
    "Oath-Sworn Spear": "oath-sworn-spear",
    "Soul Reaver": "soul-reaver",
    "Demon Blade": "demon-blade",
    "Stone of Binding": "stone-of-binding",
}

SMITEBRAIN_ITEM_SLUG_ALIASES = {
    "acorn t2 lively acorn": ["lively-acorn"],
    "acorn t3 bristlebush": ["briskberry-acorn"],
    "acorn t3 thickbark": ["ashwhorl-acorn"],
    "acorn t3 thistlethorn": ["thistlethorn-acorn", "thistlethorn"],
    "ancile shield": ["ancile"],
    "arrow": ["gilded-arrow"],
    "berserker": ["berserkers-shield"],
    "berserker's shield": ["berserkers-shield"],
    "blood forged blade": ["bloodforge"],
    "bloodsoaked shroud": ["blood-soaked-shroud"],
    "brawler's beat stick": ["brawlers-beat-stick"],
    "bumba's cudgel": ["bumbas-cudgel"],
    "bumbas dagger": ["bumbas-golden-dagger", "bumbas-dagger"],
    "crusher": ["the-crusher"],
    "curseweaver t3 tweak": ["curseweaver"],
    "death's toll": ['deaths-toll'],
    'deaths toll upgrade': ['deaths-embrace'],
    'devours gloves': ['devourers-gauntlet'],
    'draconic scales': ['draconic-scale'],
    'erosbow': ['eros-bow'],
    'gladiator shield': ['gladiators-shield'],
    'hand of the abyss': ['bracer-of-the-abyss'],
    'hideofthe nemean lion': ['hide-of-the-nemean-lion'],
    'items t3 omen drum': ['omen-drum'],
    "jotunn's revenge": ['jotunns-revenge'],
    'levianthans hide': ['leviathans-hide'],
    'magisshelter': ['magis-cloak'],
    "oni hunter's garb": ['oni-hunters-garb'],
    'parashu': ['avatars-parashu'],
    'plated armor': ['plated-metal'],
    "qin's blade": ['qins-blade'],
    'rod of ascelpius t3': ['rod-of-asclepius'],
    'sands of time upgrade': ['pendulum-of-the-ages'],
    'shape shifter shield': ['shifters-shield'],
    "shifter's shield": ['shifters-shield'],
    "shogun's kusari": ['shoguns-ofuda'],
    'soul locket': ['soul-reliquary'],
    'spearofthe magus': ['spear-of-the-magus'],
    'staff of cosmic horror': ['the-cosmic-horror'],
    'sundering axe upgrade': ['sundering-arc'],
    "titan's bane": ['titans-bane'],
    'vampiric cloak': ['vampiric-shroud'],
    'xibalban effigy new': ['xibalban-effigy'],
}


def item_slug(name: str) -> str:
    cleaned = str(name or "").strip().lower()
    cleaned = cleaned.replace("'", "-s")
    cleaned = cleaned.replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "-", cleaned)
    cleaned = re.sub(r"-+", "-", cleaned).strip("-")
    return cleaned


def tracker_slug(name: str) -> str:
    cleaned = str(name or "").strip().lower()
    cleaned = cleaned.replace("'", "")
    cleaned = cleaned.replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "-", cleaned)
    return re.sub(r"-+", "-", cleaned).strip("-")


def likely_tracker_item_slug(value: str) -> str:
    slug = tracker_slug(value)
    if not slug or len(slug) < 4 or re.fullmatch(r"[0-9a-f-]{4,}", slug):
        return ""
    return slug


def item_image_candidates(item: dict[str, Any], display_name: str) -> list[str]:
    candidates: list[str] = []
    for key in ("trackerImageUrl", "imageUrl", "iconUrl"):
        value = str(item.get(key) or "").strip()
        if value.startswith("http"):
            candidates.append(value)

    icon_path = str(item.get("itemIconPath") or "").replace("\\", "/").strip()
    if icon_path.startswith("http"):
        candidates.append(icon_path)
    elif icon_path.lower().startswith("tracker/"):
        candidates.append(f"{TRACKER_ITEM_IMAGE_BASE}/{Path(icon_path).name}")

    slug_values = [
        TRACKER_IMAGE_SLUG_ALIASES.get(display_name, ""),
        item.get("trackerId"),
        item.get("itemHexId"),
        item.get("itemMasterId"),
        display_name,
    ]
    for raw_value in slug_values:
        slug = likely_tracker_item_slug(str(raw_value or ""))
        if slug:
            candidates.append(f"{TRACKER_ITEM_IMAGE_BASE}/{slug}.jpg")

    return list(dict.fromkeys(candidates))


def strip_tags(value: str) -> str:
    value = re.sub(r"<script[\s\S]*?</script>", " ", value, flags=re.I)
    value = re.sub(r"<style[\s\S]*?</style>", " ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def extract_section_list(markup: str, heading: str) -> list[str]:
    match = re.search(rf"<h3[^>]*>\s*{re.escape(heading)}\s*</h3>([\s\S]*?)(?:<h[23]|</main>|$)", markup, flags=re.I)
    if not match:
        return []
    section = match.group(1)
    items = re.findall(r"<li[^>]*>([\s\S]*?)</li>", section, flags=re.I)
    return [strip_tags(item) for item in items if strip_tags(item)]


def extract_section_text(markup: str, heading: str) -> str:
    match = re.search(rf"<h3[^>]*>\s*{re.escape(heading)}\s*</h3>([\s\S]*?)(?:<h[23]|</main>|$)", markup, flags=re.I)
    if not match:
        return ""
    return strip_tags(match.group(1))


def meta_content(markup: str, property_name: str) -> str:
    pattern = rf'<meta[^>]+(?:property|name)="{re.escape(property_name)}"[^>]+content="([^"]*)"'
    match = re.search(pattern, markup, flags=re.I)
    return html.unescape(match.group(1)).strip() if match else ""


def extract_badges(markup: str) -> list[str]:
    hero_match = re.search(r'<h1[^>]*>[\s\S]*?</h1>([\s\S]*?)<div[^>]*class="text-warning', markup, flags=re.I)
    if not hero_match:
        return []
    return [strip_tags(value) for value in re.findall(r"<span[^>]*badge[^>]*>([\s\S]*?)</span>", hero_match.group(1), flags=re.I) if strip_tags(value)]


def extract_smitebrain_stats(markup: str) -> list[str]:
    # SmiteBrain stat rows render as paired spans inside justify-between rows.
    rows = re.findall(
        r'<div class="flex justify-between text-sm"><span class="text-neutral-content">([\s\S]*?)</span>\s*<span class="font-semibold text-white">([\s\S]*?)</span></div>',
        markup,
        flags=re.I,
    )
    return [f"{strip_tags(label)} {strip_tags(value)}" for label, value in rows if strip_tags(label) and strip_tags(value)]


def extract_smitebrain_passive(markup: str) -> str:
    # Passive/effect text is the whitespace-preserving paragraph after the stat rows.
    candidates = re.findall(r'<p class="text-neutral-content text-sm whitespace-pre-wrap">([\s\S]*?)</p>', markup, flags=re.I)
    for candidate in candidates:
        text = strip_tags(candidate).replace("â¢", "-")
        if text and not re.search(r"win rate|use rate|matches", text, flags=re.I):
            return text
    return ""


def parse_item_page(name: str, markup: str, url: str) -> dict[str, Any]:
    page_title_match = re.search(r"<h1[^>]*>([\s\S]*?)</h1>", markup, flags=re.I)
    page_title = strip_tags(page_title_match.group(1)) if page_title_match else name
    badges = extract_badges(markup)
    cost_match = re.search(r">\s*([0-9,]+)\s+gold\s*<", markup, flags=re.I)
    description = meta_content(markup, "description")
    summary = re.sub(r"\s*Pulled from top ranked.*$", "", description, flags=re.I).strip()
    if re.search(r"stats, recipe, cost, and the gods who build it most", summary, flags=re.I):
        summary = ""
    image_url = meta_content(markup, "og:image")
    return {
        "name": name,
        "displayName": page_title or name,
        "slug": item_slug(name),
        "source": "smitebrain",
        "sourceUrl": url,
        "pageTitle": page_title,
        "imageUrl": image_url,
        "summary": summary,
        "cost": int(cost_match.group(1).replace(",", "")) if cost_match else None,
        "itemType": " ".join(badges),
        "tags": badges,
        "stats": extract_smitebrain_stats(markup),
        "passive": extract_smitebrain_passive(markup),
    }


def load_history_rows_for_items(page_size: int = 150) -> list[dict[str, Any]]:
    # Raw match JSON can be large enough to make Supabase unhappy on 1000-row
    # pages, so this script intentionally reads smaller slices.
    return app.sb_select_all(
        "smitesource_match_history",
        {"select": "player,started_at,raw_match", "order": "started_at.desc"},
        page_size=page_size,
    )


def clean_item_display_name(name: str) -> str:
    words = str(name or "").strip().split()
    if not words:
        return ""
    small_words = {"of", "the", "and", "a", "an"}
    cleaned = []
    for index, word in enumerate(words):
        cleaned.append(word.lower() if index > 0 and word.lower() in small_words else word)
    return " ".join(cleaned)


def collect_used_items() -> dict[str, dict[str, Any]]:
    collected: dict[str, dict[str, Any]] = {}
    for history_row in load_history_rows_for_items():
        raw = history_row.get("raw_match") if isinstance(history_row.get("raw_match"), dict) else {}
        filtered_items = []
        starter = app.loadout_starter_item(raw)
        if starter:
            filtered_items.append(starter)
        filtered_items.extend(app.core_loadout_items(raw))

        seen_item_keys: set[str] = set()
        for item in filtered_items:
            name = clean_item_display_name(app.display_build_item_alias(str(item.get("name") or "")))
            if not name or name == "Unknown Item" or name.lower().startswith("aspect "):
                continue
            category = str(item.get("category") or "").strip()
            if category not in {"Starter", "Tier 3", "Tier 2", "Tier 1"}:
                continue
            item_key = f"{name.lower()}|{category}|{item.get('slotIndex')}|{item.get('itemMasterId') or item.get('trackerId') or item.get('itemHexId')}"
            if item_key in seen_item_keys:
                continue
            seen_item_keys.add(item_key)

            record = collected.setdefault(name.lower(), {"name": name, "images": set(), "categories": set(), "sampleCount": 0})
            record["sampleCount"] += 1
            if item.get("imageUrl"):
                record["images"].add(str(item.get("imageUrl")))
            for image_url in item_image_candidates(item, name):
                record["images"].add(image_url)
            record["categories"].add(category)
    return collected


def empty_metadata(name: str) -> dict[str, Any]:
    return {"name": name, "displayName": name, "slug": item_slug(name), "source": "history", "sourceUrl": "", "summary": "", "cost": None, "itemType": "", "tags": [], "stats": [], "passive": ""}


def fetch_metadata(name: str, timeout: int = 20) -> dict[str, Any]:
    slug = item_slug(name)
    candidates = [slug]
    candidates.extend(SMITEBRAIN_ITEM_SLUG_ALIASES.get(str(name or "").strip().lower(), []))
    if "-s-" in slug:
        candidates.append(slug.replace("-s-", "s-"))
    if slug.endswith("-s"):
        candidates.append(slug[:-2] + "s")
    last_error = ""
    for candidate in dict.fromkeys(candidates):
        url = f"{BASE_URL}/{candidate}"
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.encoding = "utf-8"
            if response.status_code == 200 and ("SmiteBrain" in response.text or "SMITE 2 Item" in response.text):
                return parse_item_page(name, response.text, url)
            last_error = f"{response.status_code} {url}"
        except requests.RequestException as exc:
            last_error = str(exc)
    return {"name": name, "displayName": name, "slug": slug, "source": "local", "sourceUrl": "", "summary": "", "cost": None, "itemType": "", "tags": [], "stats": [], "passive": "", "error": last_error}


def merge_existing_value(new_value: Any, existing_value: Any) -> Any:
    if new_value not in (None, "", [], {}):
        return new_value
    return existing_value


def json_list_value(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def db_record(row: dict[str, Any], existing: dict[str, Any] | None = None) -> dict[str, Any]:
    existing = existing or {}
    return {
        "name": row.get("name") or row.get("displayName") or "",
        "display_name": merge_existing_value(row.get("displayName") or row.get("name"), existing.get("display_name")),
        "slug": merge_existing_value(row.get("slug") or item_slug(str(row.get("name") or "")), existing.get("slug")),
        "source": merge_existing_value(row.get("source") or "manual", existing.get("source")),
        "source_url": merge_existing_value(row.get("sourceUrl"), existing.get("source_url")),
        "image_url": merge_existing_value(row.get("imageUrl"), existing.get("image_url")),
        "summary": merge_existing_value(row.get("summary"), existing.get("summary")),
        "cost": merge_existing_value(row.get("cost"), existing.get("cost")),
        "item_type": merge_existing_value(row.get("itemType"), existing.get("item_type")),
        "tags": json_list_value(merge_existing_value(row.get("tags") if isinstance(row.get("tags"), list) else [], existing.get("tags"))),
        "stats": json_list_value(merge_existing_value(row.get("stats") if isinstance(row.get("stats"), list) else [], existing.get("stats"))),
        "passive": merge_existing_value(row.get("passive"), existing.get("passive")),
        "categories_seen": json_list_value(merge_existing_value(row.get("categoriesSeen") if isinstance(row.get("categoriesSeen"), list) else [], existing.get("categories_seen"))),
        "sample_count": int(row.get("sampleCount") or existing.get("sample_count") or 0),
    }


def existing_metadata_by_name() -> dict[str, dict[str, Any]]:
    try:
        rows = app.sb_select("item_metadata", {"select": "*"})
    except Exception:
        return {}
    return {str(row.get("name") or ""): row for row in rows if row.get("name")}


def upsert_metadata_rows(rows: list[dict[str, Any]]) -> None:
    existing_rows = existing_metadata_by_name()
    records = [db_record(row, existing_rows.get(str(row.get("name") or row.get("displayName") or ""))) for row in rows if row.get("name") or row.get("displayName")]
    for start in range(0, len(records), 75):
        chunk = records[start:start + 75]
        try:
            app.sb_upsert("item_metadata", chunk, "name")
        except Exception as exc:  # noqa: BLE001
            names = ", ".join(str(record.get("name") or "") for record in chunk[:8])
            raise RuntimeError(f"item_metadata upsert failed near rows {start + 1}-{start + len(chunk)} ({names}): {exc}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch or seed SMITE 2 item metadata into data/item_metadata.json and optionally Supabase.")
    parser.add_argument("--output", default="data/item_metadata.json", help="Output JSON path.")
    parser.add_argument("--limit", type=int, default=0, help="Optional limit for testing.")
    parser.add_argument("--sleep", type=float, default=0.15, help="Delay between item page requests.")
    parser.add_argument("--no-fetch", action="store_true", help="Only collect names/images/sample counts from stored match history; do not call an item website.")
    parser.add_argument("--upsert", action="store_true", help="Upsert generated rows into the Supabase item_metadata table.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    used = collect_used_items()
    usage_rows = sorted(used.values(), key=lambda row: str(row.get("name") or "").lower())
    if args.limit:
        usage_rows = usage_rows[: args.limit]

    rows = []
    for index, usage in enumerate(usage_rows, start=1):
        name = str(usage.get("name") or "")
        metadata = empty_metadata(name) if args.no_fetch else fetch_metadata(name)
        images = sorted(usage.get("images") or [])
        metadata["imageUrl"] = metadata.get("imageUrl") or (images[0] if images else "")
        metadata["categoriesSeen"] = sorted(value for value in (usage.get("categories") or []) if value)
        metadata["sampleCount"] = usage.get("sampleCount", 0)
        rows.append(metadata)
        print(f"[{index}/{len(usage_rows)}] {name}: {'ok' if metadata.get('sourceUrl') else metadata.get('error', 'missing')}")
        time.sleep(args.sleep)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    if args.upsert:
        upsert_metadata_rows(rows)
    print(json.dumps({"ok": True, "output": str(output), "items": len(rows), "withMetadata": sum(1 for row in rows if row.get("sourceUrl")), "upserted": bool(args.upsert)}, indent=2))


if __name__ == "__main__":
    main()


