# Data Model Notes

This app uses Supabase as the source of truth, with local JSON files as fallbacks/snapshots for development and safer bootstrapping.

## Core Tables

`gods_metadata`
Stores one row per god: name, title, pantheon, role, class, attack type, damage type, tier, rank, movement.

`council_ratings`
Stores council rating values per god. This powers the god cards, ratings, average score, and tier/rank display.

`personal_rankings`
Stores each council member's personal rank per god.

`rating_history`
Stores rating/rank activity for the Activity sub-tab and recap/history views.

## Match Tables

`smitesource_match_history`
Raw-ish canonical archive for imported match rows. Despite the historical name, this can contain SmiteSource and Tracker-derived imports. Keep this as the preservation layer.

`match_player_summary`
Flattened per-player/per-match summary used for fast rater profile and chemistry reads.

`match_item_summary`
Flattened per-player/per-match item rows used for the Items catalog and god build views.

## Item Tables

`item_metadata`
Item display names, slugs, images, cost, item type, stat lines, passive text, categories, and source info.

`data/item_taxonomy.json`
Curated app-side item category layer. This is intentionally reviewed by us because keyword-only tagging misclassifies items.

## Local Data Files

`data/gods_metadata.json`, `data/council_ratings.json`, and `data/item_metadata.json` are fallback snapshots.

`data/har_files/` is for local HAR drops and should not be treated as app data.

`data/supabase_backups/` contains local exports from Supabase backup scripts.

## Backend Modules

`hub/config.py`
Shared paths, player metadata, thresholds, SmiteSource links, and secrets lookup. Local TOML secrets and Vercel environment variables resolve through the same helper.

`hub/http.py`
Shared requests session used by the Flask app and maintenance tools.

`hub/supabase.py`
Small PostgREST helper layer for selects, paged selects, inserts, upserts, and targeted deletes. `app.py` imports and re-exports these helpers so older tools continue to work.

`hub/snapshots.py`
Local JSON fallback helpers used when Supabase is unavailable or when development needs a lightweight snapshot.
