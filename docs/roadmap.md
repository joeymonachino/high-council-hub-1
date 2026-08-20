# Roadmap

## Near-Term

- God abilities data and an Abilities tab inside each god card.
- God playstyle tags after ability data exists, including aspect-specific playstyles.
- Scroll/email nudge logic: played a lot recently but no rating/rank update.
- Scroll/email readability pass with compact recent-only snapshots.
- Email links that can deep-link into a god/rate workflow if we revisit the modal routing.
- Persistent rating/ranking input cleanup on mobile so stale local draft values do not confuse players.

## Data And Imports

- Keep improving the one-stop HAR importer for both SmiteSource and Tracker-derived exports.
- Preserve `smitesource_match_history` as the raw archive while reading from summary tables wherever possible.
- Continue item patch audits after every major item patch.
- Consider deeper raw_match extraction only if storage or read speed becomes a real issue.

## Items

- Continue hand-reviewing fallback taxonomy items.
- Improve item images where source mappings are wrong or missing.
- Add item descriptions/stats/passives to item modal views in a compact format.
- Add build guidance only if we find a reliable Joust-focused source or collect enough council history.

## UI Cleanup

- Split `static/app.js` into feature modules after the repo structure settles.
- Split `static/styles.css` into base/layout/component/mobile files.
- Eventually split `app.py` into Flask routes plus service modules.
