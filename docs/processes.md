# High Council Hub Processes

This is the operational runbook for the repo. The goal is that every recurring maintenance task has a boring, repeatable command.

## Manual Match Import From HAR

Use this when SmiteSource or Tracker.gg has data you need to preserve in Supabase.

1. Export a browser HAR after loading the relevant match pages.
2. Put the file in `data/har_files/` locally.
3. Run a dry-run first:

```powershell
python tools\import_match_har_to_supabase.py --har-file data\har_files\your_file.har --dry-run
```

4. Check `missing`, `skippedExactKey`, and `skippedCrossSource`.
5. If it looks right, run the real import:

```powershell
python tools\import_match_har_to_supabase.py --har-file data\har_files\your_file.har
```

6. Rebuild summaries if needed:

```powershell
python tools\backfill_match_summary_tables.py --page-size 100 --batch-size 75
```

### Dedupe Logic

The importer writes to `smitesource_match_history` and also feeds the summary-table pipeline. It dedupes by exact `record_key` and a cross-source duplicate signature so Tracker and SmiteSource copies of the same player/match should not double-count.

## Match Summary Backfill

Use this after importer/extractor changes or after adding historical rows.

```powershell
python tools\backfill_match_summary_tables.py --page-size 100 --batch-size 75 --dry-run
python tools\backfill_match_summary_tables.py --page-size 100 --batch-size 75
```

Use `--cleanup-items` only after inspecting a dry-run. It removes `match_item_summary` rows that the current extractor no longer produces.

## Item Metadata Refresh

Use this when we need item descriptions, stats, passives, images, or new item rows.

```powershell
python tools\fetch_item_metadata.py --sleep 0.1 --upsert
```

If a site blocks or resets the connection, slow it down:

```powershell
python tools\fetch_item_metadata.py --sleep 0.5 --upsert
```

## Item Taxonomy Patch Audit

Use this after SMITE patches change items.

```powershell
python tools\build_item_taxonomy.py --audit
```

Read these fields:

- `newItems`: item metadata has an item our taxonomy has never seen.
- `removedItems`: taxonomy has an item no longer present in metadata.
- `changedItems`: item stats/passive/type/cost changed since the taxonomy was reviewed.
- `missingFingerprints`: old taxonomy rows need a rebuild to support future audits.
- `unreviewedItems`: current fallback tags, useful but not necessarily urgent.

After reviewing and editing curated tags in `tools/audit/build_item_taxonomy.py`, rebuild:

```powershell
python tools\build_item_taxonomy.py
```

For a CI-style check that exits non-zero when patch review is needed:

```powershell
python tools\build_item_taxonomy.py --audit --fail-on-changes
```

## Supabase Backup

Run before large imports, cleanup, schema changes, or anything that makes your stomach do the little goblin flip.

```powershell
python tools\backup_supabase_tables.py
```

Backups are local operational files and should generally not be committed.

## Image Asset Audit

Use this when god images are broken or placeholders appear.

```powershell
python tools\image_asset_audit.py
```

The report is written to `tools/audit/image_asset_report.md`.

## Vercel Notes

Vercel has a read-only filesystem at runtime. Anything that needs to persist must go to Supabase. Local JSON files are fallbacks/snapshots, not production storage.
