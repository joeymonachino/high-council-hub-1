# High Council Hub

High Council Hub is a Flask + vanilla HTML/CSS/JavaScript app for SMITE 2 council ratings, personal rankings, rater profiles, chemistry, match history, item analysis, and recap/email workflows.

## Quick Start

```powershell
cd C:\Users\joeym\Repos\high-council-hub-1
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

Open `http://127.0.0.1:5000`.

## Secrets

Local secrets are read from `secrets/app_secrets.toml` first. Vercel uses environment variables with the same names.

Common values:

```toml
SUPABASE_URL = "https://your-project.supabase.co"
SUPABASE_KEY = "your-service-or-rest-key"
RATER_STATS_SYNC_KEY = "manual-sync-key"
RECAP_ADMIN_KEY = "optional-email-admin-key"
RESEND_API_KEY = "optional-resend-key"
RECAP_FROM_EMAIL = "High Council Hub <onboarding@resend.dev>"
RECAP_TEST_TO_EMAIL = "joeymonachino@gmail.com"
RECAP_TO_EMAILS = "player1@example.com,player2@example.com"
PIN_JOEY = "1234"
PIN_JAMI = "1234"
PIN_DARIAN = "1234"
PIN_MIKE = "1234"
```

`secrets/` is ignored by git. Do not commit real keys.

## Important Commands

Primary manual HAR import, supports SmiteSource and Tracker.gg HARs:

```powershell
python tools\import_match_har_to_supabase.py --har-file data\har_files\your_export.har --dry-run
python tools\import_match_har_to_supabase.py --har-file data\har_files\your_export.har
```

Backfill lightweight summary tables from raw match history:

```powershell
python tools\backfill_match_summary_tables.py --page-size 100 --batch-size 75
```

Refresh item metadata and optionally upsert it:

```powershell
python tools\fetch_item_metadata.py --sleep 0.1 --upsert
```

Audit item taxonomy after a patch:

```powershell
python tools\build_item_taxonomy.py --audit
```

Rebuild item taxonomy after reviewing changes:

```powershell
python tools\build_item_taxonomy.py
```

Backup Supabase tables:

```powershell
python tools\backup_supabase_tables.py
```

More detail lives in `docs/processes.md`.

## Repo Layout

```text
app.py                  Flask routes and server-side data assembly
hub/                    Shared config, HTTP session, and Supabase REST helpers
templates/              HTML shell
static/                 Browser app, feature scripts, and styling
assets/                 Local god/pantheon artwork
data/                   App snapshots, generated catalogs, SQL, HAR drops, backups
tools/importers/        Import and metadata refresh tools
tools/maintenance/      Cleanup/backfill/dedupe tools
tools/audit/            Audit/report tools
tools/backups/          Supabase backup tools
tools/legacy/           Old one-off scripts kept for reference
```

Compatibility wrappers remain in `tools/` for the commands we use often, so old commands still work.

## Deployment

Vercel runs `app.py` through `@vercel/python` using `vercel.json`. Add the same secrets as Vercel environment variables before deploying.

The app should not write local files in production. Manual/import workflows should write to Supabase, not Vercel's read-only filesystem.

## Frontend Files

`static/core.js` contains shared client state, cached DOM handles, API helpers, and formatting helpers.

`static/app.js` is the main controller and owns bootstrap loading, tab routing, and most tab renderers.

`static/items.js` owns the item catalog, item filters, and item detail modal.

`static/council-scroll.js` owns the Council Scroll recap view.

`static/styles.css` contains broad shell/layout/component rules. `static/items.css` and `static/rater-profile.css` contain feature-specific styles loaded after the base stylesheet.
