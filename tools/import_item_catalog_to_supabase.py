from pathlib import Path
import runpy

TARGET = Path(__file__).resolve().parent / 'importers/import_item_catalog_to_supabase.py'
runpy.run_path(str(TARGET), run_name="__main__")
