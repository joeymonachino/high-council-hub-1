from pathlib import Path
import runpy

TARGET = Path(__file__).resolve().parent / 'maintenance/backfill_match_summary_tables.py'
runpy.run_path(str(TARGET), run_name="__main__")
