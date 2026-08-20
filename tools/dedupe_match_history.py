from pathlib import Path
import runpy

TARGET = Path(__file__).resolve().parent / 'maintenance/dedupe_match_history.py'
runpy.run_path(str(TARGET), run_name="__main__")
