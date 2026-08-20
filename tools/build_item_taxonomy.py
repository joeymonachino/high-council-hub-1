from pathlib import Path
import runpy

TARGET = Path(__file__).resolve().parent / 'audit/build_item_taxonomy.py'
runpy.run_path(str(TARGET), run_name="__main__")
