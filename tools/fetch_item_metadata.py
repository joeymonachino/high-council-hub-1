from pathlib import Path
import runpy

TARGET = Path(__file__).resolve().parent / 'importers/fetch_item_metadata.py'
runpy.run_path(str(TARGET), run_name="__main__")
