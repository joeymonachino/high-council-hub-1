from pathlib import Path
import runpy

TARGET = Path(__file__).resolve().parent / 'audit/image_asset_audit.py'
runpy.run_path(str(TARGET), run_name="__main__")
