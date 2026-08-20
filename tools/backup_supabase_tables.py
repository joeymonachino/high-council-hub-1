from pathlib import Path
import runpy

TARGET = Path(__file__).resolve().parent / 'backups/backup_supabase_tables.py'
runpy.run_path(str(TARGET), run_name="__main__")
