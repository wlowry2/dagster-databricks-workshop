import dagster as dg
from dagster.components import load_from_defs_folder
from pathlib import Path

defs = load_from_defs_folder(project_root=Path(__file__).parent.parent.parent)
