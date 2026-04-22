import os
import sys
from pathlib import Path


def bootstrap_project_paths() -> None:
    """Injecte la racine projet et `src` dans `sys.path` pour les imports DAG."""
    current_file = Path(__file__).resolve()
    project_root = current_file.parents[1]
    src_dir = project_root / "src"

    root_str = str(project_root)
    src_str = str(src_dir)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)

    os.environ.setdefault("ORCHESTRATION_PROJECT_ROOT", root_str)

