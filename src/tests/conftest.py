import sys
from pathlib import Path


def pytest_sessionstart(session) -> None:  # pragma: no cover
    """Ajoute `src` et la racine projet dans le path de test."""
    project_root = Path(__file__).resolve().parents[1]
    src_dir = project_root / "src"

    for candidate in (str(project_root), str(src_dir)):
        if candidate not in sys.path:
            sys.path.insert(0, candidate)

