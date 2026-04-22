from __future__ import annotations

import os
import time
from typing import Sequence

from airflow.sensors.base import BaseSensorOperator


class FileFreshnessSensor(BaseSensorOperator):
    """Vérifie qu'un fichier existe et qu'il est suffisamment récent."""

    template_fields: Sequence[str] = ("filepath",)

    def __init__(self, *, filepath: str, max_age_seconds: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.filepath = filepath
        self.max_age_seconds = max_age_seconds

    def poke(self, context: dict) -> bool:
        """Retourne `True` si le fichier existe et respecte la fenêtre de fraîcheur."""
        if not os.path.exists(self.filepath):
            self.log.info("Fichier absent: %s", self.filepath)
            return False

        age = time.time() - os.path.getmtime(self.filepath)
        if age > self.max_age_seconds:
            self.log.info("Fichier trop ancien: %.0f sec > %s sec", age, self.max_age_seconds)
            return False

        self.log.info("Fichier valide et récent: %s", self.filepath)
        return True

