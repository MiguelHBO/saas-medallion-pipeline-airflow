"""Adapter for the most common real-world origin: someone exports data and
drops it in an agreed-upon location (a shared folder, a bucket, an SFTP
server, an email attachment saved to disk). Here that location is a local
directory; swapping it for a real bucket/SFTP client later means changing
only this file.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

from include.saas.constants import DATE_FMT, ENTITY_NAMES
from include.saas.data_generator import DEFAULT_INCOMING_DIR
from include.saas.ingestion.base import BronzePayload, IngestionAdapter

logger = logging.getLogger(__name__)


class FileDropAdapter(IngestionAdapter):
    def __init__(self, incoming_dir: Path = DEFAULT_INCOMING_DIR) -> None:
        self.incoming_dir = incoming_dir

    def extract(self, execution_date: date) -> BronzePayload:
        day_dir = self.incoming_dir / execution_date.strftime(DATE_FMT)
        if not day_dir.exists():
            raise FileNotFoundError(
                f"No file-drop export found for {execution_date} at {day_dir}. "
                "Populate it first with `python -m include.saas.data_generator`."
            )

        payload: BronzePayload = {}
        for entity in ENTITY_NAMES:
            path = day_dir / f"{entity}.json"
            if not path.exists():
                logger.warning("%s missing for %s — treating as empty.", path.name, execution_date)
                payload[entity] = []
                continue
            with open(path, encoding="utf-8") as fh:
                payload[entity] = json.load(fh)
        return payload
