"""Source-agnostic Bronze ingestion.

``ingest(source, execution_date)`` is the only entry point the DAG calls —
it doesn't know or care whether the data came from a dropped file or a
database extract, and neither does anything downstream of Bronze. Adding a
real third origin later (an SFTP drop, a vendor API) means writing one more
adapter that implements :class:`~include.saas.ingestion.base.IngestionAdapter`
and registering it in ``_ADAPTERS`` below — Silver and Gold never change.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Literal

from include.saas.constants import DATE_FMT, ENTITY_NAMES
from include.saas.ingestion.base import BronzePayload, IngestionAdapter

logger = logging.getLogger(__name__)

Source = Literal["file_drop", "db_extract"]

REPO_ROOT = Path(__file__).resolve().parents[3]
BRONZE_DIR = REPO_ROOT / "include" / "data" / "bronze"


def _build_adapter(source: Source) -> IngestionAdapter:
    if source == "file_drop":
        from include.saas.ingestion.file_drop import FileDropAdapter

        return FileDropAdapter()
    if source == "db_extract":
        from include.saas.ingestion.db_extract import DbExtractAdapter

        return DbExtractAdapter()
    raise ValueError(f"Unknown ingestion source: {source!r} (expected 'file_drop' or 'db_extract')")


def ingest(source: Source, execution_date: date) -> BronzePayload:
    """Pull one day's raw payload from ``source``, normalized to the shared
    Bronze shape (see :mod:`include.saas.ingestion.base`)."""
    adapter = _build_adapter(source)
    return adapter.extract(execution_date)


def persist_bronze(
    execution_date: date, payload: BronzePayload, base_dir: Path = BRONZE_DIR
) -> Path:
    """Write the ingested payload to ``base_dir/dt=<execution_date>/*.json`` —
    the audit-trail record of "what we received," untouched by business logic
    (spec §6.2). This is also the hand-off point between the Bronze and
    Silver Airflow DAGs: they're separate DAG runs, so they communicate
    through this filesystem contract, not through XCom or an in-memory call.
    """
    day_dir = base_dir / f"dt={execution_date.strftime(DATE_FMT)}"
    day_dir.mkdir(parents=True, exist_ok=True)
    for entity in ENTITY_NAMES:
        with open(day_dir / f"{entity}.json", "w", encoding="utf-8") as fh:
            json.dump(payload.get(entity, []), fh, default=str)
    return day_dir


def read_bronze(execution_date: date, base_dir: Path = BRONZE_DIR) -> BronzePayload:
    """Read back one day's persisted Bronze payload (what Silver consumes)."""
    day_dir = base_dir / f"dt={execution_date.strftime(DATE_FMT)}"
    if not day_dir.exists():
        raise FileNotFoundError(
            f"No Bronze partition for {execution_date} at {day_dir} — "
            "has bronze_ingestion run (and validated) for this date?"
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
