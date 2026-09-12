"""Common interface every ingestion adapter must implement.

The whole point of this abstraction: whatever adapter runs, Bronze ends up
with the exact same shape — a dict keyed by entity name, each value a plain
list of JSON-serializable record dicts, containing only that day's data
(customers/subscriptions as a full as-of snapshot, the rest as deltas — see
the module docstring in ``include/saas/data_generator.py`` for why).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any

from include.saas.constants import ENTITY_NAMES

BronzePayload = dict[str, list[dict[str, Any]]]


class IngestionAdapter(ABC):
    """One adapter per real-world origin a data team actually ingests from."""

    @abstractmethod
    def extract(self, execution_date: date) -> BronzePayload:
        """Return that day's raw payload for every entity in ``ENTITY_NAMES``.

        Must not apply any business-logic transformation — only enough
        normalization (encoding, container format) to produce the shared
        shape. Business rules belong in Silver, not here: Bronze exists to
        preserve auditability of "what we received," not to interpret it.
        """
        raise NotImplementedError


def empty_payload() -> BronzePayload:
    """A payload with every entity present but empty — useful when a source
    genuinely has nothing new for a given day (not an error)."""
    return {entity: [] for entity in ENTITY_NAMES}
