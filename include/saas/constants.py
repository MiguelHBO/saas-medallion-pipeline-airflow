"""Shared domain vocabulary for the SaaS entities.

Kept in one place because the data generator, ingestion adapters, Silver
reconciliation, Pandera schemas, and Gold metrics all need to agree on the
same set of valid values.
"""

from __future__ import annotations

SEGMENTS: tuple[str, ...] = ("SMB", "Mid-Market", "Enterprise")

PLANS: tuple[str, ...] = ("Starter", "Growth", "Scale", "Enterprise")

# Monthly-normalized MRR range per plan, in USD. A plan sold on an annual
# billing cycle still contributes its monthly-equivalent value to MRR — that
# normalization is what makes "MRR" comparable across customers regardless of
# how they're billed.
PLAN_MRR_RANGE: dict[str, tuple[float, float]] = {
    "Starter": (49.0, 99.0),
    "Growth": (299.0, 599.0),
    "Scale": (999.0, 2999.0),
    "Enterprise": (3000.0, 15000.0),
}

BILLING_CYCLES: tuple[str, ...] = ("monthly", "annual")

SUBSCRIPTION_STATUSES: tuple[str, ...] = ("trialing", "active", "past_due", "canceled")

# Statuses that count as "currently paying" for MRR/ARR purposes.
MRR_ELIGIBLE_STATUSES: tuple[str, ...] = ("active", "past_due")

SUBSCRIPTION_EVENT_TYPES: tuple[str, ...] = (
    "created",
    "upgraded",
    "downgraded",
    "canceled",
    "reactivated",
)

INVOICE_STATUSES: tuple[str, ...] = ("paid", "failed", "pending")

USAGE_FEATURES: tuple[str, ...] = (
    "dashboard",
    "reports",
    "api",
    "integrations",
    "admin_settings",
    "billing_portal",
    "data_export",
    "automation",
)

COUNTRIES: tuple[str, ...] = (
    "US",
    "BR",
    "GB",
    "DE",
    "FR",
    "CA",
    "AU",
    "PT",
    "ES",
    "NL",
)

ENTITY_NAMES: tuple[str, ...] = (
    "customers",
    "subscriptions",
    "subscription_events",
    "invoices",
    "usage_events",
)

DATE_FMT = "%Y-%m-%d"
