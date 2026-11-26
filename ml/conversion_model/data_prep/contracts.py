"""Shared event and raw-input definitions for purchase prediction."""

from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class Event:
    source_member: str
    source_crc32: str
    source_record: int
    time: datetime
    event_type: str
    user_id: str
    product_id: str
    category_id: str | None
    brand: str | None
    raw_price: str

    @property
    def event_id(self) -> str:
        return f"{self.source_member}:{self.source_crc32}:{self.source_record}"


@dataclass(frozen=True)
class RawFeatures:
    view_count: int
    cart_count: int
    distinct_product_count: int
    visit_elapsed_seconds: int
    previous_event_gap_seconds: int
    is_cart: int
    price: Decimal | None
    price_missing: int
    brand: str | None
    category_id: str | None


FEATURE_COLUMNS = tuple(field.name for field in fields(RawFeatures))


def utc_iso(value: datetime | None) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z") if value is not None else ""
