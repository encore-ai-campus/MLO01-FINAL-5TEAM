"""Build visits from all events using the user's time and original record order."""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from ml.conversion_model.data_prep.contracts import Event


INACTIVITY = timedelta(minutes=30)


@dataclass(frozen=True)
class Visit:
    events: tuple[Event, ...]

    @property
    def visit_id(self) -> str:
        return self.events[0].event_id

    @property
    def start(self) -> datetime:
        return self.events[0].time

    @property
    def deadline(self) -> datetime:
        return self.events[-1].time + INACTIVITY

    @property
    def first_purchase(self) -> datetime | None:
        return next((event.time for event in self.events if event.event_type == "purchase"), None)


def group_visits(events: Iterable[Event]) -> Iterator[Visit]:
    ordered = sorted(events, key=lambda event: (event.user_id, event.time, event.source_member, event.source_record))
    current: list[Event] = []
    for event in ordered:
        if current and (current[-1].user_id != event.user_id or event.time - current[-1].time > INACTIVITY):
            yield Visit(tuple(current))
            current.clear()
        current.append(event)
    if current:
        yield Visit(tuple(current))
