"""Decide eligibility and purchase labels after the relevant observation cutoff."""

from dataclasses import dataclass
from datetime import datetime, timedelta

from ml.conversion_model.data_prep.visits import INACTIVITY, Visit


CURRENT_LABEL_VERSION = "purchase-label-v3"


@dataclass(frozen=True)
class Window:
    name: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class Label:
    status: str
    reason: str = ""
    value: int | None = None
    available_at: datetime | None = None


def pending_visit_reason(started: datetime, ended: datetime | None, window_start: datetime,
                         window_end: datetime, complete_before: datetime) -> str:
    """Distinguish a known window crossing from a visit whose close is not yet observed."""
    if started < window_start:
        return "crosses_window_boundary"
    if ended is None or ended >= complete_before:
        return "awaiting_visit_close"
    if ended >= window_end:
        return "crosses_window_boundary"
    return ""


def label_candidate(
    visit: Visit,
    event_time: datetime,
    first_purchase: datetime | None,
    window: Window,
    observation_start: datetime,
    complete_before: datetime,
) -> Label:
    label = _label_at_visit_close(visit, event_time, first_purchase, window, observation_start, complete_before)
    if label.available_at is not None and label.available_at >= min(window.end, complete_before):
        return Label("pending", "awaiting_truth")
    return label


def _label_at_visit_close(visit, event_time, first_purchase, window, observation_start, complete_before):
    if first_purchase == event_time:
        return Label("excluded", "same_time_as_first_purchase")
    if visit.start <= observation_start + INACTIVITY:
        return Label("excluded", "insufficient_history")
    reason = pending_visit_reason(visit.start, visit.deadline, window.start, window.end, complete_before)
    if reason:
        return Label("pending", reason)
    # A watermark is exclusive. Source timestamps are in whole seconds.
    return Label("confirmed", value=int(first_purchase is not None), available_at=visit.deadline + timedelta(seconds=1))


def legacy_dataset_label(visit, event_time, first_purchase, window, observation_start, complete_before, input_issue):
    """Keep the historical row selection and reason codes for frozen experiments."""
    if input_issue and first_purchase != event_time:
        return Label("excluded", input_issue)
    label = _label_at_visit_close(visit, event_time, first_purchase, window, observation_start, complete_before)
    if label.status == "pending" and (visit.start < window.start
                                      or window.end <= complete_before and visit.deadline >= window.end):
        return Label("excluded", "crosses_window_boundary")
    return label
