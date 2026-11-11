"""Pure scoring helpers shared by IHC ranking jobs.

All values handled here are in the persisted point scale (the legacy score x10).
"""

from __future__ import annotations

from datetime import date
from typing import Iterable


SUPPORTED_ALGORITHM_VERSIONS = {"v1", "v2"}


def validate_algorithm_version(raw: str | None, *, default: str = "v1") -> str:
    version = (raw or default).strip().lower()
    if version not in SUPPORTED_ALGORITHM_VERSIONS:
        supported = ", ".join(sorted(SUPPORTED_ALGORITHM_VERSIONS))
        raise ValueError(
            f"Unsupported ranking algorithm version: {version!r}. "
            f"Expected one of: {supported}."
        )
    return version


def calculate_conversion_bonus_awarded(
    current_popularity_delta: float | int | None,
    prior_positive_impulses: float | int,
    *,
    bonus_per_repeat: float = 5.0,
) -> float:
    """Return the newly earned conversion bonus for today's positive units.

    The first positive impulse in the rolling window earns no conversion bonus.
    Every subsequent positive unit earns ``bonus_per_repeat`` once. This means a
    rare +2 day can earn one repeat bonus even when there was no earlier +1.
    """

    current_positive = max(float(current_popularity_delta or 0), 0.0)
    prior_positive = max(float(prior_positive_impulses or 0), 0.0)
    repeats_before = max(prior_positive - 1.0, 0.0)
    repeats_after = max(prior_positive + current_positive - 1.0, 0.0)
    new_repeat_units = repeats_after - repeats_before
    return new_repeat_units * bonus_per_repeat


def calculate_conversion_momentum(
    bonus_events: Iterable[tuple[date, float | int]],
    target_date: date,
    *,
    decay_rate: float = 0.9,
    window_days: int = 28,
) -> float:
    """Return the decayed value of bonus events active on ``target_date``.

    A 28-day window includes event ages 0 through 27. Future events and events
    aged 28 days or more are ignored.
    """

    if not 0 < decay_rate <= 1:
        raise ValueError("decay_rate must be greater than 0 and at most 1.")
    if window_days <= 0:
        raise ValueError("window_days must be greater than 0.")

    total = 0.0
    for event_date, raw_amount in bonus_events:
        age_days = (target_date - event_date).days
        if age_days < 0 or age_days >= window_days:
            continue
        total += float(raw_amount or 0) * (decay_rate**age_days)
    return total
