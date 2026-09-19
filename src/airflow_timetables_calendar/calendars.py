"""Calendar resolution, independent of Airflow.

This module answers one question -- "is this day closed?" -- for as many calendars
as possible, by delegating to libraries that already maintain the data:

* ``holidays``                -> public holidays for 500+ countries/subdivisions,
                                 plus financial-market calendars
* ``pandas_market_calendars`` -> trading days of 200+ exchanges (optional extra)

It deliberately has **no Airflow import**: the business-day rule engine in
:mod:`airflow_timetables_calendar.rules` builds on this module, and keeping the
two Airflow-free means the rules can be reused in a non-Airflow scheduler (an
in-house batch runner, a one-off date calculator, ...) without pulling in the
whole Airflow dependency tree.

Calendars are identified by a string. A bare code is resolved against both
registries, so ``"JP"`` (a country) and ``"NYSE"`` (an exchange) both just work.
Where a code exists in both, an explicit prefix forces one:

    ``country:`` / ``holidays:`` / ``financial:``   -> ``holidays``
    ``exchange:`` / ``market:``                     -> ``pandas_market_calendars``

``"NONE"`` means "no holiday data at all", i.e. plain Mon-Fri.
"""

from __future__ import annotations

import logging
from datetime import date
from functools import cache
from typing import Protocol, runtime_checkable

log = logging.getLogger(__name__)

#: ``calendar_id`` value meaning "do not consult any holiday data".
NO_CALENDAR = "NONE"

_HOLIDAYS_PREFIXES = ("country:", "holidays:", "financial:")
_EXCHANGE_PREFIXES = ("exchange:", "market:")


class UnknownCalendarError(LookupError):
    """Raised when a calendar id is in neither registry."""


@runtime_checkable
class WorkingDayCalendar(Protocol):
    """Minimal calendar interface the rule engine needs.

    A ``Protocol`` rather than a base class so that callers can pass any object
    with the right shape -- including their own -- without importing this library's
    classes. :class:`~airflow_timetables_calendar.timetable.CalendarTimetable`
    satisfies it.
    """

    def is_working_day(self, day: date) -> bool:
        """Whether ``day`` is a working day."""
        ...

    def holiday_name(self, day: date) -> str | None:
        """The holiday's name, or None. Optional; used only for diagnostics."""
        return None


def _optional_import(name: str):
    """Import a soft dependency, returning None and warning once if absent."""
    try:
        return __import__(name)
    except ImportError:  # pragma: no cover - depends on the environment
        log.warning(
            "%r is not installed, so calendars from that registry are unavailable. "
            "Install the %r extra to enable them.",
            name,
            "exchanges" if name == "pandas_market_calendars" else name,
        )
        return None


@cache
def _subdivisions(country_code: str) -> tuple[str, ...]:
    """Subdivision codes supported for a country, e.g. ('CA', 'NY', ...) for 'US'."""
    holidays = _optional_import("holidays")
    if holidays is None:
        return ()
    try:
        supported = holidays.list_supported_countries(include_aliases=False)
    except Exception:  # pragma: no cover - defensive: older `holidays` API
        return ()
    return tuple(supported.get(country_code.upper(), ()))


@cache
def _country_calendar(calendar_id: str):
    """Return a ``holidays`` calendar for a code, or None.

    ``holidays`` covers countries (``"JP"``), subdivisions (``"US-CA"``) and
    financial markets (``"XNYS"``, aliased by ``"NYSE"``), all through the same
    constructor, so one lookup serves all three.
    """
    holidays = _optional_import("holidays")
    if holidays is None:
        return None

    code = calendar_id.strip().upper().replace("_", "-")
    country, _, subdivision = code.partition("-")
    try:
        if subdivision:
            return holidays.country_holidays(country, subdiv=subdivision)
        return holidays.country_holidays(country)
    except NotImplementedError:
        return None
    except KeyError:  # unknown subdivision
        return None


@cache
def _exchange_calendar(calendar_id: str):
    """Return a ``pandas_market_calendars`` schedule for an exchange, or None."""
    mcal = _optional_import("pandas_market_calendars")
    if mcal is None:
        return None
    try:
        return mcal.get_calendar(calendar_id)
    except Exception:  # mcal raises a variety of errors for unknown codes
        return None


def _is_holiday_holidays(calendar_id: str, day: date) -> bool:
    calendar = _country_calendar(calendar_id)
    if calendar is None:
        return False
    # ``holidays`` entries also cover the observed/substitute day, so a Sunday
    # holiday that moves to Monday marks the Monday too.
    return day in calendar


def _is_holiday_exchange(calendar_id: str, day: date) -> bool:
    """True when an exchange calendar says ``day`` is not a trading day.

    ``pandas_market_calendars`` will happily extrapolate far into the future, but
    exchange rules are only authoritative over the range the calendar was built
    from, so refuse to guess outside it.
    """
    schedule = _exchange_calendar(calendar_id)
    if schedule is None:
        return False
    try:
        valid_days = schedule.valid_days(start_date=day, end_date=day)
    except Exception:  # pragma: no cover - defensive: bad range / bad code
        log.warning("exchange calendar %r could not evaluate %s", calendar_id, day)
        return False
    return len(valid_days) == 0


def _normalize_country(raw: str) -> str:
    return raw.strip().upper().replace("_", "-")


def _unresolved_message(raw: str) -> str:
    """Build an actionable error, including valid subdivisions when relevant."""
    code = _normalize_country(raw)
    country, _, _subdivision = code.partition("-")
    if country and _country_calendar(country) is not None:
        subs = _subdivisions(country)
        if subs:
            return (
                f"{raw!r} is not a known calendar, but {country!r} is. "
                f"Valid subdivisions: {', '.join(subs)}"
            )
    return (
        f"{raw!r} is neither a known `holidays` code nor a known exchange "
        f"code. Prefix with 'country:', 'financial:' or 'exchange:' to "
        f"disambiguate. "
        f"{len(available_country_calendars())} `holidays` and "
        f"{len(available_exchange_calendars())} exchange calendars are available."
    )


def resolve_calendar(calendar_id: str):
    """Return ``(kind, normalized_id, checker)`` for ``calendar_id``.

    ``calendar_id`` may be a country code (``JP``, ``US-CA``), an exchange code
    (``TSE``, ``NYSE``), ``NONE``, or either of those with an explicit prefix.
    """
    raw = (calendar_id or NO_CALENDAR).strip()
    if not raw or raw.upper() == NO_CALENDAR:
        return "none", NO_CALENDAR, None

    lowered = raw.lower()
    for prefix in _HOLIDAYS_PREFIXES:
        if lowered.startswith(prefix):
            code = raw[len(prefix) :]
            if _country_calendar(code) is None:
                raise UnknownCalendarError(
                    f"{code!r} is not a known code in the `holidays` library"
                )
            return "holidays", _normalize_country(code), _is_holiday_holidays

    for prefix in _EXCHANGE_PREFIXES:
        if lowered.startswith(prefix):
            code = raw[len(prefix) :].upper()
            if _exchange_calendar(code) is None:
                raise UnknownCalendarError(f"{code!r} is not a known exchange code")
            return "exchange", code, _is_holiday_exchange

    # No prefix: try `holidays` first (it covers countries, subdivisions and
    # financial markets), then the exchange calendars.
    if _country_calendar(raw) is not None:
        return "holidays", _normalize_country(raw), _is_holiday_holidays
    upper = raw.upper()
    if _exchange_calendar(upper) is not None:
        return "exchange", upper, _is_holiday_exchange

    raise UnknownCalendarError(_unresolved_message(raw))


def available_country_calendars() -> list[str]:
    """Country/subdivision codes supported by the installed ``holidays``."""
    holidays = _optional_import("holidays")
    if holidays is None:
        return []
    return sorted(holidays.list_supported_countries())


def available_exchange_calendars() -> list[str]:
    """Exchange codes supported by the installed ``pandas_market_calendars``."""
    mcal = _optional_import("pandas_market_calendars")
    if mcal is None:
        return []
    return sorted(mcal.get_calendar_names())


__all__ = [
    "NO_CALENDAR",
    "UnknownCalendarError",
    "WorkingDayCalendar",
    "available_country_calendars",
    "available_exchange_calendars",
    "resolve_calendar",
]
