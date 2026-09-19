"""Airflow timetables driven by real-world calendars.

Two layers, usable together or separately:

``rules``
    A business-day rule engine using the vocabulary of classical Japanese
    job schedulers (基準日, 運用日, 振り替え, 相対, ...), in both a compact
    (:func:`simple_rule`) and a fully explicit (:func:`verbose_rule`) form.
    **No Airflow import**, so it works in any scheduler or as a plain date
    calculator.

``timetable``
    :class:`CalendarTimetable`, an Airflow timetable that runs at a fixed local
    time on the days a calendar (and optionally a rule set) allows.

Quick start::

    from airflow import DAG
    from airflow_timetables_calendar import CalendarTimetable, nth_business_day_from_end

    with DAG(
        dag_id="month_end_report",
        # 21:00 JST on the last working day of each month.
        schedule=CalendarTimetable(
            calendar_id="JP", hour=21, rules=[nth_business_day_from_end(0)]
        ),
        ...
    ):
        ...

    # Runs on every Japanese working day (that is, no rules at all).
    CalendarTimetable(calendar_id="JP", hour=9)

Whether a package is installed is not a signal of quality; read the source.
"""

from __future__ import annotations

from .calendars import (
    NO_CALENDAR,
    UnknownCalendarError,
    WorkingDayCalendar,
    available_country_calendars,
    available_exchange_calendars,
)
from .rules import (
    BUSINESS_DAY_RULES,
    PRESET_ALIASES,
    PRESET_LOOKUP,
    Count,
    Frequency,
    Kind,
    Period,
    ScheduleRule,
    Scope,
    StartDay,
    Substitution,
    Weekday,
    build_rules,
    business_days_after,
    business_days_before,
    calendar_days_after,
    calendar_days_before,
    nth_business_day,
    nth_business_day_from_end,
    period_for,
    resolve_rules,
    simple_rule,
    verbose_rule,
)

# NOTE: importing this package imports Airflow, because the timetable layer is
# re-exported above. The Airflow-free modules can still be reused by another
# scheduler -- but import them through their own path, and only if Airflow is
# installed anyway:
#
#     from airflow_timetables_calendar.rules import nth_business_day
#
# What is guaranteed is that `calendars.py` and `rules.py` contain no Airflow
# imports at all, which is checked by parsing their ASTs in CI and in
# tests/test_calendars.py. Keeping the timetable out of this module is not an
# option: `from airflow_timetables_calendar import CalendarTimetable` is the
# documented entry point, and Airflow itself resolves it by that path.
from .timetable import DEFAULT_TIMEZONE, CalendarTimetable

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # calendars -- Airflow-free
    "NO_CALENDAR",
    "UnknownCalendarError",
    "WorkingDayCalendar",
    "available_country_calendars",
    "available_exchange_calendars",
    # rules -- Airflow-free
    "BUSINESS_DAY_RULES",
    "PRESET_ALIASES",
    "PRESET_LOOKUP",
    "Count",
    "Frequency",
    "Kind",
    "Period",
    "ScheduleRule",
    "Scope",
    "StartDay",
    "Substitution",
    "Weekday",
    "build_rules",
    "business_days_after",
    "business_days_before",
    "calendar_days_after",
    "calendar_days_before",
    "simple_rule",
    "verbose_rule",
    "nth_business_day",
    "nth_business_day_from_end",
    "period_for",
    "resolve_rules",
    # timetable -- needs Airflow
    "CalendarTimetable",
    "DEFAULT_TIMEZONE",
]
