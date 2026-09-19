"""Airflow timetables driven by real-world calendars.

Two layers, usable together or separately:

``rules``
    A business-day rule engine modelled on JP1/AJS3 and NEC WebSAM JobCenter
    vocabulary (基準日, 運用日, 振り替え, 相対, ...). **No Airflow import**, so it
    works in any scheduler or as a plain date calculator.

``timetable``
    :class:`CalendarTimetable`, an Airflow timetable that runs at a fixed local
    time on the days a calendar (and optionally a rule set) allows.

Quick start::

    from airflow import DAG
    from airflow_timetables_calendar import CalendarTimetable, nth_business_day

    with DAG(
        dag_id="month_end_report",
        # 21:00 JST on the last working day of each month.
        schedule=CalendarTimetable(calendar_id="JP", hour=21, rules=nth_business_day(1)),
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
    jobcenter,
    jp1,
    nth_business_day,
    nth_business_day_from_end,
    period_for,
    resolve_rules,
)
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
    "jobcenter",
    "jp1",
    "nth_business_day",
    "nth_business_day_from_end",
    "period_for",
    "resolve_rules",
    # timetable -- needs Airflow
    "CalendarTimetable",
    "DEFAULT_TIMEZONE",
]
