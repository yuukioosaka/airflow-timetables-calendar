"""Fixtures shared by the test suite."""

from __future__ import annotations

from datetime import date

import pytest

from airflow_timetables_calendar import CalendarTimetable
from airflow_timetables_calendar.calendars import WorkingDayCalendar


class SyntheticCalendar:
    """A deterministic calendar: weekends plus an explicit closed set.

    Tests that care about *rule* behaviour use this rather than the real JP
    calendar, so a change in Japanese holiday data cannot make them flap.
    """

    def __init__(self, closed: set[date] | None = None):
        self.closed = closed or set()

    def is_working_day(self, day: date) -> bool:
        return day.weekday() < 5 and day not in self.closed

    def holiday_name(self, day: date) -> str | None:
        return "closed" if not self.is_working_day(day) else None


#: A September 2026 pattern: 21/22/23 (a real JP holiday block, mirrored here) and
#: a mid-month 10th/11th block so multi-day walks are exercised.
SEPTEMBER_CLOSED = {
    date(2026, 9, 21),
    date(2026, 9, 22),
    date(2026, 9, 23),
    date(2026, 9, 10),
    date(2026, 9, 11),
}


@pytest.fixture
def calendar() -> SyntheticCalendar:
    """The default synthetic calendar."""
    return SyntheticCalendar(SEPTEMBER_CLOSED)


@pytest.fixture
def open_calendar() -> SyntheticCalendar:
    """Weekends closed, no extra holidays."""
    return SyntheticCalendar()


@pytest.fixture
def jp_timetable() -> CalendarTimetable:
    """A real Japanese calendar, 21:00 JST, no rules."""
    return CalendarTimetable(calendar_id="JP", hour=21)


@pytest.fixture
def workdays_of_jp(jp_timetable):
    """Return a function giving the working days of a JP month, in order."""

    def _workdays(year: int, month: int) -> list[date]:
        days: list[date] = []
        day = date(year, month, 1)
        while day.month == month:
            if jp_timetable.is_working_day(day):
                days.append(day)
            day = day.fromordinal(day.toordinal() + 1)
        return days

    return _workdays


__all__ = ["SEPTEMBER_CLOSED", "SyntheticCalendar", "WorkingDayCalendar"]
