"""Tests for calendar id resolution in :mod:`airflow_timetables_calendar.calendars`.

``resolve_calendar`` returns a ``(kind, normalized_id, checker)`` triple rather
than a calendar object, so most of these assert on the triple and on the checker
behaviour. End-to-end behaviour through the timetable is in ``test_timetable.py``.
"""

from __future__ import annotations

from datetime import date

import pytest

from airflow_timetables_calendar.calendars import (
    NO_CALENDAR,
    UnknownCalendarError,
    WorkingDayCalendar,
    available_country_calendars,
    available_exchange_calendars,
    resolve_calendar,
)


class TestBareCodes:
    """A bare code is tried as a country first, then as an exchange."""

    def test_country_code_resolves_as_holidays(self):
        kind, code, checker = resolve_calendar("JP")
        assert kind == "holidays"
        assert code == "JP"
        assert checker is not None
        assert checker(code, date(2026, 1, 1)) is True  # 元日
        assert checker(code, date(2026, 9, 18)) is False

    def test_lowercase_is_normalised(self):
        assert resolve_calendar("jp")[1] == "JP"

    def test_underscores_become_hyphens(self):
        kind, code, _ = resolve_calendar("US_CA")
        assert kind == "holidays"
        assert code == "US-CA"

    def test_unknown_code_raises(self):
        with pytest.raises(UnknownCalendarError):
            resolve_calendar("ZZZZ")

    def test_unknown_code_error_lists_the_prefixes(self):
        with pytest.raises(UnknownCalendarError) as excinfo:
            resolve_calendar("ZZZZ")
        message = str(excinfo.value)
        assert "ZZZZ" in message
        assert "exchange:" in message

    def test_a_valid_country_with_a_bad_subdivision_names_the_country(self):
        with pytest.raises(UnknownCalendarError) as excinfo:
            resolve_calendar("JP-XX")
        message = str(excinfo.value)
        assert "JP-XX" in message
        assert "exchange:" in message


class TestForcedPrefixes:
    """Prefixes remove the guesswork about which registry to consult."""

    @pytest.mark.parametrize("prefix", ["country:", "holidays:", "financial:"])
    def test_holidays_prefixes(self, prefix):
        kind, code, checker = resolve_calendar(f"{prefix}JP")
        assert kind == "holidays"
        assert code == "JP"
        assert checker(code, date(2026, 1, 1)) is True

    @pytest.mark.parametrize("prefix", ["exchange:", "market:"])
    def test_exchange_prefixes(self, prefix):
        result = resolve_calendar(f"{prefix}XLON")
        assert result[0] == "exchange"
        assert result[1] == "XLON"

    def test_an_exchange_prefix_is_uppercased(self):
        assert resolve_calendar("exchange:xlon")[1] == "XLON"

    def test_a_bad_code_under_a_forced_prefix_raises(self):
        with pytest.raises(UnknownCalendarError):
            resolve_calendar("exchange:NOT_A_MARKET")
        with pytest.raises(UnknownCalendarError):
            resolve_calendar("country:NOT_A_COUNTRY")


class TestNone:
    """``NONE`` is the plain Mon-Fri calendar with no holiday data at all."""

    @pytest.mark.parametrize("code", ["NONE", "none", "None", ""])
    def test_none_has_no_checker(self, code):
        kind, name, checker = resolve_calendar(code)
        assert kind == "none"
        assert name == NO_CALENDAR
        assert checker is None

    def test_none_is_the_default_for_a_blank_id(self):
        assert resolve_calendar("   ")[0] == "none"


class TestWorkingDayCalendarProtocol:
    """The rules engine depends on the protocol, not on a concrete class."""

    def test_a_timetable_satisfies_the_protocol(self):
        # `isinstance` works on the runtime_checkable protocol, but `issubclass`
        # does not: Airflow's Timetable brings properties into the structural
        # match, and protocols with non-method members refuse issubclass.
        from airflow_timetables_calendar import CalendarTimetable

        tt = CalendarTimetable(calendar_id="JP")
        # `isinstance` is available because the protocol is runtime_checkable, but
        # it also requires holiday_name(), whose protocol body is `...` rather
        # than a default implementation. The timetable defines both.
        assert isinstance(tt, WorkingDayCalendar)
        assert callable(tt.holiday_name)
        assert tt.is_working_day(date(2026, 9, 18)) is True
        assert tt.holiday_name(date(2026, 9, 21)) is not None

    def test_an_ad_hoc_object_is_accepted_by_the_rules_engine(self):
        # The protocol is structural and duck-typed in practice: the rules engine
        # only ever calls is_working_day(), so a caller can pass any object with
        # that method and never import this library's classes.
        from airflow_timetables_calendar import ScheduleRule, period_for

        class MyCalendar:
            def is_working_day(self, day: date) -> bool:
                return day.weekday() < 5

            def holiday_name(self, day: date) -> str | None:
                return None

        rule = ScheduleRule(kind="absolute", day=15)
        assert rule.resolve(period_for(date(2026, 9, 1)), MyCalendar()) == date(2026, 9, 15)


class TestRegistryListings:
    """The listings back the error message and are useful for discovery."""

    def test_holidays_countries_are_listed(self):
        codes = available_country_calendars()
        assert "JP" in codes
        assert codes == sorted(codes)

    def test_exchanges_are_listed_when_the_extra_is_installed(self):
        codes = available_exchange_calendars()
        # pandas_market_calendars is an optional extra; if it is present, the
        # names are mostly 4-letter MIC codes.
        if codes:
            assert any(len(code) == 4 for code in codes)
            assert codes == sorted(codes)
        else:  # pragma: no cover - depends on the environment
            pytest.skip("pandas_market_calendars is not installed")
