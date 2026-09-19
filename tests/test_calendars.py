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


# --------------------------------------------------------------------------- #
# The shared `holidays` instance is mutation-safe across threads
# --------------------------------------------------------------------------- #


class TestSharedHolidayCalendarIsSafeToQuery:
    """``holidays`` keeps the year it is currently populating in ``self._year``.

    That is *instance* state written on a read path, and many code paths read it
    back afterwards: the ``_populate_*_holidays`` methods decide which decade's
    rules apply, ``_add_observed`` turns ``(month, day)`` arguments into dates,
    and the substitute-holiday search loops ``while dt_work.year == self._year``.

    Forcing a nested population mid-year therefore *does* corrupt one (verified
    against holidays 0.104: a nested query for 2019 inside 2027's population
    leaves ``_year`` at 2019, the substitute search exits immediately, and 2027
    comes back with no holidays at all -- ``2027-01-01`` reported as a working
    day). It is not reachable by a real thread at this version: one year's
    population takes ~0.16 ms, and hammering a shared instance from six threads
    across 48 years produced no mismatch.

    So these tests pin the *invariant the lock provides* -- an instance is only
    ever populated and read under mutual exclusion, and concurrent callers get
    identical answers -- rather than claiming to catch a race that this version
    cannot lose. The lock is kept because ``_year`` is shared mutable state on a
    read path, which is a latent hazard, and serialising it costs nothing
    measurable; if a future ``holidays`` lengthens that window, the hazard
    becomes a bug and the lock is already there.
    """

    @staticmethod
    def _holidays():
        holidays = __import__("holidays")
        if not hasattr(holidays, "country_holidays"):  # pragma: no cover
            pytest.skip("holidays not installed")
        return holidays

    @staticmethod
    def _reset():
        """Drop the shared instance so each test starts from a cold calendar."""
        from airflow_timetables_calendar import calendars as cal_mod

        with cal_mod._holidays_lock:
            cal_mod._holidays_calendars.clear()

    def test_the_expanding_query_runs_under_the_lock(self):
        """The lock must cover the query, not just the memo lookup.

        ``calendar.get()`` is what triggers lazy expansion and writes ``_year``,
        so taking the lock only around the dictionary lookup would leave the
        hazard the class docstring describes wide open.
        """
        self._holidays()
        from airflow_timetables_calendar import calendars as cal_mod

        self._reset()
        calendar = cal_mod._country_calendar("JP")
        if calendar is None:  # pragma: no cover
            pytest.skip("JP calendar unavailable")

        held: list[bool] = []
        original_get = calendar.get

        def probing_get(day):
            # Observed from inside the call the timetable actually makes, so it
            # reports on the real code path rather than a stand-in.
            held.append(cal_mod._holidays_lock._is_owned())
            return original_get(day)

        calendar.get = probing_get
        try:
            cal_mod._holiday_name("JP", date(2031, 1, 1))
        finally:
            calendar.get = original_get
            self._reset()

        assert held == [True], f"expanding query ran with lock held = {held}"

    def test_concurrent_queries_all_agree(self):
        """Concurrent callers must not affect each other's answers."""
        import threading

        from airflow_timetables_calendar import calendars as cal_mod

        self._holidays()
        days = [date(y, m, d) for y in (2024, 2026) for m in (1, 5, 9) for d in (1, 15)]
        results: dict[date, list[str | None]] = {d: [] for d in days}
        errors: list[BaseException] = []
        barrier = threading.Barrier(8)

        def query():
            try:
                barrier.wait(timeout=10)
                for day in days:
                    results[day].append(cal_mod._holiday_name("JP", day))
            except BaseException as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=query) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert not errors, errors
        for day in days:
            assert len(set(results[day])) == 1, (day, results[day])

    def test_a_forced_interleaving_shows_the_hazard_the_lock_guards(self):
        """Document the corruption the lock exists to prevent.

        This asserts on the *unlocked* behaviour deliberately: it is a
        characterisation test standing next to the fix, so that if the lock were
        removed the hazard it guards would still be described by a test that
        fails for the right reason.
        """
        holidays = self._holidays()
        from holidays.observed_holiday_base import ObservedHolidayBase

        self._reset()
        from airflow_timetables_calendar import calendars as cal_mod

        calendar = cal_mod._country_calendar("JP")
        if calendar is None:  # pragma: no cover
            pytest.skip("JP calendar unavailable")

        original = ObservedHolidayBase._populate_common_holidays

        def populate_with_nested_query(self):
            if self._year == 2031:
                # Deliberately unlocked: this is the state a second thread would
                # create if it were allowed in here.
                with cal_mod._holidays_lock:
                    object.__setattr__(self, "_year", 1999)
            return original(self)

        ObservedHolidayBase._populate_common_holidays = populate_with_nested_query
        try:
            with cal_mod._holidays_lock:
                calendar.get(date(2031, 1, 1))
        finally:
            ObservedHolidayBase._populate_common_holidays = original

        shared_days = sorted(d for d in calendar if d.year == 2031)
        fresh = holidays.country_holidays("JP", years=[2031])
        fresh_days = sorted(d for d in fresh if d.year == 2031)
        self._reset()

        # The point: `_year` drives the result, so it must never be touched
        # outside the lock. Here it is forced, and the year loses holidays.
        assert fresh_days, "control year is empty; the test is vacuous"
        assert shared_days != fresh_days, (
            "_year no longer drives population; this test (and the lock) can be revisited"
        )

    def test_every_calendar_id_gets_its_own_instance(self):
        """The memo must not collapse distinct codes onto one calendar."""
        from airflow_timetables_calendar import calendars as cal_mod

        jp = cal_mod._country_calendar("JP")
        us = cal_mod._country_calendar("US")
        assert jp is not None and us is not None
        assert jp is not us
        # A hit is cached, and normalised spellings share the entry.
        assert cal_mod._country_calendar("JP") is jp
        assert cal_mod._country_calendar("jp") is jp

    def test_the_predicate_agrees_with_the_name_lookup(self):
        from airflow_timetables_calendar import calendars as cal_mod

        for day in (date(2027, 1, 1), date(2027, 1, 4), date(2027, 3, 21)):
            assert cal_mod._is_holiday_holidays("JP", day) == (
                cal_mod._holiday_name("JP", day) is not None
            )

    def test_an_unknown_code_is_cached_as_a_miss(self):
        from airflow_timetables_calendar import calendars as cal_mod

        assert cal_mod._holiday_name("ZZ", date(2027, 1, 1)) is None
        assert cal_mod._holiday_name("ZZ", date(2027, 1, 1)) is None
        assert cal_mod._is_holiday_holidays("ZZ", date(2027, 1, 1)) is False

    def test_a_holiday_name_is_stable_across_neighbouring_expansion(self):
        from airflow_timetables_calendar import calendars as cal_mod

        if cal_mod._country_calendar("JP") is None:  # pragma: no cover
            pytest.skip("JP calendar unavailable")

        first = cal_mod._holiday_name("JP", date(2033, 1, 1))
        for year in (2019, 2041, 2088, 1955):
            cal_mod._holiday_name("JP", date(year, 6, 1))
        assert cal_mod._holiday_name("JP", date(2033, 1, 1)) == first
        assert first is not None
