"""Tests for :class:`CalendarTimetable` -- calendar resolution, holiday handling,
rule dispatch and the no-rules default.

The Airflow-facing parts (serialization, the plugin entry point, cron stepping)
live in ``test_serialization.py``; this module only needs a timetable instance
as a *calendar*, so it stays close to the rules engine.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from airflow_timetables_calendar import CalendarTimetable, nth_business_day
from airflow_timetables_calendar.calendars import UnknownCalendarError


class TestConstruction:
    def test_summary_names_the_calendar_and_hour(self):
        tt = CalendarTimetable(calendar_id="JP", hour=21)
        assert "21" in tt.summary
        assert "holidays:JP" in tt.summary

    def test_an_unknown_rule_form_is_rejected(self):
        with pytest.raises(TypeError):
            CalendarTimetable(calendar_id="JP", rules=[123])

    def test_summary_counts_rules(self):
        tt = CalendarTimetable(calendar_id="JP", hour=21, rules=[nth_business_day(1)])
        assert "rules: 1" in tt.summary

    def test_summary_of_the_plain_calendar(self):
        tt = CalendarTimetable(calendar_id="NONE", hour=9, minute=30)
        assert "NONE" in tt.summary

    def test_unknown_calendar_is_rejected_at_construction(self):
        with pytest.raises(UnknownCalendarError):
            CalendarTimetable(calendar_id="ZZZZ")

    def test_calendar_id_is_normalised(self):
        assert CalendarTimetable(calendar_id="JP").calendar_id == "holidays:JP"
        assert CalendarTimetable(calendar_id="NONE").calendar_id == "NONE"
        assert CalendarTimetable(calendar_id="exchange:XLON").calendar_id == "exchange:XLON"

    def test_hour_accepts_the_48_hour_clock(self):
        for hour in (-47, -24, -1, 0, 9, 21, 23, 24, 25, 47):
            assert CalendarTimetable(calendar_id="JP", hour=hour).hour == hour

    @pytest.mark.parametrize("hour", [48, -48, 100, -100])
    def test_hour_outside_the_48_hour_clock_is_rejected(self, hour):
        with pytest.raises(ValueError, match="48-hour clock"):
            CalendarTimetable(calendar_id="JP", hour=hour)

    def test_minute_must_be_in_range(self):
        with pytest.raises(ValueError):
            CalendarTimetable(calendar_id="JP", minute=60)


class TestIsWorkingDay:
    """``is_working_day`` combines weekends, the calendar, and the date overrides."""

    def test_weekends_are_closed(self):
        tt = CalendarTimetable(calendar_id="NONE")
        assert tt.is_working_day(date(2026, 9, 19)) is False  # Saturday
        assert tt.is_working_day(date(2026, 9, 20)) is False  # Sunday

    def test_plain_weekday_is_open(self):
        tt = CalendarTimetable(calendar_id="NONE")
        assert tt.is_working_day(date(2026, 9, 18)) is True

    def test_holiday_is_closed(self):
        tt = CalendarTimetable(calendar_id="JP")
        assert tt.is_working_day(date(2026, 9, 21)) is False  # 敬老の日

    def test_shutdown_days_are_closed(self):
        tt = CalendarTimetable(calendar_id="NONE", exclude_dates=[date(2026, 9, 18)])
        assert tt.is_working_day(date(2026, 9, 18)) is False

    def test_include_dates_reopen_a_closed_day(self):
        # The company works the year-end shutdown, which the JP calendar closes.
        tt = CalendarTimetable(calendar_id="JP", include_dates=[date(2026, 12, 31)])
        assert tt.is_working_day(date(2026, 12, 31)) is True

    def test_include_dates_can_reopen_a_weekend(self):
        tt = CalendarTimetable(calendar_id="NONE", include_dates=[date(2026, 9, 19)])
        assert tt.is_working_day(date(2026, 9, 19)) is True

    def test_exclude_and_include_are_checked_in_that_order(self):
        # The order in is_working_day is: exclude, then include, then weekend,
        # then the holiday calendar. include_dates is consulted *first* and
        # short-circuits, so an include wins a tie against the exclude list --
        # the exclude branch is never reached for that day.
        tt = CalendarTimetable(
            calendar_id="NONE",
            include_dates=[date(2026, 9, 18)],
            exclude_dates=[date(2026, 9, 18)],
        )
        assert tt.is_working_day(date(2026, 9, 18)) is True


class TestNoRulesDefault:
    """An empty ``rules`` list means "every working day" -- the older behaviour."""

    def test_every_working_day_matches(self):
        tt = CalendarTimetable(calendar_id="JP", hour=21)
        assert tt.matches_rules(date(2026, 9, 18)) is True  # Friday
        assert tt.matches_rules(date(2026, 9, 24)) is True  # Thursday

    def test_closed_days_do_not_match(self):
        tt = CalendarTimetable(calendar_id="JP", hour=21)
        assert tt.matches_rules(date(2026, 9, 19)) is False  # Saturday
        assert tt.matches_rules(date(2026, 9, 21)) is False  # holiday

    def test_none_calendar_matches_any_weekday(self):
        tt = CalendarTimetable(calendar_id="NONE")
        assert tt.matches_rules(date(2026, 9, 21)) is True  # holiday is a workday here


class TestRulesDispatch:
    """``rules`` accepts presets, kwargs dicts and rules, mixed."""

    def test_a_bare_preset_name_works(self):
        tt = CalendarTimetable(calendar_id="JP", hour=21, rules=["当月末営業日"])
        assert tt.matches_rules(date(2026, 9, 30)) is True
        assert tt.matches_rules(date(2026, 9, 29)) is False

    def test_a_kwargs_dict_works(self):
        tt = CalendarTimetable(calendar_id="JP", hour=21, rules=[nth_business_day(10)])
        assert tt.matches_rules(date(2026, 9, 14)) is True
        assert tt.matches_rules(date(2026, 9, 15)) is False

    def test_mixed_forms_compose(self):
        tt = CalendarTimetable(
            calendar_id="JP",
            hour=21,
            rules=["当月末営業日", nth_business_day(1)],
        )
        assert tt.matches_rules(date(2026, 9, 30)) is True
        assert tt.matches_rules(date(2026, 9, 1)) is True
        assert tt.matches_rules(date(2026, 9, 15)) is False

    def test_an_unknown_preset_name_is_rejected(self):
        with pytest.raises(KeyError, match="unknown preset"):
            CalendarTimetable(calendar_id="JP", rules=["存在しない"])

    def test_a_rule_that_lands_on_a_closed_day_is_dropped(self):
        # 実行しない (the default substitution) on a weekend anchor.
        tt = CalendarTimetable(
            calendar_id="NONE",
            rules=[{"kind": "absolute", "day": 19}],  # Saturday
        )
        assert tt.matches_rules(date(2026, 9, 19)) is False

    def test_base_day_is_passed_through(self):
        # 前月末営業日 with 基準日=26 reaches back a whole period. 月末指定 is 「基
        # 準日の指定に基づいた期間を1か月とし，「月の最終日から何日前の運用日」」,
        # so each period's run is the closing 運用日 of the period *before* it.
        #
        # The period containing 09-30 runs 09-26..10-25, so its run is the last
        # working day of 08-26..09-25 -- 09-25, a Friday. 09-30 belongs to that
        # same period and is *not* a run: it is the following period, whose own
        # reach lands on 10-23, that owns the September-into-October month.
        tt = CalendarTimetable(calendar_id="NONE", rules=["前月末営業日"], base_day=26)
        assert tt.is_working_day(date(2026, 9, 30)) is True
        assert tt.matches_rules(date(2026, 9, 25)) is True
        assert tt.matches_rules(date(2026, 9, 29)) is False
        assert tt.matches_rules(date(2026, 9, 30)) is False
        # Every period reaches back exactly one period, so the rule fires once a
        # month -- the closing 運用日 of the previous period.
        assert tt.matches_rules(date(2026, 10, 23)) is True


class TestExcludeIncludeAndRules:
    """``matches_rules`` deliberately does *not* re-check the calendar.

    The rules engine is given the timetable as its calendar, so a rule that
    already respects holidays does not need a second filter -- and adding one
    would silently break ``RunAnyway``.
    """

    def test_rules_run_anyway_on_an_excluded_day(self):
        tt = CalendarTimetable(
            calendar_id="NONE",
            exclude_dates=[date(2026, 9, 18)],
            rules=[{"kind": "absolute", "day": 18, "substitution": "run_anyway"}],
        )
        assert tt.is_working_day(date(2026, 9, 18)) is False
        assert tt.matches_rules(date(2026, 9, 18)) is True


class TestFortyEightHourClock:
    """The 48-hour clock shifts which business date a run belongs to."""

    HOURS = (-47, -24, -1, 0, 9, 21, 23, 24, 25, 47)

    @pytest.mark.parametrize("hour", HOURS)
    def test_run_lands_on_a_working_business_date(self, hour):
        tt = CalendarTimetable(calendar_id="JP", hour=hour, rules=["毎営業日"])
        moment = datetime(2026, 6, 1, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
        for _ in range(60):
            moment = tt._get_next(moment)
            assert tt.is_working_day(tt._business_date(moment)), moment

    @pytest.mark.parametrize("hour", HOURS)
    def test_get_prev_mirrors_get_next(self, hour):
        tt = CalendarTimetable(calendar_id="JP", hour=hour, rules=["毎営業日"])
        moment = datetime(2026, 6, 1, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
        forward = []
        for _ in range(40):
            moment = tt._get_next(moment)
            forward.append(moment)

        backward = []
        cursor = forward[-1] + timedelta(minutes=1)
        for _ in range(len(forward)):
            cursor = tt._get_prev(cursor)
            backward.append(cursor)
        backward.reverse()

        assert backward == forward

    @pytest.mark.parametrize(
        ("hour", "day_offset", "hour_of_day"),
        [
            (0, 0, 0),
            (9, 0, 9),
            (21, 0, 21),
            (23, 0, 23),
            (24, 1, 0),
            (25, 1, 1),
            (47, 1, 23),
            (-1, -1, 23),
            (-24, -1, 0),
            (-47, -2, 1),
        ],
    )
    def test_hour_splits_into_offset_and_wall_clock(self, hour, day_offset, hour_of_day):
        tt = CalendarTimetable(calendar_id="NONE", hour=hour)
        assert (tt._day_offset, tt._hour_of_day) == (day_offset, hour_of_day)

    @pytest.mark.parametrize(
        ("hour", "wall", "business"),
        [
            (21, "2026-09-18 21:00", "2026-09-18"),
            (24, "2026-09-19 00:00", "2026-09-18"),
            (25, "2026-09-19 01:00", "2026-09-18"),
            (47, "2026-09-19 23:00", "2026-09-18"),
            (-1, "2026-09-18 23:00", "2026-09-19"),
            (-24, "2026-09-18 00:00", "2026-09-19"),
        ],
    )
    def test_business_date_shifts_across_the_day_boundary(self, hour, wall, business):
        tt = CalendarTimetable(calendar_id="NONE", hour=hour)
        moment = datetime.strptime(wall, "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo("Asia/Tokyo"))
        assert tt._business_date(moment).isoformat() == business

    @pytest.mark.parametrize("hour", [-47, -1, 24, 25, 47])
    def test_serialize_round_trip_keeps_the_declared_hour(self, hour):
        tt = CalendarTimetable(calendar_id="JP", hour=hour)
        restored = CalendarTimetable.deserialize(tt.serialize())
        assert restored.hour == hour
        assert restored == tt
