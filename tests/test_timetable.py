"""Tests for :class:`CalendarTimetable` -- calendar resolution, holiday handling,
rule dispatch and the no-rules default.

The Airflow-facing parts (serialization, the plugin entry point, cron stepping)
live in ``test_serialization.py``; this module only needs a timetable instance
as a *calendar*, so it stays close to the rules engine.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import ClassVar
from zoneinfo import ZoneInfo

import pytest

from airflow_timetables_calendar import (
    CalendarTimetable,
    Kind,
    ScheduleRule,
    nth_business_day,
    period_for,
)
from airflow_timetables_calendar.calendars import UnknownCalendarError
from airflow_timetables_calendar.rules import _log_dead_flag
from conftest import SyntheticCalendar


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


class TestNoDayOfWeekFilterInCron:
    """The cron expression must not filter on the *run* weekday.

    A run belongs to a business date that the 48-hour clock can shift by a day,
    so ``hour=25`` makes a Friday business date run on a Saturday. A ``1-5``
    day-of-week field filters the run date instead, which silently dropped every
    Friday for every hour at or above 24.
    """

    def test_cron_expression_has_no_day_of_week_restriction(self):
        tt = CalendarTimetable(calendar_id="JP", hour=25)
        assert tt._expression.split()[-1] == "*"

    @pytest.mark.parametrize("hour", [24, 25, 47])
    def test_next_day_hours_still_produce_friday_business_dates(self, hour):
        tt = CalendarTimetable(calendar_id="JP", hour=hour, rules=["毎営業日"])
        moment = datetime(2026, 6, 1, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
        business_dates = []
        for _ in range(90):
            moment = tt._get_next(moment)
            business_dates.append(tt._business_date(moment))
        assert any(d.weekday() == 4 for d in business_dates)

    @pytest.mark.parametrize("hour", [24, 25, 47])
    def test_next_day_hours_cover_the_same_business_dates_as_2100(self, hour):
        # The 48-hour clock moves *when* a run happens, never *which* business
        # dates exist, so the set must be identical to the plain 21:00 schedule.
        def dates_for(hours):
            tt = CalendarTimetable(calendar_id="JP", hour=hours, rules=["毎営業日"])
            moment = datetime(2026, 6, 1, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
            out = []
            for _ in range(60):
                moment = tt._get_next(moment)
                out.append(tt._business_date(moment))
            return set(out)

        assert dates_for(hour) == dates_for(21)

    def test_next_day_run_lands_on_the_adjacent_weekday(self):
        # A Friday business date under hour=25 runs on the Saturday, which is
        # exactly what the removed filter used to reject.
        tt = CalendarTimetable(calendar_id="JP", hour=25, rules=["毎営業日"])
        moment = datetime(2026, 6, 1, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
        for _ in range(90):
            moment = tt._get_next(moment)
            local = moment.astimezone(ZoneInfo("Asia/Tokyo"))
            business = tt._business_date(moment)
            if business.weekday() == 4:
                assert local.weekday() == 5
                assert local.hour == 1
                return
        raise AssertionError("no Friday business date found")

    @pytest.mark.parametrize("hour", [0, 9, 21, 23, 24, 25, 47, -1, -24, -47])
    def test_no_business_date_is_produced_twice(self, hour):
        tt = CalendarTimetable(calendar_id="JP", hour=hour, rules=["毎営業日"])
        moment = datetime(2026, 6, 1, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
        seen = []
        for _ in range(60):
            moment = tt._get_next(moment)
            seen.append(tt._business_date(moment))
        assert len(seen) == len(set(seen))
        assert all(tt.is_working_day(d) for d in seen)


class TestMinuteIsStoredNotParsed:
    """``minute`` must not be recovered from the parent's cron expression."""

    @pytest.mark.parametrize("minute", [0, 1, 30, 59])
    def test_minute_round_trips(self, minute):
        assert CalendarTimetable(calendar_id="JP", minute=minute).minute == minute

    @pytest.mark.parametrize("minute", [0, 30, 59])
    def test_minute_survives_serialization(self, minute):
        tt = CalendarTimetable(calendar_id="JP", minute=minute)
        assert CalendarTimetable.deserialize(tt.serialize()).minute == minute

    def test_an_invalid_minute_raises_before_the_parent_is_built(self):
        # The value is validated before super().__init__, so the error is this
        # class's own and not whatever croniter makes of the expression.
        with pytest.raises(ValueError, match="minute must be between 0 and 59"):
            CalendarTimetable(calendar_id="JP", minute=60)

    def test_an_invalid_base_day_raises_before_the_parent_is_built(self):
        with pytest.raises(ValueError, match="base_day must be between 1 and 31"):
            CalendarTimetable(calendar_id="JP", base_day=32)


class TestStoredPayloadUpgrade:
    """Reading back a payload an earlier version wrote.

    The fields below are carried for transcription fidelity and rejected when a
    user writes them *now* -- the call site is where the mistake is visible. A
    payload already in Airflow's database is a different situation: it is a
    record rather than a request, and refusing it turns an upgrade into a DAG
    that will not parse with no way for the user to repair the stored data.
    """

    #: A rule payload shaped as the version that accepted the dead fields wrote
    #: it: every dataclass field present, including the two that are now dead.
    LEGACY_RULE: ClassVar[dict] = {
        "kind": "operating",
        "start_day": "day",
        "day": 1,
        "weekday": None,
        "month_offset": 0,
        "substitution": "skip",
        "grace_days": 0,
        "offset": 0,
        "count": "operating",
        "frequency": "monthly",
        "scope": "period",
        "include_start": True,
        "virtual": False,
    }

    def _payload(self, **overrides):
        return {
            "calendar_id": "NONE",
            "hour": 9,
            "minute": 0,
            "timezone": "Asia/Tokyo",
            "exclude_dates": [],
            "include_dates": [],
            "rules": [dict(self.LEGACY_RULE, **overrides)],
            "base_day": 1,
        }

    def test_a_stored_virtual_rule_still_loads(self):
        # Named the regression: this raised NotImplementedError during DAG parse.
        timetable = CalendarTimetable.deserialize(self._payload(virtual=True))
        assert len(timetable.rules) == 1

    def test_a_stored_include_start_rule_still_loads(self):
        timetable = CalendarTimetable.deserialize(self._payload(include_start=False))
        assert len(timetable.rules) == 1

    def test_the_stored_rule_still_resolves(self):
        # Loading is not enough -- the rule has to keep producing the schedule
        # it produced before the upgrade, or the payload survives while the
        # schedule silently changes.
        timetable = CalendarTimetable.deserialize(self._payload(virtual=True))
        rule = timetable.rules[0]
        got = rule.resolve(period_for(date(2026, 9, 1)), SyntheticCalendar())
        assert got == date(2026, 9, 1)

    def test_a_stored_rule_warns_about_each_dropped_flag(self, caplog):
        # The user must learn the flag was dropped. The report is memoised (the
        # read path runs per timestamp the scheduler considers), so clear the
        # cache first -- otherwise a previous test's warning suppresses this one.
        _log_dead_flag.cache_clear()
        with caplog.at_level("WARNING", logger="airflow_timetables_calendar.rules"):
            CalendarTimetable.deserialize(self._payload(virtual=True, include_start=False))
        messages = [r.message for r in caplog.records if "Stored schedule rule" in r.message]
        assert len(messages) == 2, messages
        assert any("virtual" in m for m in messages)
        assert any("include_start" in m for m in messages)

    def test_the_warning_is_memoised_across_repeats(self, caplog):
        # A scheduler calls matches() per timestamp; an undamped warning would
        # flood the log for a payload that has already been reported.
        _log_dead_flag.cache_clear()
        with caplog.at_level("WARNING", logger="airflow_timetables_calendar.rules"):
            for _ in range(5):
                CalendarTimetable.deserialize(self._payload(virtual=True))
        messages = [r.message for r in caplog.records if "Stored schedule rule" in r.message]
        assert len(messages) == 1, messages

    def test_writing_the_fields_by_hand_is_still_rejected(self):
        # The whole point of the rejection: a rule a user writes now must fail.
        with pytest.raises(NotImplementedError):
            ScheduleRule(kind=Kind.ABSOLUTE, day=1, virtual=True)
        with pytest.raises(NotImplementedError):
            ScheduleRule(kind=Kind.ABSOLUTE, day=1, include_start=False)

    def test_new_payloads_do_not_carry_the_dead_fields(self):
        # Otherwise every save re-propagates a vocabulary nothing honours, and
        # each future change of contract has to keep tolerating it.
        timetable = CalendarTimetable(calendar_id="NONE", rules=["first_business_day_of_month"])
        payload = timetable.serialize()
        assert "virtual" not in payload["rules"][0]
        assert "include_start" not in payload["rules"][0]

    def test_a_round_trip_loses_nothing(self):
        original = CalendarTimetable(
            calendar_id="JP", hour=21, rules=["first_business_day_of_month"]
        )
        restored = CalendarTimetable.deserialize(original.serialize())
        assert restored.summary == original.summary
        assert len(restored.rules) == len(original.rules)

    def test_an_unknown_stored_field_is_ignored_not_fatal(self, caplog):
        # Forward compatibility: a payload written by a *later* version names
        # fields this one has never heard of. Refusing to load it would make a
        # downgrade, or a mixed-version cluster, unable to read its own stored
        # data. Dropping the field is the only option -- but silence would hide
        # a changed schedule, so it is reported.
        from airflow_timetables_calendar.rules import _log_unknown_stored_fields

        _log_unknown_stored_fields.cache_clear()
        payload = self._payload()
        payload["rules"][0]["some_future_field"] = "value"
        with caplog.at_level("WARNING", logger="airflow_timetables_calendar.rules"):
            timetable = CalendarTimetable.deserialize(payload)
        assert len(timetable.rules) == 1
        messages = [r.message for r in caplog.records if "does not know" in r.message]
        assert len(messages) == 1, messages
        assert "some_future_field" in messages[0]

    def test_an_unknown_field_is_reported_once(self, caplog):
        from airflow_timetables_calendar.rules import _log_unknown_stored_fields

        _log_unknown_stored_fields.cache_clear()
        payload = self._payload()
        payload["rules"][0]["another_future_field"] = 1
        with caplog.at_level("WARNING", logger="airflow_timetables_calendar.rules"):
            for _ in range(3):
                CalendarTimetable.deserialize(payload)
        messages = [r.message for r in caplog.records if "does not know" in r.message]
        assert len(messages) == 1, messages

    def test_a_failed_stored_load_does_not_leave_the_door_open(self):
        # from_stored_payload arms a permissive path on the class for the
        # duration of the call; a failure inside construction must not leave it
        # armed, or every later caller silently loses the rejection.
        with pytest.raises(ValueError, match="not-a-kind"):
            CalendarTimetable.deserialize(self._payload(kind="not-a-kind"))
        with pytest.raises(NotImplementedError):
            ScheduleRule(kind=Kind.ABSOLUTE, day=1, virtual=True)


class TestConstructorValidation:
    """Non-integer values must fail at construction, not at scheduling time.

    These arrive from ``deserialize`` as well as from callers, so a stored
    payload with a bad value has to fail where the operator can see it. A
    chained comparison accepts a float, which used to build the expression
    ``0 21.5 * * *`` and only fail later, inside the scheduler, as a croniter
    error.
    """

    @pytest.mark.parametrize("hour", [21.5, "21", None, True, [21]])
    def test_a_non_integer_hour_is_rejected(self, hour):
        with pytest.raises(ValueError, match="hour must be an int"):
            CalendarTimetable(calendar_id="NONE", hour=hour)

    @pytest.mark.parametrize("minute", [0.5, "0", None, True])
    def test_a_non_integer_minute_is_rejected(self, minute):
        with pytest.raises(ValueError, match="minute must be an int"):
            CalendarTimetable(calendar_id="NONE", minute=minute)

    @pytest.mark.parametrize("base_day", [1.5, "1", None, True])
    def test_a_non_integer_base_day_is_rejected(self, base_day):
        with pytest.raises(ValueError, match="base_day must be an int"):
            CalendarTimetable(calendar_id="NONE", base_day=base_day)

    def test_a_stored_payload_gets_the_same_failure(self):
        # The reason integers are checked at all: this is reachable from stored
        # data, where a croniter error at scheduling time is much harder to trace.
        payload = {
            "calendar_id": "NONE",
            "hour": 21.5,
            "minute": 0,
            "timezone": "Asia/Tokyo",
            "exclude_dates": [],
            "include_dates": [],
            "rules": [],
            "base_day": 1,
        }
        with pytest.raises(ValueError, match="hour must be an int"):
            CalendarTimetable.deserialize(payload)

    @pytest.mark.parametrize("hour", [-47, 0, 21, 47])
    def test_integer_hours_still_work(self, hour):
        assert CalendarTimetable(calendar_id="NONE", hour=hour).hour == hour


class TestStepBudget:
    """A calendar that never yields a working day must not hang the scheduler."""

    def test_a_long_exclusion_window_is_survivable(self):
        days = [date(2026, 1, 2) + timedelta(days=i) for i in range(398)]
        timetable = CalendarTimetable(
            calendar_id="NONE", hour=9, exclude_dates=[d.isoformat() for d in days]
        )
        got = timetable._get_next(datetime(2026, 1, 1, 10, 0, tzinfo=ZoneInfo("Asia/Tokyo")))
        assert got is not None

    def test_past_the_budget_the_error_names_the_calendar(self):
        # The bound is a hang guard, so it must stay -- but the message has to
        # say what gave up, because this surfaces inside the scheduler.
        days = [date(2026, 1, 2) + timedelta(days=i) for i in range(401)]
        timetable = CalendarTimetable(
            calendar_id="NONE", hour=9, exclude_dates=[d.isoformat() for d in days]
        )
        with pytest.raises(RuntimeError, match="NONE"):
            timetable._get_next(datetime(2026, 1, 1, 10, 0, tzinfo=ZoneInfo("Asia/Tokyo")))
