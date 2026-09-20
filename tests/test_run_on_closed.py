"""Tests for ``run_on="closed"`` -- running on the days a calendar closes.

The whole feature is one negation, so these tests are mostly about proving the
negation is *exact* and that nothing else quietly bypasses it: ``include_dates``,
``exclude_dates`` and the rule engine all read the calendar through the same
``is_working_day``, and each has to follow the flag.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from airflow_timetables_calendar import CalendarTimetable

TZ = ZoneInfo("Asia/Tokyo")

#: 2026-09-21 is 敬老の日 and 2026-09-23 is 秋分の日; 2026-09-22 is the
#: 国民の休日 that falls between them. A full closed block Mon-Wed.
HOLIDAY_BLOCK = (date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 23))


def walk(tt: CalendarTimetable, start: date, n: int) -> list[date]:
    """The business dates of the next ``n`` runs, from ``start``."""
    moment = datetime(start.year, start.month, start.day, tzinfo=TZ)
    out = []
    for _ in range(n):
        moment = tt._get_next(moment)
        out.append(tt._business_date(moment))
    return out


class TestConstruction:
    def test_defaults_to_the_open_days(self):
        assert CalendarTimetable(calendar_id="JP").run_on == "open"

    def test_accepts_closed(self):
        assert CalendarTimetable(calendar_id="JP", run_on="closed").run_on == "closed"

    @pytest.mark.parametrize("value", ["holidays", "CLOSED", "Open", "", "true", None, 1])
    def test_rejects_anything_else(self, value):
        with pytest.raises(ValueError, match="run_on must be"):
            CalendarTimetable(calendar_id="JP", run_on=value)

    def test_summary_marks_the_inverted_mode(self):
        # Two timetables on the same calendar and expression run on disjoint
        # days, so the UI text has to distinguish them.
        assert "run on: closed" in CalendarTimetable(calendar_id="JP", run_on="closed").summary
        assert "run on" not in CalendarTimetable(calendar_id="JP").summary


class TestTheNegationIsExact:
    def test_every_day_of_a_year_disagrees(self):
        open_tt = CalendarTimetable(calendar_id="JP", hour=9)
        closed_tt = CalendarTimetable(calendar_id="JP", hour=9, run_on="closed")
        day = date(2026, 1, 1)
        while day < date(2027, 1, 1):
            assert open_tt.is_working_day(day) != closed_tt.is_working_day(day), day
            day = date.fromordinal(day.toordinal() + 1)

    def test_weekends_and_holidays_run_under_closed(self):
        tt = CalendarTimetable(calendar_id="JP", hour=9, run_on="closed")
        assert tt.is_working_day(date(2026, 9, 19))  # Saturday
        assert tt.is_working_day(date(2026, 9, 20))  # Sunday
        for holiday in HOLIDAY_BLOCK:
            assert tt.is_working_day(holiday), holiday

    def test_ordinary_weekdays_do_not_run_under_closed(self):
        tt = CalendarTimetable(calendar_id="JP", hour=9, run_on="closed")
        assert not tt.is_working_day(date(2026, 9, 18))  # Friday
        assert not tt.is_working_day(date(2026, 9, 24))  # Thursday


class TestIncludeAndExcludeInvert:
    """The flag inverts the finished verdict, not just the calendar lookup.

    "The opposite of the current calendar" has to include the caller's own
    overrides, otherwise a date forced open would still be reported as a closed
    day and the two lists would mean different things in the two modes.
    """

    def test_a_forced_open_day_is_not_a_closed_day(self):
        tt = CalendarTimetable(
            calendar_id="JP", hour=9, run_on="closed", include_dates=["2026-09-19"]
        )
        assert not tt.is_working_day(date(2026, 9, 19))

    def test_a_forced_closed_day_is_a_run(self):
        tt = CalendarTimetable(
            calendar_id="JP", hour=9, run_on="closed", exclude_dates=["2026-09-19"]
        )
        assert tt.is_working_day(date(2026, 9, 19))

    def test_the_same_lists_still_work_in_open_mode(self):
        tt = CalendarTimetable(
            calendar_id="JP", hour=9, include_dates=["2026-09-19"], exclude_dates=["2026-09-18"]
        )
        assert tt.is_working_day(date(2026, 9, 19))
        assert not tt.is_working_day(date(2026, 9, 18))


class TestRulesFollowTheFlag:
    """``rules`` read the calendar through ``is_working_day``, so they invert too.

    This is the point of putting the flag on the timetable rather than in the
    rule vocabulary: 毎営業日, 休業日の振り替え and 第n営業日 all change meaning
    together, with no rule aware that a mode exists.
    """

    def test_every_business_day_becomes_every_closed_day(self):
        tt = CalendarTimetable(calendar_id="JP", hour=9, run_on="closed", rules=["毎営業日"])
        runs = walk(tt, date(2026, 9, 1), 5)
        assert runs == [
            date(2026, 9, 5),
            date(2026, 9, 6),
            date(2026, 9, 12),
            date(2026, 9, 13),
            date(2026, 9, 19),
        ]

    def test_no_rules_means_every_closed_day(self):
        tt = CalendarTimetable(calendar_id="JP", hour=9, run_on="closed")
        assert walk(tt, date(2026, 9, 1), 5) == walk(
            CalendarTimetable(calendar_id="JP", hour=9, run_on="closed", rules=["毎営業日"]),
            date(2026, 9, 1),
            5,
        )

    def test_open_and_closed_runs_never_collide(self):
        start = date(2026, 9, 1)
        open_runs = set(walk(CalendarTimetable(calendar_id="JP", hour=9), start, 40))
        closed_runs = set(
            walk(CalendarTimetable(calendar_id="JP", hour=9, run_on="closed"), start, 40)
        )
        assert not (open_runs & closed_runs)

    def test_a_month_end_rule_resolves_against_closed_days(self):
        # 当月末営業日 under the flag is the month's last *closed* day, because
        # the rule's own working-day test now asks the inverted question.
        tt = CalendarTimetable(calendar_id="JP", hour=9, run_on="closed", rules=["当月末営業日"])
        runs = walk(tt, date(2026, 9, 1), 3)
        assert runs[0] == date(2026, 9, 27)  # the last Sunday of September 2026
        assert all(not CalendarTimetable(calendar_id="JP").is_working_day(r) for r in runs)


class TestExchangeCalendars:
    def test_xtks_closures_run(self):
        tt = CalendarTimetable(calendar_id="XTKS", hour=9, run_on="closed")
        # 2026-09-19 is a Saturday in both modes, but the flag also picks up
        # weekday exchange closures; only assert the negation invariant here so
        # the test does not depend on a particular exchange holiday.
        open_tt = CalendarTimetable(calendar_id="XTKS", hour=9)
        for day in (date(2026, 9, 19), date(2026, 9, 21), date(2026, 9, 22)):
            assert tt.is_working_day(day) != open_tt.is_working_day(day), day


class TestRoundTrip:
    def test_run_on_survives_serialization(self):
        tt = CalendarTimetable(calendar_id="JP", hour=21, run_on="closed")
        payload = tt.serialize()
        assert payload["run_on"] == "closed"
        back = CalendarTimetable.deserialize(payload)
        assert back.run_on == "closed"
        assert back.is_working_day(date(2026, 9, 19))

    def test_a_payload_from_before_the_flag_reads_as_open(self):
        # Backward compatibility: every stored payload written by an earlier
        # version meant "run on the open days", so the absent key must not be
        # an error and must not change the schedule.
        legacy = {"calendar_id": "JP", "hour": 21, "base_day": 1}
        tt = CalendarTimetable.deserialize(legacy)
        assert tt.run_on == "open"
        assert tt.is_working_day(date(2026, 9, 18))
        assert not tt.is_working_day(date(2026, 9, 19))


class Test48HourClock:
    def test_the_flag_does_not_disturb_the_business_date(self):
        # hour=25 runs at 01:00 the next morning but stays the declared date's
        # run; the inversion must not shift that alignment.
        tt = CalendarTimetable(calendar_id="JP", hour=25, run_on="closed")
        moment = datetime(2026, 9, 18, tzinfo=TZ)
        run = tt._get_next(moment)
        assert tt._business_date(run) == date(2026, 9, 19)  # Saturday
        assert run.astimezone(TZ).date() == date(2026, 9, 20)  # runs next morning
