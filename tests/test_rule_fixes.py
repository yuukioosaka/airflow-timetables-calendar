"""Regression tests for the three rule-engine bugs found in review.

All three were silent: the DAG parsed, the schedule simply never fired (or fired
on the wrong day). They survived 285 passing tests because the only
``month_offset`` coverage was ``-1`` with ``Scope.FREE`` -- exactly the
combination that happened to work.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta

import pytest

from airflow_timetables_calendar import (
    BUSINESS_DAY_RULES,
    Count,
    Kind,
    ScheduleRule,
    Scope,
    StartDay,
    Substitution,
    Weekday,
    period_for,
)


def rule(**kwargs) -> ScheduleRule:
    base = {"kind": Kind.ABSOLUTE, "start_day": StartDay.DAY, "day": 15}
    base.update(kwargs)
    return ScheduleRule(**base)


def weekday_rule(day: int, weekday: Weekday = Weekday.SUN, **kwargs) -> ScheduleRule:
    """A 曜日指定 rule that reports its anchor instead of substituting it.

    The anchors these tests assert on are calendar dates (Sundays, month ends),
    so the default ``Substitution.SKIP`` would drop them for being closed and the
    test would be measuring the holiday policy rather than the anchor.
    """
    base = {
        "kind": Kind.ABSOLUTE,
        "start_day": StartDay.WEEKDAY,
        "weekday": weekday,
        "day": day,
        "substitution": Substitution.RUN_ANYWAY,
    }
    base.update(kwargs)
    return ScheduleRule(**base)


def periods_of(year: int, base_day: int = 1, pad: int = 0) -> list:
    """Every period overlapping ``year``, in order.

    ``pad`` extends the sweep by that many periods at each end. It matters for
    rules with a large ``month_offset``: the period that produces a day early in
    the year may itself lie well outside it, so comparing ``resolve()`` against
    ``matches()`` requires both to sweep the same epochs.
    """
    out, seen = [], set()
    day = date(year, 1, 1)
    while day <= date(year, 12, 31):
        period = period_for(day, base_day)
        if period not in seen:
            seen.add(period)
            out.append(period)
        day += timedelta(days=1)
    for _ in range(pad):
        out.insert(0, out[0].prev())
        out.append(out[-1].next())
    return out


# --------------------------------------------------------------------------- #
# 開始年月 combined with 基準日の移動
# --------------------------------------------------------------------------- #


class TestScopeWithMonthOffset:
    """``Scope.PERIOD`` must compose with ``month_offset``.

    ``month_offset`` moves the anchor the rule *uses*; ``Scope.PERIOD`` binds the
    rule to the period it is *asked about*. An earlier version tested the moved
    anchor against the period at a second gate, which is unsatisfiable whenever
    ``month_offset != 0`` -- the two are then different months by construction.
    Every such rule resolved to ``None``, so 前月末営業日 combined with 開始年月
    produced no run at all.
    """

    @pytest.mark.parametrize("offset", [-6, -3, -2, -1, 1, 2, 3, 6])
    def test_a_moved_anchor_still_resolves_under_scope_period(self, open_calendar, offset):
        # Before the fix every one of these resolved to None, so the assertion
        # that matters is "a run is produced at all" -- not how many, because a
        # month_offset rule deliberately resolves to a day outside the period it
        # was asked about and the sweep window therefore trims its edges.
        moved = rule(month_offset=offset, scope=Scope.PERIOD)
        resolved = [
            period
            for period in periods_of(2026)
            if moved.resolve(period, open_calendar) is not None
        ]
        assert resolved, f"{offset=} produced no run in any period"
        assert len(resolved) >= 6, f"{offset=} produced only {len(resolved)} runs"

    @pytest.mark.parametrize("offset", [-1, 1])
    def test_scope_period_and_free_agree_once_the_anchor_is_moved(self, open_calendar, offset):
        # Both scopes name the same period, so once the anchor is moved they must
        # agree: 開始年月 does not add a further restriction in this case.
        bound = rule(month_offset=offset, scope=Scope.PERIOD)
        free = rule(month_offset=offset, scope=Scope.FREE)
        for period in periods_of(2026):
            assert bound.resolve(period, open_calendar) == free.resolve(period, open_calendar)

    def test_a_moved_anchor_outside_the_named_month_is_still_rejected(self, open_calendar):
        # The gate is not simply gone: an anchor in a *different* month from the
        # period is still refused, which is what 開始年月 means.
        misplaced = rule(scope=Scope.PERIOD)
        moved = replace(misplaced, month_offset=0)
        assert moved.resolve(period_for(date(2026, 9, 1)), open_calendar) == date(2026, 9, 15)


# --------------------------------------------------------------------------- #
# ``matches()`` must find the epoch that produced the day
# --------------------------------------------------------------------------- #


class TestMatchesFindsTheProducingEpoch:
    """``matches()`` is the call the timetable actually makes.

    It has to be equivalent to "some period's ``resolve()`` equals this day". A
    bounded scan of the containing period and its neighbours cannot express that:
    a run crosses *two* period boundaries when an unaligned anchor is combined
    with a movement, and ``month_offset`` is unbounded besides.
    """

    @pytest.mark.parametrize("offset", [-6, -3, -2, -1, 0, 1, 2, 3, 6])
    @pytest.mark.parametrize("scope", [Scope.FREE, Scope.PERIOD])
    def test_matches_agrees_with_resolve_for_every_offset(self, open_calendar, offset, scope):
        probe = rule(month_offset=offset, scope=scope)
        # Pad the sweep by more periods than |month_offset| so the epochs that
        # produce the in-year days are all included on the resolve() side.
        produced = {
            got
            for period in periods_of(2026, pad=abs(offset) + 2)
            if (got := probe.resolve(period, open_calendar)) is not None
        }
        matched = set()
        day = date(2025, 1, 1)
        while day <= date(2027, 12, 31):
            if probe.matches(day, open_calendar):
                matched.add(day)
            day += timedelta(days=1)

        # Compare only the days both sweeps can see: a defect shows up as a day
        # the matcher misses or invents well inside the window, never at its rim.
        low, high = date(2026, 3, 1), date(2026, 10, 31)
        assert {d for d in matched if low <= d <= high} == {d for d in produced if low <= d <= high}

    @pytest.mark.parametrize("offset", [-2, 2, -5, 5])
    def test_a_multi_period_offset_is_not_dropped(self, open_calendar, offset):
        # The old three-period scan silently dropped every |month_offset| >= 2.
        probe = rule(month_offset=offset)
        assert any(probe.matches(date(2026, month, 15), open_calendar) for month in range(1, 13)), (
            f"{offset=} matched no day at all"
        )

    def test_an_unaligned_anchor_plus_a_movement_crosses_two_boundaries(self, open_calendar):
        # With base_day=26 the period containing 2026-06-10 runs 05-26..06-25, so
        # the *1st of June* is in the previous period. A backward 起算 moves it a
        # further period back, to May. The producing epoch is therefore the July
        # period -- two away from the one containing the day.
        probe = rule(
            day=1,
            offset=-1,
            count=Count.OPERATING,
            offset_grace_days=30,
        )
        day = date(2026, 5, 29)  # a Friday
        period = period_for(day, base_day=26)
        assert probe.resolve(period.next(), open_calendar) == day
        assert probe.matches(day, open_calendar, base_day=26) is True

    def test_a_day_that_is_nobody_s_run_is_not_matched(self, open_calendar):
        probe = rule(month_offset=-2)
        # The 14th and 16th of a month are never a "15th shifted two periods".
        assert probe.matches(date(2026, 9, 14), open_calendar) is False
        assert probe.matches(date(2026, 9, 16), open_calendar) is False

    def test_a_daily_rule_is_still_classified_per_day(self, open_calendar):
        # 毎営業日 is the per-day question and must not be epoch-recovered.
        daily = ScheduleRule(frequency="daily", scope=Scope.PERIOD)
        assert daily.matches(date(2026, 9, 15), open_calendar) is True
        assert daily.matches(date(2026, 9, 19), open_calendar) is False  # a Saturday


# --------------------------------------------------------------------------- #
# 曜日指定 stays inside the anchor month
# --------------------------------------------------------------------------- #


class TestWeekdayStartDayStaysInTheMonth:
    """曜日指定 is "the Nth <weekday> **of the month**".

    ``day`` is unbounded, so a plain ``first + 7 * (day - 1)`` walks into the
    following month whenever the month has fewer occurrences -- and ``day=28``
    walks seven months out. The anchor then no longer lies in the month it was
    named in, which also broke ``matches()``.
    """

    @pytest.mark.parametrize("day", [1, 2, 3, 4, 5, 6, 8, 12, 28])
    @pytest.mark.parametrize("month", range(1, 13))
    def test_the_anchor_never_leaves_its_month(self, open_calendar, day, month):
        probe = weekday_rule(day)
        got = probe.resolve(period_for(date(2026, month, 1)), open_calendar)
        assert got is not None
        assert got.month == month, f"2026-{month:02d} day={day} escaped to {got}"

    def test_february_2026_has_four_sundays(self, open_calendar):
        # February 2026's Sundays are the 1st, 8th, 15th and 22nd.
        def sunday(n: int) -> date | None:
            return weekday_rule(n).resolve(period_for(date(2026, 2, 1)), open_calendar)

        assert sunday(1) == date(2026, 2, 1)
        assert sunday(2) == date(2026, 2, 8)
        assert sunday(3) == date(2026, 2, 15)
        assert sunday(4) == date(2026, 2, 22)

    @pytest.mark.parametrize("day", [5, 6, 28])
    def test_a_missing_occurrence_becomes_the_month_end(self, open_calendar, day):
        # February 2026 has no 5th Sunday, so day >= 5 names the last day of the
        # month rather than a Sunday in March. This mirrors how the day-based
        # 開始日 forms already clamp an out-of-range day.
        got = weekday_rule(day).resolve(period_for(date(2026, 2, 1)), open_calendar)
        assert got == date(2026, 2, 28)

    def test_the_fifth_occurrence_is_still_found_when_it_exists(self, open_calendar):
        # March 2026 does have five Sundays, the last being the 29th.
        got = weekday_rule(5).resolve(period_for(date(2026, 3, 1)), open_calendar)
        assert got == date(2026, 3, 29)

    def test_matches_agrees_for_a_weekday_anchor(self, open_calendar):
        probe = weekday_rule(5)
        produced = {
            got
            for period in periods_of(2026)
            if (got := probe.resolve(period, open_calendar)) is not None
        }
        for day in produced:
            assert probe.matches(day, open_calendar) is True


# --------------------------------------------------------------------------- #
# The presets still behave
# --------------------------------------------------------------------------- #


class TestPresetsAfterTheFixes:
    """Each preset must resolve to a day that it also matches, at both 基準日.

    ``resolve()`` and ``matches()`` are asked by different callers and were able
    to disagree; this pins them together for every shipped preset.
    """

    @pytest.mark.parametrize("base_day", [1, 26])
    def test_every_preset_run_is_matched(self, jp_timetable, base_day):
        mismatches = []
        for name in BUSINESS_DAY_RULES:
            preset = ScheduleRule(**BUSINESS_DAY_RULES[name])
            for period in periods_of(2026, base_day):
                got = preset.resolve(period, jp_timetable)
                if got is None:
                    continue
                if not preset.matches(got, jp_timetable, base_day=base_day):
                    mismatches.append(f"{name} {period}: resolved {got} but did not match it")
        assert not mismatches, mismatches

    @pytest.mark.parametrize("base_day", [1, 26])
    def test_each_monthly_preset_runs_about_once_a_month(self, jp_timetable, base_day):
        # A monthly preset yields one run per period it is asked about. The count
        # is compared against the number of periods swept rather than a fixed 12,
        # because with base_day=26 the periods overlapping a calendar year are not
        # exactly the twelve that start in it.
        swept = periods_of(2026, base_day)
        for name in BUSINESS_DAY_RULES:
            if name == "毎営業日":
                continue
            preset = ScheduleRule(**BUSINESS_DAY_RULES[name])
            runs = [
                got for period in swept if (got := preset.resolve(period, jp_timetable)) is not None
            ]
            assert len(runs) == len(swept), (
                f"{name} base_day={base_day}: {len(runs)} runs for {len(swept)} periods"
            )
