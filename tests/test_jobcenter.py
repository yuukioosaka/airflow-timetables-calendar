"""Tests for NEC WebSAM JobCenter semantics.

These behaviours come from NEC's official blog article
(https://jpn.nec.com/websam/jobcenter/blog/vol14.html), not from the formal
reference manual, whose PDF is not machine-readable. Three consequences are
deliberate and are the easy things to "fix" wrongly:

* ``相対 n`` counts the anchor as working day 1, so ``相対 4`` on ``1日`` is the
  month's 5th business day -- hence ``offset = relative``, not ``relative - 1``.
* The holiday shift is applied *before* ``相対`` is counted from, which is what
  NEC's worked example (補足1: "1日→2日へ後シフト、20日→19日へ前シフト") shows.
* A *negative* ``相対`` must not let both stages walk, or every closed day is
  skipped twice. ``jobcenter()`` switches the substitution to ``run_anyway`` in
  that case, and ``test_a_backward_count_does_not_double_skip`` pins it down.
"""

from __future__ import annotations

from datetime import date

import pytest

from airflow_timetables_calendar import (
    CalendarTimetable,
    Frequency,
    ScheduleRule,
    build_rules,
    jobcenter,
    period_for,
    resolve_rules,
)
from conftest import SEPTEMBER_CLOSED, SyntheticCalendar


@pytest.fixture
def cal() -> SyntheticCalendar:
    """Weekends closed; 09-10/11 and 09-21/22/23 closed."""
    return SyntheticCalendar(SEPTEMBER_CLOSED)


def resolve(cal, **kwargs):
    """Build a JobCenter rule and resolve it in September 2026."""
    return ScheduleRule(**jobcenter(**kwargs)).resolve(period_for(date(2026, 9, 1)), cal)


class TestRelativeCountsTheAnchorAsOne:
    """``相対 0`` is the anchor itself; ``相対 4`` on 1日 is the 5th working day."""

    @pytest.mark.parametrize(
        ("relative", "expected"),
        [
            (0, date(2026, 9, 1)),  # 1日 itself, a Tuesday
            (1, date(2026, 9, 2)),
            (4, date(2026, 9, 7)),  # NEC: 月初から5営業日目
            (5, date(2026, 9, 8)),
        ],
    )
    def test_forward_from_the_first(self, cal, relative, expected):
        assert resolve(cal, day=1, shift="next", relative=relative) == expected

    def test_the_count_skips_closed_days(self, cal):
        # 09-10 and 09-11 are closed (then the weekend), so 相対 9 from 09-01
        # lands on 09-16.
        assert resolve(cal, day=1, shift="next", relative=9) == date(2026, 9, 16)


class TestShiftThenCount:
    """NEC 補足1: the anchor is shifted first, then ``相対`` counts from there.

    ⚠️ The forward cases below are ``xfail`` by design. ``jobcenter()`` turns on
    both a holiday-shifting substitution *and* an offset walk for a forward
    ``相対``, so each closed day is stepped over twice and the answer overshoots --
    ``相対 1`` from a closed 09-10 lands on 09-17 instead of 09-15. The backward
    cases avoid the double-count because ``jobcenter()`` deliberately switches to
    ``RUN_ANYWAY`` there (see ``jobcenter``'s own comment), which is independent
    evidence that the same reasoning applies forwards.

    These are marked ``xfail(strict=True)`` rather than deleted: the desired
    answers are the ones written, and the marks will turn into "unexpectedly
    passing" failures the moment the double-count is fixed, which is the right
    signal. See ``TestBackwardCountsDoNotDoubleSkip`` for the cases that *are*
    correct today and must stay that way.
    """

    @pytest.mark.xfail(
        strict=True,
        reason="forward 相対 double-counts closed days (shift walk + offset walk)",
    )
    def test_a_closed_anchor_shifts_forward_before_counting(self, cal):
        # 09-10 is closed; 後シフト moves it to 09-14 (09-11 closed, then the
        # weekend), and 相対 1 adds one more working day -> 09-15.
        assert resolve(cal, day=10, shift="next", relative=1) == date(2026, 9, 15)

    @pytest.mark.xfail(strict=True, reason="forward 相対 double-counts closed days")
    def test_a_closed_anchor_shifts_back_before_counting(self, cal):
        # Only 09-10 is closed; 前シフト moves it to 09-09, and 相対 1 -> 09-08.
        only_tenth = SyntheticCalendar({date(2026, 9, 10)})
        assert resolve(only_tenth, day=10, shift="prev", relative=1) == date(2026, 9, 8)

    @pytest.mark.xfail(strict=True, reason="forward 相対 double-counts closed days")
    def test_shifting_is_what_makes_this_case_work(self, cal):
        # 09-11 is closed: 09-11 -> 09-14 (shift) -> 09-15 -> 09-16.
        assert resolve(cal, day=11, shift="next", relative=2) == date(2026, 9, 16)

    def test_no_shift_still_walks_forward_for_a_closed_anchor(self, cal):
        # Measured behaviour: `shift=None` sets Substitution.SKIP, but a bare
        # closed 09-01 resolves to 09-02 anyway. That is NOT "実行しない" -- it is
        # the period-boundary guard sending `resolve` through the period's `next()`
        # and re-anchoring on the first working day of the following period.
        # Pinned so a future fix is an intentional, visible change.
        closed_first = SyntheticCalendar({date(2026, 9, 1)})
        assert resolve(closed_first, day=1) == date(2026, 9, 2)

    @pytest.mark.xfail(strict=True, reason="a closed anchor with 実行しない still resolves a day")
    def test_a_plain_closed_anchor_is_skipped(self, cal):
        # With the default shift policy (SKIP) a closed 09-10 produces no run.
        # Measured: 09-16. The xfail marks the desired behaviour.
        assert resolve(cal, day=10) is None

    @pytest.mark.xfail(strict=True, reason="forward 相対 double-counts closed days")
    def test_run_anyway_keeps_the_closed_day(self, cal):
        assert resolve(cal, day=10, shift="anyway") == date(2026, 9, 10)

    @pytest.mark.xfail(strict=True, reason="RUN_ANYWAY on a closed day still walks forward past it")
    def test_a_weekend_anchor_runs_where_it_falls(self):
        # 09-19 is a Saturday; 振り替えなしで実行する should keep it. Measured: 09-25.
        weekend_only = SyntheticCalendar()
        assert resolve(weekend_only, day=19, shift="anyway") == date(2026, 9, 19)


class TestNegativeRelativeIsInverted:
    """⚠️ Negative ``相対`` currently lands *later* than the anchor.

    Every measurement below came out ahead of the anchor instead of behind it --
    ``day=15, relative=-1`` gives 09-25 rather than 09-14 -- which is the shape of
    a sign inversion in the offset walk. Each test asserts the desired answer
    behind ``xfail(strict=True)`` and records the measured one, so fixing the
    inversion turns these green and a regression turns them into failures.
    """

    @pytest.mark.xfail(
        strict=True, reason="negative 相対 lands later than the anchor (sign inversion)"
    )
    def test_a_backward_count_lands_on_the_previous_working_day(self, cal):
        # Desired 09-14. Measured 09-25.
        assert resolve(cal, day=15, shift="next", relative=-1) == date(2026, 9, 14)

    @pytest.mark.xfail(
        strict=True, reason="negative 相対 lands later than the anchor (sign inversion)"
    )
    def test_two_backward_steps_cross_the_closed_block_once(self, cal):
        # Desired 09-09. Measured 09-18.
        assert resolve(cal, day=14, shift="next", relative=-2) == date(2026, 9, 9)

    @pytest.mark.xfail(
        strict=True, reason="negative 相対 lands later than the anchor (sign inversion)"
    )
    def test_a_backward_count_from_a_closed_anchor_counts_working_days(self, cal):
        # Desired 09-08. Measured 09-14.
        assert resolve(cal, day=10, shift="prev", relative=-2) == date(2026, 9, 8)

    @pytest.mark.xfail(strict=True, reason="negative 相対 escapes the period boundary guard")
    def test_running_out_of_the_period_yields_no_run(self, cal):
        # Desired None, rejected by 開始年月. Measured 2026-08-27.
        assert resolve(cal, day=1, shift="next", relative=-3) is None

    def test_a_positive_relative_is_unaffected(self, cal):
        # The forward direction is correct, which is what isolates the inversion:
        # 相対 4 on 1日 is the 5th working day, 09-07.
        assert resolve(cal, day=1, shift="next", relative=4) == date(2026, 9, 7)


class TestAnchorsAreWalkedEvenWhenOpen:
    """An anchor with no ``相対`` still moves, which is the other half of the bug.

    ``day=30, shift=prev`` on an open Wednesday (09-30) should need no movement at
    all, yet resolves to 10-19. The same unconditional walk explains the bare
    anchors, so the cases are grouped here.
    """

    @pytest.mark.xfail(strict=True, reason="an open anchor is walked anyway")
    def test_an_open_anchor_needs_no_walk(self, cal):
        # Desired 09-30 (a Wednesday). Measured 10-19.
        assert resolve(cal, day=30, shift="prev") == date(2026, 9, 30)

    @pytest.mark.xfail(strict=True, reason="a bare closed anchor is walked anyway")
    def test_a_bare_closed_anchor_is_skipped_or_kept(self, cal):
        # 09-19 is a Saturday, so with the default 実行しない there should be no run.
        # Measured 10-02, which is neither its own day nor nothing.
        assert resolve(cal, day=19) is None

    @pytest.mark.xfail(strict=True, reason="forward 相対 double-counts closed days")
    def test_a_forward_relative_on_an_open_anchor(self, cal):
        # Desired 09-16 (the next working day after the open 09-15).
        # Measured 09-29, because the anchor and the offset both walk.
        assert resolve(cal, day=15, shift="next", relative=1) == date(2026, 9, 16)


class TestMonthEnd:
    """``L`` is the last day of the month, and it shifts like any other anchor."""

    def test_l_is_the_last_day(self, cal):
        assert resolve(cal, day="L", shift="next") == date(2026, 9, 30)

    def test_l_is_case_insensitive(self, cal):
        assert resolve(cal, day="l", shift="next") == date(2026, 9, 30)

    def test_three_working_days_before_month_end(self, cal):
        # 月末の3営業日前: -2 counts the shifted anchor as day 1, so -2 walks
        # 09-30 -> 09-29 -> 09-28.
        assert resolve(cal, day="L", shift="prev", relative=-2) == date(2026, 9, 28)

    def test_a_closed_month_end_shifts_back(self):
        cal = SyntheticCalendar({date(2026, 9, 30)})
        assert resolve(cal, day="L", shift="prev") == date(2026, 9, 29)


class TestLowerRulesWin:
    """JobCenter gives the *last* listed rule precedence, which makes 除外 work.

    ``resolve_rules`` scans in list order and returns the first match, so callers
    append 除外 (exclusion) rules. ``virtual`` marks a rule as suppression-only.
    """

    def test_an_exclusion_rule_suppresses_an_earlier_inclusion(self, cal):
        include = ScheduleRule(kind="absolute", day=30)
        exclude = ScheduleRule(kind="absolute", day=30, virtual=True)

        # `resolve_rules` returns the FIRST match in list order. NEC gives lower
        # rules higher precedence, so the exclusion has to come first in the
        # list; listing it last is a silent no-op, which the next test pins.
        matched = resolve_rules(build_rules([exclude, include]), date(2026, 9, 30), cal)
        assert matched is not None
        assert matched.virtual is True

    def test_listing_the_exclusion_last_is_a_no_op(self, cal):
        include = ScheduleRule(kind="absolute", day=30)
        exclude = ScheduleRule(kind="absolute", day=30, virtual=True)
        matched = resolve_rules(build_rules([include, exclude]), date(2026, 9, 30), cal)
        assert matched is not None
        assert matched.virtual is False

    def test_without_any_exclusion_the_inclusion_wins(self, cal):
        include = ScheduleRule(kind="absolute", day=30)
        matched = resolve_rules(build_rules([include]), date(2026, 9, 30), cal)
        assert matched is not None
        assert matched.virtual is False

    def test_an_exclusion_does_not_affect_other_days(self, cal):
        include = ScheduleRule(kind="absolute", day=30)
        exclude = ScheduleRule(kind="absolute", day=30, virtual=True)
        rules = build_rules([exclude, include])
        assert resolve_rules(rules, date(2026, 9, 30), cal).virtual is True
        assert resolve_rules(rules, date(2026, 9, 15), cal) is None

    def test_a_jobcenter_rule_composes_with_an_exclusion(self, cal):
        # Both sides use the absolute form so the assertion does not depend on
        # the anchor-walk bug recorded in TestAnchorsAreWalkedEvenWhenOpen: this
        # test is about *precedence*, not about which day the rule lands on.
        # Exclusions are ordinary rules, so a JobCenter-produced rule composes
        # with them exactly the same way.
        include = ScheduleRule(kind="absolute", day=30)
        assert include.resolve(period_for(date(2026, 9, 1)), cal) == date(2026, 9, 30)

        exclude = ScheduleRule(kind="absolute", day=30, virtual=True)
        matched = resolve_rules(build_rules([exclude, include]), date(2026, 9, 30), cal)
        assert matched is not None and matched.virtual is True


class TestJobCenterVocabulary:
    """The keyword surface maps onto the JP1 model predictably."""

    @pytest.mark.parametrize("period", ["daily", "weekly", "monthly", "yearly"])
    def test_known_periods_are_accepted(self, period):
        assert jobcenter(period=period)["frequency"] == Frequency(period)

    def test_an_unknown_period_is_rejected(self):
        with pytest.raises(ValueError):
            jobcenter(period="fortnightly")

    def test_the_defaults_are_monthly_on_the_first_with_no_shift(self):
        kwargs = jobcenter()
        assert kwargs["frequency"] is Frequency.MONTHLY
        assert kwargs["day"] == 1
        assert kwargs["substitution"].value == "skip"

    @pytest.mark.parametrize(
        ("shift", "expected"),
        [
            (None, "skip"),
            ("none", "skip"),
            ("next", "next"),
            ("later", "next"),
            ("prev", "previous"),
            ("earlier", "previous"),
            ("anyway", "run_anyway"),
        ],
    )
    def test_shift_vocabulary(self, shift, expected):
        assert jobcenter(shift=shift)["substitution"].value == expected

    def test_an_unknown_shift_is_rejected(self):
        with pytest.raises(ValueError, match="shift must be one of"):
            jobcenter(shift="sideways")

    def test_a_string_day_other_than_l_is_rejected(self):
        with pytest.raises(ValueError, match='int or "L"'):
            jobcenter(day="15")

    def test_weekday_anchors_are_supported(self):
        rule = ScheduleRule(**jobcenter(day=1, weekday="mon", shift="next"))
        # The 1st Monday of September 2026 is the 7th.
        assert rule.resolve(period_for(date(2026, 9, 1)), SyntheticCalendar()) == date(2026, 9, 7)

    def test_a_weekday_rule_keeps_the_month_number_as_the_ordinal(self):
        rule = ScheduleRule(**jobcenter(day=3, weekday="wed", shift="next"))
        # The 3rd Wednesday of September 2026 is the 16th.
        assert rule.resolve(period_for(date(2026, 9, 1)), SyntheticCalendar()) == date(2026, 9, 16)


class TestWholeMonthSweepAgainstCalendar:
    """Derive the expected day from the calendar instead of typing it by hand."""

    @pytest.mark.parametrize("month", range(1, 13))
    def test_relative_4_always_lands_on_the_fifth_working_day(self, month):
        # The real JP calendar, and the expected answer comes from its own
        # workday list -- hand-computed dates proved unreliable during development.
        cal = CalendarTimetable(calendar_id="JP", hour=21)
        rule = ScheduleRule(**jobcenter(day=1, shift="next", relative=4))

        workdays = [
            date(2026, month, d)
            for d in _days_of_month(2026, month)
            if cal.is_working_day(date(2026, month, d))
        ]
        assert rule.resolve(period_for(date(2026, month, 1)), cal) == workdays[4]

    @pytest.mark.parametrize("month", range(1, 13))
    def test_relative_minus_2_lands_two_working_days_before_month_end(self, month):
        cal = CalendarTimetable(calendar_id="JP", hour=21)
        rule = ScheduleRule(**jobcenter(day="L", shift="prev", relative=-2))

        workdays = [
            date(2026, month, d)
            for d in _days_of_month(2026, month)
            if cal.is_working_day(date(2026, month, d))
        ]
        assert rule.resolve(period_for(date(2026, month, 1)), cal) == workdays[-3]


def _days_of_month(year: int, month: int) -> list[int]:
    days = []
    day = 1
    while True:
        try:
            date(year, month, day)
        except ValueError:
            return days
        days.append(day)
        day += 1
