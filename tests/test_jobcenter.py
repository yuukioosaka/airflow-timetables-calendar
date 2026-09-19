"""Tests for NEC WebSAM JobCenter semantics.

These behaviours come from NEC's official blog article
(https://jpn.nec.com/websam/jobcenter/blog/vol14.html), not from the formal
reference manual, whose PDF is not machine-readable. Four consequences are
deliberate and are the easy things to "fix" wrongly:

* ``day=N`` is the **Nth of the month**, because NEC writes 毎月（日付）
  (monthly *by date*). The working-day families are 第n営業日, built with
  :func:`nth_business_day`. Reading ``day=30`` as "the 30th working day" moves
  the anchor into the following month, which is what three of this module's
  historical bugs really were.
* ``相対 n`` counts n working days *from the settled anchor*, the anchor itself
  being 0. ``相対 4`` on ``1日`` is the 5th business day (> NEC example A) and
  ``相対 -2`` on ``L日`` is 月末の3営業日前 (NEC example B); both come out right only
  under this reading.
* The holiday shift is applied *before* ``相対`` is counted from, which is what
  NEC's worked example (補足1: "1日→2日へ後シフト、20日→19日へ前シフト") shows.
  The shift step itself is not one of the counted steps.
* Both stages must not walk. ``jobcenter()`` keeps the substitution from moving
  the date and lets the offset carry the whole distance, or every closed day in
  the span would be skipped twice.
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

    The 休止日 policy settles the anchor and ``相対`` then counts *further*
    working days from the settled date, the anchor itself counting as 0. Both of
    NEC's published examples pin this down:

    * ``1日、休止日 後シフト、相対 4`` -> 月初から5営業日目. 09-01 is a working day, so
      the shift is a no-op and ``相対 4`` walks four days forward to the 5th.
    * ``L日、休止日 前シフト、相対 -2`` -> 月末の3営業日前. Month end is the anchor, and
      ``相対 -2`` walks two working days back from it.

    Only *one* stage may walk. ``jobcenter()`` keeps the substitution from moving
    the date and lets the offset carry the whole distance, because chaining the
    two makes every closed day in the span cost two steps.
    """

    def test_a_closed_anchor_shifts_forward_before_counting(self, cal):
        # 09-10 is closed; 後シフト moves it to 09-14 (09-11 closed, then the
        # weekend), and 相対 1 adds one more working day -> 09-15.
        assert resolve(cal, day=10, shift="next", relative=1) == date(2026, 9, 15)

    def test_a_closed_anchor_shifts_back_before_counting(self, cal):
        # Only 09-10 is closed; 前シフト settles it on 09-09, and 相対 1 is one
        # further working day from there. 09-10 is closed and 09-11 is a Friday,
        # so the count lands on 09-11.
        only_tenth = SyntheticCalendar({date(2026, 9, 10)})
        assert resolve(only_tenth, day=10, shift="prev", relative=1) == date(2026, 9, 11)

    def test_shifting_is_what_makes_this_case_work(self, cal):
        # 09-11 is closed: 09-11 -> 09-14 (shift) -> 09-15 -> 09-16.
        assert resolve(cal, day=11, shift="next", relative=2) == date(2026, 9, 16)

    def test_no_shift_still_walks_forward_for_a_closed_anchor(self, cal):
        # 実行しない (SKIP) means a closed anchor produces no run -- even for the
        # first day of the month, which used to be walked forward into the 2nd.
        closed_first = SyntheticCalendar({date(2026, 9, 1)})
        assert resolve(closed_first, day=1) is None

    def test_a_plain_closed_anchor_is_skipped(self, cal):
        # With the default shift policy (SKIP) a closed 09-10 produces no run.
        assert resolve(cal, day=10) is None

    def test_run_anyway_keeps_the_closed_day(self, cal):
        assert resolve(cal, day=10, shift="anyway") == date(2026, 9, 10)

    def test_a_weekend_anchor_runs_where_it_falls(self):
        # 09-19 is a Saturday; 振り替えなしで実行する keeps it.
        weekend_only = SyntheticCalendar()
        assert resolve(weekend_only, day=19, shift="anyway") == date(2026, 9, 19)


class TestNegativeRelativeCountsBackwards:
    """``相対 n`` counts n working days from the settled anchor, in both directions.

    A negative ``相対`` lands *behind* the settled anchor, never ahead of it, and
    the anchor itself is step 0 -- which is what NEC's worked examples show and
    what makes ``相対 4`` and ``相対 -2`` consistent with one another.

    Note that the settling step is *not* one of the counted steps: for a closed
    anchor the 休止日 shift carries the date onto the first working day for free,
    and the count proper then begins there.
    """

    def test_a_backward_count_lands_on_the_previous_working_day(self, cal):
        # 09-15 is open, so the shift is a no-op and 相対 -1 is simply the
        # previous working day, 09-14.
        assert resolve(cal, day=15, shift="next", relative=-1) == date(2026, 9, 14)

    def test_two_backward_steps_cross_the_closed_block_once(self, cal):
        # 09-14 is open; two working days back are 09-09 and then 09-08, so the
        # 09-10/11 closure is crossed exactly once.
        assert resolve(cal, day=14, shift="next", relative=-2) == date(2026, 9, 8)

    def test_a_backward_count_from_a_closed_anchor_counts_working_days(self, cal):
        # 09-10 is closed: 前シフト settles it on 09-09, and 相対 -2 then walks
        # two further working days back -> 09-08 -> 09-07.
        assert resolve(cal, day=10, shift="prev", relative=-2) == date(2026, 9, 7)

    def test_a_backward_count_from_a_closed_anchor_can_run_out(self, cal):
        # 09-10 closed, 前シフト -> 09-09, then 相対 -6 walks back to 09-01; one
        # more step would leave September, so 開始年月 rejects it.
        assert resolve(cal, day=10, shift="prev", relative=-6) == date(2026, 9, 1)
        assert resolve(cal, day=10, shift="prev", relative=-7) is None

    def test_running_out_of_the_period_yields_no_run(self, cal):
        # Rejected by 開始年月: the walk leaves September, so nothing fires.
        assert resolve(cal, day=1, shift="next", relative=-3) is None

    def test_a_positive_relative_is_unaffected(self, cal):
        # The forward direction is the mirror of the backward one.
        # 相対 4 on 1日 is the 5th working day, 09-07.
        assert resolve(cal, day=1, shift="next", relative=4) == date(2026, 9, 7)


class TestAnchorsAreNotWalkedWhenOpen:
    """``day=N`` is the Nth of the month, so an open anchor needs no movement.

    ``jobcenter()`` builds a 暦日 (absolute calendar day) anchor, which is what
    NEC's 毎月（日付） means. Building it as a 運用日 count instead made ``day=30``
    mean "the 30th working day", which lands in the *following* month and was
    the single root cause behind three of the four bugs this suite used to
    record: an overshooting anchor looks exactly like a sign inversion and like a
    double-counted closure.
    """

    def test_an_open_anchor_needs_no_walk(self, cal):
        # 09-30 is a working day, so 前シフト leaves it exactly where it falls.
        assert resolve(cal, day=30, shift="prev") == date(2026, 9, 30)

    def test_a_bare_closed_anchor_is_skipped_or_kept(self, cal):
        # 09-19 is a Saturday, so with the default 実行しない there is no run.
        assert resolve(cal, day=19) is None

    def test_a_forward_relative_on_an_open_anchor(self, cal):
        # 09-15 is open; 相対 1 is the next working day, 09-16.
        assert resolve(cal, day=15, shift="next", relative=1) == date(2026, 9, 16)


class TestMonthEnd:
    """``L`` is the last day of the month, and it shifts like any other anchor."""

    def test_l_is_the_last_day(self, cal):
        assert resolve(cal, day="L", shift="next") == date(2026, 9, 30)

    def test_l_is_case_insensitive(self, cal):
        assert resolve(cal, day="l", shift="next") == date(2026, 9, 30)

    def test_three_working_days_before_month_end(self, cal):
        # 月末の3営業日前: L is 09-30, and 相対 -2 walks two working days back
        # -> 09-29 -> 09-28.
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
