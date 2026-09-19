"""Tests for the JP1/AJS3 rule engine.

The whole-year sweep at the bottom is the highest-value test here: it is what
actually caught two off-by-one bugs during development (``_apply_offset`` counting
one working day short, and ``_scan`` silently returning ``None`` past a 1900-day
step budget). Individual case assertions did not catch either.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta

import pytest

from airflow_timetables_calendar import (
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
    nth_business_day,
    nth_business_day_from_end,
    period_for,
    resolve_rules,
)
from conftest import SEPTEMBER_CLOSED, SyntheticCalendar


def rule(**kwargs) -> ScheduleRule:
    return ScheduleRule(**kwargs)


# --------------------------------------------------------------------------- #
# 種別 + 開始日
# --------------------------------------------------------------------------- #


class TestNthOperatingDay:
    """第n営業日 -- the count is inclusive, so n=1 is 月初営業日."""

    @pytest.mark.parametrize(
        ("n", "expected"),
        [
            (1, date(2026, 9, 1)),
            (2, date(2026, 9, 2)),
            (4, date(2026, 9, 4)),
            (5, date(2026, 9, 7)),  # skips the weekend
            (7, date(2026, 9, 9)),
            (8, date(2026, 9, 14)),  # skips 10th/11th, which are closed
            (13, date(2026, 9, 24)),  # skips the 21st-23rd block
        ],
    )
    def test_september_2026(self, calendar, n, expected):
        got = rule(**nth_business_day(n)).resolve(period_for(date(2026, 9, 1)), calendar)
        assert got == expected

    def test_n_below_one_is_rejected(self):
        with pytest.raises(ValueError):
            nth_business_day(0)

    def test_a_large_n_crosses_into_the_next_month(self, open_calendar):
        # 40 working days from 2026-09-01 is well into October. An earlier version
        # capped the walk at ~5 years of steps *from the start date* and returned
        # None here, which reads as "no run this month" rather than an error.
        got = rule(kind=Kind.OPERATING, day=40, substitution=Substitution.RUN_ANYWAY).resolve(
            period_for(date(2026, 9, 1)), open_calendar
        )
        assert got == date(2026, 10, 26)


class TestNthOperatingDayFromMonthEnd:
    """月末n営業日前 -- n is inclusive of the last day, so n=0 is 月末営業日."""

    @pytest.mark.parametrize(
        ("n", "expected"),
        [
            (0, date(2026, 9, 30)),
            (1, date(2026, 9, 29)),
            (2, date(2026, 9, 28)),
            (3, date(2026, 9, 25)),  # Fri: skips the weekend
            (6, date(2026, 9, 17)),
        ],
    )
    def test_september_2026(self, calendar, n, expected):
        got = rule(**nth_business_day_from_end(n)).resolve(period_for(date(2026, 9, 1)), calendar)
        assert got == expected

    def test_negative_n_is_rejected(self):
        with pytest.raises(ValueError):
            nth_business_day_from_end(-1)


class TestAbsoluteAndRelativeDays:
    def test_absolute_day(self, calendar):
        got = rule(kind=Kind.ABSOLUTE, day=15).resolve(period_for(date(2026, 9, 1)), calendar)
        assert got == date(2026, 9, 15)

    def test_absolute_day_is_clamped_to_short_months(self, calendar):
        got = rule(kind=Kind.ABSOLUTE, day=31).resolve(period_for(date(2026, 9, 1)), calendar)
        assert got == date(2026, 9, 30)

    def test_relative_day_counts_from_the_anchor(self, open_calendar):
        # 相対日 -- calendar days, not working days. Runs even on a weekend,
        # because RUN_ANYWAY means "do not move off a closed day".
        got = rule(kind=Kind.RELATIVE, day=6, substitution=Substitution.RUN_ANYWAY).resolve(
            period_for(date(2026, 9, 1)), open_calendar
        )
        assert got == date(2026, 9, 6)  # a Sunday

    def test_relative_day_with_default_skip_yields_nothing_when_closed(self, open_calendar):
        got = rule(kind=Kind.RELATIVE, day=6).resolve(period_for(date(2026, 9, 1)), open_calendar)
        assert got is None


class TestClosedDays:
    """休業日 -- counts closed days, and each weekend day counts separately."""

    def test_first_closed_day_is_saturday(self, calendar):
        got = rule(kind=Kind.CLOSED, day=1, substitution=Substitution.RUN_ANYWAY).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert got == date(2026, 9, 5)

    def test_saturday_and_sunday_are_counted_separately(self, calendar):
        # September's closed days in order are 05, 06, 10, 11, 12, 13, ...
        # The 4th is therefore Friday the 11th, NOT Thursday the 10th.
        got = rule(kind=Kind.CLOSED, day=4, substitution=Substitution.RUN_ANYWAY).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert got == date(2026, 9, 11)


class TestWeekdayStartDay:
    """曜日指定 -- the Nth <weekday> of the month."""

    def test_second_tuesday(self, calendar):
        got = rule(start_day=StartDay.WEEKDAY, weekday=Weekday.TUE, day=2).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert got == date(2026, 9, 8)

    def test_first_monday(self, calendar):
        got = rule(start_day=StartDay.WEEKDAY, weekday=Weekday.MON, day=1).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert got == date(2026, 9, 7)

    def test_missing_weekday_raises(self, calendar):
        with pytest.raises(ValueError, match="requires weekday"):
            rule(start_day=StartDay.WEEKDAY).resolve(period_for(date(2026, 9, 1)), calendar)


# --------------------------------------------------------------------------- #
# 休業日の振り替え
# --------------------------------------------------------------------------- #


class TestSubstitution:
    """休業日の振り替え."""

    @pytest.fixture
    def closed_wednesday(self) -> SyntheticCalendar:
        # 2026-09-30 is a Wednesday; close it so substitution has work to do.
        return SyntheticCalendar(SEPTEMBER_CLOSED | {date(2026, 9, 30)})

    def test_skip_produces_no_run(self, closed_wednesday):
        got = rule(kind=Kind.ABSOLUTE, day=30, substitution=Substitution.SKIP).resolve(
            period_for(date(2026, 9, 1)), closed_wednesday
        )
        assert got is None

    def test_previous_moves_earlier(self, closed_wednesday):
        got = rule(
            kind=Kind.ABSOLUTE, day=30, substitution=Substitution.PREVIOUS, grace_days=10
        ).resolve(period_for(date(2026, 9, 1)), closed_wednesday)
        assert got == date(2026, 9, 29)

    def test_next_moves_later_and_may_leave_the_month(self, closed_wednesday):
        got = rule(
            kind=Kind.ABSOLUTE, day=30, substitution=Substitution.NEXT, grace_days=10
        ).resolve(period_for(date(2026, 9, 1)), closed_wednesday)
        assert got == date(2026, 10, 1)

    def test_run_anyway_keeps_the_closed_day(self, closed_wednesday):
        got = rule(kind=Kind.ABSOLUTE, day=30, substitution=Substitution.RUN_ANYWAY).resolve(
            period_for(date(2026, 9, 1)), closed_wednesday
        )
        assert got == date(2026, 9, 30)

    def test_a_working_day_is_never_moved(self, calendar):
        for substitution in Substitution:
            got = rule(kind=Kind.ABSOLUTE, day=15, substitution=substitution).resolve(
                period_for(date(2026, 9, 1)), calendar
            )
            assert got == date(2026, 9, 15)


class TestGraceDays:
    """振り替え猶予日数 -- running out of window means no run, not an error."""

    @pytest.fixture
    def long_closure(self) -> SyntheticCalendar:
        return SyntheticCalendar({date(2026, 9, 20) + timedelta(days=i) for i in range(20)})

    def test_exhausted_window_yields_nothing(self, long_closure):
        got = rule(
            kind=Kind.ABSOLUTE, day=21, substitution=Substitution.NEXT, grace_days=3
        ).resolve(period_for(date(2026, 9, 1)), long_closure)
        assert got is None

    def test_sufficient_window_finds_the_next_working_day(self, long_closure):
        # The closure runs to 10-09 (a Friday), so the next working day is Monday.
        got = rule(
            kind=Kind.ABSOLUTE, day=21, substitution=Substitution.NEXT, grace_days=25
        ).resolve(period_for(date(2026, 9, 1)), long_closure)
        assert got == date(2026, 10, 12)


class TestScope:
    """``Scope.PERIOD`` rejects a result that leaves the anchor month."""

    def test_substitution_leaving_the_period_is_rejected(self):
        closed = SyntheticCalendar(SEPTEMBER_CLOSED | {date(2026, 9, 30)})
        got = rule(
            kind=Kind.ABSOLUTE,
            day=30,
            substitution=Substitution.NEXT,
            grace_days=10,
            scope=Scope.PERIOD,
        ).resolve(period_for(date(2026, 9, 1)), closed)
        assert got is None

    def test_an_in_period_result_is_kept(self, calendar):
        got = rule(kind=Kind.ABSOLUTE, day=15, scope=Scope.PERIOD).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert got == date(2026, 9, 15)

    def test_an_offset_leaving_the_period_is_rejected(self, calendar):
        got = rule(
            kind=Kind.ABSOLUTE,
            day=15,
            offset=60,
            count=Count.CALENDAR,
            scope=Scope.PERIOD,
        ).resolve(period_for(date(2026, 9, 1)), calendar)
        assert got is None


# --------------------------------------------------------------------------- #
# 起算スケジュール
# --------------------------------------------------------------------------- #


class TestOffsetSchedule:
    """起算スケジュール -- ``offset`` is a signed count of steps to take."""

    @pytest.fixture
    def base(self) -> ScheduleRule:
        return rule(kind=Kind.ABSOLUTE, day=15)

    def test_operating_days_forward(self, calendar, base):
        got = replace(base, offset=3, count=Count.OPERATING, offset_grace_days=20).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert got == date(2026, 9, 18)  # 15 -> 16 -> 17 -> 18

    def test_operating_days_backward(self, calendar, base):
        # 15 -> 14 -> (skip 10th/11th closed, 12th/13th weekend) -> 9 -> 8
        got = replace(base, offset=-3, count=Count.OPERATING, offset_grace_days=20).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert got == date(2026, 9, 8)

    def test_calendar_days_ignore_working_days(self, calendar, base):
        got = replace(base, offset=5, count=Count.CALENDAR).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert got == date(2026, 9, 20)  # a Sunday, deliberately

    def test_offset_grace_exhaustion_yields_nothing(self, calendar, base):
        got = replace(base, offset=5, count=Count.OPERATING, offset_grace_days=2).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert got is None

    def test_zero_offset_is_the_anchor_unchanged(self, calendar, base):
        got = replace(base, offset=0).resolve(period_for(date(2026, 9, 1)), calendar)
        assert got == date(2026, 9, 15)

    @pytest.mark.parametrize("n", [1, 2, 3, 5])
    def test_n_back_is_exactly_n_working_days(self, open_calendar, n):
        # Guards the off-by-one that made every offset land one working day short.
        anchor = rule(kind=Kind.ABSOLUTE, day=25, substitution=Substitution.RUN_ANYWAY)
        got = business_days_before(anchor, n)
        resolved = got.resolve(period_for(date(2026, 9, 1)), open_calendar)
        expected = date(2026, 9, 25)
        for _ in range(n):
            expected -= timedelta(days=1)
            while not open_calendar.is_working_day(expected):
                expected -= timedelta(days=1)
        assert resolved == expected

    @pytest.mark.parametrize("n", [1, 2, 3, 5])
    def test_n_forward_is_exactly_n_working_days(self, open_calendar, n):
        anchor = rule(kind=Kind.ABSOLUTE, day=7, substitution=Substitution.RUN_ANYWAY)
        got = business_days_after(anchor, n)
        resolved = got.resolve(period_for(date(2026, 9, 1)), open_calendar)
        expected = date(2026, 9, 7)
        for _ in range(n):
            expected += timedelta(days=1)
            while not open_calendar.is_working_day(expected):
                expected += timedelta(days=1)
        assert resolved == expected


class TestCalendarDayHelpers:
    """起算スケジュール with ``Count.CALENDAR`` ignores working days entirely."""

    def test_days_before(self, calendar):
        # Anchor on Friday the 18th; the shift lands mid-week so substitution is
        # not what is being measured here.
        anchor = rule(kind=Kind.ABSOLUTE, day=18)
        got = calendar_days_before(anchor, 5).resolve(period_for(date(2026, 9, 1)), calendar)
        assert got == date(2026, 9, 13)

    def test_days_after(self, calendar):
        anchor = rule(kind=Kind.ABSOLUTE, day=8)
        got = calendar_days_after(anchor, 5).resolve(period_for(date(2026, 9, 1)), calendar)
        assert got == date(2026, 9, 13)

    def test_calendar_days_land_on_a_weekend_when_asked(self, open_calendar):
        # Count.CALENDAR does not consult the calendar, so the result is a Sunday.
        anchor = rule(kind=Kind.ABSOLUTE, day=7)
        got = calendar_days_after(anchor, 6).resolve(period_for(date(2026, 9, 1)), open_calendar)
        assert got == date(2026, 9, 13)

    def test_an_anchor_on_a_closed_day_yields_nothing(self, open_calendar):
        # Substitution runs *before* the offset, so a Saturday anchor is skipped
        # even though the calendar-day shift itself would have worked.
        anchor = rule(kind=Kind.ABSOLUTE, day=19)  # Saturday
        assert (
            calendar_days_before(anchor, 5).resolve(period_for(date(2026, 9, 1)), open_calendar)
            is None
        )

    def test_they_require_a_rule_not_a_dict(self):
        # These helpers take ScheduleRule (they use dataclasses.replace).
        with pytest.raises(TypeError):
            business_days_before({"kind": Kind.ABSOLUTE}, 1)


# --------------------------------------------------------------------------- #
# 基準日 / month_offset
# --------------------------------------------------------------------------- #


class TestMonthOffset:
    def test_previous_month_end_working_day(self, calendar):
        got = rule(**BUSINESS_DAY_RULES["前月末営業日"]).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert got == date(2026, 8, 31)  # a Monday

    def test_previous_month_end_working_day_next_period(self, calendar):
        got = rule(**BUSINESS_DAY_RULES["前月末営業日"]).resolve(
            period_for(date(2026, 10, 1)), calendar
        )
        assert got == date(2026, 9, 30)

    def test_day_before_month_end(self, calendar):
        got = rule(**BUSINESS_DAY_RULES["月末前営業日"]).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert got == date(2026, 9, 29)

    def test_a_run_that_lands_in_the_previous_month_still_matches(self, calendar):
        # `resolve()` was never wrong here; `matches()` was. It asked only which
        # period *contains* the day, so a rule whose run belongs to the following
        # period -- which is exactly what month_offset=-1 produces -- was invisible
        # to the per-day question the timetable actually asks. The preset looked
        # like dead code even though resolve() returned the right day.
        presets = build_rules(["前月末営業日"])
        assert resolve_rules(presets, date(2026, 8, 31), calendar) is not None
        assert presets[0].matches(date(2026, 8, 31), calendar) is True
        # A day that is nobody's run is still not matched.
        assert presets[0].matches(date(2026, 8, 28), calendar) is False

    def test_every_period_contributes_exactly_one_run(self, calendar):
        # Per day, September has exactly one run: 09-30, which is September's own
        # last working day. It is produced by the *October* period, whose
        # 前月末営業日 is the month end of September -- which is the whole point of
        # the preset and exactly what the old `matches()` could not see.
        #
        # (August's last working day, 08-31, is the September period's own run,
        # but it lands outside this month so it is not in this list.)
        presets = build_rules(["前月末営業日"])
        runs = [
            day for day in _september_2026() if resolve_rules(presets, day, calendar) is not None
        ]
        assert runs == [date(2026, 9, 30)]
        assert calendar.is_working_day(runs[0])


class TestBaseDayIntegration:
    """Rules evaluated inside a 26th-to-25th business month.

    With ``base_day=26`` the period containing 2026-09-10 runs
    2026-08-26..2026-09-25, and it is anchored in **August**: the day-based 開始日
    forms (日付指定 / 月末指定 / 曜日指定) count within that anchor month, because
    that is where the 基準日 sits.
    """

    @pytest.fixture
    def period(self) -> Period:
        return period_for(date(2026, 9, 10), base_day=26)

    def test_period_bounds(self, period):
        assert period.start == date(2026, 8, 26)
        assert period.end == date(2026, 9, 25)

    def test_anchor_month_is_august(self, period):
        assert period.anchor_month == date(2026, 8, 1)

    def test_month_end_working_day_is_augusts(self, period, open_calendar):
        # 2026-08-31 is a Monday, so it is August's last working day -- which falls
        # inside this period, as intended.
        got = rule(**nth_business_day_from_end(0)).resolve(period, open_calendar)
        assert got == date(2026, 8, 31)
        assert period.contains(got)

    def test_first_working_day_counted_within_august(self, period, open_calendar):
        # 2026-08-26 is a Wednesday, so it is both the period start and the first
        # working day of the anchor month that is >= the period start.
        got = rule(**nth_business_day(1)).resolve(period, open_calendar)
        assert got == date(2026, 8, 3)  # counting from the 1st of August, per JP1

    def test_month_offset_moves_a_whole_period(self, period, open_calendar):
        # 前月末営業日 = the previous *period's* month-end, i.e. July's.
        got = rule(**BUSINESS_DAY_RULES["前月末営業日"]).resolve(period, open_calendar)
        assert got == date(2026, 7, 31)

    def test_scope_period_keeps_the_result_inside(self, period, open_calendar):
        # 第3営業日 of August (Mon the 3rd) is before the period opens, so scoping
        # rejects it -- a useful guard when mixing 基準日 with 開始日.
        # nth_business_day() already supplies a scope, so override it rather
        # than passing it twice.
        got = replace(ScheduleRule(**nth_business_day(3)), scope=Scope.PERIOD).resolve(
            period, open_calendar
        )
        assert got is None


# --------------------------------------------------------------------------- #
# Presets and builders
# --------------------------------------------------------------------------- #


class TestPresets:
    def test_every_preset_builds_a_rule(self):
        for name, kwargs in BUSINESS_DAY_RULES.items():
            assert isinstance(ScheduleRule(**kwargs), ScheduleRule), name

    def test_presets_return_plain_dicts(self):
        for name, kwargs in BUSINESS_DAY_RULES.items():
            assert isinstance(kwargs, dict), name

    def test_daily_preset_is_marked_daily(self):
        assert rule(**BUSINESS_DAY_RULES["毎営業日"]).frequency is Frequency.DAILY

    def test_daily_preset_matches_every_working_day(self, calendar):
        # `resolve` answers for one period and returns a single day -- for a DAILY
        # rule that is the anchor, the month's 1st working day. What makes it
        # "毎営業日" is `matches`, which asks the per-day question and so honours
        # 処理サイクル.
        daily = rule(**BUSINESS_DAY_RULES["毎営業日"])
        assert daily.frequency is Frequency.DAILY
        assert daily.resolve(period_for(date(2026, 9, 1)), calendar) == date(2026, 9, 1)

        assert daily.matches(date(2026, 9, 1), calendar)
        assert daily.matches(date(2026, 9, 15), calendar)
        assert daily.matches(date(2026, 9, 30), calendar)
        assert not daily.matches(date(2026, 9, 19), calendar)  # Saturday
        assert not daily.matches(date(2026, 9, 10), calendar)  # closed

    def test_monthly_preset_matches_only_its_own_occurrence(self, calendar):
        monthly = rule(**BUSINESS_DAY_RULES["第1営業日"])
        assert monthly.frequency is Frequency.MONTHLY
        assert monthly.matches(date(2026, 9, 1), calendar)
        assert not monthly.matches(date(2026, 9, 15), calendar)

    def test_monthly_and_daily_differ_on_the_same_month(self, calendar):
        # The regression guard for the frequency check: a monthly preset must not
        # widen into "every day" just because the machinery is shared.
        monthly = rule(**BUSINESS_DAY_RULES["第1営業日"])
        daily = rule(**BUSINESS_DAY_RULES["毎営業日"])
        assert [d for d in _september_2026() if monthly.matches(d, calendar)] == [date(2026, 9, 1)]
        assert [d for d in _september_2026() if daily.matches(d, calendar)] == [
            d for d in _september_2026() if calendar.is_working_day(d)
        ]

    def test_month_end_and_this_month_end_agree(self, calendar):
        a = rule(**BUSINESS_DAY_RULES["月末営業日"]).resolve(period_for(date(2026, 9, 1)), calendar)
        b = rule(**BUSINESS_DAY_RULES["当月末営業日"]).resolve(
            period_for(date(2026, 9, 1)), calendar
        )
        assert a == b == date(2026, 9, 30)

    def test_first_business_day_and_month_start_agree(self, calendar):
        a = rule(**BUSINESS_DAY_RULES["第1営業日"]).resolve(period_for(date(2026, 9, 1)), calendar)
        b = rule(**BUSINESS_DAY_RULES["月初営業日"]).resolve(period_for(date(2026, 9, 1)), calendar)
        assert a == b == date(2026, 9, 1)


class TestBuildRules:
    def test_accepts_names_dicts_and_rules(self):
        rules = build_rules(
            [
                "第1営業日",
                nth_business_day_from_end(0),
                rule(kind=Kind.ABSOLUTE, day=20),
            ]
        )
        assert len(rules) == 3
        assert all(isinstance(r, ScheduleRule) for r in rules)

    def test_preserves_order(self):
        rules = build_rules(["第1営業日", nth_business_day_from_end(0)])
        assert [r.day for r in rules] == [1, 0]

    def test_unknown_preset_names_the_valid_ones(self):
        with pytest.raises(KeyError, match="known presets"):
            build_rules(["存在しない"])

    def test_rejects_unsupported_types(self):
        with pytest.raises(TypeError):
            build_rules([42])


class TestResolveRules:
    def test_first_matching_rule_wins(self, calendar):
        rules = build_rules([nth_business_day_from_end(0), rule(kind=Kind.ABSOLUTE, day=30)])
        matched = resolve_rules(rules, date(2026, 9, 30), calendar)
        assert matched is rules[0]

    def test_no_match_returns_none(self, calendar):
        rules = build_rules(["第1営業日"])
        assert resolve_rules(rules, date(2026, 9, 15), calendar) is None

    def test_first_match_in_list_order_is_returned(self, calendar):
        # `resolve_rules` scans in list order and returns the first rule that
        # yields the day. JobCenter gives *lower* rules higher precedence, so
        # callers list 除外 rules last and the first match is the one that wins.
        daily = rule(**BUSINESS_DAY_RULES["毎営業日"])
        explicit = rule(kind=Kind.ABSOLUTE, day=15)
        rules = build_rules([daily, explicit])
        # build_rules() normalises into a new list, so compare by value.
        assert resolve_rules(rules, date(2026, 9, 15), calendar) == rules[0]

    def test_a_later_rule_is_reachable_when_the_first_does_not_match(self, calendar):
        first = rule(kind=Kind.ABSOLUTE, day=1)
        second = rule(kind=Kind.ABSOLUTE, day=15)
        rules = build_rules([first, second])
        assert resolve_rules(rules, date(2026, 9, 15), calendar) == rules[1]


# --------------------------------------------------------------------------- #
# Enum coercion (serialization safety)
# --------------------------------------------------------------------------- #


class TestEnumCoercion:
    """A rule must survive being round-tripped through plain strings."""

    def test_strings_become_enums(self):
        r = rule(
            kind="operating",
            start_day="month_end",
            substitution="previous",
            count="operating",
            frequency="monthly",
            scope="free",
            weekday="tue",
        )
        assert r.kind is Kind.OPERATING
        assert r.start_day is StartDay.MONTH_END
        assert r.substitution is Substitution.PREVIOUS
        assert r.count is Count.OPERATING
        assert r.frequency is Frequency.MONTHLY
        assert r.scope is Scope.FREE
        assert r.weekday is Weekday.TUE

    def test_a_string_kind_still_resolves(self, calendar):
        # Regression: without coercion, `kind is Kind.OPERATING` never matched and
        # resolution raised "unhandled kind 'operating'".
        from_strings = rule(kind="operating", day=5, substitution="next", grace_days=30)
        from_enums = rule(kind=Kind.OPERATING, day=5, substitution=Substitution.NEXT, grace_days=30)
        period = period_for(date(2026, 9, 1))
        assert from_strings.resolve(period, calendar) == from_enums.resolve(period, calendar)

    def test_an_invalid_value_is_rejected(self):
        with pytest.raises(ValueError):
            rule(kind="nonsense")


# --------------------------------------------------------------------------- #
# The high-value test: exhaustive agreement with a derived calendar
# --------------------------------------------------------------------------- #


class TestWholeYearSweep:
    """Cross-check every nth/from-end rule over a full year of the real JP calendar.

    Individual examples are easy to get wrong by hand (as this test suite's own
    development proved); comparing against a workday list derived directly from the
    calendar is not.
    """

    def test_nth_business_day_matches_the_derived_list(self, jp_timetable, workdays_of_jp):
        mismatches = []
        for month in range(1, 13):
            days = workdays_of_jp(2026, month)
            for n in range(1, min(len(days), 15) + 1):
                got = rule(**nth_business_day(n)).resolve(
                    period_for(date(2026, month, 1)), jp_timetable
                )
                if got != days[n - 1]:
                    mismatches.append(f"2026-{month:02d} 第{n}営業日: {got} != {days[n - 1]}")
        assert not mismatches, mismatches

    def test_nth_business_day_from_end_matches_the_derived_list(self, jp_timetable, workdays_of_jp):
        mismatches = []
        for month in range(1, 13):
            days = workdays_of_jp(2026, month)
            for n in range(0, min(len(days), 15)):
                got = rule(**nth_business_day_from_end(n)).resolve(
                    period_for(date(2026, month, 1)), jp_timetable
                )
                if got != days[-1 - n]:
                    mismatches.append(f"2026-{month:02d} 月末{n}営業日前: {got} != {days[-1 - n]}")
        assert not mismatches, mismatches

    def test_every_swept_rule_is_a_real_working_day(self, jp_timetable, workdays_of_jp):
        for month in range(1, 13):
            days = set(workdays_of_jp(2026, month))
            for n in range(1, 13):
                got = rule(**nth_business_day(n)).resolve(
                    period_for(date(2026, month, 1)), jp_timetable
                )
                if got is not None and got.month == month:
                    assert got in days, f"{got} is not a working day"


def _september_2026() -> list[date]:
    """Every day of September 2026, in order."""
    days: list[date] = []
    day = date(2026, 9, 1)
    while day.month == 9:
        days.append(day)
        day += timedelta(days=1)
    return days
