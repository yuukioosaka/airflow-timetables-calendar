"""Tests for the base-date-aware business month (:class:`Period`)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from airflow_timetables_calendar import Period, period_for


class TestDefaultBaseDay:
    """``base_day=1`` is the ordinary calendar month."""

    def test_start_is_the_first(self):
        assert period_for(date(2026, 9, 10)).start == date(2026, 9, 1)

    def test_end_is_the_last(self):
        assert period_for(date(2026, 9, 10)).end == date(2026, 9, 30)

    def test_february_length(self):
        assert period_for(date(2026, 2, 14)).end == date(2026, 2, 28)

    def test_leap_february_length(self):
        assert period_for(date(2028, 2, 14)).end == date(2028, 2, 29)


class TestBaseDay26:
    """The classic Japanese 26th-to-25th business month."""

    def test_period_containing_the_10th(self):
        period = period_for(date(2026, 9, 10), base_day=26)
        assert period.start == date(2026, 8, 26)
        assert period.end == date(2026, 9, 25)

    def test_on_the_base_date_itself(self):
        assert period_for(date(2026, 9, 26), 26).start == date(2026, 9, 26)

    def test_before_the_base_date_belongs_to_previous_month(self):
        assert period_for(date(2026, 8, 25), 26).start == date(2026, 7, 26)

    def test_last_day_before_the_base_date(self):
        assert period_for(date(2026, 9, 25), 26).start == date(2026, 8, 26)

    def test_next(self):
        assert period_for(date(2026, 9, 10), 26).next().start == date(2026, 9, 26)

    def test_prev(self):
        assert period_for(date(2026, 9, 10), 26).prev().start == date(2026, 7, 26)

    def test_year_boundary_next(self):
        assert period_for(date(2026, 12, 30), 26).next().start == date(2027, 1, 26)

    def test_year_boundary_prev(self):
        # 2027-01-10 sits in the period anchored 2026-12-26, so the one before it
        # is anchored 2026-11-26.
        assert period_for(date(2027, 1, 10), 26).prev().start == date(2026, 11, 26)

    def test_year_boundary_anchor_month(self):
        # The period's 基準日 is 2026-12-26, but the month the day-based 開始日
        # forms count within is December.
        period = period_for(date(2027, 1, 10), 26)
        assert period.anchor_month == date(2026, 12, 1)


class TestClampedBaseDay:
    """A base day past the end of a short month must clamp, not raise or skip."""

    def test_last_day_of_september_with_base_31(self):
        # 2026-09-30 is the last day, so it anchors its own period.
        period = period_for(date(2026, 9, 30), 31)
        assert period.start == date(2026, 9, 30)
        assert period.end == date(2026, 10, 30)

    def test_day_before_the_clamped_anchor(self):
        assert period_for(date(2026, 9, 29), 31).start == date(2026, 8, 31)

    def test_periods_are_contiguous(self):
        # A clamped anchor must still leave no gap: Aug's period ends 09-29,
        # which is exactly the day before September's period opens.
        assert period_for(date(2026, 9, 29), 31).end == date(2026, 9, 29)

    def test_every_day_belongs_to_exactly_one_period(self):
        # The regression this guards: an earlier version compared the raw
        # base_day instead of the clamped anchor, so days between the clamped
        # anchor and the nominal base day fell outside every period.
        day = date(2026, 1, 1)
        for _ in range(365):
            period = period_for(day, 31)
            assert period.contains(day), f"{day} fell outside {period}"
            day += timedelta(days=1)

    def test_february_base_30(self):
        period = period_for(date(2027, 2, 28), 30)
        assert period.start == date(2027, 2, 28)
        assert period.end == date(2027, 3, 29)


class TestPeriodNavigation:
    def test_anchor_offsets(self):
        period = period_for(date(2026, 9, 1))
        assert period.anchor(0) == date(2026, 9, 1)
        assert period.anchor(1) == date(2026, 10, 1)
        assert period.anchor(-1) == date(2026, 8, 1)
        assert period.anchor(12) == date(2027, 9, 1)
        assert period.anchor(-12) == date(2025, 9, 1)

    def test_contains(self):
        period = period_for(date(2026, 9, 1))
        assert period.contains(date(2026, 9, 1))
        assert period.contains(date(2026, 9, 30))
        assert not period.contains(date(2026, 8, 31))
        assert not period.contains(date(2026, 10, 1))

    def test_base_day_validation(self):
        with pytest.raises(ValueError):
            Period(2026, 9, 0)
        with pytest.raises(ValueError):
            Period(2026, 9, 40)
