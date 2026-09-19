"""Business-day schedule rules, in the style of classical Japanese job
schedulers.

Japanese enterprise job schedulers do not express schedules as plain cron
expressions. A schedule is an **anchor date** plus a chain of **modifiers**, and
the exact vocabulary is surprisingly consistent across vendors. This module
implements that model on top of a calendar (see :mod:`.calendars`).

The Japanese terms are kept because they are the vocabulary this class of
scheduling is normally discussed in, and no vendor's documentation is
reproduced.

The classical model
-------------------
The classical model derives an execution date from three things (種別 / 開始年月 / 開始日):

* **基準日 (base date)** -- a calendar defines where a "month" starts, e.g. base
  date 26 means Aug 26..Sep 25 is treated as "August".
* **基準時刻 (base time)** -- the hour at which a *business date* rolls over.
  With base time 08:00, 08:00..next-day 07:59 is one business day. This is why a
  job can legitimately run at "25:00" and still belong to the previous date.
* **種別 (kind)** -- what the day offset counts:

  ===========  ==========================================================
  ``ABSOLUTE`` 暦日: plain calendar date, month starts on the 1st.
  ``RELATIVE`` 相対日: counted from the base date, calendar days.
  ``OPERATING`` 運用日: counted from the base date, but only 運用日
                (working days). This is how 第n営業日 works.
  ``CLOSED``   休業日: counted from the base date, only 休業日.
  ``REGISTERED`` 登録日: the day the job was registered.
  ===========  ==========================================================

* **開始日 (start day)** -- ``DAY`` (日付指定 "on day N"), ``MONTH_END``
  (月末指定 "N days before month end") or ``WEEKDAY`` (曜日指定 "the Nth
  <weekday>").

Once a date is computed, two further stages may move it:

* **休業日の振り替え (holiday substitution)** -- if the computed day is not a
  working day, either do not run, run anyway, or shift to the nearest working day
  *before* (前の運用日に振り替え) or *after* (次の運用日に振り替え) it. This is
  the classic 前営業日 / 翌営業日 behaviour.
* **起算スケジュール (offset schedule)** -- a final "n working days before/after"
  adjustment (``n運用日前`` / ``n運用日後``) or plain calendar-day adjustment
  (``n日前`` / ``n日後``).

Two bounds are important:

* **振り替え猶予日数 / 起算猶予日数 (grace days)** -- the maximum distance a shift
  or offset may travel. If no qualifying day is found inside the window, that
  occurrence simply **produces no run at all**. It is not an error.
* Because so much of this is "count from an anchor in a direction", an offset can
  legitimately land in a neighbouring month. Month scoping is therefore opt-in
  via ``scope_to_period=True``.

The compact notation
-------------------
The compact notation expresses much the same thing in one line::

    +登録、毎月（日付）、1日、休止日 後シフト、相対 4、開始時刻 18:00

which reads "monthly on day 1, shift later if closed, then move 4 more days"
(equals 月初から5営業日目). The compact notation's ``相対 n`` counts the anchor
itself as working day 1, so ``相対 4`` is the 5th working day.

:func:`simple_rule` and :func:`verbose_rule` build these rules with Japanese-facing
arguments, and
:class:`~airflow_timetables_calendar.timetable.CalendarTimetable` accepts a list
of rules directly.

This module imports nothing from Airflow.

"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import date, timedelta
from enum import Enum

from .calendars import WorkingDayCalendar

# --------------------------------------------------------------------------- #
# Enumerations
# --------------------------------------------------------------------------- #


class Kind(str, Enum):
    """種別 -- what the day offset counts."""

    REGISTERED = "registered"  # 登録日
    ABSOLUTE = "absolute"  # 絶対日
    RELATIVE = "relative"  # 相対日
    OPERATING = "operating"  # 運用日  (working days -> 第n営業日)
    CLOSED = "closed"  # 休業日


class StartDay(str, Enum):
    """開始日 -- how the start day within the period is named."""

    DAY = "day"  # 日付指定
    MONTH_END = "month_end"  # 月末指定
    WEEKDAY = "weekday"  # 曜日指定


class Substitution(str, Enum):
    """休業日の振り替え -- what to do when the day is not a working day."""

    SKIP = "skip"  # 実行しない
    PREVIOUS = "previous"  # 前の運用日に振り替え (前倒し)
    NEXT = "next"  # 次の運用日に振り替え (順延)
    RUN_ANYWAY = "run_anyway"  # 振り替えなしで実行する


class Count(str, Enum):
    """起算スケジュール -- what the offset counts."""

    OPERATING = "operating"  # n運用日前 / n運用日後
    CALENDAR = "calendar"  # n日前 / n日後 (ignores working days)


class Frequency(str, Enum):
    """処理サイクル -- the repeat period."""

    DAILY = "daily"  # 1日毎
    WEEKLY = "weekly"  # 1週毎
    MONTHLY = "monthly"  # 1月毎
    YEARLY = "yearly"  # 1年毎


class Weekday(str, Enum):
    MON = "mon"
    TUE = "tue"
    WED = "wed"
    THU = "thu"
    FRI = "fri"
    SAT = "sat"
    SUN = "sun"

    @property
    def index(self) -> int:
        """Monday-based index, matching ``date.weekday()``."""
        return ["mon", "tue", "wed", "thu", "fri", "sat", "sun"].index(self.value)


class Scope(str, Enum):
    """How far a rule is allowed to move the date."""

    #: Result must stay inside the anchor's period (開始年月 semantics).
    PERIOD = "period"
    #: Any date is acceptable as long as the grace window allows it.
    FREE = "free"


# --------------------------------------------------------------------------- #
# Calendar protocol
# --------------------------------------------------------------------------- #

# `WorkingDayCalendar` lives in `calendars` and is re-exported here so the rule
# engine's input contract sits with the rules. It is a `typing.Protocol`, so any
# object exposing `is_working_day` satisfies it -- including a caller's own class,
# with no import from this library required.


# --------------------------------------------------------------------------- #
# Period (base-date aware month)
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Period:
    """One scheduler "month", honouring 基準日 (base date).

    ``month`` is the 1-based month the period's anchor falls in. With
    ``base_day=26`` the period labelled *August* covers 2026-08-26..2026-09-25,
    and one labelled *September* covers 2026-09-26..2026-10-25.

    The anchor is clamped to the month's length, and the *next* period starts the
    day after it -- so ``base_day=31`` yields a 31-day July period followed by the
    first 30 days of August, rather than overlapping or skipping days.
    """

    year: int
    month: int
    base_day: int = 1

    def __post_init__(self) -> None:
        if not 1 <= self.base_day <= 31:
            raise ValueError(f"base_day must be between 1 and 31, got {self.base_day!r}")
        if not 1 <= self.month <= 12:
            raise ValueError(f"month must be between 1 and 12, got {self.month!r}")

    @property
    def start(self) -> date:
        """The 基準日 itself: where this period opens."""
        return _clamp_day(self.year, self.month, self.base_day)

    @property
    def end(self) -> date:
        """Last day of the period -- the day before the next period opens."""
        return self.next().start - timedelta(days=1)

    def next(self) -> Period:
        """The following period.

        Derived from the clamped start rather than a bare month arithmetic, so a
        clamped base date (``base_day=31`` in a 30-day month) still advances by a
        whole year in December and never repeats itself.
        """
        anchor = self.start
        if anchor.month == 12:
            year, month = anchor.year + 1, 1
        else:
            year, month = anchor.year, anchor.month + 1
        return Period(year, month, self.base_day)

    def prev(self) -> Period:
        anchor = self.start
        if anchor.month == 1:
            year, month = anchor.year - 1, 12
        else:
            year, month = anchor.year, anchor.month - 1
        return Period(year, month, self.base_day)

    def contains(self, day: date) -> bool:
        return self.start <= day <= self.end

    @property
    def anchor_month(self) -> date:
        """The first day of the month the 基準日 falls in.

        This is the reference point for the day-based 開始日 forms (日付指定 /
        月末指定 / 曜日指定): "day 5" means the 5th of the month the 基準日 is in.
        """
        return date(self.year, self.month, 1)

    def anchor(self, period_offset: int = 0) -> date:
        """The period's base date, shifted by whole periods."""
        period = self
        for _ in range(abs(period_offset)):
            period = period.next() if period_offset > 0 else period.prev()
        return period.start

    def __str__(self) -> str:  # pragma: no cover - debugging aid
        return f"[{self.start}..{self.end}]"


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        return 31
    return (date(year, month + 1, 1) - timedelta(days=1)).day


def _clamp_day(year: int, month: int, day: int) -> date:
    """``date(year, month, day)``, clamped to the month's last day."""
    return date(year, month, min(day, _days_in_month(year, month)))


def period_for(day: date, base_day: int = 1) -> Period:
    """The :class:`Period` that contains ``day``."""
    if base_day <= 1:
        return Period(day.year, day.month, 1)

    # Compare against the *clamped* anchor, not the raw base_day, so that a
    # base_day past the end of a short month (e.g. 31 in September) still yields
    # a period that contains the day instead of one that opens after it.
    anchor = _clamp_day(day.year, day.month, base_day)
    if day >= anchor:
        return Period(day.year, day.month, base_day)

    if day.month == 1:
        year, month = day.year - 1, 12
    else:
        year, month = day.year, day.month - 1
    return Period(year, month, base_day)


# --------------------------------------------------------------------------- #
# The rule
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ScheduleRule:
    """One classical-style schedule rule.

    A rule is only evaluated for the :class:`Period` it falls in, so
    :meth:`resolve` is always anchored to "the occurrence in this month" rather
    than producing an unbounded series.

    :param kind: What the day offset counts (種別).
    :param start_day: How the day is named (開始日).
    :param day: The N in "day N" / "the Nth weekday". For
        :attr:`StartDay.MONTH_END` it counts **back from** month end and is
        inclusive: ``day=0`` is the last day of the month, ``day=1`` the day
        before it, ``day=2`` the day before that. Note that this differs from
        :func:`simple_rule`'s ``L`` notation, where ``相対`` counts the
        anchor as working day 1.
    :param weekday: Required when ``start_day`` is :attr:`StartDay.WEEKDAY`.
    :param month_offset: Shift the whole anchor by N periods first.
    :param substitution: Holiday policy (休業日の振り替え).
    :param grace_days: Max distance the substitution may travel.
    :param offset: Final adjustment (起算スケジュール), signed.
    :param count: What ``offset`` counts.
    :param offset_grace_days: Max distance the offset may travel.
    :param shift_direction: Direction the 休止日 shift travels when the offset
        stage is the one that carries a closed anchor onto a working day.
    :param scope: 開始年月 -- whether the rule is bound to the anchor month.
    :param frequency: Repeat period (処理サイクル).
    :param include_start: Whether ``start_date`` itself may produce a run.
    :param virtual: If set, this rule only suppresses others (``除外``).
    """

    kind: Kind = Kind.OPERATING
    start_day: StartDay = StartDay.DAY
    day: int = 1
    weekday: Weekday | None = None
    month_offset: int = 0

    substitution: Substitution = Substitution.SKIP
    grace_days: int = 0

    offset: int = 0
    count: Count = Count.OPERATING
    offset_grace_days: int = 0
    #: Direction of the 休止日 shift when it is the *offset* stage that carries
    #: the date out of a closed anchor: ``+1`` 後シフト, ``-1`` 前シフト, ``0``
    #: unset. `simple_rule()` sets this so a closed anchor settles the way its own
    #: 休止日 rule says even though the substitution itself is a no-op there.
    shift_direction: int = 0

    scope: Scope = Scope.FREE
    frequency: Frequency = Frequency.MONTHLY
    include_start: bool = True
    virtual: bool = False

    def __post_init__(self) -> None:
        """Coerce raw strings back into enums.

        A rule round-trips through the DAG serializer as plain strings (see
        ``CalendarTimetable.serialize``), and this class tests its fields with
        ``is`` throughout. Without this coercion a deserialized rule carries
        ``kind='operating'`` instead of ``Kind.OPERATING``, every comparison falls
        through, and the rule resolves to nothing -- or raises "unhandled kind".
        ``object.__setattr__`` is required because the dataclass is frozen.
        """
        for name, enum in (
            ("kind", Kind),
            ("start_day", StartDay),
            ("substitution", Substitution),
            ("count", Count),
            ("frequency", Frequency),
            ("scope", Scope),
        ):
            value = getattr(self, name)
            if value is not None and not isinstance(value, enum):
                object.__setattr__(self, name, enum(value))
        if self.weekday is not None and not isinstance(self.weekday, Weekday):
            object.__setattr__(self, "weekday", Weekday(self.weekday))

    # ---------------------------------------------------------------- resolve

    def resolve(self, period: Period, cal: WorkingDayCalendar) -> date | None:
        """The execution date this rule produces in ``period``, or None.

        ``None`` means "no run this period", which is a normal outcome (the
        grace windows ran out, or the day is closed and the policy is to skip).

        ``month_offset`` moves whole *periods*, not whole calendar months. With the
        default ``base_day=1`` the two are the same; with e.g. ``base_day=26``
        "one period back" from [2026-08-26..2026-09-25] is [2026-07-26..2026-08-25],
        which is what 前月末営業日 needs.

        Note that 処理サイクル (:attr:`frequency`) is *not* consulted here:
        ``resolve()`` answers "the one day this rule produces in ``period``",
        which for 毎営業日 is its anchor -- the month's 1st working day.
        "Every working day" is the per-day question and lives in :meth:`matches`.
        """
        anchor = period
        for _ in range(abs(self.month_offset)):
            anchor = anchor.next() if self.month_offset > 0 else anchor.prev()

        # 処理サイクル, per-month fast path: if day-based 開始日 places the anchor
        # in a different month from the period, this period cannot fire. This is
        # exact for the day-based forms -- which is where `matches()` ends up for
        # 第n営業日/月末営業日 and friends -- and months are close to
        # period-length, so mixed base_day/開始日 configurations may be rejected
        # up to one month early rather than a day early.
        #
        # It deliberately does NOT apply once month_offset has moved the anchor:
        # 前月末営業日 asks for a day that is *supposed* to land in the previous
        # month, so comparing against the anchor month there would reject it.
        if (
            not self.month_offset
            and self.kind is not Kind.REGISTERED
            and (period.year, period.month) != (anchor.year, anchor.month)
        ):
            return None

        # 開始年月 as it is usually written constrains the date the rule *names*.
        # Asking it of the anchor, rather than of the final result, is what makes
        # `simple_rule(day="L", shift="prev", relative=1)` mean "the first working
        # day after month end" -- previously the result was rejected for landing
        # outside the period, and the schedule silently never fired at all.
        #
        # `month_bound` is kept for the one constraint that does still apply after
        # the move: a date that walks back past the anchor month is rejected.
        month_bound: tuple[int, int] | None = None
        if self.scope is Scope.PERIOD:
            month_bound = (anchor.year, anchor.month)
            if self.kind is not Kind.REGISTERED and ((period.year, period.month) != month_bound):
                return None

        day = self._anchor_day(anchor, cal)
        if day is None:
            return None

        day = self._substitute(day, cal)
        if day is None:
            return None

        if self.offset:
            day = self._apply_offset(day, cal)
            if day is None:
                return None

        # 振り替え and 起算 are movements, so the moved date may leave the period --
        # 基準日 26 deliberately places the 25th's run on the 26th of the next
        # month, and 前月末営業日 resolves to the previous period outright. But
        # 開始年月 does reject a date that walks *back* past the month it names, so
        # `simple_rule(day=1, shift="next", relative=-3)` still produces no run.
        if month_bound is not None and (day.year, day.month) < month_bound:
            return None

        return day

    # ------------------------------------------------------------- internals

    def _anchor_day(self, period: Period, cal: WorkingDayCalendar) -> date | None:
        """The date named by 種別 + 開始日, before holiday substitution.

        The day-based kinds (日付指定 / 月末指定 / 曜日指定) are all described
        relative to *the period's anchor month*, so they take the month that the
        基準日 falls in -- not ``period.start``, which is the period's first day and
        equals the 基準日 only in the trivial ``base_day=1`` case.
        """
        month = period.anchor_month
        if self.kind is Kind.REGISTERED:
            # 登録日 is the job's registration date, which has nothing to do with
            # the period; resolve against the period's own start.
            return period.start

        if self.start_day is StartDay.WEEKDAY:
            return self._nth_weekday(month)

        if self.start_day is StartDay.MONTH_END:
            return self._from_month_end(month, cal)

        # StartDay.DAY: for absolute/relative days this is simply ``day``.
        # For the working/closed-day kinds it is a *count* within the period, so
        # the off-by-one shift lives in those methods, not here.
        if self.kind is Kind.ABSOLUTE:
            return _clamp_day(month.year, month.month, self.day)
        if self.kind is Kind.RELATIVE:
            return month + timedelta(days=self.day - 1)
        if self.kind is Kind.OPERATING:
            return self._nth_working_day_in_period(month, self.day, cal)
        if self.kind is Kind.CLOSED:
            return self._nth_closed_day_in_period(month, self.day, cal)
        raise AssertionError(f"unhandled kind {self.kind!r}")

    def _nth_weekday(self, anchor: date) -> date:
        """曜日指定: the Nth <weekday> of the anchor's month."""
        if self.weekday is None:
            raise ValueError("start_day=WEEKDAY requires weekday=<Weekday>")
        first = date(anchor.year, anchor.month, 1)
        delta = (self.weekday.index - first.weekday()) % 7
        return first + timedelta(days=delta + 7 * (self.day - 1))

    def _from_month_end(self, anchor: date, cal: WorkingDayCalendar) -> date | None:
        """月末指定: walk back ``day`` days from the anchor month's last day.

        The count is inclusive of the last day, so ``OPERATING, day=0`` is
        月末営業日 and ``day=1`` is 月末の前営業日.
        """
        last = date(anchor.year, anchor.month, _days_in_month(anchor.year, anchor.month))
        if self.kind is Kind.OPERATING:
            return self._nth_working_day_back_from(last, self.day, cal)
        if self.kind is Kind.CLOSED:
            return self._nth_closed_day_back_from(last, self.day, cal)
        return last - timedelta(days=self.day)

    def _nth_working_day_in_period(
        self, anchor: date, n: int, cal: WorkingDayCalendar
    ) -> date | None:
        """The n-th 運用日 from the start of the anchor's month (n=1 is 月初営業日)."""
        if n < 1:
            return None
        return self._scan(date(anchor.year, anchor.month, 1), 1, n - 1, cal.is_working_day)

    def _nth_closed_day_in_period(
        self, anchor: date, n: int, cal: WorkingDayCalendar
    ) -> date | None:
        """The n-th 休業日 from the start of the anchor's month (n=1 is the first one)."""
        if n < 1:
            return None
        return self._scan(
            date(anchor.year, anchor.month, 1), 1, n - 1, lambda d: not cal.is_working_day(d)
        )

    def _nth_working_day_back_from(
        self, last: date, n: int, cal: WorkingDayCalendar
    ) -> date | None:
        return self._scan(last, -1, n, cal.is_working_day)

    def _nth_closed_day_back_from(self, last: date, n: int, cal: WorkingDayCalendar) -> date | None:
        return self._scan(last, -1, n, lambda d: not cal.is_working_day(d))

    @staticmethod
    def _scan(start: date, step: int, n: int, predicate: Callable[[date], bool]) -> date | None:
        """Walk from ``start`` until ``n`` matching days have been *skipped*.

        The first matching day is the answer when ``n == 0``, so this never
        returns a day it walked past.

        The walk is deliberately unbounded in wall-clock terms and stops only when
        the date arithmetic itself gives out. Bounding it by a step count was a
        bug: ``1900`` steps is barely five years, which silently returned ``None``
        ("no run this period") for requests that have a perfectly good answer.
        Walking day-by-day over a century of ``date`` objects costs microseconds,
        so there is no reason to cap it -- and unlike the grace windows, a large
        count here is a legitimate request, not a configuration error.
        """
        day = start
        remaining = n
        while True:
            if predicate(day):
                if remaining == 0:
                    return day
                remaining -= 1
            try:
                day += timedelta(days=step)
            except OverflowError:  # pragma: no cover - date range exhausted
                return None

    def _substitute(self, day: date, cal: WorkingDayCalendar) -> date | None:
        """休業日の振り替え."""
        if cal.is_working_day(day):
            return day

        if self.substitution is Substitution.SKIP:
            return None
        if self.substitution is Substitution.RUN_ANYWAY:
            # The rule keeps the date but can end up "繰り越し未実行"; here the run
            # simply happens on the closed day.
            return day

        step = -1 if self.substitution is Substitution.PREVIOUS else 1
        limit = self.grace_days if self.grace_days else _DEFAULT_GRACE
        candidate = day + timedelta(days=step)
        for _ in range(limit):
            if cal.is_working_day(candidate):
                return candidate
            candidate += timedelta(days=step)
        # Nothing inside the grace window (振り替え猶予日数): no run this period.
        return None

    def _apply_offset(self, day: date, cal: WorkingDayCalendar) -> date | None:
        """起算スケジュール: ``n運用日前`` / ``n運用日後`` / ``n日前`` / ``n日後``.

        ``offset`` is a *signed* count of steps to take, so the first matching day
        encountered is step 1. (``_scan`` inverts that convention because its
        ``n`` counts matching days to *skip*; the two are not interchangeable.)
        """
        if not self.offset:
            return day
        if self.count is Count.CALENDAR:
            return day + timedelta(days=self.offset)

        # A closed anchor has still to be carried onto a working day before the
        # count proper begins, and that carry is the 休止日 shift rather than one
        # of the `n` steps -- so it must not spend one. Without this, the compact
        # notation's "shift the anchor, then count from it" composition overshoots by a day
        # whenever the anchor lands on a closed day, and -- because the
        # substitution and the offset would then both walk -- by one step per
        # closed day in between.
        #
        # When the anchor is already open this is a no-op and the walk is the
        # plain n-working-days count it has always been.
        on_shift = not cal.is_working_day(day)

        step = 1 if self.offset > 0 else -1
        # The free step out of a closed anchor belongs to the 休止日 shift, so it
        # follows the shift's direction when one is known -- 前シフト must settle
        # on the previous working day even when 相対 points forward.
        shift_step = self.shift_direction or step
        remaining = abs(self.offset)
        candidate = day
        limit = self.offset_grace_days if self.offset_grace_days else _DEFAULT_GRACE

        for _ in range(limit):
            candidate += timedelta(days=shift_step if on_shift else step)
            if cal.is_working_day(candidate):
                if on_shift:
                    # This is the shift step; the count starts from here.
                    on_shift = False
                    continue
                remaining -= 1
                if remaining == 0:
                    return candidate
        # 起算猶予日数 exceeded: no schedule is generated at all for this
        # occurrence rather than erroring, and we match that.
        return None

    # ------------------------------------------------------------ predicates

    def matches(self, day: date, cal: WorkingDayCalendar, base_day: int = 1) -> bool:
        """Whether this rule yields exactly ``day``.

        This is the per-day question, so 処理サイクル belongs here rather than in
        :meth:`resolve` (which answers for one period and returns a single day).
        A MONTHLY rule fires on the one occurrence in its own month; a DAILY rule
        re-evaluates every day, so any closed day inside the period is skipped and
        every working day is a run. Without this check "毎営業日" would only match the
        month's first working day -- its anchor -- and mean "毎月1営業日".

        A MONTHLY rule matches a day in a month it is not anchored to, as long as
        some neighbouring period's own resolution lands on that day. Both
        directions occur:

        * 前月末営業日 resolves, for period N, to a day in period N-1, so the day is
          the run of the period *after* the one containing it.
        * A forward 相対 that crosses the month end (`day="L", relative=1`)
          resolves, for period N, to a day in period N+1, so the day is the run of
          the period *before* the one containing it.

        Hence both neighbours are consulted. They cannot produce a false positive:
        periods are defined by their anchors and consecutive periods do not
        overlap, so at most one period's ``resolve()`` can equal any given day.
        """
        period = period_for(day, base_day)
        if self.frequency is Frequency.DAILY:
            # Each day is eligible, but the day still has to be a working day:
            # 毎営業日 means every *business* day.
            return cal.is_working_day(day)

        return (
            self.resolve(period, cal) == day
            or self.resolve(period.prev(), cal) == day
            or self.resolve(period.next(), cal) == day
        )


# Bound the substitution / offset walks so a misconfigured rule cannot wander
# forever. Unlike `ScheduleRule.day`, these distance windows *are* part of the
# classical model (振り替え猶予日数 / 起算猶予日数): running out of window means "no run",
# silently, rather than an error -- so an unbounded search would be wrong here,
# and `grace_days=0` means "use this default" rather than "zero tolerance".
_DEFAULT_GRACE = 366

# The compact notation has no grace-day concept of its own. This window is wide
# enough to cross any realistic holiday block (year-end/New Year runs to ~6 days)
# without becoming an implicit unbounded search.
_COMPACT_GRACE = 60


# --------------------------------------------------------------------------- #
# Compact-notation constructors
# --------------------------------------------------------------------------- #


def simple_rule(
    *,
    period: str = "monthly",
    day: int | str = 1,
    weekday: Weekday | None = None,
    shift: str | None = None,
    relative: int = 0,
) -> dict:
    """Build rule kwargs using the compact notation.

    Mirrors ``+登録、<period>、<day>、休止日 <shift>、相対 <relative>``.

    ``day`` is a **calendar day of the month**, which is what 毎月（日付）
    (monthly *by date*) means. The notation's rule text is ``＋登録、毎月（日付）、
    1日、休止日 後シフト、相対 4`` and its documented outcome is 月初から5営業日目.
    "Day 1" is therefore the 1st, not the month's 1st working day -- the
    *working*-day families are 第n営業日, built by :func:`nth_business_day`
    instead. Anything else makes ``day=30`` mean "the 30th working day of the
    month", which lands in the *following* month and explains three of this
    module's historical bugs.

    ``relative`` counts *further* working days from the anchor once the anchor
    has settled, so ``相対 0`` is the anchor itself and ``相対 4`` on 1日 is the
    5th working day. The second worked example (``L日``, 前シフト, ``相対 -2`` -> three
    working days before month end) confirms the anchor is not itself counted as
    one of the ``relative`` steps.

    The two stages are combined into a *single* walk rather than chained,
    because chaining them double-steps every closed day in the span:

    * the 休止日 policy is honoured as ``RUN_ANYWAY`` so that the anchor's own
      substitution stage cannot move the date, and
    * the whole distance -- including the shift the substitution would have
      made -- is carried by the offset stage.

    The offset always carries the value of ``relative``; it is
    :meth:`ScheduleRule._apply_offset` that knows a closed start date needs one
    free step onto a working day before the count begins. That step follows the
    休止日 shift's direction (recorded in ``shift_direction``), so 前シフト settles
    on the *previous* working day even when 相対 points forwards, and it does not
    consume one of the ``n`` counted days.

    :param period: ``"daily"``, ``"weekly"``, ``"monthly"`` or ``"yearly"``.
    :param day: A day number, or ``"L"`` for the last day of the month.
    :param weekday: For weekly rules.
    :param shift: ``"next"`` (後シフト), ``"prev"`` (前シフト), ``"none"``.
    :param relative: Additional working-day offset (``相対 n``).
    """
    substitution = {
        None: Substitution.SKIP,
        "none": Substitution.SKIP,
        "next": Substitution.NEXT,
        "prev": Substitution.PREVIOUS,
        "later": Substitution.NEXT,
        "earlier": Substitution.PREVIOUS,
        "anyway": Substitution.RUN_ANYWAY,
    }
    if shift not in substitution:
        raise ValueError(
            f"shift must be one of {sorted(k for k in substitution if k)}, got {shift!r}"
        )

    kwargs: dict = {
        "frequency": Frequency(period),
        "kind": Kind.OPERATING,
        "substitution": substitution[shift],
        # 開始年月 bounds the *generated* date, so a 相対 that walks out of the
        # month produces no run. This is what keeps `relative=-3` from silently
        # landing in the previous month.
        "scope": Scope.PERIOD,
    }

    if weekday is not None:
        kwargs["start_day"] = StartDay.WEEKDAY
        kwargs["weekday"] = weekday
        kwargs["day"] = day if isinstance(day, int) else 1
        # The Nth <weekday> is a calendar-date anchor, not a working-day count.
        kwargs["kind"] = Kind.ABSOLUTE
    elif day in ("L", "l"):
        # L is the month's last day; 月末指定 counts back from it in 運用日.
        kwargs["start_day"] = StartDay.MONTH_END
        kwargs["day"] = 0
    elif isinstance(day, str):
        raise ValueError(f'day must be an int or "L", got {day!r}')
    else:
        # 毎月（日付）: an absolute calendar day of the month.
        kwargs["start_day"] = StartDay.DAY
        kwargs["day"] = int(day)
        kwargs["kind"] = Kind.ABSOLUTE

    if relative:
        kwargs["count"] = Count.OPERATING
        kwargs["offset_grace_days"] = _COMPACT_GRACE
        # Only ONE stage may walk: the substitution must never move the date, so
        # that the offset's walk is the only thing that steps. (Chaining the two
        # makes every closed day in the span cost two steps.)
        kwargs["substitution"] = Substitution.RUN_ANYWAY
        # 相対 keeps its own sign; the 休止日 shift does not redirect it. The
        # rule text "休止日 後シフト、相対 4" reads as: the shift settles the
        # anchor, then 相対 counts from that settled day, in 相対's direction. A
        # 前シフト turn a positive 相対 backwards would make "前シフト、相対 1" move
        # away from the anchor, which is not what the rule reads as.
        #
        # `_apply_offset` carries a closed start date onto the first working day
        # for free, so the walk begins at the settled anchor without needing the
        # substitution stage to move anything.
        kwargs["offset"] = relative
        kwargs["shift_direction"] = {
            Substitution.PREVIOUS: -1,
            Substitution.NEXT: 1,
        }.get(substitution[shift], 0)
    elif substitution[shift] in (Substitution.NEXT, Substitution.PREVIOUS):
        kwargs["grace_days"] = _COMPACT_GRACE

    return kwargs


def verbose_rule(
    *,
    kind: Kind = Kind.OPERATING,
    start_day: StartDay = StartDay.DAY,
    day: int = 1,
    weekday: Weekday | None = None,
    month_offset: int = 0,
    substitution: Substitution = Substitution.SKIP,
    grace_days: int = 0,
    offset: int = 0,
    count: Count = Count.OPERATING,
    offset_grace_days: int = 0,
    shift_direction: int = 0,
    frequency: Frequency = Frequency.MONTHLY,
    scope: Scope = Scope.FREE,
) -> dict:
    """Build rule kwargs using the classical, fully-explicit vocabulary.

    Argument names map 1:1 onto the 種別 / 開始日 / 振り替え / 起算 concepts, so
    an existing definition from such a scheduler can be transcribed directly.
    """
    return {
        "kind": kind,
        "start_day": start_day,
        "day": day,
        "weekday": weekday,
        "month_offset": month_offset,
        "substitution": substitution,
        "grace_days": grace_days,
        "offset": offset,
        "count": count,
        "offset_grace_days": offset_grace_days,
        "shift_direction": shift_direction,
        "frequency": frequency,
        "scope": scope,
    }


# --------------------------------------------------------------------------- #
# Common Japanese business-day phrases (第n営業日 / 月末 / 前営業日 ...)
# --------------------------------------------------------------------------- #

#: Named presets for the schedules that come up over and over in Japanese
#: back-office work. Each maps to the construct that produces it, so the
#: presets double as documentation.
BUSINESS_DAY_RULES: dict[str, dict] = {
    # 月初営業日 / 第n営業日 (anchor-inclusive: 第1営業日 == 月初営業日)
    "第1営業日": verbose_rule(
        kind=Kind.OPERATING,
        start_day=StartDay.DAY,
        day=1,
        substitution=Substitution.NEXT,
        grace_days=30,
        frequency=Frequency.MONTHLY,
    ),
    "月初営業日": verbose_rule(
        kind=Kind.OPERATING,
        start_day=StartDay.DAY,
        day=1,
        substitution=Substitution.NEXT,
        grace_days=30,
        frequency=Frequency.MONTHLY,
    ),
    # 月末営業日 / 当月末営業日
    "月末営業日": verbose_rule(
        kind=Kind.OPERATING,
        start_day=StartDay.MONTH_END,
        day=0,
        substitution=Substitution.PREVIOUS,
        grace_days=30,
        frequency=Frequency.MONTHLY,
    ),
    "当月末営業日": verbose_rule(
        kind=Kind.OPERATING,
        start_day=StartDay.MONTH_END,
        day=0,
        substitution=Substitution.PREVIOUS,
        grace_days=30,
        frequency=Frequency.MONTHLY,
    ),
    # 前月末営業日 (previous month's last working day)
    "前月末営業日": verbose_rule(
        kind=Kind.OPERATING,
        start_day=StartDay.MONTH_END,
        day=0,
        month_offset=-1,
        substitution=Substitution.PREVIOUS,
        grace_days=30,
        frequency=Frequency.MONTHLY,
        scope=Scope.FREE,
    ),
    # 月末営業日の前営業日 / 翌営業日 (relative to the anchor, per 起算)
    "月末前営業日": verbose_rule(
        kind=Kind.OPERATING,
        start_day=StartDay.MONTH_END,
        day=0,
        offset=-1,
        count=Count.OPERATING,
        offset_grace_days=30,
        frequency=Frequency.MONTHLY,
    ),
    "翌営業日": verbose_rule(
        substitution=Substitution.NEXT,
        grace_days=30,
    ),
    "前営業日": verbose_rule(
        substitution=Substitution.PREVIOUS,
        grace_days=30,
    ),
    "毎営業日": verbose_rule(frequency=Frequency.DAILY),
}


def nth_business_day(n: int) -> dict:
    """第n営業日 from the start of the month (n=1 is 月初営業日)."""
    if n < 1:
        raise ValueError(
            "n must be >= 1; count backwards from month end with nth_business_day_from_end()"
        )
    return verbose_rule(
        kind=Kind.OPERATING,
        start_day=StartDay.DAY,
        day=n,
        substitution=Substitution.NEXT,
        grace_days=30,
        frequency=Frequency.MONTHLY,
    )


def nth_business_day_from_end(n: int) -> dict:
    """月末のn営業日前. ``n=0`` is 月末営業日 itself."""
    if n < 0:
        raise ValueError("n must be >= 0")
    return verbose_rule(
        kind=Kind.OPERATING,
        start_day=StartDay.MONTH_END,
        day=n,
        substitution=Substitution.PREVIOUS,
        grace_days=30,
        frequency=Frequency.MONTHLY,
    )


def business_days_before(anchor: ScheduleRule, n: int) -> dict:
    """Shift an anchor rule ``n`` working days earlier (起算スケジュール)."""
    return replace(anchor, offset=-n, count=Count.OPERATING, offset_grace_days=60)


def business_days_after(anchor: ScheduleRule, n: int) -> dict:
    """Shift an anchor rule ``n`` working days later (起算スケジュール)."""
    return replace(anchor, offset=n, count=Count.OPERATING, offset_grace_days=60)


def calendar_days_before(anchor: ScheduleRule, n: int) -> dict:
    """Shift an anchor rule ``n`` plain days earlier (ignores working days)."""
    return replace(anchor, offset=-n, count=Count.CALENDAR)


def calendar_days_after(anchor: ScheduleRule, n: int) -> dict:
    """Shift an anchor rule ``n`` plain days later (ignores working days)."""
    return replace(anchor, offset=n, count=Count.CALENDAR)


def build_rules(items: Iterable[dict | ScheduleRule | str]) -> list[ScheduleRule]:
    """Normalise a mixed list of rule dicts, rules and preset names."""
    rules: list[ScheduleRule] = []
    for item in items:
        if isinstance(item, ScheduleRule):
            rules.append(item)
        elif isinstance(item, str):
            if item not in BUSINESS_DAY_RULES:
                known = ", ".join(sorted(BUSINESS_DAY_RULES))
                raise KeyError(f"unknown preset {item!r}; known presets: {known}")
            rules.append(ScheduleRule(**BUSINESS_DAY_RULES[item]))
        elif isinstance(item, dict):
            rules.append(ScheduleRule(**item))
        else:
            raise TypeError(f"cannot build a schedule rule from {type(item).__name__}")
    return rules


def resolve_rules(
    rules: Sequence[ScheduleRule], day: date, cal: WorkingDayCalendar, base_day: int = 1
) -> ScheduleRule | None:
    """The first rule (in order) that yields ``day``, or None.

    simple_rule gives later rules higher precedence, so callers should pass rules
    in ascending priority and take the first match -- i.e. ``除外`` rules last.
    """
    for rule in rules:
        if rule.matches(day, cal, base_day):
            return rule
    return None


__all__ = ["WorkingDayCalendar"]
