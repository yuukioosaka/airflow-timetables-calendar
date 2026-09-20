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
The classical model derives an execution date from 種別 (kind) and 開始日
(start day) together, optionally bounded below by 開始年月:

* **基準日 (base date)** -- a calendar defines where a "month" starts, e.g. base
  date 26 means Aug 26..Sep 25 is treated as "August".
* **基準時刻 (base time)** -- the hour at which a *business date* rolls over.
  With base time 08:00, 08:00..next-day 07:59 is one business day. This is why a
  job can legitimately run at "25:00" and still belong to the previous date.
* **種別 (kind)** -- what the day offset counts:

  ===========  ==========================================================
  ``ABSOLUTE`` 暦日: plain calendar date, month starts on the 1st.
  ``RELATIVE`` 相対日: counted from the 基準日, in calendar days. Note that
                ``base_day=1`` makes this identical to ``ABSOLUTE``, because
                the 基準日 is then the 1st.
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

import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from dataclasses import fields as dataclass_fields
from datetime import date, timedelta
from enum import Enum
from functools import cache
from typing import Any

from .calendars import WorkingDayCalendar

log = logging.getLogger(__name__)

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
    """処理サイクル -- the repeat period.

    Only :attr:`DAILY` and :attr:`MONTHLY` change the schedule. ``WEEKLY`` and
    ``YEARLY`` are part of the vocabulary and are accepted by the constructor,
    but no resolution logic consults them, so a rule carrying one would repeat
    monthly -- silently wrong rather than obviously wrong. They are therefore
    rejected when a rule is built. See :func:`_reject_unsupported_frequency`.
    """

    DAILY = "daily"  # 1日毎
    WEEKLY = "weekly"  # 1週毎
    MONTHLY = "monthly"  # 1月毎
    YEARLY = "yearly"  # 1年毎


#: 処理サイクル values this engine implements. The rest of the vocabulary is
#: accepted by the enum but cannot be resolved, so it must not reach a rule.
_SUPPORTED_FREQUENCIES = frozenset({Frequency.DAILY, Frequency.MONTHLY})


def _reject_unimplemented_fields(rule: ScheduleRule, *, stored: bool = False) -> None:
    """Fail loudly on vocabulary this engine records but cannot honour.

    Both fields below are part of the classical vocabulary and are carried so a
    transcription round-trips, but neither has a referent in this engine. A rule
    that sets one and is then quietly treated as ordinary does the *opposite* of
    what was asked -- it produces the runs the caller meant to suppress, or
    ignores the boundary the caller meant to draw -- and a wrong schedule that
    looks plausible is the one failure mode this library exists to avoid. So
    they are rejected where the mistake is visible, at construction.

    * ``virtual`` (除外) is an ordering decision: "suppress days another rule
      produced". :func:`resolve_rules` returns the *first* rule that matches and
      its callers use that to ask only "does any rule yield this day?", so a
      virtual rule changes nothing about the outcome -- it is returned like any
      other rule and the day runs exactly as if it had not been listed. Nothing
      in this package reads the flag, which is also why the tests that appeared
      to cover it only asserted that the returned object *had* the flag set.

    * ``include_start`` is the inclusive/exclusive edge of 開始年月. This engine
      has no 開始年月 *value* to compare against: a :class:`Period` is derived
      from the day under test, so a rule never learns "which month it started
      from", and there is nothing for the flag to test. Bounding the window is
      the job of the caller, which knows its own start date.

    ``stored`` marks the one caller that is *reading a payload back* rather than
    accepting configuration -- see :meth:`ScheduleRule.from_stored_payload`. A
    hand-written rule that sets either field gets the exception below, pointing
    at the line that is wrong. A rule rebuilt from data an earlier version wrote
    does not: the payload is already in Airflow's database and the caller cannot
    act on the error, so raising would turn a survivable upgrade into a DAG that
    will not parse. The fields are still rejected for anything written now.
    """
    if stored:
        _warn_dead_flags(rule)
        return
    if rule.virtual:
        raise NotImplementedError(
            "virtual=True (除外) is not implemented: nothing consults the flag, so a "
            "rule that sets it suppresses nothing and the day it names still runs. "
            "Express the exclusion with the timetable's exclude_dates, or leave the "
            "rule out."
        )
    if not rule.include_start:
        raise NotImplementedError(
            "include_start=False is not implemented: this engine carries no 開始年月 "
            "value, so a rule cannot tell whether the period it is resolving is the one "
            "it started from. Bound the window on the timetable instead -- pass the "
            "month you want the schedule to begin with as the timetable's start_date."
        )


#: The value each carried-but-dead field takes when it asks for nothing.
_NEUTRAL_FLAG_VALUE = {"virtual": False, "include_start": True}

#: Why each one cannot be honoured, phrased for a log line rather than a traceback.
_DEAD_FLAG_REASON = {
    "virtual": (
        "virtual=True (除外) suppresses nothing here, so the rule runs like any other. "
        "Express the exclusion with the timetable's exclude_dates instead."
    ),
    "include_start": (
        "include_start=False is ignored: this engine carries no 開始年月 value to test "
        "the boundary against. Bound the window with the timetable's start_date instead."
    ),
}


def _warn_dead_flags(rule: ScheduleRule) -> None:
    """Warn (once per field/value pair) about stored flags this engine ignores.

    Deduplicated because the read path is hot: ``matches()`` runs for every
    timestamp the scheduler considers, so an undamped warning would repeat
    indefinitely for a payload that has already been reported and understood. The
    cache is keyed on the value too, so a *different* stored payload is still
    reported rather than being silenced by the first.
    """
    for name, neutral in _NEUTRAL_FLAG_VALUE.items():
        value = getattr(rule, name)
        if value == neutral:
            continue
        _log_dead_flag(name, repr(value), _DEAD_FLAG_REASON[name])


@cache
def _log_unknown_stored_fields(names: tuple[str, ...]) -> None:
    """Warn (once per field tuple) that a stored payload carries unknown fields.

    Forward compatibility: a payload written by a later version may name fields
    this one does not have. Raising would make a downgrade or a mixed-version
    cluster unable to read its own stored data, so the fields are ignored -- but
    reported, because a dropped field can mean a different schedule.
    """
    log.warning(
        "Stored schedule rule carries field(s) this version does not know: %s. "
        "They are ignored, so the schedule may differ from the one recorded.",
        ", ".join(names),
    )


@cache
def _log_dead_flag(name: str, value: str, reason: str) -> None:
    log.warning(
        "Stored schedule rule carries %s=%s, which this version does not implement. %s",
        name,
        value,
        reason,
    )


def _reject_unsupported_frequency(frequency: Frequency) -> Frequency:
    """Fail loudly on a 処理サイクル the engine cannot honour.

    ``resolve()``/``matches()`` answer for a month-shaped period, so an
    unimplemented ``frequency`` would quietly repeat monthly. Raising at
    construction keeps the mistake at the call site (DAG-parse time) instead of
    turning it into a schedule that looks plausible and runs on the wrong days.
    """
    if frequency not in _SUPPORTED_FREQUENCIES:
        supported = ", ".join(sorted(f.value for f in _SUPPORTED_FREQUENCIES))
        raise NotImplementedError(
            f"frequency={frequency.value!r} is not implemented; supported: {supported}"
        )
    return frequency


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

    def period_month(self, base_day: int) -> tuple[int, int]:
        """The ``(year, month)`` whose *calendar month end* closes this period.

        ``base_day=26`` makes the period that opens on 2026-08-26 run through
        2026-09-25, so the period's last day belongs to **September** -- one
        calendar month later than the period's own label. With ``base_day=1``
        the period already ends on its own month's last day, so the answer is
        the label itself.

        This is the origin the 月末指定 forms are named against once the 種別
        is anything other than 絶対日.
        """
        if base_day <= 1:
            return (self.year, self.month)
        if self.start.month < 12:
            return (self.start.year, self.start.month + 1)
        return (self.start.year + 1, 1)

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


def _month_end(year: int, month: int) -> date:
    """The calendar month's last day."""
    return date(year, month, _days_in_month(year, month))


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
    """One schedule rule in the classical model.

    A rule is only evaluated for the :class:`Period` it falls in, so
    :meth:`resolve` is always anchored to "the occurrence in this month" rather
    than producing an unbounded series. That is also why :attr:`scope` exists as
    a *mode* rather than as a stored 開始年月: the engine learns the period from
    the day it is asked about, never from the rule.

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
    :param scope: Whether the rule is bound to the anchor month
        (:attr:`Scope.PERIOD`, which is what 開始年月 semantics amount to once a
        caller has bounded the window itself) or free to move anywhere the grace
        windows allow (:attr:`Scope.FREE`).
    :param frequency: Repeat period (処理サイクル).
    :param include_start: 開始年月's inclusive/exclusive edge. Carried for
        vocabulary completeness only: **not implemented**, and rejected when set
        to ``False``, because this engine holds no 開始年月 value to compare
        against. See :func:`_reject_unimplemented_fields`.
    :param virtual: ``除外``. Not implemented, and rejected when set: no code in
        this package consults it, so a virtual rule suppresses nothing and the
        day it names still runs. See :func:`_reject_unimplemented_fields`.
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
        # 除外 and the 開始年月 boundary are requests this engine would answer by
        # doing the opposite, so they must not reach resolution. ``_from_stored``
        # is set only by :meth:`from_stored_payload`, which is the one path that
        # is reading data back rather than accepting configuration.
        _reject_unimplemented_fields(self, stored=getattr(self, "_from_stored", False))
        # After the coercion, so a raw "weekly" string is caught too. A 処理サイクル
        # nothing consults would repeat monthly, so it must not reach resolution.
        _reject_unsupported_frequency(self.frequency)

    @classmethod
    def from_stored_payload(cls, payload: Mapping[str, Any]) -> ScheduleRule:
        """Rebuild a rule that was serialized by an earlier version.

        The public constructor rejects the carried-but-unimplemented vocabulary,
        because a hand-written rule that sets it is making a request this engine
        would answer by doing the opposite -- and the call site is where that is
        visible. The same payload read back from Airflow's database is not a
        request but a record: it was written by a version that accepted those
        fields, and refusing to load it would leave the deployment with a DAG
        that stops parsing and no way to repair the stored data.

        So this path loads the rule, ignores the dead flags, and warns once about
        each.

        The marker goes on the *class* for the duration of the call.
        ``__post_init__`` runs inside ``cls(**payload)`` and is what consults it,
        so it has to be in place before construction -- setting an instance
        attribute afterwards would be too late to suppress the exception. It is
        cleared in a ``finally`` so a failed construction cannot leave the
        permissive path armed for later callers.

        The payload is taken as a mapping of ``Any`` because that is what a
        stored rule holds: raw strings for the enum fields, which
        ``__post_init__`` coerces. Declaring the field types here would claim the
        payload was already a valid rule, which is precisely what is being read
        back.
        """
        # A transient marker on the class, not a dataclass field: it exists only
        # for the duration of this call, which is why it is set here rather than
        # declared. ``__post_init__`` reads it through the same door.
        known = {f.name for f in dataclass_fields(cls)}
        accepted = {name: value for name, value in payload.items() if name in known}
        unknown = set(payload) - known
        if unknown:
            # A payload from a *newer* version carries fields this one has never
            # heard of. Reading stored data must not fail on that: the same
            # reasoning as the dead flags above. Warn, because silently dropping
            # a field could change the schedule the payload described.
            _log_unknown_stored_fields(tuple(sorted(unknown)))

        setattr(cls, "_from_stored", True)  # noqa: B010
        try:
            rule = cls(**accepted)  # type: ignore[arg-type]
        finally:
            delattr(cls, "_from_stored")
        # The constructor already warned that something was dropped; this says
        # *what*, once per field and value.
        _warn_dead_flags(rule)
        return rule

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

        # 開始年月 acts as a *lower* bound on the months the rule applies from,
        # and a 開始日 of "day 1" is what supplies the run in each later month.
        # It is set alongside 種別 / 開始日,
        # which is what actually names the anchor -- so it constrains *which
        # periods the rule covers*, never where inside a period the anchor sits.
        #
        # An earlier version additionally compared the anchor's month against the
        # period here. Because ``month_offset`` exists precisely to move the
        # anchor out of the period it is evaluated for (前月末営業日 asks a period
        # for a day in the *previous* period), that comparison is unsatisfiable
        # for every ``month_offset != 0`` and silently turned the whole rule into
        # "no run at all". The 処理サイクル fast path above already enforces the
        # one month-alignment constraint that is real, and it is deliberately
        # skipped once the anchor has been moved.
        month_bound: tuple[int, int] | None = None
        if self.scope is Scope.PERIOD:
            month_bound = (period.year, period.month)

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

        # 振り替え and 起算 are movements, so the moved date may leave the month --
        # 基準日 26 deliberately places the 25th's run on the 26th of the next
        # month, and 前月末営業日 resolves to the previous period outright. What
        # 開始年月 does reject is a date that walks back *past the month the rule
        # anchors in*, so `simple_rule(day=1, shift="next", relative=-3)` produces
        # no run. The bound is the anchor's month rather than the period's: with
        # month_offset the anchor is a different month by design, and comparing
        # against the period there would reject the move the caller asked for.
        if month_bound is not None and (day.year, day.month) < (anchor.year, anchor.month):
            return None

        return day

    # ------------------------------------------------------------- internals

    def _anchor_day(self, period: Period, cal: WorkingDayCalendar) -> date | None:
        """The date named by 種別 + 開始日, before holiday substitution.

        Every 開始日 names its anchor against one of three origins, and the 種別
        picks which -- the two axes are not independent:

        * 絶対日 uses **the calendar month** for all three 開始日 forms: it is the
          kind that reads the date off the calendar, where the month begins on
          the 1st.
        * 相対日 / 運用日 / 休業日 use **the period the 基準日 defines**, i.e. the
          scheduler month, for 月末指定 -- 月末指定 measures its N days back from
          the end of that period.
        * 相対日 counts 日付指定 from **the 基準日 itself**, while 絶対日 counts it
          from the calendar month's first day: both spell a day as "day N", and
          the 基準日 is what decides where N is counted from.

        Every one of these collapses to the same answer when ``base_day=1``,
        because the 基準日 is then the 1st and the scheduler month is the
        calendar month. That is why ``base_day=1`` is the default and why the
        distinction only surfaces once a 基準日 is configured.
        """
        calendar_month = period.anchor_month

        if self.kind is Kind.REGISTERED:
            # 登録日 is the job's registration date, which has nothing to do with
            # the period; resolve against the period's own start.
            return period.start

        # 絶対日 alone is named against the calendar month; every other kind is
        # named against the scheduler month that 基準日 defines.
        relative_to_period = self.kind is not Kind.ABSOLUTE

        if self.start_day is StartDay.WEEKDAY:
            if relative_to_period:
                return self._nth_weekday_in_period(period)
            return self._nth_weekday(calendar_month)

        if self.start_day is StartDay.MONTH_END:
            if relative_to_period:
                # 月末指定 counts back from the end of the period the 基準日
                # defines, not from the end of the calendar month.
                return self._from_month_end(period.end, cal)
            return self._from_month_end(_month_end(calendar_month.year, calendar_month.month), cal)

        if self.kind is Kind.ABSOLUTE:
            return _clamp_day(calendar_month.year, calendar_month.month, self.day)
        if self.kind is Kind.RELATIVE:
            # 相対日 counts calendar days from the 基準日 itself. 何日 is 1-based
            # (the same convention 絶対日 uses), so day=1 IS the 基準日.
            return period.start + timedelta(days=self.day - 1)
        if self.kind is Kind.OPERATING:
            origin = period.start if relative_to_period else calendar_month
            return self._nth_working_day_in_period(origin, self.day, cal)
        if self.kind is Kind.CLOSED:
            origin = period.start if relative_to_period else calendar_month
            return self._nth_closed_day_in_period(origin, self.day, cal)
        raise AssertionError(f"unhandled kind {self.kind!r}")

    def _nth_weekday(self, anchor: date) -> date:
        """曜日指定: the Nth <weekday> of the anchor's month.

        ``day`` has no upper bound of its own, so it is clamped to the anchor
        month's last day. Without the clamp ``day=5`` walks into the *next* month
        in every month that has only four of that weekday -- and ``day=28`` walks
        seven months out -- because the date is computed by adding whole weeks
        from the first occurrence. That puts the anchor outside the very month it
        was named in, which contradicts 曜日指定 ("the Nth <weekday> **of the
        month**") and is unreachable by construction in the vocabularies this
        models. Clamping matches how the day-based 開始日 forms already behave: an
        out-of-range ``day`` names the month's last day rather than a day of
        another month.
        """
        if self.weekday is None:
            raise ValueError("start_day=WEEKDAY requires weekday=<Weekday>")
        first = date(anchor.year, anchor.month, 1)
        last_of_month = date(anchor.year, anchor.month, _days_in_month(anchor.year, anchor.month))
        delta = (self.weekday.index - first.weekday()) % 7
        # The Nth occurrence may not exist (a month has at most five of any
        # weekday, and most have four), and ``day`` has no bound of its own.
        # ``last_occurrence`` is inclusive: day == last_occurrence is the real
        # final occurrence, and only day > last_occurrence has to be clamped.
        # Walk to the anchor month's *last day* in that case rather than
        # stepping into the following month, which is what a plain
        # ``first + 7 * (day - 1)`` does -- and which would put the anchor
        # outside the very month it was named in.
        last_occurrence = (last_of_month.day - 1 - delta) // 7 + 1
        if self.day > last_occurrence:
            return last_of_month
        return first + timedelta(days=delta + 7 * (self.day - 1))

    def _nth_weekday_in_period(self, period: Period) -> date:
        """曜日指定 counted from the 基準日 rather than the calendar month.

        相対日's 曜日指定 counts weeks from the 基準日, so week 1 is the week
        containing the 基準日 -- "the Nth <weekday>" as counted from there. The *first*
        occurrence is therefore the first <weekday> on or after ``period.start``,
        and the search runs forward from there without leaving the period -- an
        occurrence past ``period.end`` names the period's last day, the same way
        an out-of-range ``day`` names the month's last day in the 絶対日 reading.
        """
        if self.weekday is None:
            raise ValueError("start_day=WEEKDAY requires weekday=<Weekday>")
        first = period.start + timedelta(days=(self.weekday.index - period.start.weekday()) % 7)
        last = period.end
        if first > last:
            return last
        occurrence = first + timedelta(days=7 * (self.day - 1))
        return min(occurrence, last)

    def _from_month_end(self, last: date, cal: WorkingDayCalendar) -> date | None:
        """月末指定: walk back ``day`` days from the period's last day.

        ``last`` is the closing day of whichever month the caller decided the
        種別 names -- the calendar month for 絶対日, the scheduler month defined
        by 基準日 for 相対日 / 運用日 / 休業日.

        The count is inclusive of the last day, so ``OPERATING, day=0`` is
        月末営業日 and ``day=1`` is 月末の前営業日.
        """
        if self.kind is Kind.OPERATING:
            return self._nth_working_day_back_from(last, self.day, cal)
        if self.kind is Kind.CLOSED:
            return self._nth_closed_day_back_from(last, self.day, cal)
        return last - timedelta(days=self.day)

    def _nth_working_day_in_period(
        self, anchor: date, n: int, cal: WorkingDayCalendar
    ) -> date | None:
        """The n-th 運用日 counting from ``anchor`` (n=1 is the anchor's own day).

        ``anchor`` is the period's start for ``OPERATING`` (the 基準日-defined
        month) and the calendar month's first day for the ``ABSOLUTE`` reading,
        which is what lets 第n営業日 follow 基準日 while 絶対日 keeps the calendar
        month. Counting from the first of the month regardless put ``base_day=26``
        answers *before* the period they belonged to.

        日付指定 counts *within* the anchor month, so the walk is clamped to the
        anchor month's last 運用日. Without the bound the walk simply kept going:
        第20営業日 of a month holding only 18 運用日 answered with a date in the
        *next* month (2026-02 gave 2026-03-03, 2026-05 gave 2026-06-02 under the
        ``JP`` calendar), so the schedule fired on a day the rule did not name --
        and collided with the following month's 第1営業日, which made the two
        rules fire together in one month and never in the other.

        The clamp is the month's last *運用日*, not its last *day*. The day-based
        families (絶対日 ``day=31``, 曜日指定 past the final occurrence) clamp to
        the last day because a day is what they name; 日付指定 names a 運用日, so
        the last day is only the right answer when it happens to be one. A month
        closing on a Saturday must not answer with that Saturday -- that would
        hand back the one thing a 運用日 rule can never return. It is also what
        the period-bounded reading already gives: when the period ends on the
        month's last day (``第1営業日`` with ``base_day=1``), the period's closing
        運用日 *is* the month's closing 運用日.

        A month with no 運用日 at all has no such day and yields no run, the same
        outcome 月末指定 has when its count runs past the month start.
        """
        if n < 1:
            return None
        return self._scan(anchor, 1, n - 1, cal.is_working_day, month_bounded=True)

    def _nth_closed_day_in_period(
        self, anchor: date, n: int, cal: WorkingDayCalendar
    ) -> date | None:
        """The n-th 休業日 counting from ``anchor`` (n=1 is ``anchor`` if it is closed).

        Same origin rule as :meth:`_nth_working_day_in_period`: the period start
        for the 基準日-relative kinds, the calendar month's first day for 絶対日.
        The month bound is shared too, and for the same reason -- a 休業日 count
        that runs out of the month would otherwise name a day of the next one.
        The clamp is the month's last *matching* day, so unlike the 運用日 case it
        is found by walking back from the month end rather than by starting at it:
        the month's actual last day is 休業日 only when it is a weekend or holiday.
        """
        if n < 1:
            return None
        return self._scan(anchor, 1, n - 1, lambda d: not cal.is_working_day(d), month_bounded=True)

    def _nth_working_day_back_from(
        self, last: date, n: int, cal: WorkingDayCalendar
    ) -> date | None:
        # 月末指定 counts *within* the anchor month: the walk starts at the
        # month's last day and must not leave the month. A month with fewer
        # than ``n + 1`` 運用日 therefore yields no run rather than a day of a
        # neighbouring month -- the same 開始日 semantics the other families
        # already have, and the reason ``matches()`` can bound the epochs a run
        # may have come from.
        first = date(last.year, last.month, 1)
        return self._scan(last, -1, n, cal.is_working_day, floor=first)

    def _nth_closed_day_back_from(self, last: date, n: int, cal: WorkingDayCalendar) -> date | None:
        first = date(last.year, last.month, 1)
        return self._scan(last, -1, n, lambda d: not cal.is_working_day(d), floor=first)

    @staticmethod
    def _scan(
        start: date,
        step: int,
        n: int,
        predicate: Callable[[date], bool],
        floor: date | None = None,
        month_bounded: bool = False,
    ) -> date | None:
        """Walk from ``start`` until ``n`` matching days have been *skipped*.

        The first matching day is the answer when ``n == 0``, so this never
        returns a day it walked past.

        The walk is unbounded in wall-clock terms and stops when the date
        arithmetic itself gives out. Bounding it by a step count was a bug:
        ``1900`` steps is barely five years, which silently returned ``None``
        ("no run this period") for requests that have a perfectly good answer.
        Walking day-by-day over a century of ``date`` objects costs
        microseconds, so there is no reason to cap it -- and unlike the grace
        windows, a large count here is a legitimate request, not a
        configuration error.

        ``floor`` bounds the walk to a caller-supplied window instead. 月末指定
        counts within the anchor month, so it passes the month's first day: a
        month that lacks the requested number of 運用日 / 休業日 produces no run
        rather than a day of a neighbouring month. Leaving it unbounded here
        put the anchor outside the month it was derived from, which also made
        the epoch a run came from impossible to bound.

        ``month_bounded`` is the mirror image, for the counts that start at the
        beginning of the month and run *forward*: the walk stops at the anchor
        month's last matching day. The answer is exact when the requested day
        exists, and the closing day of the month when it does not -- a *clamp*,
        matching how 絶対日 ``day=31`` and 曜日指定 past the final occurrence
        behave, rather than 月末指定's no-run. Both readings cannot be right for
        the same overshoot, and clamping is the one the day-based families
        already committed to; the alternative silently dropped a run for every
        month that happened to be a day or two short.

        The clamping day is a matching day by construction, so a 運用日 count can
        never answer with a 休業日 even when the month ends on a weekend. Only a
        month with no matching day at all yields no run. ``step`` is always +1
        here; the backward family uses ``floor`` instead.
        """
        day = start
        remaining = n
        limit: date | None = None
        if month_bounded:
            assert step > 0, "month_bounded describes a forward walk"
            limit = _month_end(start.year, start.month)
        while True:
            if floor is not None and ((step < 0 and day < floor) or (step > 0 and day > floor)):
                # Ran out of the window the caller allows: no run, rather
                # than walking into the neighbouring period.
                return None
            if limit is not None and day > limit:
                # The month has no matching day as late as ``day`` asked for.
                # Clamp to the month's last one, searching back from its end.
                return ScheduleRule._scan(limit, -1, 0, predicate)
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

        A MONTHLY rule matches a day in a period it is not anchored to, as long
        as some other period's own resolution lands on that day. A run reaches
        across a period boundary for several independent reasons, and they
        compose:

        * 前月末営業日 resolves, for period N, to a day in period N-1.
        * A forward 相対 that crosses the month end (`day="L", relative=1`)
          resolves, for period N, to a day in period N+1.
        * Either of those movements can be combined with ``month_offset``, which
          moves the anchor by any number of periods at all.

        So the day is offered to the rule at the epoch that produced it, and that
        epoch is recovered from the day arithmetically rather than by scanning.
        See the body for why a bounded neighbour scan cannot be correct.

        This cannot produce a false positive: periods are defined by their
        anchors and consecutive periods do not overlap, so at most one epoch's
        ``resolve()`` can equal any given day.
        """
        period = period_for(day, base_day)
        if self.frequency is Frequency.DAILY:
            # Each day is eligible, but the day still has to be a working day:
            # 毎営業日 means every *business* day.
            return cal.is_working_day(day)

        # A run reaches a neighbouring period for two independent reasons, and
        # they compose -- so a bounded scan of "the containing period and its
        # neighbours" is not enough. With ``base_day=26`` a ``StartDay.DAY``
        # anchor whose ``day`` is below the 基準日 sits in the period *before* the
        # one it was built from, and a backward movement pushes it one period
        # further still, so ``day=1, offset=-1`` lands two periods behind its
        # epoch. ``month_offset`` is unbounded on top of that.
        #
        # The producing epoch is therefore recovered arithmetically. A day in
        # period ``q`` was produced by an epoch ``p`` with
        #
        #     q = (p moved by month_offset) + m,    m = -margin .. +margin
        #
        # where ``m`` counts the whole periods the anchor's own position within
        # its period plus any 振り替え / 起算 can add. ``_period_margin`` bounds
        # that from the 猶予日数 windows, which are themselves the bound the
        # model intends: a movement cannot travel further than its window allows.
        margin = self._period_margin()
        for offset in range(-margin, margin + 1):
            epoch = period
            for _ in range(abs(offset + self.month_offset)):
                epoch = epoch.next() if (offset + self.month_offset) < 0 else epoch.prev()
            if self.resolve(epoch, cal) == day:
                return True
        return False

    def _period_margin(self) -> int:
        """How many whole periods a movement of this rule may cross.

        Both walks (振り替え and 起算) are bounded by a *day* window, and a period
        spans at least 28 days, so ``window // 28 + 2`` is a safe bound on the
        periods it can reach. An unset window means the default grace, which is
        what :meth:`_substitute` and :meth:`_apply_offset` use.
        """
        window = 0
        if self.substitution in (Substitution.NEXT, Substitution.PREVIOUS):
            window = max(window, self.grace_days or _DEFAULT_GRACE)
        if self.offset:
            window = max(window, self.offset_grace_days or _DEFAULT_GRACE)
        if window == 0:
            return 1
        return window // 28 + 2


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
#:
#: Every preset also has a stable English name, registered in
#: :data:`PRESET_ALIASES`. The Japanese names are the canonical keys because
#: they are what the source definitions are written in, but the English names
#: are the ones to use when the caller's own vocabulary is English -- both are
#: accepted anywhere a preset name is accepted, and both produce an identical
#: :class:`ScheduleRule`.
BUSINESS_DAY_RULES: dict[str, dict] = {
    # 月初営業日 (anchor-inclusive: 第1営業日)
    "月初営業日": verbose_rule(
        kind=Kind.OPERATING,
        start_day=StartDay.DAY,
        day=1,
        substitution=Substitution.NEXT,
        grace_days=30,
        frequency=Frequency.MONTHLY,
    ),
    # 当月末営業日 (the current month's last working day)
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

#: English spelling for each preset. Kept separate from
#: :data:`BUSINESS_DAY_RULES` so the canonical table stays a direct transcription
#: of the Japanese vocabulary, and so a preset cannot accidentally exist in one
#: language only -- :func:`build_rules` resolves through this table, and
#: ``tests/test_rules.py`` asserts the two views cover the same rules.
PRESET_ALIASES: dict[str, str] = {
    "first_business_day_of_month": "月初営業日",
    "last_business_day_of_month": "当月末営業日",
    "last_business_day_of_previous_month": "前月末営業日",
    "business_day_before_month_end": "月末前営業日",
    "next_business_day": "翌営業日",
    "previous_business_day": "前営業日",
    "every_business_day": "毎営業日",
}

#: Every accepted preset name, Japanese and English, mapped to its canonical
#: Japanese key.
PRESET_LOOKUP: dict[str, str] = {**{name: name for name in BUSINESS_DAY_RULES}, **PRESET_ALIASES}


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


def business_days_before(anchor: ScheduleRule, n: int) -> ScheduleRule:
    """Shift an anchor rule ``n`` working days earlier (起算スケジュール)."""
    return replace(anchor, offset=-n, count=Count.OPERATING, offset_grace_days=60)


def business_days_after(anchor: ScheduleRule, n: int) -> ScheduleRule:
    """Shift an anchor rule ``n`` working days later (起算スケジュール)."""
    return replace(anchor, offset=n, count=Count.OPERATING, offset_grace_days=60)


def calendar_days_before(anchor: ScheduleRule, n: int) -> ScheduleRule:
    """Shift an anchor rule ``n`` plain days earlier (ignores working days)."""
    return replace(anchor, offset=-n, count=Count.CALENDAR)


def calendar_days_after(anchor: ScheduleRule, n: int) -> ScheduleRule:
    """Shift an anchor rule ``n`` plain days later (ignores working days)."""
    return replace(anchor, offset=n, count=Count.CALENDAR)


def build_rules(
    items: Iterable[dict | ScheduleRule | str], *, stored: bool = False
) -> list[ScheduleRule]:
    """Normalise a mixed list of rule dicts, rules and preset names.

    ``stored=True`` marks the items as having been read back from a serialized
    payload rather than handed over as configuration -- the path that has to
    tolerate vocabulary an earlier version recorded. See
    :meth:`ScheduleRule.from_stored_payload`.
    """
    rules: list[ScheduleRule] = []
    for item in items:
        if isinstance(item, ScheduleRule):
            rules.append(item)
        elif isinstance(item, str):
            # A preset name is a name, not stored data: it always resolves to the
            # current definition, so the stored path has nothing extra to allow.
            canonical = PRESET_LOOKUP.get(item)
            if canonical is None:
                known = ", ".join(sorted(PRESET_LOOKUP))
                raise KeyError(f"unknown preset {item!r}; known presets: {known}")
            rules.append(ScheduleRule(**BUSINESS_DAY_RULES[canonical]))
        elif isinstance(item, dict):
            if stored:
                # A stored payload holds raw JSON values -- strings for the enum
                # fields -- which ``ScheduleRule.__post_init__`` coerces. The
                # dict is typed for the writing path, so the mismatch is stated
                # here rather than claimed away.
                rules.append(ScheduleRule.from_stored_payload(item))
            else:
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
