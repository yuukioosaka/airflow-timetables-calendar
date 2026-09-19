"""A calendar-driven Airflow timetable, with optional business-day rules.

One timetable class, many calendars: instead of hand-writing holiday maths for
every region, this delegates to maintained libraries (see :mod:`.calendars`).

Usage::

    from airflow_timetables_calendar import CalendarTimetable

    # Public holidays, weekdays only
    CalendarTimetable(calendar_id="JP", hour=9)             # Japan
    CalendarTimetable(calendar_id="US", hour=9)             # United States
    CalendarTimetable(calendar_id="US-CA", hour=9)          # California
    CalendarTimetable(calendar_id="GB", hour=9)             # United Kingdom
    CalendarTimetable(calendar_id="SG", hour=9)             # Singapore

    # Exchange trading days (needs the `exchanges` extra)
    CalendarTimetable(calendar_id="TSE", hour=9)            # Tokyo Stock Exchange
    CalendarTimetable(calendar_id="NYSE", hour=9)           # New York Stock Exchange
    CalendarTimetable(calendar_id="exchange:XLON", hour=8)  # London Stock Exchange

    # No holidays at all: plain Mon-Fri
    CalendarTimetable(calendar_id="NONE", hour=9)

    # Escape hatch: your own explicit list of non-working days
    CalendarTimetable(calendar_id="NONE", hour=9, exclude_dates=["2026-12-29"])

    # rules from the classical model instead of "every working day"
    from airflow_timetables_calendar import (
        Kind,
        nth_business_day,
        simple_rule,
        verbose_rule,
    )

    CalendarTimetable(calendar_id="JP", rules=["月末営業日"], hour=21)
    CalendarTimetable(calendar_id="JP", rules=[verbose_rule(kind=Kind.ABSOLUTE, day=15)], hour=21)
    CalendarTimetable(
        calendar_id="JP",
        rules=[simple_rule(day="L", shift="prev", relative=-2)],
        hour=21,
    )

The bare calendar id is resolved against both registries, so ``"JP"`` (a country
code) and ``"NYSE"`` (an exchange) can be used without saying which one it is.
Use an explicit prefix when a code is ambiguous, e.g. ``"exchange:XLON"`` to
force ``pandas_market_calendars`` over the ``holidays`` financial calendar.

:param calendar_id: Calendar to follow, or ``"NONE"``.
:param hour: Hour of day to run, in the timetable's timezone.
:param minute: Minute of the hour to run.
:param timezone: Timezone the hour/minute are interpreted in.
:param exclude_dates: Extra ``YYYY-MM-DD`` dates to skip.
:param include_dates: Extra ``YYYY-MM-DD`` dates to run on, even if the calendar
    would skip them (useful for one-off out-of-hours runs).
:param rules: schedule rules (see :mod:`.rules`).
    Accepts preset names, ``verbose_rule()`` / ``simple_rule()`` kwargs dicts, or
    ``ScheduleRule`` objects, in ascending priority. **Empty or None means "run on
    every working day"**, which is the plain holiday-only behaviour and stays the
    default so a timetable without rules is unaffected.
:param base_day: 基準日 -- the day of month a business "month" starts on.
    ``26`` makes 2026-08-26..2026-09-25 the "August" business month.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from functools import cache

from airflow.timetables.trigger import CronTriggerTimetable
from croniter import croniter

from ._timezones import convert_to_utc, make_aware, make_naive, parse_timezone
from .calendars import (
    NO_CALENDAR,
    _country_calendar,
    available_country_calendars,
    available_exchange_calendars,
    resolve_calendar,
)
from .rules import ScheduleRule, build_rules, resolve_rules

DEFAULT_TIMEZONE = "Asia/Tokyo"

# Bound the holiday-stepping loops so a bad calendar can never hang the scheduler.
_MAX_STEPS = 400


@cache
def _parse_date(value: str) -> date:
    return date.fromisoformat(value) if isinstance(value, str) else value


class CalendarTimetable(CronTriggerTimetable):
    """Run at a fixed local time on the working days of one calendar.

    Weekends are excluded by the cron expression. ``_get_next`` / ``_get_prev``
    then step over any further non-working day reported by the calendar.

    This derives from the *core* ``CronTriggerTimetable`` rather than the SDK
    one. The SDK base (``airflow.sdk.bases.timetable.BaseTimetable``) is missing
    several attributes that core code reads unconditionally -- ``partitioned``
    (``dag_processing.collection``) and a usable ``asset_condition``
    (``SerializedDAG.detect_dag_dependencies``) -- so subclassing it breaks DAG
    parsing. The trade-off is that this class is not in the serializer's builtin
    dispatch table, which is why the plugin registers it explicitly.
    """

    def __init__(
        self,
        calendar_id: str = NO_CALENDAR,
        hour: int = 9,
        minute: int = 0,
        timezone: str = DEFAULT_TIMEZONE,
        exclude_dates: list[str] | None = None,
        include_dates: list[str] | None = None,
        rules: list[dict | ScheduleRule | str] | None = None,
        base_day: int = 1,
    ) -> None:
        super().__init__(f"{minute} {hour} * * 1-5", timezone=timezone)

        # Validates the id now, so a typo fails at DAG-parse time with a clear
        # message instead of silently scheduling on holidays.
        self._calendar_kind, self._calendar_code, self._checker = resolve_calendar(calendar_id)
        if not 0 <= hour <= 23:
            raise ValueError(f"hour must be between 0 and 23, got {hour!r}")
        if not 0 <= minute <= 59:
            raise ValueError(f"minute must be between 0 and 59, got {minute!r}")
        if not 1 <= base_day <= 31:
            raise ValueError(f"base_day must be between 1 and 31, got {base_day!r}")

        self.exclude_dates = frozenset(_parse_date(d) for d in (exclude_dates or ()))
        self.include_dates = frozenset(_parse_date(d) for d in (include_dates or ()))
        # Empty means "run on every working day", the pre-rules behaviour.
        self._rules = tuple(build_rules(rules)) if rules else ()
        self._base_day = base_day

    # ------------------------------------------------------------------ config

    @property
    def calendar_id(self) -> str:
        if self._calendar_kind == "none":
            return NO_CALENDAR
        return f"{self._calendar_kind}:{self._calendar_code}"

    @property
    def hour(self) -> int:
        return int(self._expression.split()[1])

    @property
    def minute(self) -> int:
        return int(self._expression.split()[0])

    @property
    def tz(self):
        """Timezone as a tzinfo, normalising the string form if needed."""
        tz = self._timezone
        return parse_timezone(tz) if isinstance(tz, str) else tz

    @property
    def rules(self) -> tuple[ScheduleRule, ...]:
        """The configured schedule rules, highest priority first."""
        return self._rules

    @property
    def base_day(self) -> int:
        """基準日: the day of month a business "month" starts on."""
        return self._base_day

    @property
    def summary(self) -> str:
        base = f"{self._expression} (calendar: {self.calendar_id}"
        if self._base_day != 1:
            base += f", base day: {self._base_day}"
        if self._rules:
            base += f", rules: {len(self._rules)}"
        return base + ")"

    # ------------------------------------------- serialization (Airflow 3 DAG)

    @classmethod
    def deserialize(cls, data: dict) -> CalendarTimetable:
        return cls(
            calendar_id=data.get("calendar_id", NO_CALENDAR),
            hour=data.get("hour", 9),
            minute=data.get("minute", 0),
            timezone=data.get("timezone", DEFAULT_TIMEZONE),
            exclude_dates=data.get("exclude_dates"),
            include_dates=data.get("include_dates"),
            rules=data.get("rules"),
            base_day=data.get("base_day", 1),
        )

    def serialize(self) -> dict:
        return {
            "calendar_id": self.calendar_id,
            "hour": self.hour,
            "minute": self.minute,
            "timezone": _timezone_name(self._timezone),
            "exclude_dates": sorted(d.isoformat() for d in self.exclude_dates),
            "include_dates": sorted(d.isoformat() for d in self.include_dates),
            "rules": [_rule_to_dict(r) for r in self._rules],
            "base_day": self._base_day,
        }

    # ------------------------------------------------------- holiday decision

    def _local_date(self, moment: datetime) -> date:
        return moment.astimezone(self.tz).date()

    def is_working_day(self, day: date) -> bool:
        """Whether ``day`` produces a run.

        Public because :mod:`.rules` consumes it: a timetable with no
        ``rules`` behaves as a single implicit "every working day" rule, and the
        rule engine delegates back here to decide what a working day is.
        """
        if day in self.include_dates:
            return True
        if day in self.exclude_dates:
            return False
        if day.weekday() >= 5:  # Sat/Sun
            return False
        if self._checker is None:
            return True
        return not self._checker(self._calendar_code, day)

    def holiday_name(self, day: date) -> str | None:
        """The holiday's name for ``day``, or None if it is not a holiday.

        Required by the :class:`~airflow_timetables_calendar.calendars.WorkingDayCalendar`
        protocol, which this class is documented as satisfying -- without it
        ``isinstance(timetable, WorkingDayCalendar)`` is False and the class
        cannot be handed to the helpers that accept that protocol. Also used for
        diagnostics, where "元日" reads better than a bare closed-day count.
        """
        if self._calendar_kind != "holidays":
            # `holidays` is the only registry with names; exchange calendars and
            # the NONE calendar have nothing to report.
            return None
        calendar = _country_calendar(self._calendar_code)
        if calendar is None:
            return None
        name = calendar.get(day)
        return str(name) if name else None

    def matches_rules(self, day: date) -> bool:
        """Whether ``day`` is yielded by any of this timetable's ``rules``.

        With no rules configured every working day runs, which keeps the plain
        holiday-only behaviour of an earlier version of this class.
        """
        if not self._rules:
            return self.is_working_day(day)
        return resolve_rules(self._rules, day, self, self._base_day) is not None

    def _is_skipped(self, moment: datetime) -> bool:
        return not self.matches_rules(self._local_date(moment))

    # ------------------------------------------------------------- cron steps

    def _cron_next(self, current: datetime) -> datetime:
        """Raw cron step forward, before calendar filtering."""
        naive = make_naive(current, self.tz)
        scheduled = croniter(self._expression, start_time=naive).get_next(datetime)
        return convert_to_utc(make_aware(scheduled, self.tz))

    def _cron_prev(self, current: datetime) -> datetime:
        """Raw cron step backward, before calendar filtering."""
        naive = make_naive(current, self.tz)
        scheduled = croniter(self._expression, start_time=naive).get_prev(datetime)
        return convert_to_utc(make_aware(scheduled, self.tz))

    def _get_next(self, current: datetime) -> datetime:
        candidate = self._cron_next(current)
        for _ in range(_MAX_STEPS):
            if not self._is_skipped(candidate):
                return candidate
            candidate = self._cron_next(candidate)
        raise RuntimeError(
            f"no working day found within {_MAX_STEPS} steps for calendar {self.calendar_id!r}"
        )

    def _get_prev(self, current: datetime) -> datetime:
        candidate = self._cron_prev(current)
        for _ in range(_MAX_STEPS):
            if not self._is_skipped(candidate):
                return candidate
            candidate = self._cron_prev(candidate)
        raise RuntimeError(
            f"no working day found within {_MAX_STEPS} steps for calendar {self.calendar_id!r}"
        )


def _timezone_name(tz) -> str:
    """Best-effort conversion of a tzinfo/str back to a zone name."""
    if isinstance(tz, str):
        return tz
    key = getattr(tz, "key", None)  # pendulum.Timezone
    return key if isinstance(key, str) else str(tz)


def _rule_to_dict(rule: ScheduleRule) -> dict:
    """Render a rule as JSON-safe data.

    ``dataclasses.asdict`` would emit the ``str``-enum members as ``Enum`` objects,
    which the serializer's JSON encoder cannot represent (``Cal`` enums such as
    ``Substitution`` are not ``str`` subclasses in the same way ``Kind`` is, and
    relying on that is too subtle). Emitting ``.value`` explicitly keeps the
    payload independent of the enum implementation.
    """
    from dataclasses import fields

    out: dict = {}
    for field in fields(rule):
        value = getattr(rule, field.name)
        out[field.name] = value.value if isinstance(value, Enum) else value
    return out


# Re-exported for backwards compatibility: these used to be defined here.

__all__ = [
    "DEFAULT_TIMEZONE",
    "CalendarTimetable",
    "available_country_calendars",
    "available_exchange_calendars",
]
