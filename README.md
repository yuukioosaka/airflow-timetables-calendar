# airflow-timetables-calendar

Calendar-driven [Airflow](https://airflow.apache.org/) timetables, plus a
business-day rule engine using the vocabulary of classical Japanese job
schedulers (種別 / 開始日 / 振り替え / 起算 / 猶予日数).

The point of this package is that "run on the last working day of the month" is
not something a cron expression can say, and hand-rolling the arithmetic for every
region, exchange and holiday rule is how schedulers go quietly wrong. So instead
of reimplementing calendars, this delegates to libraries that already maintain the
data, and layers the scheduling vocabulary Japanese enterprise job schedulers have
used for decades on top.

```bash
pip install airflow-timetables-calendar

# exchange calendars (Tokyo Stock Exchange, NYSE, LSE, ...) are opt-in
pip install "airflow-timetables-calendar[exchanges]"
```

## Quick start

```python
from airflow.sdk import DAG
from airflow_timetables_calendar import CalendarTimetable

with DAG(
    dag_id="daily_report",
    # 21:00 JST, every Japanese working day (Mon-Fri minus public holidays).
    schedule=CalendarTimetable(calendar_id="JP", hour=21),
    ...
):
    ...
```

`CalendarTimetable` requires no rules; without them it behaves as "run at this
local time on every working day of the calendar".

## Calendars

The bare id is resolved against both registries, so a country code and an exchange
code are used the same way.

```python
CalendarTimetable(calendar_id="JP", hour=9)             # Japan
CalendarTimetable(calendar_id="US", hour=9)             # United States
CalendarTimetable(calendar_id="US-CA", hour=9)          # California
CalendarTimetable(calendar_id="DE-BY", hour=9)          # Bavaria
CalendarTimetable(calendar_id="TSE", hour=9)            # Tokyo Stock Exchange
CalendarTimetable(calendar_id="NYSE", hour=9)           # New York Stock Exchange
CalendarTimetable(calendar_id="exchange:XLON", hour=8)  # London Stock Exchange
CalendarTimetable(calendar_id="NONE", hour=9)           # plain Mon-Fri
```

Backed by [`holidays`](https://pypi.org/project/holidays/) (500+ country and
subdivision calendars) and, with the `exchanges` extra,
[`pandas_market_calendars`](https://pypi.org/project/pandas_market_calendars/)
(200+ exchanges, including maintenance and special closures).

A few codes exist in both registries — `TSE` is both a `holidays`
financial-market alias and a `pandas_market_calendars` name, and they disagree
about 2026-01-02. Use a prefix to be explicit:

```python
CalendarTimetable(calendar_id="exchange:TSE")   # force pandas_market_calendars
CalendarTimetable(calendar_id="country:JP")     # force holidays
```

## Business-day rules

`rules` takes preset names, `verbose_rule()` / `simple_rule()` kwargs dicts,
or `ScheduleRule` objects, in ascending priority. Passing rules **replaces**
"every working day": only the days a rule yields will run.

```python
from airflow_timetables_calendar import (
    CalendarTimetable,
    simple_rule,
    verbose_rule,
    nth_business_day,
    nth_business_day_from_end,
    Kind,
)

# Preset names
CalendarTimetable(calendar_id="JP", hour=21, rules=["月末営業日"])

# 第10営業日 and 月末営業日
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    rules=[nth_business_day(10), nth_business_day_from_end(0)],
)

# The explicit form: argument names map 1:1 onto
# 種別 / 開始日 / 振り替え / 起算, so an existing definition can be transcribed directly.
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    rules=[verbose_rule(kind=Kind.ABSOLUTE, day=15, substitution="next", grace_days=3)],
)

# The compact form: one line per schedule, the way 休止日 / 相対 are normally written
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    rules=[simple_rule(day="L", shift="prev", relative=-2)],  # 月末の3営業日前
)
```

Built-in presets: 第1営業日, 月初営業日, 月末営業日, 当月末営業日, 前月末営業日,
月末前営業日, 翌営業日, 前営業日, 毎営業日.

### The model

A date is derived from an **anchor** plus a chain of **modifiers**, following
the classical model:

| Concept | Meaning |
|---|---|
| 基準日 (base date) | where a "month" starts. `base_day=26` makes 2026-08-26..2026-09-25 the "August" business month |
| 種別 (kind) | what the offset counts: `ABSOLUTE` 暦日, `RELATIVE` 相対日, `OPERATING` 運用日 (working days → 第n営業日), `CLOSED` 休業日, `REGISTERED` 登録日 |
| 開始日 (start day) | `DAY` 日付指定, `MONTH_END` 月末指定, `WEEKDAY` 曜日指定 |
| 休業日の振り替え | what to do when the day is closed: `SKIP` 実行しない, `PREVIOUS` 前の運用日, `NEXT` 次の運用日, `RUN_ANYWAY` 振り替えなし |
| 起算スケジュール | a final `n` working-day (`OPERATING`) or calendar-day (`CALENDAR`) adjustment |
| 猶予日数 (grace days) | the maximum distance a shift may travel. **Beyond it, that occurrence produces no run at all** — matching the classical model, this is not an error |

`simple_rule()` is the compact spelling of the same model, taken from the rule
text `＋登録、毎月（日付）、1日、休止日 後シフト、相対 4` (月初から5営業日目).
`相対 n` counts `n` working days from the *settled* anchor, with the anchor itself
counting as 0, so `相対 4` on `1日` is the 5th working day and `相対 -2` on `L日`
is 月末の3営業日前. The 休止日 shift settles the anchor first and `相対` then
counts from that settled day, in `相対`'s own direction. `verbose_rule()` instead
applies 起算 to the anchor the rule names, without a preceding substitution.

Note that only *one* stage is allowed to walk: `simple_rule()` keeps the
substitution from moving the date and lets the offset carry the whole distance.
Chaining the two would make every closed day in the span cost two steps.

Because both `相対` and 振り替え are movements, their result may cross the end of
the month — `day="L", shift="prev", relative=1` genuinely means "the first
working day after month end". 開始年月 bounds the month the rule *anchors* in, so
it rejects a date that walks back past it, but not one that walks forward out of
it. The same applies with a `base_day`, where the end-of-month run lands in the
following month by design.

```python
# 月末業務日報: last working day of each business month starting on the 26th
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    base_day=26,
    rules=[nth_business_day_from_end(0)],
)
```

### Rules without Airflow

`airflow_timetables_calendar.rules` and `.calendars` import nothing from
Airflow, so they work as a plain date calculator or inside another scheduler.
They are unit-tested without an Airflow install, and CI parses their ASTs to
keep it that way.

One caveat: importing *the package* imports Airflow, because `__init__`
re-exports `CalendarTimetable`. So the rule engine is reusable only where
Airflow is installed anyway — it is the dependency that is avoidable, not the
installation.

```python
from datetime import date

# Through the package (needs Airflow installed):
from airflow_timetables_calendar import nth_business_day, period_for, ScheduleRule

class MyCalendar:
    def is_working_day(self, day: date) -> bool:
        return day.weekday() < 5

rule = ScheduleRule(**nth_business_day(5))
rule.resolve(period_for(date(2026, 9, 1)), MyCalendar())  # 2026-09-07
```

Any object with `is_working_day(day)` satisfies the `WorkingDayCalendar`
protocol — no import from this library is needed, so a host scheduler can pass
its own calendar object straight in.

## Serialization

Custom timetables have to be resolvable when a DAG is deserialized, in every
Airflow component. Installing the package is enough: an `airflow.plugins` entry
point registers the timetable and teaches the DAG serializer how to encode it.

## Scope and compatibility

- Tested against Airflow 3.x. The timetable derives from the **core**
  `CronTriggerTimetable`, not the SDK `BaseTimetable`, because the SDK base is
  missing attributes that core code reads unconditionally (see
  `timetable.py` for the details).
- The serializer hook uses a private Airflow attribute. It is guarded: an
  unsupported Airflow logs a clear error instead of taking the scheduler down.
- Not associated with or endorsed by the Apache Software Foundation. "Airflow" is
  used descriptively.

## Licence

MIT. See [`LICENSE`](LICENSE).

## Sources

The rule vocabulary and its edge cases follow the conventions that classical
Japanese job schedulers share — 種別 / 開始日 / 休業日の振り替え / 起算スケジュール /
猶予日数, plus the compact 休止日・相対 form. That vocabulary is an industry
convention rather than a public standard, so the implementation is a faithful
*model* of the documented behaviour, not a byte-compatible parser of any
particular product's configuration files. No vendor documentation or product
name is reproduced here.
