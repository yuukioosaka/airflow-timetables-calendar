# airflow-timetables-calendar

Calendar-driven [Airflow](https://airflow.apache.org/) timetables, plus a
business-day rule engine using the vocabulary of classical enterprise job
schedulers (kind / start day / substitution / offset / grace days).

The point of this package is that "run on the last working day of the month" is
not something a cron expression can say, and hand-rolling the arithmetic for every
region, exchange and holiday rule is how schedulers go quietly wrong. So instead
of reimplementing calendars, this delegates to libraries that already maintain the
data, and layers the scheduling vocabulary enterprise job schedulers have used for
decades on top.

```bash
pip install airflow-timetables-calendar

# exchange calendars (Tokyo Stock Exchange, NYSE, LSE, ...) are opt-in
pip install "airflow-timetables-calendar[exchanges]"
```

Japanese documentation: [`README.JP.md`](README.JP.md).

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

A bare code is resolved against `holidays` first, then the exchange registry.
The two registries sometimes use different codes for the same market, so use
a prefix when you mean a specific one:

```python
CalendarTimetable(calendar_id="country:JP")      # force holidays (country)
CalendarTimetable(calendar_id="exchange:XTKS")   # Tokyo Stock Exchange, mcal
CalendarTimetable(calendar_id="exchange:XLON")   # London Stock Exchange, mcal
```

Exchange ids are the `pandas_market_calendars` names, which are usually the
MIC code rather than the familiar abbreviation — `XTKS`, not `TSE`. Check
`available_exchange_calendars()` when in doubt; an unknown id raises
`UnknownCalendarError` at DAG-parse time rather than scheduling silently.

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
CalendarTimetable(calendar_id="JP", hour=21, rules=["last_business_day"])

# The 10th working day, and the month's last working day
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    rules=[nth_business_day(10), nth_business_day_from_end(0)],
)

# The explicit form: argument names map 1:1 onto
# kind / start day / substitution / offset, so an existing definition can be
# transcribed directly.
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    rules=[verbose_rule(kind=Kind.ABSOLUTE, day=15, substitution="next", grace_days=3)],
)

# The compact form: one line per schedule, the way the shift/relative notation
# is normally written.
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    rules=[simple_rule(day="L", shift="prev", relative=-2)],  # 3 working days before month end
)
```

| Preset | Meaning |
|---|---|
| `first_business_day` | the month's 1st working day |
| `first_business_day_of_month` | the month's opening working day |
| `last_business_day` | the month's last working day |
| `last_business_day_of_month` | the current month's last working day |
| `last_business_day_of_previous_month` | the *previous* month's last working day |
| `business_day_before_month_end` | the working day before the month's last |
| `every_business_day` | every working day |

Each preset also has a native-language spelling, which is the canonical key
because it is what the source definitions are written in. `PRESET_ALIASES` holds
the English names and `PRESET_LOOKUP` maps every accepted spelling to its
canonical key, so a tool can offer both. An unknown name raises at DAG-parse time
and lists every accepted spelling. See [`README.JP.md`](README.JP.md) for the
native table.

### The model

A date is derived from an **anchor** plus a chain of **modifiers**, following
the classical model:

| Concept | Meaning |
|---|---|
| base date | where a "month" starts. `base_day=26` makes 2026-08-26..2026-09-25 the "August" business month |
| base time | how the classical model rolls a *business date* over, so that 08:00..next-day 07:59 is one business day and a run at "25:00" still belongs to the previous date. **Not modelled here**: `CalendarTimetable` uses a plain wall-clock `hour`/`minute`, so the range is 0–23 and a 48-hour-clock schedule cannot be expressed. Use the `timezone` to place the run, and see [Not modelled](#not-modelled) |
| kind | what the offset counts: `ABSOLUTE` calendar date, `RELATIVE` calendar days from the base date (identical to `ABSOLUTE` when `base_day=1`), `OPERATING` working days (→ "the Nth working day"), `CLOSED` closed days, `REGISTERED` the registration date |
| start day | `DAY` a date of the month, `MONTH_END` days before month end, `WEEKDAY` the Nth weekday. What the resulting day is *measured from* depends on the kind — see [Where a start day is measured from](#where-a-start-day-is-measured-from) |
| substitution | what to do when the day is closed: `SKIP` do not run, `PREVIOUS` the previous working day, `NEXT` the next working day, `RUN_ANYWAY` do not substitute |
| offset schedule | a final `n` working-day (`OPERATING`) or calendar-day (`CALENDAR`) adjustment |
| grace days | the maximum distance a shift may travel, counted in *calendar* days. **Beyond it, that occurrence produces no run at all** — matching the classical model, this is not an error. The window also bounds how far a rule may reach, so a wider grace window costs more work in `matches()`. **`grace_days=0` means "use the default", not "zero tolerance"** — omit it unless you need a tighter window |

`simple_rule()` is the compact spelling of the same model. `relative n` counts `n`
working days from the *settled* anchor, with the anchor itself counting as 0, so
`relative=4` on day 1 is the 5th working day and `relative=-2` on `L` is 3 working
days before month end. The shift settles the anchor first and `relative` then
counts from that settled day, in `relative`'s own direction. `verbose_rule()`
instead applies the offset to the anchor the rule names, without a preceding
substitution.

Note that only *one* stage is allowed to walk: `simple_rule()` keeps the
substitution from moving the date and lets the offset carry the whole distance.
Chaining the two would make every closed day in the span cost two steps.

Because both `relative` and substitution are movements, their result may cross the
end of the month — `day="L", shift="prev", relative=1` genuinely means "the first
working day after month end". The start year-month bounds the month the rule
*anchors* in, so it rejects a date that walks back past it, but not one that walks
forward out of it. The same applies with a `base_day`, where the end-of-month run
lands in the following month by design.

```python
# Last working day of each business month starting on the 26th
CalendarTimetable(
    calendar_id="JP",
    hour=21,
    base_day=26,
    rules=[nth_business_day_from_end(0)],
)
```

### Where a start day is measured from

The kind and the start day are not independent: which origin a day is counted from
is decided by the kind, and the two axes are easy to conflate because they collapse
to the same answer whenever `base_day=1`.

| kind | a date of the month | days before month end | the Nth weekday |
|---|---|---|---|
| `ABSOLUTE` | the **calendar month** | the **calendar month**'s last day | the **calendar month**'s weeks |
| `RELATIVE` | the **base date** (1-based) | the **period**'s last day | weeks counted **from the base date** |
| `OPERATING` | the period's Nth working day | the **period**'s last working day | — |
| `CLOSED` | the period's Nth closed day | the **period**'s last closed day | — |

"Period" means the base-date-defined business month. So with `base_day=26`, the
period opening 2026-08-26 closes on **2026-09-25**, and:

```python
# Last working day of the *period* -> 2026-09-25
CalendarTimetable(calendar_id="JP", hour=21, base_day=26,
                  rules=[nth_business_day_from_end(0)])

# ABSOLUTE + MONTH_END keeps the calendar reading -> 2026-08-31
CalendarTimetable(calendar_id="JP", hour=21, base_day=26,
                  rules=[verbose_rule(kind=Kind.ABSOLUTE,
                                      start_day=StartDay.MONTH_END, day=0)])
```

The weekday form has a matching split: `ABSOLUTE` counts weeks from the 1st of the
calendar month, while `RELATIVE` counts them from the base date, so with
`base_day=26` "the 1st Monday" is 2026-08-31 under `RELATIVE` but 2026-08-03 under
`ABSOLUTE`. An occurrence that does not exist within the period names the period's
last day rather than walking into the next one.

Set `base_day=1` (the default) and both readings are the same thing, because the
base date *is* the 1st. That is why this only matters once a base date is
configured.

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

## Not modelled

The rule vocabulary is a *model* of the classical behaviour, not a complete
reimplementation of any one product. These parts are deliberately absent, and
the README describes them only so that the rest makes sense:

- **base time / 48-hour clock.** A business date rolling over at, say, 08:00, and
  the 0:00–47:59 time range that lets a job run at "25:00" and still belong to the
  previous date. `CalendarTimetable` takes a plain `hour`/`minute` in a
  `timezone`, so the range is 0–23. A run that must be dated to the previous
  business day is not expressible; use the offset schedule
  (`business_days_before`) or a different `hour` instead.
- **a rule validity end date.** The start year-month bounds a rule from *below*
  only. There is no upper bound, and therefore no interaction between the grace
  window and an expiry — the classical model lets the window override the expiry,
  which cannot arise here.
- **the registration date.** `Kind.REGISTERED` resolves to the period's own start,
  which is a stand-in for "when this was registered" rather than a real
  registration timestamp. It is not wired to any Airflow run state.
- **substitution grace bounds.** The classical model documents a 1–31 day window.
  `grace_days` is not range-checked, and `grace_days=0` selects the default window
  rather than a zero-day one.
- **the Nth weekday for working/closed days.** The specification's start-day table
  defines the weekday form only for `ABSOLUTE` and `RELATIVE`, so the two
  working-day / closed-day combinations are undefined. They are accepted here and
  resolve against the calendar month, which is a local choice rather than a
  documented behaviour.

If you need any of these, they are the natural next things to add — see
`rules.py`, where each is a self-contained stage.

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
enterprise job schedulers share — kind / start day / substitution / offset
schedule / grace days, plus the compact shift-and-relative notation. That
vocabulary is an industry convention rather than a public standard, so the
implementation is a faithful *model* of the documented behaviour, not a
byte-compatible parser of any particular product's configuration files. No vendor
documentation or product name is reproduced here.
