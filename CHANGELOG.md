# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-09-20

### Added

- **The 48-hour clock.** `CalendarTimetable(hour=...)` now accepts `-47`..`47`
  instead of `0`..`23`. A run whose declared hour lies outside `0`..`23` is placed
  on the adjacent calendar day but is still evaluated against the rules of the
  *declared* day: `hour=25` with `last_business_day_of_month` runs at 01:00 the
  following morning and belongs to the last working day of the month. The negative
  half is the mirror image, so `hour=-1` runs at 23:00 the previous evening and
  still belongs to the declared day.
  Negative hours and hours below `-47`/above `47` are rejected with a clear
  `ValueError`.
  (`tests/test_timetable.py::TestFortyEightHourClock`)
- `CalendarTimetable` now exposes `is_working_day()` for the resolved calendar.
- Documentation for the 48-hour clock in `README.md` and `README.JP.md`.

### Changed

- **Breaking.** The month-end and working-day presets were pruned to remove
  byte-identical duplicates: `第1営業日`/`first_business_day` and
  `月末営業日`/`last_business_day` were removed. Use `月初営業日`/
  `first_business_day_of_month` and `当月末営業日`/`last_business_day_of_month`.
- `summary` now appends `hour: <n>` only when the declared hour lies outside
  `0`..`23`, so ordinary schedules keep their existing summary text.

### Fixed

- Serialization round-trips the declared `hour` for values outside `0`..`23`
  rather than the normalised wall-clock hour.

## [0.1.0] - 2026-09-20

First public release.

Requires **apache-airflow >= 3.2**. That floor is not arbitrary: a custom
timetable needs both registration in the plugin registry *and*, from 3.2, a
serializer registered against `_Serializer.serialize_timetable` in
`airflow.serialization.encoders`. On 3.0 and 3.1 that module does not exist and
the encoder raises `TimetableNotRegistered` before reaching
`timetable.serialize()`, so a DAG using this timetable cannot be serialized. The
CI matrix found this; `plugin.py` handles the absence explicitly so the failure
is a clear log line rather than a traceback at import.


### Fixed

Four documented bugs, all of which turned out to share a single root cause:
`simple_rule()` built its anchor with `kind=Kind.OPERATING`, so `day=N` meant
*the Nth working day of the month* rather than *the Nth of the month*. The compact
rule text is `＋登録、毎月（日付）、1日、休止日 後シフト、相対 4` — 毎月（日付）
(monthly **by date**) — so `day=N` must be a 暦日 anchor (`Kind.ABSOLUTE`). An
anchor displaced by weeks looks exactly like a sign inversion and like a
double-counted closure, which is why three separate symptoms came from one
mistake.

- **`simple_rule(day=N)` is now a calendar day, not a working-day count.** The
  working-day families remain available as 第n営業日 via `nth_business_day()`.
  `simple_rule(day=30, shift="prev")` now resolves to 2026-09-30 (it used to give
  2026-10-19 — the 30th *working* day of September).
  (`tests/test_simple_rule.py::TestAnchorsAreNotWalkedWhenOpen`)
- **Negative `相対` lands behind the anchor.** `相対 n` counts `n` working days
  from the settled anchor, the anchor itself being step 0, in both directions.
  `simple_rule(day=15, shift="next", relative=-1)` now gives 2026-09-14 where it
  used to give 2026-09-25. Both of the documented worked examples fall out of this
  one rule: `相対 4` on `1日` is 月初から5営業日目 and `相対 -2` on `L日` is
  月末の3営業日前.
  (`tests/test_simple_rule.py::TestNegativeRelativeCountsBackwards`)
- **Forward `相対` no longer double-counts closed days.** The 休止日 shift and the
  起算スケジュール offset are now carried by a *single* walk instead of two
  chained ones, so a closed day inside the span is crossed once.
  `simple_rule(day=10, shift="next", relative=1)` gives 2026-09-11 — the 10th is
  open, so 起算 moves one 運用日 forward from it. The shift step itself is not one
  of the counted steps.
- **`前月末営業日` fires again.** This one was unrelated to the anchor kind:
  `ScheduleRule.matches()` only consulted the period *containing* the queried
  day, so a rule whose run belongs to the *following* period — which is exactly
  what `month_offset=-1` produces — was never matched by the per-day question a
  timetable actually asks. `resolve()` had been returning the right day all
  along, which is why the bug survived the existing tests. The preset now
  produces 12 runs a year, each on the last working day of the month it lands in.
  (`tests/test_serialization.py::TestEquivalenceAcrossAWholeYear`)

- **A forward `相対` that crosses the month end now fires.** `Scope.PERIOD`
  (開始年月) was tested against the *period* rather than against the month the
  rule's anchor falls in, so a rule whose result legitimately left the period was
  rejected outright — and with the default `base_day=1` the period ends on the
  last calendar day, so the *first working day after month end* was exactly the
  case that got dropped. `simple_rule(day="L", shift="prev", relative=1)` — "the
  first working day after month end" — resolved to `None` for every month and
  therefore never ran. The backward direction did the same thing legitimately,
  which is why it looked asymmetric.
  (`tests/test_simple_rule.py::TestForwardRelativeCrossesMonthEnd`)
- **`matches()` consults the previous period too.** A run may now reach
  *forwards* out of its own period as well as backwards, and `matches()` — the
  call that actually decides scheduling — only looked at the containing period
  and the next one. Both neighbours are checked; consecutive periods do not
  overlap, so this cannot produce a false positive.
  (`tests/test_rules.py::TestScope`)
- **`base_day` no longer silently suppresses a rule.** With `base_day=26`, a
  period runs 08-26..09-25 and the end-of-month occurrence *is* the 25th, so a
  `相対` moving it by a day or two necessarily lands outside the period.
  `simple_rule(day=25, shift="next", relative=1)` resolved to `None` for every
  month instead of the 26th of August, September, and so on.

Two documented behaviours changed as a direct consequence, and their tests were
updated rather than kept:

- A closed anchor with the default 実行しない (`Substitution.SKIP`) now produces no
  run. It previously walked forward into the next working day, because the anchor
  it was walking from was not the day the rule named.
- `CalendarTimetable(..., rules=["前月末営業日"], base_day=26).matches_rules(
  date(2026, 9, 30))` is now `True`: that day is the run of the period anchored in
  October. It was `False` only because of the `matches()` bug above.

Three further defects, found in review and all equally silent: the DAG parsed,
the schedule simply never fired, or fired on a day nobody asked for. They survived
the round above because the only `month_offset` coverage was `-1` with
`Scope.FREE` — precisely the one combination that happened to work.

- **`month_offset` now composes with `Scope.PERIOD`.** `resolve()` compared the
  *moved* anchor's month against the period a second time, at a gate that did not
  skip when `month_offset` was set. Since `month_offset` exists precisely to move
  the anchor out of the period it is evaluated for, that comparison is
  unsatisfiable for every non-zero offset, so 前月末営業日 combined with 開始年月
  resolved to `None` in every period. 開始年月 is a lower bound on the months a
  rule applies from — *"開始年月以降の月についても「1日」が実行日となります"* — and is
  set alongside 種別 / 開始日, which name the anchor; it does not constrain where
  inside a period the anchor sits. The redundant gate is gone.
  (`tests/test_rule_fixes.py::TestScopeWithMonthOffset`)
- **`matches()` finds the period that produced a day, for any `month_offset`.**
  It scanned only the containing period and its two neighbours, which silently
  dropped every `|month_offset| >= 2` — the schedule never fired, with no error
  anywhere. A bounded scan cannot be correct either, because a run crosses *two*
  period boundaries when an unaligned anchor is combined with a movement: with
  `base_day=26`, `day=1, offset=-1` resolves to a day two periods behind its
  epoch. The producing epoch is now recovered arithmetically, bounded by the
  猶予日数 windows, and cross-checked against `resolve()` over a randomised sweep
  of 5,952 rule/day combinations.
  (`tests/test_rule_fixes.py::TestMatchesFindsTheProducingEpoch`)
- **曜日指定 no longer leaves the month it names.** `_nth_weekday` computed the
  anchor as `first + 7 * (day - 1)` with no bound, so any `day` beyond the month's
  last occurrence walked into the following month — for a Sunday weekday in
  February 2026, `day=5` gave 2026-03-01 and `day=28` gave 2026-08-09.
  曜日指定 means "the Nth <weekday> of
  the month", so an occurrence that does not exist now names the anchor month's
  last day, matching how the day-based 開始日 forms already clamp an out-of-range
  `day`. (`tests/test_rule_fixes.py::TestWeekdayStartDayStaysInTheMonth`)
- **月末指定 no longer walks out of the anchor month.** `_nth_closed_day_back_from`
  and `_nth_working_day_back_from` scanned backwards without a floor, so a month
  with fewer than the requested 休業日 produced a date in a neighbouring month
  instead of producing no run. `_scan()` takes an optional `floor` for this.

- **`Kind.RELATIVE` (相対日) now counts from the 基準日.** It previously counted
  from `period.anchor_month`, the *first day of the month the 基準日 falls in*,
  which is exactly what 絶対日 does — so `RELATIVE` and `ABSOLUTE` were the same
  rule under two names, and only diverged when a `base_day` was set. 相対日 is
  「基準日として指定した日付から起算した日付」, so it counts from the 基準日
  itself (`period.start`), 1-based like 絶対日. With `base_day=26`, `day=1` is now
  2026-08-26 rather than an October date, and with the default `base_day=1` the
  two readings coincide, which is why nothing else moved.
  (`tests/test_rules.py::TestAbsoluteAndRelativeDays`)
- **月末指定 and 曜日指定 now use the period, not the calendar month, for every
  種別 other than 絶対日.** 開始日 is not measured from the same origin in every
  case: 絶対日 is defined as 暦の上での日付, so it names a day of the calendar
  month, but 相対日 / 運用日 / 休業日 are defined as 基準日の指定に基づいた期間を
  1か月として, so they name a day of the **business month the 基準日 defines**.
  All of them were using the calendar month.

  With `base_day=26`, the period opening 2026-08-26 closes on **2026-09-25**, so
  `当月末営業日` now resolves to 2026-09-25 where it used to give the calendar
  August's 2026-08-31, and 前月末営業日 gives 2026-08-25 where it used to reach
  two months back to 2026-07-31. 曜日指定 for 相対日 counts weeks from the
  基準日, so "the 1st Monday" is 2026-08-31 rather than the calendar month's
  2026-08-03. `base_day=1` is unaffected, which is why the default path and the
  production fingerprint do not move.
  (`tests/test_rules.py::TestStartDayOriginFollowsTheKind`)

### Changed

- `Scope.PERIOD` (開始年月) is now a bound on **the months a rule applies from**
  rather than a second, independent check on the anchor's month. 開始年月 is set
  alongside 種別 / 開始日, which are what name the anchor, so it constrains which
  periods the rule covers — not where inside a period the anchor sits. The
  alignment constraint that *is* real (a day-based 開始日 whose anchor month cannot
  match the period) is still enforced by the 処理サイクル fast path, which is
  deliberately skipped once `month_offset` has moved the anchor. A movement may
  carry the result forward or backward out of the period, which is what 振り替え
  and 起算 are for. `Scope.FREE` is unchanged.
- The 種別 decides which origin a 開始日 is measured from, since the classical
  model defines three. The README now tabulates them: 絶対日 is the only kind
  named against the **calendar month**; 相対日 / 運用日 / 休業日 use the period
  for 月末指定 and 相対日 counts 日付指定 and 曜日指定 from the **基準日 itself**.
  `base_day=1` makes the 基準日 the 1st and every reading coincides, so this only
  surfaces once a `base_day` is set.
- The 開始年月 documentation is now explicit that it is a *lower* bound only.
  There is no 有効期日 (validity end date), and therefore no interaction between
  it and the 猶予日数 window — the classical model lets a grace window override an
  expiry, which cannot arise here. See README "Not modelled".

### Removed

- `第1営業日` / `first_business_day` and `月末営業日` / `last_business_day` are
  gone, in both spellings. Each was a byte-identical duplicate of a preset that
  stays -- `第1営業日` == `月初営業日` and `月末営業日` == `当月末営業日` -- so the
  table offered two names for one rule with nothing to choose between them. The
  surviving spellings are the ones that say which month they refer to, which is
  the axis `前月末営業日` and `当月末営業日` actually differ on. `BUSINESS_DAY_RULES`
  is now seven presets and `PRESET_ALIASES` seven aliases, still in 1:1
  correspondence.

### Added
- **Every preset now has an English name as well as its Japanese one.** The
  Japanese spellings stay canonical -- they are what the source definitions are
  written in -- but `next_business_day`, `every_business_day` and the other five
  are accepted anywhere a preset name is accepted, and build an identical
  `ScheduleRule`. `PRESET_ALIASES` holds the English names and `PRESET_LOOKUP`
  maps every accepted spelling to its canonical key, so a UI can offer both. An
  unknown name lists the whole vocabulary.
  (`tests/test_rules.py::TestPresetAliases`)
- `README.JP.md`, a Japanese translation of the README. `README.md` is now
  entirely in English, with the Japanese vocabulary retained only as terminology
  the API itself uses (preset names, enum labels) and as the industry terms the
  concepts are named after. `pyproject.toml` still ships `README.md` to PyPI and
  the Japanese file links back to it.
- `ScheduleRule.shift_direction` records which way the 休止日 shift travels
  (`+1` 後シフト, `-1` 前シフト, `0` unset). `simple_rule()` sets it so that a closed
  anchor settles the way its own 休止日 rule says even though the substitution
  stage is deliberately a no-op there — without it a 前シフト could be dragged
  forwards by a positive `相対`. It round-trips through the serializer.


- `CalendarTimetable`, an Airflow timetable that fires on working days only,
  resolved from a calendar id — a country (`JP`, `US-CA`), an exchange (`TSE`,
  `XNYS`), or `NONE` for plain Monday–Friday. Ids accept a `country:` /
  `holidays:` / `financial:` or `exchange:` / `market:` prefix to disambiguate.
- A business-day rule engine covering the constructs that recur in Japanese
  back-office scheduling, in the vocabulary that classical Japanese job
  schedulers share:
  種別 (登録日 / 絶対日 / 相対日 / 運用日 / 休業日), 開始日 (日付指定 / 月末指定 /
  曜日指定), 休業日の振り替え with 振り替え猶予日数, 起算スケジュール with 起算猶予日数,
  開始年月 scoping and 処理サイクル.
- `BUSINESS_DAY_RULES` presets: 月初営業日, 当月末営業日, 前月末営業日,
  月末前営業日, 毎営業日.
- Constructors `nth_business_day`, `nth_business_day_from_end`, `verbose_rule`,
  `simple_rule`, `business_days_before`, `business_days_after`,
  `calendar_days_before`, `calendar_days_after`, and the helpers `build_rules`,
  `resolve_rules`, `period_for`.
- Serializer registration through the `airflow.plugins` entry point, so
  `pip install` alone is enough — no file has to be copied into `plugins/`.
- `exclude_dates` / `include_dates` overrides for shutdown days and for working
  days the calendar does not know about.
- The calendar layer (`calendars.py`) and rule engine (`rules.py`) import no
  Airflow at all, so they can be reused by another scheduler. This is checked in
  CI by installing the package with `--no-deps` and importing them.
- `[exchanges]` extra for `pandas_market_calendars`, kept optional because it
  pulls in pandas; `holidays` is a core dependency.

[0.1.0]: https://github.com/yuukioosaka/airflow-timetables-calendar/releases/tag/v0.1.0
