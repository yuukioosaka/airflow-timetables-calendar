# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

Four documented bugs, all of which turned out to share a single root cause:
`jobcenter()` built its anchor with `kind=Kind.OPERATING`, so `day=N` meant *the
Nth working day of the month* rather than *the Nth of the month*. NEC's rule text
is `＋登録、毎月（日付）、1日、休止日 後シフト、相対 4` — 毎月（日付） (monthly **by
date**) — so `day=N` must be a 暦日 anchor (`Kind.ABSOLUTE`). An anchor displaced
by weeks looks exactly like a sign inversion and like a double-counted closure,
which is why three separate symptoms came from one mistake.

- **`jobcenter(day=N)` is now a calendar day, not a working-day count.** The
  working-day families remain available as 第n営業日 via `nth_business_day()`.
  `jobcenter(day=30, shift="prev")` now resolves to 2026-09-30 (it used to give
  2026-10-19 — the 30th *working* day of September).
  (`tests/test_jobcenter.py::TestAnchorsAreNotWalkedWhenOpen`)
- **Negative `相対` lands behind the anchor.** `相対 n` counts `n` working days
  from the settled anchor, the anchor itself being step 0, in both directions.
  `jobcenter(day=15, shift="next", relative=-1)` now gives 2026-09-14 where it
  used to give 2026-09-25. Both of NEC's published examples fall out of this one
  rule: `相対 4` on `1日` is 月初から5営業日目 and `相対 -2` on `L日` is
  月末の3営業日前.
  (`tests/test_jobcenter.py::TestNegativeRelativeCountsBackwards`)
- **Forward `相対` no longer double-counts closed days.** The 休止日 shift and the
  起算スケジュール offset are now carried by a *single* walk instead of two
  chained ones, so a closed day inside the span is crossed once.
  `jobcenter(day=10, shift="next", relative=1)` gives 2026-09-15 (used to give
  2026-09-17). The shift step itself is not one of the counted steps.
- **`前月末営業日` fires again.** This one was unrelated to the anchor kind:
  `ScheduleRule.matches()` only consulted the period *containing* the queried
  day, so a rule whose run belongs to the *following* period — which is exactly
  what `month_offset=-1` produces — was never matched by the per-day question a
  timetable actually asks. `resolve()` had been returning the right day all
  along, which is why the bug survived the existing tests. The preset now
  produces 12 runs a year, each on the last working day of the month it lands in.
  (`tests/test_serialization.py::TestEquivalenceAcrossAWholeYear`)

Two documented behaviours changed as a direct consequence, and their tests were
updated rather than kept:

- A closed anchor with the default 実行しない (`Substitution.SKIP`) now produces no
  run. It previously walked forward into the next working day, because the anchor
  it was walking from was not the day the rule named.
- `CalendarTimetable(..., rules=["前月末営業日"], base_day=26).matches_rules(
  date(2026, 9, 30))` is now `True`: that day is the run of the period anchored in
  October. It was `False` only because of the `matches()` bug above.

### Added

- `ScheduleRule.shift_direction` records which way the 休止日 shift travels
  (`+1` 後シフト, `-1` 前シフト, `0` unset). `jobcenter()` sets it so that a closed
  anchor settles the way its own 休止日 rule says even though the substitution
  stage is deliberately a no-op there — without it a 前シフト could be dragged
  forwards by a positive `相対`. It round-trips through the serializer.

## [0.1.0] - 2026-09-19

Initial release.

Requires **apache-airflow >= 3.2**. That floor is not arbitrary: a custom
timetable needs both registration in the plugin registry *and*, from 3.2, a
serializer registered against `_Serializer.serialize_timetable` in
`airflow.serialization.encoders`. On 3.0 and 3.1 that module does not exist and
the encoder raises `TimetableNotRegistered` before reaching
`timetable.serialize()`, so a DAG using this timetable cannot be serialized. The
CI matrix found this; `plugin.py` handles the absence explicitly so the failure
is a clear log line rather than a traceback at import.

### Added

- `CalendarTimetable`, an Airflow timetable that fires on working days only,
  resolved from a calendar id — a country (`JP`, `US-CA`), an exchange (`TSE`,
  `XNYS`), or `NONE` for plain Monday–Friday. Ids accept a `country:` /
  `holidays:` / `financial:` or `exchange:` / `market:` prefix to disambiguate.
- A business-day rule engine covering the constructs that recur in Japanese
  back-office scheduling, modelled on JP1/AJS3 and NEC WebSAM JobCenter:
  種別 (登録日 / 絶対日 / 相対日 / 運用日 / 休業日), 開始日 (日付指定 / 月末指定 /
  曜日指定), 休業日の振り替え with 振り替え猶予日数, 起算スケジュール with 起算猶予日数,
  開始年月 scoping and 処理サイクル.
- `BUSINESS_DAY_RULES` presets: 第1営業日, 月初営業日, 月末営業日, 当月末営業日,
  前月末営業日, 月末前営業日, 翌営業日, 前営業日, 毎営業日.
- Constructors `nth_business_day`, `nth_business_day_from_end`, `jp1`,
  `jobcenter`, `business_days_before`, `business_days_after`,
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

[Unreleased]: https://github.com/yuukioosaka/airflow-timetables-calendar/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/yuukioosaka/airflow-timetables-calendar/releases/tag/v0.1.0
