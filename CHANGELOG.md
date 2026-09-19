# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Nothing yet.

### Known issues

These are recorded as `xfail(strict=True)` tests rather than quietly worked
around. Because the marks are strict, fixing any of them turns the test green,
and breaking one again turns it into a failure.

- **Negative `相対` is inverted.** A negative `offset` lands *after* the anchor
  instead of before it: `jobcenter(day=15, shift="next", relative=-1)` resolves
  to 2026-09-25 where the previous working day, 2026-09-14, is expected.
  (`tests/test_jobcenter.py::TestNegativeRelativeIsInverted`)
- **An anchor is walked even when it is already a working day.**
  `jobcenter(day=30, shift="prev")` resolves to 2026-10-19 although 2026-09-30 is
  an open Wednesday needing no shift at all.
  (`tests/test_jobcenter.py::TestAnchorsAreWalkedEvenWhenOpen`)
- **A forward `相対` double-counts closed days.** Both the holiday-shifting stage
  and the offset stage step over closed days, so a closed day inside the span is
  crossed twice.
- **`前月末営業日` never fires.** `month_offset=-1` puts the anchor in the previous
  month, which the anchor-month guard then rejects for every period, so the
  preset produces no runs at all in 2026.
  (`tests/test_serialization.py::TestEquivalenceAcrossAWholeYear`)

## [0.1.0] - 2026-09-19

Initial release.

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
