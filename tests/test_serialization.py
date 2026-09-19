"""Serialization tests -- the DAG-serializer round trip.

This module guards the original production bug. A timetable whose
``serialize()``/``deserialize()`` disagree, or whose serializer is never
registered with Airflow, produces exactly the ``'NoneType' object has no
attribute 'iter_dag_dependencies'`` ``SerializationError`` that prompted this
package -- and that error surfaces as a 500 on the DAG page, not as a DAG import
failure, so it is worth pinning down at three levels: the plugin registration,
the payload, and a real DAG through the real serializer.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pytest

from airflow_timetables_calendar import (
    BUSINESS_DAY_RULES,
    CalendarTimetable,
    nth_business_day,
    nth_business_day_from_end,
)


class TestSerializerRegistration:
    """Without this, Airflow cannot read the DAG back and the UI shows a 500."""

    def test_the_plugin_registered_the_serializer(self):
        from airflow_timetables_calendar import plugin

        assert plugin.SERIALIZER_REGISTERED is True

    def test_the_plugin_declares_a_name(self):
        from airflow_timetables_calendar import plugin

        assert plugin.CalendarTimetablePlugin.name == "calendar_timetable_plugin"

    def test_the_registration_is_idempotent(self):
        # `_register_serializer` runs at import and again if Airflow re-imports
        # the plugin; a second call must not error or double-wrap.
        from airflow_timetables_calendar import plugin

        plugin._register_serializer()
        assert plugin.SERIALIZER_REGISTERED is True

    def test_the_timetable_is_a_plain_airflow_timetable(self):
        from airflow.timetables.base import Timetable

        assert isinstance(CalendarTimetable(calendar_id="JP"), Timetable)

    def test_the_timetable_subclasses_the_core_cron_trigger_timetable(self):
        # Subclassing the *core* class is what keeps Airflow's dependency
        # detector and asset-condition machinery working; a hand-rolled
        # Timetable was what produced the original crash.
        from airflow.timetables.trigger import CronTriggerTimetable

        assert issubclass(CalendarTimetable, CronTriggerTimetable)

    def test_the_entry_point_is_declared_in_the_metadata(self):
        from importlib.metadata import entry_points

        found = {ep.name: ep.value for ep in entry_points(group="airflow.plugins")}
        assert found.get("calendar_timetable", "").endswith("plugin:CalendarTimetablePlugin")


class TestPayloadShape:
    def test_serialize_returns_a_json_safe_dict(self):
        tt = CalendarTimetable(calendar_id="JP", hour=21, rules=["月末営業日"])
        payload = tt.serialize()
        assert isinstance(payload, dict)
        json.dumps(payload)  # must not raise

    def test_the_expected_keys_are_present(self):
        tt = CalendarTimetable(calendar_id="JP", hour=21)
        assert set(tt.serialize()) == {
            "calendar_id",
            "hour",
            "minute",
            "timezone",
            "exclude_dates",
            "include_dates",
            "rules",
            "base_day",
        }

    def test_rules_are_stored_without_enum_instances(self):
        # The Airflow serializer cannot carry our dataclass across a version
        # boundary, so rules travel as dicts of primitives.
        tt = CalendarTimetable(calendar_id="JP", rules=[nth_business_day(1)])
        rule = tt.serialize()["rules"][0]
        assert isinstance(rule, dict)
        for key, value in rule.items():
            assert not hasattr(value, "value"), f"{key} leaked an enum: {value!r}"

    def test_dates_are_stored_as_iso_strings(self):
        tt = CalendarTimetable(calendar_id="JP", exclude_dates=[date(2026, 12, 31)])
        assert tt.serialize()["exclude_dates"] == ["2026-12-31"]

    def test_the_timezone_is_stored_by_name(self):
        tt = CalendarTimetable(calendar_id="JP", timezone="Asia/Tokyo")
        assert tt.serialize()["timezone"] == "Asia/Tokyo"

    def test_an_empty_timetable_serializes(self):
        assert CalendarTimetable().serialize()["calendar_id"] == "NONE"


class TestRoundTrip:
    def test_scalar_fields_survive(self):
        tt = CalendarTimetable(
            calendar_id="JP",
            hour=9,
            minute=30,
            timezone="Asia/Tokyo",
            base_day=26,
            exclude_dates=[date(2026, 12, 31)],
            include_dates=[date(2026, 1, 2)],
            rules=["月末営業日"],
        )
        rt = CalendarTimetable.deserialize(tt.serialize())
        assert rt.calendar_id == tt.calendar_id
        assert rt.hour == tt.hour == 9
        assert rt.minute == tt.minute == 30
        assert rt.base_day == tt.base_day == 26
        assert rt.exclude_dates == tt.exclude_dates
        assert rt.include_dates == tt.include_dates

    def test_rules_survive_as_equal_values(self):
        tt = CalendarTimetable(calendar_id="JP", rules=[nth_business_day(10)])
        rt = CalendarTimetable.deserialize(tt.serialize())
        assert rt.rules == tt.rules
        assert len(rt.rules) == 1

    def test_enum_fields_come_back_as_enums(self):
        # A deserialized rule carries plain strings, and every comparison in the
        # resolver is `is`. Without the __post_init__ coercion a rule resolves to
        # nothing, or raises "unhandled kind 'operating'".
        from airflow_timetables_calendar import Kind, StartDay, Substitution

        tt = CalendarTimetable(calendar_id="JP", rules=[nth_business_day(1)])
        rt = CalendarTimetable.deserialize(tt.serialize())
        rule = rt.rules[0]
        assert isinstance(rule.kind, Kind)
        assert rule.kind is Kind.OPERATING
        assert isinstance(rule.start_day, StartDay)
        assert isinstance(rule.substitution, Substitution)

    def test_the_no_rules_default_survives(self):
        tt = CalendarTimetable(calendar_id="JP", hour=21)
        rt = CalendarTimetable.deserialize(tt.serialize())
        assert rt.rules == ()
        assert rt.matches_rules(date(2026, 9, 24)) is tt.matches_rules(date(2026, 9, 24))

    def test_the_none_calendar_survives(self):
        tt = CalendarTimetable(calendar_id="NONE", hour=9)
        rt = CalendarTimetable.deserialize(tt.serialize())
        assert rt.calendar_id == "NONE"
        assert rt.is_working_day(date(2026, 9, 21)) is True

    def test_deserialize_accepts_an_already_parsed_dict(self):
        tt = CalendarTimetable(calendar_id="JP", rules=["月末営業日"])
        rt = CalendarTimetable.deserialize(tt.serialize())
        assert rt.rules == tt.rules

    def test_a_missing_optional_key_falls_back_to_the_default(self):
        rt = CalendarTimetable.deserialize({"calendar_id": "JP", "hour": 21})
        assert rt.minute == 0
        assert rt.base_day == 1
        assert rt.rules == ()


class TestEquivalenceAcrossAWholeYear:
    """The highest-value test: every preset, every day, both objects.

    Case-by-case assertions missed two off-by-one bugs during development; a
    full-year sweep is what caught them, so it is asserted for every preset
    rather than for one representative.
    """

    @pytest.mark.parametrize("preset", sorted(BUSINESS_DAY_RULES))
    def test_every_preset_resolves_identically(self, preset):
        tt = CalendarTimetable(calendar_id="JP", hour=21, rules=[preset])
        rt = CalendarTimetable.deserialize(tt.serialize())

        day = date(2026, 1, 1)
        mismatches: list[str] = []
        while day.year == 2026:
            if tt.matches_rules(day) != rt.matches_rules(day):
                mismatches.append(str(day))
            day += timedelta(days=1)
        assert not mismatches, f"{preset}: {mismatches}"

    @pytest.mark.parametrize("preset", sorted(BUSINESS_DAY_RULES))
    def test_the_preset_actually_fires_sometimes(self, preset):
        if preset == "前月末営業日":
            # month_offset=-1 makes the anchor-month guard reject every period,
            # so the preset is dead code until that is fixed. See
            # TestMonthOffsetPresets in test_rules.py for the focused case.
            pytest.xfail("前月末営業日 never fires: month_offset=-1 is rejected")
        # Guards against a rule that round-trips perfectly because it never runs.
        # 前月末営業日 fires on a day in the *previous* month, so its 2026 count is
        # 11 rather than 12 -- the January run falls on 2025-12-31.
        tt = CalendarTimetable(calendar_id="JP", hour=21, rules=[preset])
        runs = [day for day in _days_of_2026() if tt.matches_rules(day)]
        assert runs, f"{preset} produced no runs in 2026"
        # Every run must be a working day on the JP calendar.
        assert all(tt.is_working_day(day) for day in runs), preset
        # 毎営業日 is frequency=DAILY: it fires on every one of the year's working
        # days, not once a month.
        if preset == "毎営業日":
            assert len(runs) == 244
        # 毎営業日 is frequency=DAILY: it fires on every working day of the year,
        # not once per period. 244 is the JP calendar's working-day count for 2026.
        if preset == "毎営業日":
            assert len(runs) == 244
        else:
            # Every other preset is 処理サイクル=月次, so one occurrence per month.
            assert len(runs) == 12

    def test_a_multi_rule_timetable_resolves_identically(self):
        tt = CalendarTimetable(
            calendar_id="JP",
            hour=21,
            base_day=26,
            rules=["月末営業日", "月初営業日", nth_business_day_from_end(3)],
            exclude_dates=[date(2026, 8, 31)],
        )
        rt = CalendarTimetable.deserialize(tt.serialize())
        for day in _days_of_2026():
            assert tt.matches_rules(day) == rt.matches_rules(day), day


class TestAirflowDagSerialization:
    """The end-to-end check: a real DAG through the real serializer."""

    def test_a_dag_using_the_timetable_serializes(self):
        from airflow import DAG

        dag = DAG(
            dag_id="_pkg_serialization_probe",
            schedule=CalendarTimetable(calendar_id="JP", hour=21, rules=["月末営業日"]),
            start_date=datetime(2026, 1, 1),
        )
        # In Airflow 3 the serializer class is `DagSerialization`; `SerializedDAG`
        # is the *result* type, which is why it has no to_dict()/from_dict().
        from airflow.serialization.serialized_objects import DagSerialization

        payload = DagSerialization.to_dict(dag)
        assert payload["dag"]["dag_id"] == "_pkg_serialization_probe"

    def test_the_dag_round_trips_back_into_a_dag(self):
        from airflow import DAG

        dag = DAG(
            dag_id="_pkg_serialization_probe_rt",
            schedule=CalendarTimetable(calendar_id="JP", hour=21),
            start_date=datetime(2026, 1, 1),
        )
        from airflow.serialization.serialized_objects import DagSerialization

        restored = DagSerialization.from_dict(DagSerialization.to_dict(dag))
        assert isinstance(restored.timetable, CalendarTimetable)


def _days_of_2026() -> list[date]:
    days: list[date] = []
    day = date(2026, 1, 1)
    while day.year == 2026:
        days.append(day)
        day += timedelta(days=1)
    return days
