"""Tests for the layering claim: `calendars` and `rules` import no Airflow.

The package re-exports the Airflow-dependent timetable from `__init__`, so
importing *the package* always needs Airflow. What must hold is that the two
reusable modules themselves contain no Airflow imports -- that is what makes them
usable by another scheduler, and what keeps the rule engine unit-testable without
an Airflow install.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

from airflow_timetables_calendar import calendars as calendars_mod
from airflow_timetables_calendar import rules as rules_mod


def _top_level_imports(module) -> set[str]:
    """Every module named by an import statement anywhere in the file."""
    tree = ast.parse(pathlib.Path(module.__file__).read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


@pytest.mark.parametrize("module", [calendars_mod, rules_mod], ids=["calendars", "rules"])
def test_module_has_no_airflow_import(module):
    roots = {name.split(".")[0] for name in _top_level_imports(module)}
    assert "airflow" not in roots, (
        f"{module.__name__} imports Airflow, which breaks the promise that the "
        f"rule engine and calendar layer can be reused by another scheduler"
    )


@pytest.mark.parametrize("module", [calendars_mod, rules_mod], ids=["calendars", "rules"])
def test_module_has_no_croniter_import(module):
    # croniter is only ever reached through the timetable's cron stepping.
    roots = {name.split(".")[0] for name in _top_level_imports(module)}
    assert "croniter" not in roots


def test_importing_the_submodule_pulls_the_package_init_in():
    """Documents the caveat, so it is a known cost rather than a surprise.

    Python imports the parent package before the submodule, and the parent
    re-exports `CalendarTimetable`, so Airflow ends up in `sys.modules` anyway.
    If this ever stops being true the docs can be simplified; until then the
    limitation is asserted rather than glossed over.
    """
    import sys

    assert "airflow" in sys.modules  # the parent package imported it

    # The module's own namespace, however, has no Airflow attribute reachable
    # through it -- there is no `calendars.airflow`.
    assert not hasattr(calendars_mod, "airflow")
    assert not hasattr(rules_mod, "airflow")


def test_the_reusable_modules_work_with_an_ad_hoc_calendar():
    """The practical version of the layering claim: no Airflow type is needed.

    A plain object with `is_working_day` is enough to drive the whole rule
    engine, which is what a non-Airflow scheduler would pass in.
    """
    from datetime import date

    from airflow_timetables_calendar.rules import ScheduleRule, period_for

    class PlainCalendar:
        def is_working_day(self, day: date) -> bool:
            return day.weekday() < 5

        def holiday_name(self, day: date) -> str | None:
            return None

    rule = ScheduleRule(kind="absolute", day=15, scope="free")
    assert rule.resolve(period_for(date(2026, 9, 1)), PlainCalendar()) == date(2026, 9, 15)
