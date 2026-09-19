"""The serializer registration must survive both Airflow serializer layouts.

`airflow.serialization.encoders` exists from Airflow 3.2 only, so a registration
that imports it unconditionally breaks on 3.0/3.1 -- which is what the CI matrix
found. These tests exercise the fallback branches directly, by importing the
plugin's helper with the module patched out, so the behaviour is pinned without
needing several Airflow versions installed at once.
"""

from __future__ import annotations

import importlib
import sys

from airflow_timetables_calendar import plugin


class TestRegistrationOnTheCurrentVersion:
    def test_the_serializer_is_registered(self):
        # This test environment is Airflow 3.2+, where the hook exists.
        assert plugin.SERIALIZER_REGISTERED is True

    def test_registering_twice_is_harmless(self):
        assert plugin._register_serializer() is True


class TestFallbackWhenEncodersIsAbsent:
    """Airflow 3.0/3.1 have no `airflow.serialization.encoders` at all.

    There is nothing to register in that layout, and that is fine: the encoder
    calls `timetable.serialize()` for a plugin-registered class. So the helper
    must report success rather than failure -- reporting failure would disable
    serialization on a version where it works.
    """

    def test_a_halted_module_is_reported_as_absent(self, monkeypatch):
        # A `sys.modules` entry of None makes `import_module` raise ImportError.
        monkeypatch.setitem(sys.modules, "airflow.serialization.encoders", None)
        assert plugin._import_encoders() is None

    def test_registration_succeeds_when_the_module_is_absent(self, monkeypatch):
        monkeypatch.setattr(plugin, "_import_encoders", lambda: None)
        assert plugin._register_serializer() is True

    def test_the_absent_module_path_is_logged(self, monkeypatch, caplog):
        import logging

        monkeypatch.setattr(plugin, "_import_encoders", lambda: None)
        with caplog.at_level(logging.DEBUG, logger="airflow_timetables_calendar.plugin"):
            plugin._register_serializer()
        assert any("encoders" in str(r.message) for r in caplog.records)


class TestFallbackWhenTheHookIsReshaped:
    """If Airflow moves the private hook, fail loudly instead of silently.

    `SERIALIZER_REGISTERED` is the difference between "DAGs serialize" and "the
    DAG page returns 500", so a reshaped private API has to be reported rather
    than swallowed.
    """

    def test_a_missing_serializer_class_reports_failure(self, monkeypatch, caplog):
        import logging
        import types

        monkeypatch.setattr(plugin, "_import_encoders", lambda: types.SimpleNamespace())

        with caplog.at_level(logging.ERROR, logger="airflow_timetables_calendar.plugin"):
            assert plugin._register_serializer() is False
        assert any("_Serializer" in str(r.message) for r in caplog.records)

    def test_a_non_dispatch_hook_reports_failure(self, monkeypatch, caplog):
        import logging
        import types

        # A plain method where a singledispatchmethod is expected: the shape of
        # the private API changed, which is the case the guard exists for.
        fake = type("_Serializer", (), {"serialize_timetable": lambda self: {}})
        monkeypatch.setattr(
            plugin,
            "_import_encoders",
            lambda: types.SimpleNamespace(_Serializer=fake),
        )

        with caplog.at_level(logging.ERROR, logger="airflow_timetables_calendar.plugin"):
            assert plugin._register_serializer() is False
        assert any("singledispatchmethod" in str(r.message) for r in caplog.records)


class TestPluginMetadata:
    def test_the_plugin_exposes_the_timetable(self):
        assert plugin.CalendarTimetablePlugin.timetables == (plugin.CalendarTimetable,)

    def test_the_plugin_module_reimports_cleanly(self):
        # Airflow imports plugin modules more than once in some components; a
        # second import must not raise or double-register.
        reloaded = importlib.reload(plugin)
        assert reloaded.SERIALIZER_REGISTERED is True
