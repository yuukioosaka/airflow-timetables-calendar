"""Airflow plugin registering :class:`~.timetable.CalendarTimetable`.

Custom timetables must be reachable when a DAG is deserialized, which happens in
the scheduler, DAG processor, triggerer and worker. Registering the class through
an :class:`~airflow.plugins_manager.AirflowPlugin` is what makes those components
able to resolve it, and the ``airflow.plugins`` entry point in ``pyproject.toml``
means installing the package is enough to register it -- nothing has to be copied
into a ``plugins/`` directory.

``CalendarTimetable`` derives from the *core* ``CronTriggerTimetable`` rather than
the SDK's ``BaseTimetable`` (see its docstring for why), and the core serializer
only dispatches on types it knows about. So this module also teaches the
serializer how to encode it.
"""

from __future__ import annotations

import logging

from airflow.plugins_manager import AirflowPlugin

from .timetable import CalendarTimetable

log = logging.getLogger(__name__)


def _register_serializer() -> bool:
    """Teach the core DAG serializer how to encode ``CalendarTimetable``.

    ``_Serializer.serialize_timetable`` is a ``functools.singledispatchmethod``, so
    the object to register against is its ``.dispatcher``. Calling ``.register`` on
    the descriptor itself appears to work but only sets a useless attribute on the
    descriptor and never takes effect -- a silent failure that surfaces much later
    as an unreadable DAG.

    The whole thing reaches into a private attribute, so it is guarded: on a future
    Airflow where this shape changes, we want a loud log line rather than an
    ``AttributeError`` during plugin import that would take the scheduler down.

    :return: True if the serializer was registered.
    """
    try:
        from airflow.serialization.encoders import _Serializer

        descriptor = _Serializer.__dict__["serialize_timetable"]
        dispatcher = descriptor.dispatcher
    except (ImportError, AttributeError, KeyError) as exc:
        log.error(
            "Could not register CalendarTimetable with the DAG serializer (%s: %s). "
            "DAGs using this timetable will fail to serialize. This usually means "
            "the installed Airflow version is unsupported; please report it.",
            type(exc).__name__,
            exc,
        )
        return False

    @dispatcher.register(CalendarTimetable)
    def _serialize_calendar_timetable(serializer, timetable: CalendarTimetable) -> dict:
        return timetable.serialize()

    return True


SERIALIZER_REGISTERED = _register_serializer()


class CalendarTimetablePlugin(AirflowPlugin):
    """Exposes :class:`CalendarTimetable` to every Airflow component."""

    name = "calendar_timetable_plugin"

    # A tuple, not a list: this is a plugin manifest, and a mutable class
    # attribute here is one accidental append away from leaking across plugins.
    timetables = (CalendarTimetable,)


__all__ = ["CalendarTimetablePlugin"]
