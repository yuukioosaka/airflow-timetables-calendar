"""Airflow plugin registering :class:`~.timetable.CalendarTimetable`.

Serialization needs two separate things, and getting only one of them is the
trap this module exists to avoid:

1. **Registration in the plugin registry.** ``AirflowPlugin.timetables`` below
   puts the class in ``plugins_manager.get_timetables_plugins()``. The encoder
   looks the class up there by import path and raises
   ``TimetableNotRegistered`` if it is absent -- on *every* supported Airflow,
   so this is the part that actually matters.

2. **A serializer for the payload.** From Airflow 3.2 there is also a
   singledispatch hook (``_Serializer.serialize_timetable``) that must be taught
   about the class; see :func:`_register_serializer`.

The ``airflow.plugins`` entry point in ``pyproject.toml`` wires both up on
``pip install``, so nothing has to be copied into a ``plugins/`` directory.

``CalendarTimetable`` derives from the *core* ``CronTriggerTimetable`` rather than
the SDK's ``BaseTimetable`` (see its docstring for why).
"""

from __future__ import annotations

import importlib
import logging

from airflow.plugins_manager import AirflowPlugin

from .timetable import CalendarTimetable

log = logging.getLogger(__name__)


def _import_encoders():
    """Return ``airflow.serialization.encoders``, or None when it is absent.

    Airflow 3.0 and 3.1 have no such module, so absence is an expected outcome
    rather than an error. Isolated in a helper because it is the single place
    this package touches a private Airflow module, which keeps the tests able to
    substitute a fake without reaching into the registration logic.
    """
    try:
        return importlib.import_module("airflow.serialization.encoders")
    except ImportError:
        return None


def _register_serializer() -> bool:
    """Teach the DAG serializer how to encode ``CalendarTimetable``.

    The timetable type itself is only reachable through the plugin registry
    (``AirflowPlugin.timetables``), which is the mechanism that exists on every
    supported Airflow. What this function adds is the *payload* encoder, which
    only became pluggable in Airflow 3.2:

        Airflow 3.0, 3.1   no ``airflow.serialization.encoders`` module. The
                           encoder calls ``timetable.serialize()`` for a
                           registered class, and that is enough, so this returns
                           True having done nothing.
        Airflow 3.2+       ``_Serializer`` holds ``serialize_timetable`` as a
                           ``functools.singledispatchmethod``, so a class the
                           serializer has never seen needs a registered variant.

    The hook is located by scanning the class ``__dict__`` rather than by
    importing a private name, because the private name is precisely what moved
    between releases. Note that ``getattr`` cannot be used for this: reading a
    ``singledispatchmethod`` through the class gives the underlying plain
    function, which has no ``.dispatcher``. The descriptor has to come out of
    ``__dict__``.

    :return: True if serialization is supported on this Airflow.
    """
    encoders = _import_encoders()
    if encoders is None:
        # Airflow 3.0/3.1: no dispatch hook to register with. `serialize()` is
        # called directly for a plugin-registered class, which is all we need.
        log.debug(
            "airflow.serialization.encoders is absent (Airflow < 3.2); "
            "relying on plugin registration and CalendarTimetable.serialize()"
        )
        return True

    serializer_cls = getattr(encoders, "_Serializer", None)
    if serializer_cls is None:
        log.error(
            "airflow.serialization.encoders has no _Serializer; the DAG "
            "serializer has been reshaped. DAGs using this timetable will fail "
            "to serialize. This usually means the installed Airflow version is "
            "unsupported; please report it."
        )
        return False

    descriptor = serializer_cls.__dict__.get("serialize_timetable")
    dispatcher = getattr(descriptor, "dispatcher", None)
    if dispatcher is None:
        log.error(
            "_Serializer.serialize_timetable is no longer a singledispatchmethod "
            "(found %r). DAGs using this timetable will fail to serialize. "
            "This usually means the installed Airflow version is unsupported; "
            "please report it.",
            type(descriptor).__name__,
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
