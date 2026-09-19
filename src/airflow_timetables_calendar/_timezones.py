"""Timezone helpers, isolated behind one import.

Airflow moved these functions between releases: they live in
``airflow.utils.timezone`` through Airflow 2.x and in
``airflow._shared.timezones.timezone`` from Airflow 3. The underscore prefix means
the new location is *private*, so it can move again without notice.

Confining the version dance to this module keeps the rest of the package free of
``try: import ... except ImportError`` chains, and gives one place to fix when
Airflow reorganises again.
"""

from __future__ import annotations

try:  # Airflow 3 (private, but the only location in 3.x)
    from airflow._shared.timezones.timezone import (  # type: ignore[import-not-found]
        convert_to_utc,
        make_aware,
        make_naive,
        parse_timezone,
    )
except ImportError:  # pragma: no cover - Airflow 2.x and earlier
    try:
        from airflow.utils.timezone import (  # type: ignore[no-redef]
            convert_to_utc,
            make_aware,
            make_naive,
            parse_timezone,
        )
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "Could not import Airflow's timezone helpers from either "
            "airflow._shared.timezones.timezone (Airflow 3) or "
            "airflow.utils.timezone (Airflow 2). This package needs "
            "apache-airflow>=3.0; the installed version is unsupported."
        ) from exc


__all__ = ["convert_to_utc", "make_aware", "make_naive", "parse_timezone"]
