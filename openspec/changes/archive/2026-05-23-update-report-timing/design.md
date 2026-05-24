## Context

The bot's periodic reports (daily summary, inactivity warnings, etc.) are currently triggered at 10:00 AM UTC. Administrators have requested this to be moved to 07:00 AM local time in Madrid (`Europe/Madrid`). Since Madrid observes Daylight Saving Time (DST), a simple UTC offset change is insufficient; the bot must be aware of the specific timezone to maintain the 07:00 target year-round.

## Goals / Non-Goals

**Goals:**
- Shift automated periodic reports to 07:00 Europe/Madrid.
- Handle DST transitions automatically.
- Ensure deployment on UTC servers doesn't break the local time schedule.

**Non-Goals:**
- Changing the content or frequency of the reports.
- Modifying admin-requested (manual) reports.

## Decisions

- **Timezone Library**: Use Python's built-in `zoneinfo` (available in Python 3.12, as used in the project's Dockerfile).
- **Configuration Variable**: Update `HORA_REPORTE` to use `ZoneInfo("Europe/Madrid")`.
- **Implementation**:
  - `from zoneinfo import ZoneInfo`
  - `MADRID_TZ = ZoneInfo("Europe/Madrid")`
  - `HORA_REPORTE = time(hour=7, minute=0, second=0, tzinfo=MADRID_TZ)`
- **Rationale**: `zoneinfo` is the modern standard for timezone handling in Python and avoids external dependencies like `pytz`. It correctly handles the complex DST rules for `Europe/Madrid`.

## Risks / Trade-offs

- **[Risk] Missing TZ data** → **Mitigation**: The `python:3.12-slim-bookworm` image usually includes `tzdata` (the system package), but if not, we may need to install the `tzdata` Python package or `apt-get install tzdata`.
- **[Risk] DST transition edge cases** → **Mitigation**: The `python-telegram-bot` job queue handles `datetime.time` with `tzinfo` correctly by calculating the next run time based on the provided timezone.
