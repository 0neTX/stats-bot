## Why

Currently, periodic reports are hardcoded to 10:00 AM UTC. To better align with the administrator's local schedule and the group's activity cycle, all automatic periodic reports should be sent at 07:00 AM Europe/Madrid time. This ensures information is ready when the day starts locally.

## What Changes

- **Update Report Schedule**: Change the trigger time for all automated periodic reports from 10:00 UTC to 07:00 Europe/Madrid.
- **Timezone Awareness**: Implement proper handling of the `Europe/Madrid` timezone to account for Daylight Saving Time (DST) changes, ensuring the report always arrives at 07:00 local time.

## Capabilities

### New Capabilities
- `timezone-aware-scheduling`: Ability to schedule tasks based on a specific local timezone instead of fixed UTC.

### Modified Capabilities
<!-- None -->

## Impact

- **Bot Configuration**: Modification of `HORA_REPORTE` and scheduling logic in `bot_estadisticas.py`.
- **Dependencies**: May require `pytz` or use Python's built-in `zoneinfo` (if Python 3.9+) to handle the `Europe/Madrid` timezone correctly.
