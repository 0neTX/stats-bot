## Why

Automatic public reports sent to the group currently trigger on every bot restart and during daily scheduled tasks. This leads to duplicate messages being sent to the group if the bot restarts multiple times in a day, which can be perceived as spam by group members.

## What Changes

- **Once-per-day Limit**: All automatic public reports sent to the group (`GRUPO_ID`) will be restricted to a maximum of one occurrence per calendar day (UTC).
- **Persistence**: The last sent date for each type of public report will be persisted in `bot_state.json` to ensure the limit survives bot restarts.
- **Reporting Logic Update**: Reporting functions will verify the last sent date before executing the send operation.

## Capabilities

### New Capabilities
- `report-rate-limiting`: Generic mechanism to track and enforce frequency limits for automated reports, persisting state in `bot_state.json`.

### Modified Capabilities
- `group-inactivity-notification`: Update requirements to explicitly state that the notification must only be sent once per day, even across restarts.

## Impact

- `bot_estadisticas.py`: Modification of `enviar_aviso_grupo_inactivos` and `bot_state` management functions (`leer_bot_state`, `guardar_bot_state`).
- `bot_state.json`: Schema update to include last sent timestamps for reports.
