## Why

Admin users currently lack a quick way to retrieve detailed statistics and metadata for a specific user (by ID or name) directly within the Telegram interface. Currently, they have to check logs, query the database manually, or wait for automated reports. The `/infouser` command will provide immediate transparency and ease of management.

## What Changes

- **New Admin Command**: Addition of `/infouser <userid or name>` command, restricted to `ADMIN_ID`.
- **User Lookup Logic**: Search by `user_id`, `username` (exact or fuzzy), or `nombre` (display name).
- **Detailed Report**: The command will return:
  - User ID and names (Display name/Username).
  - Join date (`fecha_registro`).
  - Last activity date (`ultimo_mensaje`).
  - Total message count (`total_mensajes`).
  - Current status (e.g., Active vs. Inactive based on configured thresholds).

## Capabilities

### New Capabilities
- `user-info-query`: Ability to query and display all stored database information for a specific user via a Telegram command.

### Modified Capabilities
<!-- No existing specs found. -->

## Impact

- **Bot Logic**: New handler in `bot_estadisticas.py`.
- **Database**: Read-only queries against the `usuarios` table.
- **Admin Experience**: Improved diagnostic capabilities for administrators.
