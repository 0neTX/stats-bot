## Context

The bot sends automated reports to the group (`GRUPO_ID`) both during startup (`post_init`) and daily scheduled tasks (`enviar_resumen_diario`). If the bot restarts multiple times within the same day, it triggers duplicate messages. We need a way to persist the "last sent" state to survive restarts and prevent these duplicates.

## Goals / Non-Goals

**Goals:**
- Prevent duplicate automated public reports on the same calendar day (UTC).
- Persist reporting state across bot restarts.
- Minimal impact on existing codebase structure.

**Non-Goals:**
- Rate limiting admin-only reports (they should still trigger on restart for visibility).
- Changing the scheduled time of reports.
- Implementing complex per-user rate limits.

## Decisions

### 1. Persistence in `bot_state.json`
We will extend the existing `bot_state.json` schema to include a `last_sent_reports` object.
- **Rationale**: `bot_state.json` is already used for persistence of `ultimo_registro` and `fecha_arranque`. It's lightweight and avoids adding new tables to SQLite for metadata.
- **Schema**:
  ```json
  {
    "fecha_arranque": "...",
    "ultimo_registro": "...",
    "last_sent_reports": {
      "grupo_inactivos": "2026-05-24"
    }
  }
  ```

### 2. UTC-based Date Comparison
We will use the UTC calendar day (`YYYY-MM-DD`) for comparison.
- **Rationale**: The bot is configured to run on UTC (e.g., `HORA_REPORTE`). Using ISO dates for comparison is simple and robust against timezone shifts if the server environment changes.

### 3. Reporting Helper Functions
Two new helper functions will be introduced to encapsulate the logic:
- `should_send_public_report(report_id: str) -> bool`: Reads state and compares current UTC date with stored date.
- `mark_public_report_sent(report_id: str)`: Updates state with current UTC date and saves to file.

### 4. Integration Point
The check will be added inside `enviar_aviso_grupo_inactivos`.
- **Rationale**: This is the primary (and currently only) automated public report. Adding the check here ensures it works regardless of whether it's called from `post_init` or `enviar_resumen_diario`.

## Risks / Trade-offs

- **[Risk] Race conditions during state update** → **Mitigation**: The bot is single-threaded async. `bot_state.json` is updated synchronously after `send_message`.
- **[Risk] Manual report triggering** → **Mitigation**: Admin commands (like `/report`) will bypass this check as they are not "automated" reports.
