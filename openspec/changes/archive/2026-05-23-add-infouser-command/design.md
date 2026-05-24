## Context

The bot currently lacks a direct query command for individual user statistics. Administrators must rely on general reports or direct database access. This design introduces a new command, `/infouser`, that leverages existing database connections and inactivity logic to provide a comprehensive view of a single user's status.

## Goals / Non-Goals

**Goals:**
- Provide a detailed summary of a user's data (ID, name, username, messages, dates, status).
- Support lookup by multiple criteria: ID, @username, or display name.
- Restrict access to `ADMIN_ID`.
- Format the output in a clean, readable HTML message.

**Non-Goals:**
- Allowing non-admins to query their own stats (this could be a future separate feature).
- Bulk queries (handled by other reports like `/noparticipa`).
- Modifying user data via this command.

## Decisions

- **Lookup Strategy**: 
  - If the input starts with `@`, search by `username`.
  - If the input is numeric, search by `user_id`.
  - Otherwise, search by `nombre` (ILIKE/fuzzy).
  - *Rationale*: Intuitive for users. Telegram handles usernames with @, IDs are stable, and names are useful when others are unknown.
- **Handling Multiple Matches**: 
  - If a search by name returns multiple users, list the top matches (up to 5) with their IDs so the admin can refine the search.
  - *Rationale*: Avoids overwhelming the admin while providing the necessary info to disambiguate.
- **Inactivity Logic**: 
  - Reuse the existing calculation logic (comparing `ultimo_mensaje` or `fecha_registro` against `MAX_DAYS_INACTIVE_WARNING/REMOVAL`).
  - *Rationale*: Consistency with existing bot behavior.
- **Access Control**:
  - Use the established pattern: `if update.effective_user.id != ADMIN_ID: return`.
  - *Rationale*: Standard security practice in this project.

## Risks / Trade-offs

- **[Risk] SQL Injection** → **Mitigation**: Use parameterized queries for all user inputs.
- **[Risk] HTML Parsing Errors** → **Mitigation**: Use the existing `_escape_html` helper for all user-supplied data (names, usernames).
- **[Risk] Privacy** → **Mitigation**: The command is restricted to the administrator.
