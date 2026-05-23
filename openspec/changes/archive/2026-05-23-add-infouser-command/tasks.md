## 1. Database & Helpers

- [x] 1.1 Create `get_user_info(search_term)` function to query the `usuarios` table. (Implemented as `buscar_usuarios`)
- [x] 1.2 Implement lookup logic: handle ID (numeric), Username (@prefix), and Name (fuzzy string).
- [x] 1.3 Implement inactivity status calculation helper based on `ultimo_mensaje`/`fecha_registro`.

## 2. Command Handler Implementation

- [x] 2.1 Define `handler_infouser` in `bot_estadisticas.py`.
- [x] 2.2 Add admin-only check (`ADMIN_ID`).
- [x] 2.3 Implement input parsing and validation (ensure search term is provided).
- [x] 2.4 Implement response formatting using HTML and `_escape_html`.
- [x] 2.5 Handle multiple matches (list summary of matching users).
- [x] 2.6 Handle user not found case.

## 3. Integration & Registration

- [x] 3.1 Register `CommandHandler("infouser", handler_infouser)` in `main()`.
- [x] 3.2 Update `/help` handler to include the new command (for admins).

## 4. Verification

- [x] 4.1 Verify lookup by User ID.
- [x] 4.2 Verify lookup by @username.
- [x] 4.3 Verify lookup by Name (exact and fuzzy).
- [x] 4.4 Verify unauthorized access rejection.
- [x] 4.5 Verify correct inactivity status display.
