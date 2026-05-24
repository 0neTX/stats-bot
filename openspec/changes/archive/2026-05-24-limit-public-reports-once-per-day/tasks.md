## 1. State Management

- [x] 1.1 Update `guardar_bot_state` and `leer_bot_state` to support a `last_sent_reports` dictionary.
- [x] 1.2 Implement `should_send_public_report(report_id: str) -> bool` helper to check UTC date in `bot_state.json`.
- [x] 1.3 Implement `mark_public_report_sent(report_id: str)` helper to update `bot_state.json` with current UTC date.

## 2. Reporting Logic

- [x] 2.1 Integrate the check and marking logic into `enviar_aviso_grupo_inactivos`.
- [x] 2.2 Verify that `post_init` and `enviar_resumen_diario` correctly handle the skipped reports.

## 3. Verification

- [x] 3.1 Test duplicate prevention by restarting the bot twice within the same day.
- [x] 3.2 Test successful reporting by manually modifying `bot_state.json` to an old date.
- [x] 3.3 Confirm admin-only reports (stats, inactivos warning for admin) still trigger on every restart as expected.
