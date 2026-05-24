## 1. Logic Implementation

- [x] 1.1 Implement `enviar_aviso_grupo_inactivos(bot, usuarios)` function in `bot_estadisticas.py`.
- [x] 1.2 Implement the HTML message builder for the group inactivity report with the friendly tone and reduced info (Name, ID, days).
- [x] 1.3 Ensure proper HTML escaping for user names in the group message.

## 2. Integration & Triggers

- [x] 2.1 Integrate `enviar_aviso_grupo_inactivos` into `check_nuevos_proximos_a_vencer` so it triggers when the admin is also notified of upcoming expirations.
- [x] 2.2 Verify that the message is sent to `GRUPO_ID` and not `ADMIN_ID`.
- [x] 2.3 Ensure the group message is only sent if there are actually users to report.

## 3. Verification & Testing

- [x] 3.1 Verify the group message content: Check for greeting, purpose statement, user list, and warning.
- [x] 3.2 Verify privacy: Ensure no usernames or other sensitive info are included in the group report.
- [x] 3.3 Verify timing: Check that the report is sent during `post_init` and the daily summary (as part of `check_nuevos_proximos_a_vencer`).
- [x] 3.4 Verify tone: Ensure the message is "agradable y amigable" as requested.
