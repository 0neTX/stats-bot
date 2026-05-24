## 1. Security & Unauthorized Access

- [x] 1.1 Implement `handler_unauthorized` to capture private messages from non-admin users.
- [x] 1.2 Construct the admin notification message with user details and original message content.
- [x] 1.3 Register the unauthorized handler with low priority in the Telegram application.

## 2. Query & Reporting Optimization

- [x] 2.1 Remove hardcoded `LIMIT 10` from `obtener_usuarios_para_expulsar` and `buscar_usuarios` (or increase to a much safer high limit).
- [x] 2.2 Refactor `_send_long_message` to better handle HTML tag integrity during splitting.

## 3. Rate Limiting & Stability

- [x] 3.1 Implement `asyncio.sleep(0.5)` between individual user expulsions in all maintenance handlers.
- [x] 3.2 Add explicit handling for `RetryAfter` exceptions in the expulsion loop to resume after the requested wait time.

## 4. Validation

- [x] 4.1 Simulate an unauthorized interaction and confirm the admin receives the notification.
- [x] 4.2 Trigger a large report (e.g., via `/noparticipa` with many users) to verify HTML-aware pagination.
