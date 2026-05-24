## Why

This change aims to improve the bot's robustness and security. Currently, some limits are hardcoded and unauthorized access is silently ignored. Implementing rate limiting and proper pagination will ensure stability under heavy load or large group sizes.

## What Changes

- **Expulsion Limit Removal**: Remove the hardcoded 10-user limit for expulsion reports, allowing administrators to manage all inactive users at once.
- **Unauthorized Access Feedback**: Implement a notification system that alerts the `ADMIN_ID` whenever an unauthorized user attempts to interact with the bot in private.
- **Rate Limiting & Stability**: Add explicit `asyncio.sleep` between bulk operations (like mass kicks) and handle `RetryAfter` exceptions to comply with Telegram API best practices.
- **Improved Pagination**: Enhance the `_send_long_message` utility to ensure HTML tags are not broken during message splitting and to use dynamic length calculations.

## Capabilities

### New Capabilities
- `unauthorized-access-notification`: Notifies the administrator of private interactions from users other than `ADMIN_ID`.
- `api-rate-limiting`: Manages Telegram API limits through backoff and interval-based execution.

### Modified Capabilities
- `group-inactivity-notification`: Requirement for capped reports (10 users) is removed to support full group maintenance.

## Impact

- `bot_estadisticas.py`: Significant refactoring of admin handlers and utility functions.
- `init_historial.py`: No changes expected.
- `Dockerfile` / `docker-compose.yml`: No changes expected.
