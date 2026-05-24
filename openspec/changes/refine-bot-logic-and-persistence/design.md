## Context

The Stats Bot currently has several administrative features with hardcoded limits (10 users) and lacks explicit rate limiting, making them less robust for larger groups. Additionally, unauthorized access to private admin commands is currently ignored without feedback.

## Goals / Non-Goals

**Goals:**
- Implement proactive notifications for unauthorized private interactions.
- Ensure compliance with Telegram API rate limits during mass maintenance.
- Provide a robust HTML message splitting utility to handle large reports.
- Remove hardcoded expulsion limits for full group maintenance.

**Non-Goals:**
- Changing the database schema or file location.
- Implementing a multi-admin system.
- Refactoring the Telethon recovery logic.

## Decisions

### Decision 1: Unauthorized Interaction Forwarding
**Option:** Add a low-priority `MessageHandler` that catches all private messages not originating from `ADMIN_ID`.
**Rationale:** Ensures the administrator is aware of any attempts to interact with the bot. This is critical for security and user support (e.g., if a user is trying to ask for a manual review).
**Alternative:** Silently logging to a file. Rejected as the user specifically requested active notification.

### Decision 2: Mass Operation Throttling
**Option:** Implement a `_batch_execute` helper or explicit `asyncio.sleep(0.5)` in loops for mass kicks/updates.
**Rationale:** Prevents the bot from being temporarily banned by Telegram for "flooding". 
**Alternative:** Using a global rate-limiter library. Rejected to keep dependencies minimal.

### Decision 3: HTML-Aware Message Splitter
**Option:** Update `_send_long_message` to use a basic stack-based approach for tracking open HTML tags (<b>, <i>, <code>).
**Rationale:** Prevents broken rendering in Telegram when a long list of users is split across two messages.
**Alternative:** Stripping HTML before splitting. Rejected as formatting is important for readability.

## Risks / Trade-offs

- **[Risk] Telegram FloodWait** → Even with 0.5s delays, very large groups might still trigger limits.
  - **Mitigation**: Implement a try-except block specifically for `RetryAfter` to pause execution dynamically.
