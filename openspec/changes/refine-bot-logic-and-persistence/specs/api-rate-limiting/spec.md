## ADDED Requirements

### Requirement: Bulk Operation Throttling
The system SHALL implement an artificial delay between consecutive Telegram API calls during bulk operations (e.g., mass expulsions or mass message updates).

#### Scenario: Mass expulsion throttling
- **WHEN** the bot executes a list of `ban_chat_member` + `unban_chat_member` operations
- **THEN** it SHALL wait at least 0.5 seconds between each user to avoid triggering Telegram's anti-spam flood protection.

### Requirement: FloodWait Exception Handling
The system SHALL explicitly catch `RetryAfter` (or equivalent FloodWait) exceptions from the Telegram API.

#### Scenario: Triggering FloodWait
- **WHEN** the Telegram API returns a 429 error or a "RetryAfter" response
- **THEN** the system SHALL pause execution for the duration specified by the API and then resume the operation.
