## ADDED Requirements

### Requirement: Unlimited Expulsion Reporting
The system SHALL NOT impose an arbitrary hardcoded limit on the number of users included in inactivity or maintenance reports sent to the administrator.

#### Scenario: Full group maintenance
- **WHEN** the administrator requests a list of candidates for expulsion (via `/report` or `/noparticipa`)
- **THEN** the system SHALL return all users matching the criteria, regardless of the quantity, using pagination if necessary.

### Requirement: Robust HTML Pagination
The system SHALL implement a message splitting utility that ensures HTML integrity and respects Telegram's 4096-character limit.

#### Scenario: Very long report message
- **WHEN** a report exceeds 4096 characters
- **THEN** the system SHALL split the message into multiple parts, ensuring that no HTML tags (e.g., `<b>`, `<code>`) are left open across different messages.
