## ADDED Requirements

### Requirement: Unauthorized Access Alert
The system SHALL detect any message sent in private to the bot by a user whose ID does not match the `ADMIN_ID` environment variable.

#### Scenario: Unauthorized message received
- **WHEN** a user with `id != ADMIN_ID` sends a message to the bot in private
- **THEN** the system SHALL NOT respond to that user and SHALL instead forward a notification to the `ADMIN_ID`.

### Requirement: Unauthorized Interaction Details
The notification sent to the `ADMIN_ID` MUST include:
- The unauthorized user's full name and username (if available).
- The user's Telegram ID.
- The content of the message they sent.

#### Scenario: Content of the admin notification
- **WHEN** the bot forwards an unauthorized message to the admin
- **THEN** it SHALL use HTML formatting to present the user's data and the message text clearly.
