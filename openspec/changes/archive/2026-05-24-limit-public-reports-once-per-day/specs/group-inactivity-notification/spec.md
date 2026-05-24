## MODIFIED Requirements

### Requirement: Friendly Group Inactivity Message
The system SHALL send a message to the managed group (`GRUPO_ID`) identifying new users who have joined but haven't participated yet. This message MUST be friendly and welcoming but also state the group's purpose and the consequences of continued inactivity. To avoid spamming the group, this message SHALL be sent at most once per calendar day (UTC), even across bot restarts or multiple trigger events.

#### Scenario: Sending the group notification
- **WHEN** the bot identifies users with `total_mensajes = 0` who have exceeded the initial inactivity threshold
- **AND** no such notification has been sent to the group yet today (UTC)
- **THEN** the system SHALL send a message to the group containing the name and ID of these users, the number of days since they joined, and a standardized friendly warning text.

#### Scenario: Skipping duplicate group notification
- **WHEN** the bot identifies users for notification
- **AND** a group notification was already sent earlier today (UTC)
- **THEN** the system SHALL skip sending the message to the group.
