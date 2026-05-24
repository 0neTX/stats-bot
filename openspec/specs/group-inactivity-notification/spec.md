# group-inactivity-notification Specification

## Purpose
TBD - created by archiving change group-inactivity-report. Update Purpose after archive.
## Requirements
### Requirement: Friendly Group Inactivity Message
The system SHALL send a message to the managed group (`GRUPO_ID`) identifying new users who have joined but haven't participated yet. This message MUST be friendly and welcoming but also state the group's purpose and the consequences of continued inactivity.

#### Scenario: Sending the group notification
- **WHEN** the bot identifies users with `total_mensajes = 0` who have exceeded the initial inactivity threshold
- **THEN** the system SHALL send a message to the group containing the name and ID of these users, the number of days since they joined, and a standardized friendly warning text.

### Requirement: Information Reduction for Group Privacy
The message sent to the group SHALL ONLY contain the following user details:
- Display Name (`nombre`)
- User ID
- Days since registration (`fecha_registro`)

#### Scenario: Content verification
- **WHEN** the group message is generated
- **THEN** it MUST NOT include `username` (unless part of the display name), full activity history, or any other metadata present in the admin-only report.

### Requirement: Standardized Friendly Tone
The message SHALL follow a specific structure:
1. A welcoming greeting.
2. A statement that the group is for exchanging content, chats, and ideas.
3. A list of inactive new members with their ID and days since joining.
4. A gentle reminder that participation is required to stay in the group and that they will be expelled if they do not participate within the established timeframe.

#### Scenario: Tone check
- **WHEN** the message is rendered in HTML
- **THEN** it MUST use a polite and encouraging language (e.g., "¡Hola! Todos somos bienvenidos...", "este es un espacio para compartir...").

