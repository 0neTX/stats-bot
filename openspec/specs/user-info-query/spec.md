# user-info-query Specification

## Purpose
TBD - created by archiving change add-infouser-command. Update Purpose after archive.
## Requirements
### Requirement: Admin command /infouser
The system SHALL provide an administrative command `/infouser <search_term>` accessible only to the configured `ADMIN_ID`. The `<search_term>` SHALL be used to lookup a user by their numerical ID, their Telegram `@username`, or their display name (`nombre`).

#### Scenario: Successful lookup by User ID
- **WHEN** the admin sends `/infouser 123456`
- **THEN** the system SHALL return a message with the user's name, username, join date, last activity, total messages, and current status.

#### Scenario: Successful lookup by Username
- **WHEN** the admin sends `/infouser @exampleuser`
- **THEN** the system SHALL return the information for the user with the matching username.

#### Scenario: Successful lookup by Display Name (Nombre)
- **WHEN** the admin sends `/infouser John Doe`
- **THEN** the system SHALL return information for the user(s) matching the display name (exact or partial).

#### Scenario: User not found
- **WHEN** the admin sends `/infouser non_existent_user` and no match is found in the database
- **THEN** the system SHALL return a "User not found" message.

#### Scenario: Unauthorized access
- **WHEN** a non-admin user sends `/infouser 123456`
- **THEN** the system SHALL ignore the command or return an unauthorized access message (following existing bot patterns).

### Requirement: User Information Formatting
The `/infouser` response SHALL be formatted in HTML and include:
- `ID`: User ID
- `Name`: Display name (`nombre`)
- `Username`: @username (if available)
- `Registered`: `fecha_registro`
- `Last Message`: `ultimo_mensaje`
- `Total Messages`: `total_mensajes`
- `Status`: Evaluated status (e.g., "Active", "Warning candidate", or "Removal candidate" based on inactivity thresholds).

#### Scenario: Response formatting
- **WHEN** a user is found
- **THEN** the response SHALL be an HTML-formatted message with all the above fields properly escaped to prevent parsing errors.

