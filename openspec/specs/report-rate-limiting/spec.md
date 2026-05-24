# report-rate-limiting Specification

## Purpose
TBD - created by archiving change limit-public-reports-once-per-day. Update Purpose after archive.
## Requirements
### Requirement: Global Report Rate Limiting
The system SHALL provide a mechanism to track the last time an automated public report was sent to the group.

#### Scenario: Tracking report execution
- **WHEN** an automated report is successfully sent to the group
- **THEN** the system SHALL update its internal state with the current date (UTC) for that specific report type.

### Requirement: Once-Per-Day Execution Check
Before sending an automated public report, the system SHALL check if that report has already been sent on the current calendar day (UTC).

#### Scenario: Blocking duplicate report on same day
- **WHEN** a report is triggered (either by restart or schedule)
- **AND** the internal state shows that the same report type was already sent today (UTC)
- **THEN** the system SHALL skip sending the report.

#### Scenario: Allowing report on new day
- **WHEN** a report is triggered
- **AND** the internal state shows that the report was last sent on a previous day (UTC) or never sent
- **THEN** the system SHALL proceed with sending the report.

### Requirement: State Persistence across Restarts
The report execution state SHALL be persisted in non-volatile storage to ensure rate limits are respected even after a bot restart.

#### Scenario: Recovery after crash
- **WHEN** the bot restarts
- **THEN** it SHALL load the last sent dates from `bot_state.json` before evaluating if reports should be sent.

