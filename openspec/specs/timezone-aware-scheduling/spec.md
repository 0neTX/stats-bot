# timezone-aware-scheduling Specification

## Purpose
TBD - created by archiving change update-report-timing. Update Purpose after archive.
## Requirements
### Requirement: Local Time Scheduling
The system SHALL execute periodic reports at 07:00 AM local time in the `Europe/Madrid` timezone.

#### Scenario: DST Spring Forward
- **WHEN** the local time in Madrid shifts from 02:00 to 03:00 (Spring)
- **THEN** the report MUST still trigger at 07:00 local time (which will be 05:00 UTC).

#### Scenario: DST Fall Back
- **WHEN** the local time in Madrid shifts from 03:00 to 02:00 (Fall)
- **THEN** the report MUST still trigger at 07:00 local time (which will be 06:00 UTC).

### Requirement: Automatic Retries/Adjustment
The scheduler SHALL automatically account for timezone offsets relative to UTC to ensure consistency regardless of the host system's local time setting.

#### Scenario: Deployment on UTC server
- **WHEN** the bot is deployed on a server configured with UTC time
- **THEN** the report MUST trigger at 07:00 Europe/Madrid time.

