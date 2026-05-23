## Why

Currently, notifications about new inactive users are only sent to the administrator. Sending a public (but friendly) notification to the group serves as both a welcome message and a gentle reminder of the group's participation expectations, encouraging engagement from the start.

## What Changes

- **Public Inactivity Notification**: Implement a new automated message sent to the main group when new users are flagged as inactive (zero messages since joining).
- **Reduced Information Disclosure**: Unlike the admin report, the group report will only contain: Name, User ID, and days since registration.
- **Tone & Content**: The message will be friendly, emphasizing that everyone is welcome but that the group is for active exchange of content and ideas. It will warn that lack of participation may lead to expulsion within the established limits.

## Capabilities

### New Capabilities
- `group-inactivity-notification`: Capability to generate and send a formatted, friendly inactivity warning directly to the group chat.

### Modified Capabilities
<!-- None -->

## Impact

- **Bot Logic**: Modification of the `post_init` or scheduled task sequence to include sending the group-facing report.
- **Message Templates**: New HTML template for the friendly group message.
- **Privacy**: Ensures sensitive data (like full user histories or admin-only details) is not shared publicly.
