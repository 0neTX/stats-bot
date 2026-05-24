# help-command Specification

## Purpose

Provide the admin with a single, always-accurate reference of every available
bot command, accessible via `/help` in a private chat with the bot.

## Requirements

### Requirement: Admin-only `/help` command

The system SHALL register a `CommandHandler` for `/help` restricted to the
`ADMIN_ID` user in a private chat (`filters.ChatType.PRIVATE &
filters.User(ADMIN_ID)`).

#### Scenario: Admin sends /help in private chat
- **WHEN** the admin sends `/help` in a private chat with the bot
- **THEN** the bot SHALL reply with a formatted HTML message listing all
  available commands grouped by category.

#### Scenario: Non-admin sends /help
- **WHEN** a non-admin user sends `/help` in a private chat
- **THEN** the command SHALL be ignored by this handler (existing
  `handler_unauthorized` will notify the admin as a side-effect via the
  catch-all private `MessageHandler`).

#### Scenario: /help sent in a group
- **WHEN** `/help` is sent in a group or supergroup
- **THEN** the command SHALL be silently ignored (filter does not match).

### Requirement: Command list completeness and accuracy

The `/help` reply SHALL include every command registered in the bot, grouped
as follows:

| Group | Commands |
|---|---|
| Estadísticas | `/report`, `/report TOP`, `/report DOWN`, `/infouser` |
| Inactividad general | `/noparticipa`, `/expulsarnoparticipa`, `/ok`, `/moratoria` |
| Nuevos usuarios | `/nuevos`, `/expulsarnuevos` |
| Herramientas | `/kick DOWN`, `/recalcularfechas [YYYY-MM-DD]` |
| Referencia | `/help` |

Dynamic values (`MAX_DAYS_INACTIVE_WARNING`, `NEW_USER_GRACE_PERIOD_DAYS`)
SHALL be interpolated at send time so the message reflects the current
environment configuration.

#### Scenario: Dynamic threshold values
- **WHEN** `MAX_DAYS_INACTIVE_WARNING=45` and `NEW_USER_GRACE_PERIOD_DAYS=14`
- **THEN** the `/help` output SHALL mention "45 días" and "14 días"
  respectively instead of hardcoded defaults.

### Requirement: HTML formatting

The reply SHALL use HTML parse mode (`reply_html` / `parse_mode="HTML"`).
All user-visible static strings are trusted; no `_escape_html()` is needed
for the help text itself.

### Requirement: Telegram command menu registration

During `post_init`, the bot SHALL call `set_my_commands` with a
`BotCommandScopeChat(chat_id=ADMIN_ID)` scope so that the Telegram client
displays the command list in the admin's command menu (the `/` shortcut bar).
Commands SHALL be registered with short English descriptions compatible with
BotFather limits (≤ 256 chars per description, ≤ 32 chars per command name).

#### Scenario: Command menu visible in Telegram client
- **WHEN** the admin opens the command input in a private chat with the bot
- **THEN** all registered commands SHALL appear in the Telegram command
  suggestion menu.

### Requirement: Defensive update handling

The handler SHALL guard against a None `update.message` (edge case: forwarded
channel posts) before calling `reply_html`. If `update.message` is None the
handler SHALL log a warning and return silently.
