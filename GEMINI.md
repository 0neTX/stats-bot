# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

Telegram group activity bot. Two components:

- **`bot_estadisticas.py`** — main bot (python-telegram-bot v20+ async). Runs persistently in Docker.
- **`init_historial.py`** — one-shot init script (Telethon userbot). Scrapes group history into SQLite.

## Running locally

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in values
python bot_estadisticas.py
```

Init script (interactive — asks phone + code on first run):
```bash
python init_historial.py [--fecha DDMMYYYY]
```

## Running with Docker

```bash
# Deploy (uses pre-built image from ghcr.io)
docker compose up -d

# First-time history import
docker compose run --rm init python /app/src/init_historial.py --fecha 01032026

# Local build
docker compose -f docker-compose.build.yml build
```

Init needs interactive TTY; pass `--fecha` explicitly as shown above (not as Docker `command:` override — the `--` separator doesn't work with this image's entrypoint).

Fix permissions on the host data dir if SQLite throws readonly errors:
```bash
sudo chown -R 1001:1001 ./data
```

## CI/CD

Push to `main` → GitHub Actions builds and pushes to `ghcr.io/0netx/stats-bot:latest` + `vN`. Keeps last 5 versioned tags, deletes older ones via GHCR API. No manual trigger needed.

## Architecture

### Database (`estadisticas_grupo.db`, SQLite WAL)

Single table `usuarios`:

| Column | Notes |
|---|---|
| `user_id` | PK |
| `nombre` / `username` | display name / @alias |
| `total_mensajes` | cumulative count |
| `ultimo_mensaje` | ISO datetime of last message; set to join time for new members |
| `fecha_registro` | immutable join date — never overwritten on conflict |

`fecha_registro` is used as the inactivity reference for users with `total_mensajes = 0`. `ultimo_mensaje` is used for users with messages. Both queries apply this split logic.

### Bot startup sequence (`post_init`)

1. Snapshot inactive users (before recovery).
2. Use Telethon to replay missed messages since last `bot_state.json` `ultimo_registro`.
3. Diff snapshots → send "recovered inactive users" summary to admin.
4. Send TOP5/DOWN5 stats report to admin.
5. Send inactivity warning list to admin.
6. Send expulsion candidates list to admin (awaits `/ok`).

### State persistence

`bot_state.json` (in `/app/data`) stores `fecha_arranque` and `ultimo_registro` (last processed message timestamp). Written on shutdown and startup. Replaces the old `metadata` DB table.

### Message filtering

Only these message types count as activity (update sticker exclusion is implicit — stickers have their own PTB filter not included here):

```python
filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL
| filters.AUDIO | filters.ANIMATION | filters.VOICE | filters.VIDEO_NOTE
```

Same filter applied in Telethon recovery: `bool(mensaje.text) or mensaje.photo or mensaje.video`.

### Admin commands (private chat only, `ADMIN_ID`)

| Command | Action |
|---|---|
| `/ok` | Kick users listed in last expulsion report |
| `/noparticipa` | List users with `total_mensajes=0` inactive > `MAX_DAYS_INACTIVE_WARNING` days |
| `/expulsarnoparticipa` | Kick users from last `/noparticipa` list |
| `/moratoria` | Reset `ultimo_mensaje = now()` for all inactive users |

Expulsion = `ban_chat_member` + immediate `unban_chat_member` (silent kick, allows re-entry).

### Inactivity logic

- **Warning threshold**: `MAX_DAYS_INACTIVE_WARNING` days (env var, default 30)
- **Removal threshold**: `MAX_DAYS_INACTIVE_REMOVAL` days (env var, default 60)
- Expulsion report is capped at **10 users**, ordered: zero-message users first (by `fecha_registro`), then users-with-messages (by `ultimo_mensaje`).

### HTML parse mode

All Telegram messages use `parse_mode="HTML"`. Never use `"Markdown"` or `"MarkdownV2"` — usernames with `_` break Markdown parsing. Use `_escape_html()` on all user-supplied strings before embedding in HTML.

## Environment variables

See `.env.example`. Key vars:

```
BOT_TOKEN, GRUPO_ID, ADMIN_ID
API_ID, API_HASH          # Telethon only (init script)
MAX_DAYS_INACTIVE_WARNING # default 30
MAX_DAYS_INACTIVE_REMOVAL # default 60
```

## Docker hardening

Container runs as uid 1001 (`botuser`), `cap_drop: ALL`, `no-new-privileges`, `read_only` filesystem. `/app/data` is the only writable mount. `/tmp` is a 32MB tmpfs. The host `./data/` dir must be owned by uid 1001.
