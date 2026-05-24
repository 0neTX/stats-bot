## 1. Setup & Dependencies

- [x] 1.1 Add `tzdata` to `requirements.txt` to ensure timezone database is available on all platforms (including Windows).

## 2. Core Implementation

- [x] 2.1 Import `ZoneInfo` from `zoneinfo` in `bot_estadisticas.py`.
- [x] 2.2 Define `MADRID_TZ = ZoneInfo("Europe/Madrid")`.
- [x] 2.3 Update `HORA_REPORTE` constant to `time(hour=7, minute=0, second=0, tzinfo=MADRID_TZ)`.

## 3. Verification

- [x] 3.1 Verify that `HORA_REPORTE` uses the correct hour (7) and timezone (Europe/Madrid).
- [x] 3.2 (Simulated) Confirm the scheduler correctly interprets the timezone (no code change, just logic check).
