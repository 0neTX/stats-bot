# Stats Bot

Bot de Telegram que registra la actividad de un grupo y envía un reporte diario con el **Top 5 de usuarios más activos**.

## Funcionalidades

- Escucha y contabiliza todos los mensajes de texto del grupo en tiempo real.
- Envía automáticamente un resumen diario al grupo a las **10:00 UTC**.
- Soporte para inicializar la base de datos con el historial completo del grupo (mediante Telethon).

## Requisitos previos

- Cuenta de Telegram y acceso al grupo donde se desplegará el bot.
- **Bot Token** — obtenido desde [@BotFather](https://t.me/BotFather).
- **API ID y API Hash** — obtenidos desde [my.telegram.org/apps](https://my.telegram.org/apps) (solo necesarios para la inicialización del historial).
- El bot debe ser **administrador** del grupo para poder leer mensajes.

## Configuración

Copia el archivo de ejemplo y completa los valores:

```bash
cp .env.example .env
```

| Variable   | Descripción                                              | Ejemplo                              |
|------------|----------------------------------------------------------|--------------------------------------|
| `BOT_TOKEN` | Token del bot de [@BotFather](https://t.me/BotFather)  | `123456789:AAFxxx...`                |
| `GRUPO_ID`  | ID numérico del grupo (negativo en supergrupos)         | `-1001234567890`                     |
| `API_ID`    | API ID de [my.telegram.org](https://my.telegram.org/apps) | `12345678`                        |
| `API_HASH`  | API Hash de [my.telegram.org](https://my.telegram.org/apps) | `abcdef1234...`                  |

> `API_ID` y `API_HASH` solo son necesarios para ejecutar `init_historial.py`.

## Ejecución con Docker (recomendado)

### 1. Construir la imagen

```bash
docker compose build
```

### 2. (Opcional) Inicializar con el historial existente del grupo

Este paso importa todos los mensajes anteriores del grupo a la base de datos. Solo se ejecuta **una vez** y requiere autenticación interactiva con tu número de teléfono.

```bash
docker compose run --rm init
```

Sigue las instrucciones en pantalla: ingresa tu número de teléfono y el código de verificación que recibirás en Telegram.

### 3. Levantar el bot

```bash
docker compose up -d bot
```

### 4. Ver logs

```bash
docker compose logs -f bot
```

### 5. Detener el bot

```bash
docker compose down
```

---

## Ejecución local (sin Docker)

### Instalar dependencias

Requiere Python 3.11 o superior.

```bash
pip install -r requirements.txt
```

### (Opcional) Inicializar historial

```bash
python init_historial.py
```

### Iniciar el bot

```bash
python bot_estadisticas.py
```

---

## Estructura del proyecto

```
stats-bot/
├── bot_estadisticas.py   # Bot principal (python-telegram-bot)
├── init_historial.py     # Script de inicialización del historial (Telethon)
├── requirements.txt      # Dependencias de Python
├── .env.example          # Plantilla de variables de entorno
├── Dockerfile
└── docker-compose.yml
```

## Base de datos

Se crea automáticamente el archivo `estadisticas_grupo.db` (SQLite) al iniciar el bot. Con Docker, este archivo se persiste en el volumen `stats_data`.

| Columna          | Descripción                              |
|------------------|------------------------------------------|
| `user_id`        | ID único de Telegram del usuario         |
| `nombre`         | Nombre completo                          |
| `username`       | Alias (@username), puede ser nulo        |
| `total_mensajes` | Cantidad acumulada de mensajes           |
| `ultimo_mensaje` | Fecha y hora del último mensaje enviado  |
