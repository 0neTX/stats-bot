"""
bot_estadisticas.py
===================
Bot principal de estadísticas de grupo para Telegram.

Funcionalidades:
  1. Escucha todos los mensajes nuevos en el grupo y actualiza la BD SQLite.
  2. Tarea programada diaria (10:00 AM UTC) que envía al grupo el Top 5
     de usuarios con más mensajes, formateado en Markdown.

Requisitos:
  - pip install "python-telegram-bot[job-queue]" python-dotenv
  - Base de datos inicializada con init_historial.py (o vacía: se crea sola)
  - Archivo .env con: BOT_TOKEN, GRUPO_ID

Uso:
  python bot_estadisticas.py
"""

import logging
import sqlite3
import os
from datetime import datetime, time, timezone

from dotenv import load_dotenv
from telegram import Update, ChatMemberUpdated
from telegram.ext import (
    Application,
    ChatMemberHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---------------------------------------------------------------------------
# Configuración de logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Variables de entorno
# ---------------------------------------------------------------------------

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GRUPO_ID  = int(os.getenv("GRUPO_ID"))
DB_PATH   = "estadisticas_grupo.db"

HORA_REPORTE = time(hour=10, minute=0, second=0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Base de datos
# ---------------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
    """Abre la conexión a la BD y habilita el modo WAL para concurrencia."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            user_id        INTEGER PRIMARY KEY,
            nombre         TEXT    NOT NULL DEFAULT '',
            username       TEXT    DEFAULT NULL,
            total_mensajes INTEGER NOT NULL DEFAULT 0,
            ultimo_mensaje TEXT    DEFAULT NULL
        )
    """)
    conn.commit()
    return conn


_conn: sqlite3.Connection = get_conn()


def registrar_miembro(user_id: int, nombre: str, username: str | None) -> None:
    """Inserta el miembro con 0 mensajes si no existe aún en la BD."""
    _conn.execute("""
        INSERT OR IGNORE INTO usuarios (user_id, nombre, username, total_mensajes, ultimo_mensaje)
        VALUES (?, ?, ?, 0, NULL)
    """, (user_id, nombre, username))
    _conn.commit()


def registrar_mensaje(user_id: int,
                      nombre: str,
                      username: str | None,
                      fecha: datetime) -> None:
    fecha_str = fecha.isoformat()
    _conn.execute("""
        INSERT INTO usuarios (user_id, nombre, username, total_mensajes, ultimo_mensaje)
        VALUES (?, ?, ?, 1, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            nombre         = excluded.nombre,
            username       = excluded.username,
            total_mensajes = total_mensajes + 1,
            ultimo_mensaje = excluded.ultimo_mensaje
    """, (user_id, nombre, username, fecha_str))
    _conn.commit()


def obtener_top5() -> list[tuple[int, str, str | None, int, str | None]]:
    """Devuelve los 5 usuarios con más mensajes ordenados de mayor a menor."""
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, ultimo_mensaje
        FROM   usuarios
        ORDER  BY total_mensajes DESC
        LIMIT  5
    """)
    return cur.fetchall()


def obtener_down5() -> list[tuple[int, str, str | None, int, str | None]]:
    """Devuelve los 5 usuarios con menos mensajes ordenados de menor a mayor."""
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, ultimo_mensaje
        FROM   usuarios
        ORDER  BY total_mensajes ASC
        LIMIT  5
    """)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

async def handler_miembro(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Registra en la BD a cualquier usuario que se une al grupo."""
    cambio: ChatMemberUpdated = update.chat_member
    nuevo = cambio.new_chat_member
    usuario = nuevo.user

    if usuario.is_bot:
        return

    estados_activos = {"member", "administrator", "creator", "restricted"}
    if nuevo.status not in estados_activos:
        return

    nombre = (
        f"{usuario.first_name or ''} {usuario.last_name or ''}".strip()
        or str(usuario.id)
    )
    registrar_miembro(usuario.id, nombre, usuario.username)
    logger.info(f"Nuevo miembro registrado: {nombre} (id={usuario.id})")


async def handler_mensaje(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg  = update.effective_message
    user = update.effective_user

    if user is None or user.is_bot:
        return

    nombre = (
        f"{user.first_name or ''} {user.last_name or ''}".strip()
        or str(user.id)
    )
    username = user.username
    fecha    = msg.date

    registrar_mensaje(user.id, nombre, username, fecha)
    logger.debug(f"Mensaje registrado: {nombre} (id={user.id})")


# ---------------------------------------------------------------------------
# Tarea programada: resumen diario
# ---------------------------------------------------------------------------

def _formatear_usuario(user_id: int, nombre: str,
                        username: str | None, total: int,
                        icono: str) -> str:
    nombre_safe = (
        nombre
        .replace("_", "\\_")
        .replace("*", "\\*")
        .replace("[", "\\[")
        .replace("`", "\\`")
    )
    alias = f"@{username}" if username else f"id:{user_id}"
    return (
        f"{icono} *{nombre_safe}* ({alias})\n"
        f"   └ {total:,} mensajes"
    )


async def enviar_resumen_diario(context: ContextTypes.DEFAULT_TYPE) -> None:
    top5  = obtener_top5()
    down5 = obtener_down5()

    if not top5:
        logger.info("Sin datos para el resumen diario.")
        return

    ahora   = datetime.now(tz=timezone.utc).strftime("%d/%m/%Y")
    medallas = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    calavers = ["💀", "😴", "🐌", "🦥", "👻"]

    lineas = [f"📊 *Estadísticas del grupo* — {ahora}\n"]

    # --- Top 5 ---
    lineas.append("🏆 *Top 5 — Más activos*\n")
    for i, (user_id, nombre, username, total, _) in enumerate(top5):
        lineas.append(_formatear_usuario(user_id, nombre, username, total, medallas[i]))

    # --- Down 5 ---
    lineas.append("\n💤 *Down 5 — Menos activos*\n")
    for i, (user_id, nombre, username, total, _) in enumerate(down5):
        lineas.append(_formatear_usuario(user_id, nombre, username, total, calavers[i]))

    lineas.append(f"\n_Actualizado cada día a las 10:00 UTC_")
    texto = "\n".join(lineas)

    await context.bot.send_message(
        chat_id=GRUPO_ID,
        text=texto,
        parse_mode="Markdown",
    )
    logger.info("Resumen diario enviado al grupo.")


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main() -> None:
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN no encontrado en el archivo .env")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(
        ChatMemberHandler(handler_miembro, ChatMemberHandler.CHAT_MEMBER)
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.ChatType.GROUPS,
            handler_mensaje,
        )
    )

    job_queue = app.job_queue
    job_queue.run_daily(
        callback=enviar_resumen_diario,
        time=HORA_REPORTE,
        name="resumen_diario",
    )
    logger.info(f"Tarea diaria programada para las {HORA_REPORTE} UTC")

    logger.info("Bot iniciado. Esperando mensajes...")
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        _conn.close()
        logger.info("BD cerrada. Bot detenido.")


if __name__ == "__main__":
    main()
