"""
bot_estadisticas.py
===================
Bot principal de estadísticas de grupo para Telegram.

Funcionalidades:
  1. Al arrancar, recupera los mensajes perdidos desde la última ejecución
     (usando Telethon) y actualiza la BD, luego envía un reporte al admin.
  2. Escucha todos los mensajes nuevos en el grupo y actualiza la BD SQLite.
  3. Tarea programada diaria (10:00 AM UTC) que envía al admin el Top 5
     y Down 5 de usuarios, formateado en Markdown.

Requisitos:
  - pip install "python-telegram-bot[job-queue]" telethon python-dotenv
  - Base de datos inicializada con init_historial.py (o vacía: se crea sola)
  - Archivo .env con: BOT_TOKEN, GRUPO_ID, ADMIN_ID, API_ID, API_HASH

Uso:
  python bot_estadisticas.py
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram import (
    BotCommand,
    BotCommandScopeChat,
    Update,
    ChatMemberUpdated,
    ChatJoinRequest,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.error import BadRequest, RetryAfter
from telegram.ext import (
    Application,
    ChatMemberHandler,
    ChatJoinRequestHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)
from telethon import TelegramClient
from telethon.tl.types import User as TelethonUser

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

BOT_TOKEN    = os.getenv("BOT_TOKEN")
GRUPO_ID     = int(os.getenv("GRUPO_ID"))
ADMIN_ID     = int(os.getenv("ADMIN_ID"))
API_ID       = int(os.getenv("API_ID"))
API_HASH     = os.getenv("API_HASH")
SESSION_NAME = "sesion_admin"

MAX_DAYS_INACTIVE_WARNING  = int(os.getenv("MAX_DAYS_INACTIVE_WARNING", "30"))
MAX_DAYS_INACTIVE_REMOVAL  = int(os.getenv("MAX_DAYS_INACTIVE_REMOVAL", "60"))

# Gestión de nuevos usuarios inactivos (0 = feature deshabilitada)
NEW_USER_GRACE_PERIOD_DAYS   = int(os.getenv("NEW_USER_GRACE_PERIOD_DAYS", "7"))
NEW_USER_WARNING_DAYS_BEFORE = int(os.getenv("NEW_USER_WARNING_DAYS_BEFORE", "3"))

# Probación de nuevos integrantes (plazos cortos con expulsión automática)
PROBATION_ENABLED        = os.getenv("PROBATION_ENABLED", "True").lower() == "true"
PROBATION_DEADLINE_1_MIN = int(os.getenv("PROBATION_DEADLINE_1_MIN", "15"))
PROBATION_DEADLINE_2_MIN = int(os.getenv("PROBATION_DEADLINE_2_MIN", "2"))

DB_PATH        = "estadisticas_grupo.db"
BOT_STATE_PATH = "bot_state.json"
MADRID_TZ      = ZoneInfo("Europe/Madrid")
HORA_REPORTE   = time(hour=7, minute=0, second=0, tzinfo=MADRID_TZ)

# Timestamp del último mensaje procesado; se persiste en bot_state.json al parar
_ultimo_registro: datetime | None = None


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
            ultimo_mensaje TEXT    DEFAULT NULL,
            fecha_registro TEXT    DEFAULT NULL
        )
    """)
    # Migración: añadir fecha_registro si la tabla ya existía sin esa columna
    try:
        conn.execute("ALTER TABLE usuarios ADD COLUMN fecha_registro TEXT DEFAULT NULL")
    except sqlite3.OperationalError:
        pass  # la columna ya existe
    ahora = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        UPDATE usuarios
        SET    fecha_registro = COALESCE(ultimo_mensaje, ?)
        WHERE  fecha_registro IS NULL
    """, (ahora,))

    conn.execute("""
        CREATE TABLE IF NOT EXISTS probacion (
            user_id        INTEGER PRIMARY KEY,
            nombre         TEXT,
            welcome_msg_id INTEGER,
            warning_msg_id INTEGER,
            expiry_date    TEXT,
            status         INTEGER DEFAULT 0
        )
    """)
    # Migración: añadir nombre si la tabla probacion ya existía sin esa columna
    try:
        conn.execute("ALTER TABLE probacion ADD COLUMN nombre TEXT")
    except sqlite3.OperationalError:
        pass  # la columna ya existe
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS config (
            clave TEXT PRIMARY KEY,
            valor TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS salidas_probacion (
            user_id  INTEGER PRIMARY KEY,
            nombre   TEXT,
            username TEXT,
            fecha    TEXT,
            motivo   TEXT
        )
    """)
    conn.commit()
    return conn


_conn: sqlite3.Connection = get_conn()


# ---------------------------------------------------------------------------
# Estado del bot (bot_state.json)
# ---------------------------------------------------------------------------

def leer_bot_state() -> dict:
    """Lee bot_state.json; devuelve dict vacío si no existe."""
    try:
        with open(BOT_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def guardar_bot_state(fecha_arranque: datetime, ultimo_registro: datetime, last_sent: dict | None = None) -> None:
    """Persiste fecha_arranque, ultimo_registro y opcionalmente last_sent_reports en bot_state.json."""
    state = leer_bot_state()
    state.update({
        "fecha_arranque":   fecha_arranque.isoformat(),
        "ultimo_registro":  ultimo_registro.isoformat(),
    })
    if last_sent is not None:
        state["last_sent_reports"] = last_sent
    
    with open(BOT_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def leer_ultimo_registro() -> datetime | None:
    """Devuelve el ultimo_registro guardado en bot_state.json, o None si no existe."""
    raw = leer_bot_state().get("ultimo_registro")
    if raw is None:
        return None
    return datetime.fromisoformat(raw)


def should_send_public_report(report_id: str) -> bool:
    """
    Comprueba si un reporte público debe enviarse (máximo una vez al día UTC).
    Devuelve True si no se ha enviado hoy o nunca se ha enviado.
    """
    state = leer_bot_state()
    last_sent_reports = state.get("last_sent_reports", {})
    last_date_str = last_sent_reports.get(report_id)
    
    if not last_date_str:
        return True
    
    hoy_utc = datetime.now(timezone.utc).date().isoformat()
    return last_date_str != hoy_utc


def mark_public_report_sent(report_id: str) -> None:
    """Marca un reporte como enviado hoy en bot_state.json."""
    state = leer_bot_state()
    last_sent_reports = state.get("last_sent_reports", {})
    
    hoy_utc = datetime.now(timezone.utc).date().isoformat()
    last_sent_reports[report_id] = hoy_utc
    
    # Necesitamos fecha_arranque y ultimo_registro para guardar_bot_state
    # Si no están, usamos valores por defecto (aunque deberían estar si el bot está corriendo)
    ahora = datetime.now(timezone.utc)
    try:
        fecha_arranque = datetime.fromisoformat(state.get("fecha_arranque", ahora.isoformat()))
        ultimo_reg     = datetime.fromisoformat(state.get("ultimo_registro", ahora.isoformat()))
    except (ValueError, TypeError):
        fecha_arranque = ahora
        ultimo_reg     = ahora
        
    guardar_bot_state(fecha_arranque, ultimo_reg, last_sent=last_sent_reports)


def registrar_miembro(user_id: int, nombre: str, username: str | None) -> None:
    """Inserta the miembro con 0 mensajes si no existe aún en la BD."""
    ahora = datetime.now(timezone.utc).isoformat()
    _conn.execute("""
        INSERT OR IGNORE INTO usuarios
            (user_id, nombre, username, total_mensajes, ultimo_mensaje, fecha_registro)
        VALUES (?, ?, ?, 0, ?, ?)
    """, (user_id, nombre, username, ahora, ahora))
    _conn.commit()


def eliminar_miembro(user_id: int) -> None:
    """Elimina al usuario de la BD cuando sale o es expulsado del grupo."""
    _conn.execute("DELETE FROM usuarios WHERE user_id = ?", (user_id,))
    _conn.execute("DELETE FROM probacion WHERE user_id = ?", (user_id,))
    _conn.commit()


def usuario_existe(user_id: int) -> bool:
    """Comprueba si un usuario existe en la tabla de miembros (usuarios)."""
    cursor = _conn.execute("SELECT 1 FROM usuarios WHERE user_id = ?", (user_id,))
    return cursor.fetchone() is not None


def obtener_mensajes_usuario(user_id: int) -> int:
    """Devuelve el total de mensajes de un usuario."""
    cursor = _conn.execute("SELECT total_mensajes FROM usuarios WHERE user_id = ?", (user_id,))
    fila = cursor.fetchone()
    return fila[0] if fila else 0


def registrar_probacion(user_id: int, nombre: str, welcome_msg_id: int, expiry_date: datetime) -> None:
    """Registra a un usuario en el periodo de probación inicial."""
    _conn.execute("""
        INSERT OR REPLACE INTO probacion (user_id, nombre, welcome_msg_id, expiry_date, status)
        VALUES (?, ?, ?, ?, 0)
    """, (user_id, nombre, welcome_msg_id, expiry_date.isoformat()))
    _conn.commit()


def actualizar_probacion(user_id: int, warning_msg_id: int, expiry_date: datetime, status: int) -> None:
    """Actualiza el estado de probación (p.ej. tras enviar el aviso)."""
    _conn.execute("""
        UPDATE probacion
        SET warning_msg_id = ?, expiry_date = ?, status = ?
        WHERE user_id = ?
    """, (warning_msg_id, expiry_date.isoformat(), status, user_id))
    _conn.commit()


def obtener_usuario_probacion(user_id: int) -> tuple | None:
    """Obtiene los datos de probación de un usuario."""
    cursor = _conn.execute(
        "SELECT welcome_msg_id, warning_msg_id, expiry_date, status FROM probacion WHERE user_id = ?",
        (user_id,)
    )
    return cursor.fetchone()


def obtener_todos_probacion() -> list[tuple]:
    """Obtiene todos los usuarios en periodo de probación."""
    cursor = _conn.execute("SELECT user_id, nombre, welcome_msg_id, warning_msg_id, expiry_date, status FROM probacion")
    return cursor.fetchall()


def eliminar_probacion(user_id: int) -> None:
    """Elimina al usuario del periodo de probación (p.ej. porque ya participó)."""
    _conn.execute("DELETE FROM probacion WHERE user_id = ?", (user_id,))
    _conn.commit()


def registrar_salida_probacion(user_id: int, nombre: str, username: str | None, motivo: str) -> None:
    """Registra una salida o expulsión durante el periodo de probación."""
    ahora = datetime.now(timezone.utc).isoformat()
    _conn.execute("""
        INSERT OR IGNORE INTO salidas_probacion (user_id, nombre, username, fecha, motivo)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, nombre, username, ahora, motivo))
    _conn.commit()


def obtener_salidas_probacion() -> list[tuple]:
    """Obtiene todas las salidas registradas durante la probación."""
    cursor = _conn.execute("SELECT user_id, nombre, username, fecha, motivo FROM salidas_probacion ORDER BY fecha ASC")
    return cursor.fetchall()


def limpiar_salidas_probacion() -> None:
    """Vacía el registro de salidas de probación."""
    _conn.execute("DELETE FROM salidas_probacion")
    _conn.commit()


async def procesar_salida_usuario(bot, user_id: int) -> None:
    """Maneja la salida de un usuario: limpia mensajes de probación y elimina de BD."""
    prob = obtener_usuario_probacion(user_id)
    if prob:
        w_msg_id, a_msg_id, _, _ = prob
        await _borrar_mensajes(bot, GRUPO_ID, [w_msg_id, a_msg_id])
        
        # Intentar obtener los datos del usuario para el registro de salidas
        nombre = "Desconocido"
        username = None
        cursor = _conn.execute("SELECT nombre, username FROM usuarios WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        if row:
            nombre, username = row
        else:
            cursor = _conn.execute("SELECT nombre FROM probacion WHERE user_id = ?", (user_id,))
            row_p = cursor.fetchone()
            if row_p:
                nombre = row_p[0]
        
        registrar_salida_probacion(user_id, nombre, username, "salida")
    
    eliminar_miembro(user_id)


def obtener_config(clave: str, default: str | None = None) -> str | None:
    """Obtiene un valor de configuración de la BD."""
    cursor = _conn.execute("SELECT valor FROM config WHERE clave = ?", (clave,))
    fila = cursor.fetchone()
    return fila[0] if fila else default


def guardar_config(clave: str, valor: str) -> None:
    """Guarda un valor de configuración en la BD."""
    _conn.execute("INSERT OR REPLACE INTO config (clave, valor) VALUES (?, ?)", (clave, valor))
    _conn.commit()


def registrar_mensaje(user_id: int,
                      nombre: str,
                      username: str | None,
                      fecha: datetime) -> None:
    global _ultimo_registro
    fecha_str = fecha.isoformat()
    ahora     = datetime.now(timezone.utc).isoformat()
    _conn.execute("""
        INSERT INTO usuarios
            (user_id, nombre, username, total_mensajes, ultimo_mensaje, fecha_registro)
        VALUES (?, ?, ?, 1, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            nombre         = excluded.nombre,
            username       = excluded.username,
            total_mensajes = total_mensajes + 1,
            ultimo_mensaje = CASE WHEN excluded.ultimo_mensaje > COALESCE(ultimo_mensaje, '')
                                 THEN excluded.ultimo_mensaje
                                 ELSE ultimo_mensaje END
    """, (user_id, nombre, username, fecha_str, ahora))
    _conn.commit()
    # Actualizar el último registro procesado en memoria
    if _ultimo_registro is None or fecha > _ultimo_registro:
        _ultimo_registro = fecha


def registrar_actividad_recovery(user_id: int,
                                 nombre: str,
                                 username: str | None,
                                 fecha: datetime) -> None:
    """Igual que registrar_mensaje pero NO incrementa total_mensajes en usuarios ya existentes.
    Usar en recuperaciones históricas para evitar doble conteo."""
    global _ultimo_registro
    fecha_str = fecha.isoformat()
    ahora     = datetime.now(timezone.utc).isoformat()
    _conn.execute("""
        INSERT INTO usuarios
            (user_id, nombre, username, total_mensajes, ultimo_mensaje, fecha_registro)
        VALUES (?, ?, ?, 1, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            nombre         = excluded.nombre,
            username       = excluded.username,
            ultimo_mensaje = CASE WHEN excluded.ultimo_mensaje > COALESCE(ultimo_mensaje, '')
                                 THEN excluded.ultimo_mensaje
                                 ELSE ultimo_mensaje END
    """, (user_id, nombre, username, fecha_str, ahora))
    _conn.commit()
    if _ultimo_registro is None or fecha > _ultimo_registro:
        _ultimo_registro = fecha


def obtener_top5() -> list[tuple[int, str, str | None, int, str | None, str | None]]:
    """Devuelve los 5 usuarios con más mensajes ordenados de mayor a menor."""
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, ultimo_mensaje, fecha_registro
        FROM   usuarios
        ORDER  BY total_mensajes DESC
        LIMIT  5
    """)
    return cur.fetchall()


def obtener_usuarios_inactivos(dias_warning: int) -> list[tuple[int, str, str | None, int, str | None, str | None]]:
    """Devuelve usuarios inactivos: sin mensajes (por fecha_registro) y con mensajes expirados."""
    limite = (datetime.now(timezone.utc) - timedelta(days=dias_warning)).isoformat()
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, ultimo_mensaje, fecha_registro
        FROM   usuarios
        WHERE  (total_mensajes = 0 AND (fecha_registro IS NULL OR fecha_registro < ?))
           OR  (total_mensajes > 0 AND ultimo_mensaje IS NOT NULL AND ultimo_mensaje < ?)
        ORDER BY
            CASE WHEN total_mensajes = 0 THEN 0 ELSE 1 END ASC,
            COALESCE(fecha_registro, ultimo_mensaje, '1970-01-01') ASC
    """, (limite, limite))
    return cur.fetchall()


def obtener_usuarios_para_expulsar() -> list[tuple[int, str, str | None, int, str | None, str | None]]:
    """Devuelve los usuarios a expulsar ordenados por prioridad.

    Prioridad: sin mensajes (total_mensajes=0, por fecha_registro) primero, con mensajes después.
    Dentro de cada grupo, los más inactivos primero.
    Solo se incluyen usuarios cuyo plazo de MAX_DAYS_INACTIVE_REMOVAL ha expirado.
    """
    limite = (datetime.now(timezone.utc) - timedelta(days=MAX_DAYS_INACTIVE_REMOVAL)).isoformat()
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, ultimo_mensaje, fecha_registro
        FROM   usuarios
        WHERE  (total_mensajes = 0 AND (fecha_registro IS NULL OR fecha_registro < ?))
           OR  (total_mensajes > 0 AND ultimo_mensaje IS NOT NULL AND ultimo_mensaje < ?)
        ORDER BY
            CASE WHEN total_mensajes = 0 THEN 0 ELSE 1 END ASC,
            COALESCE(fecha_registro, ultimo_mensaje, '1970-01-01') ASC
    """, (limite, limite))
    return cur.fetchall()


def buscar_usuarios(termino: str) -> list[tuple[int, str, str | None, int, str | None, str | None]]:
    """Busca usuarios en la BD por ID, username o nombre."""
    # Si es numérico, buscar por ID
    if termino.isdigit():
        cur = _conn.execute("""
            SELECT user_id, nombre, username, total_mensajes, ultimo_mensaje, fecha_registro
            FROM   usuarios
            WHERE  user_id = ?
        """, (int(termino),))
        return cur.fetchall()

    # Si empieza por @, buscar por username
    if termino.startswith("@"):
        username = termino[1:]
        cur = _conn.execute("""
            SELECT user_id, nombre, username, total_mensajes, ultimo_mensaje, fecha_registro
            FROM   usuarios
            WHERE  username LIKE ?
        """, (username,))
        return cur.fetchall()

    # Por defecto, buscar por nombre (fuzzy)
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, ultimo_mensaje, fecha_registro
        FROM   usuarios
        WHERE  nombre LIKE ?
    """, (f"%{termino}%",))
    return cur.fetchall()


# Usuarios pendientes de expulsión, poblado por enviar_reporte_expulsion()
_pendientes_expulsion: list[tuple[int, str, str | None, int, str]] = []


def obtener_down5() -> list[tuple[int, str, str | None, int, str | None, str | None]]:
    """
    Devuelve los 5 usuarios menos activos con prioridad:
    1. Usuarios con 0 mensajes (ghosts), los más antiguos primero (por fecha_registro).
    2. Usuarios con mensajes, los que tienen menos mensajes y llevan más tiempo sin hablar.
    """
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, ultimo_mensaje, fecha_registro
        FROM   usuarios
        ORDER  BY
            CASE WHEN total_mensajes = 0 THEN 0 ELSE 1 END ASC,
            CASE WHEN total_mensajes = 0 THEN COALESCE(fecha_registro, '1970-01-01') 
                 ELSE total_mensajes END ASC,
            COALESCE(ultimo_mensaje, '1970-01-01') ASC
        LIMIT  5
    """)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

async def handler_unauthorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Detecta mensajes privados de no-admins y avisa al ADMIN_ID."""
    user = update.effective_user
    msg  = update.effective_message
    
    if user is None or user.id == ADMIN_ID or user.is_bot:
        return

    # Solo nos interesan mensajes privados (no del grupo)
    if update.effective_chat.type != "private":
        return

    nombre = _escape_html(f"{user.first_name or ''} {user.last_name or ''}".strip() or str(user.id))
    username = f"@{_escape_html(user.username)}" if user.username else "n/a"
    texto_msg = _escape_html(msg.text or msg.caption or "[mensaje sin texto]")
    
    aviso = (
        "⚠️ <b>Intento de acceso no autorizado</b>\n\n"
        f"<b>Usuario:</b> {nombre}\n"
        f"<b>Username:</b> {username}\n"
        f"<b>ID:</b> <code>{user.id}</code>\n\n"
        f"<b>Mensaje:</b>\n{texto_msg}"
    )
    
    await _send_long_message(context.bot, ADMIN_ID, aviso, "HTML")
    logger.info(f"Intento de acceso no autorizado de {nombre} (id={user.id}) notificado al admin.")


async def handler_miembro(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Registra o elimina usuarios de la BD según entren o salgan del grupo."""
    cambio: ChatMemberUpdated = update.chat_member
    nuevo = cambio.new_chat_member
    usuario = nuevo.user

    if usuario.is_bot:
        return

    estados_activos = {"member", "administrator", "creator", "restricted"}
    estados_salida  = {"left", "kicked"}

    if nuevo.status in estados_activos:
        nombre = (
            f"{usuario.first_name or ''} {usuario.last_name or ''}".strip()
            or str(usuario.id)
        )
        registrar_miembro(usuario.id, nombre, usuario.username)
        logger.info(f"Miembro registrado: {nombre} (id={usuario.id})")

        # Iniciar periodo de probación si está habilitado
        if PROBATION_ENABLED:
            # Añadimos un pequeño retraso para que el mensaje aparezca DESPUÉS de la notificación de Telegram
            await asyncio.sleep(2)
            
            # Verificar si el usuario sigue en el grupo tras el sleep para evitar race conditions
            if not usuario_existe(usuario.id):
                logger.info(f"[probacion] Usuario {nombre} (id={usuario.id}) ya no está en la BD tras el sleep. Abortando.")
                return
                
            # Verificar si ya participó durante el periodo de sleep
            if obtener_mensajes_usuario(usuario.id) > 0:
                logger.info(f"[probacion] Usuario {nombre} (id={usuario.id}) ya participó durante el sleep. Abortando.")
                return

            expiry = datetime.now(timezone.utc) + timedelta(minutes=PROBATION_DEADLINE_1_MIN)
            
            # Obtener plantilla personalizada o usar la por defecto
            template = obtener_config("welcome_template")
            if template:
                username_txt = f"@{_escape_html(usuario.username)}" if usuario.username else "n/a"
                txt_bienvenida = template.replace("{nombre}", _escape_html(nombre)) \
                                         .replace("{username}", username_txt) \
                                         .replace("{id}", str(usuario.id)) \
                                         .replace("{minutos}", str(PROBATION_DEADLINE_1_MIN))
            else:
                txt_bienvenida = (
                    f"👋 ¡Bienvenido/a <b>{_escape_html(nombre)}</b>!\n\n"
                    f"Para mantener el grupo seguro y activo, tienes <b>{PROBATION_DEADLINE_1_MIN} minutos</b> "
                    "para enviar un mensaje (saludo o participación) o serás expulsado/a automáticamente."
                )

            try:
                msg_b = await context.bot.send_message(chat_id=GRUPO_ID, text=txt_bienvenida, parse_mode="HTML")
                registrar_probacion(usuario.id, nombre, msg_b.message_id, expiry)
                logger.info(f"Probación iniciada para {nombre} (id={usuario.id}). Expira: {expiry}")
            except Exception as e:
                logger.error(f"Error al enviar mensaje de bienvenida a {usuario.id}: {e}")

    elif nuevo.status in estados_salida:
        await procesar_salida_usuario(context.bot, usuario.id)
        logger.info(f"Miembro eliminado y probación limpiada: id={usuario.id} (estado={nuevo.status})")


async def handler_mensaje(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg  = update.effective_message
    user = update.effective_user

    if user is None or user.is_bot:
        return

    nombre = (
        f"{user.first_name or ''} {user.last_name or ''}".strip()
        or str(user.id)
    )

    # Limpiar periodo de probación si el usuario participa
    prob = obtener_usuario_probacion(user.id)
    if prob:
        w_msg_id, a_msg_id, _, _ = prob
        await _borrar_mensajes(context.bot, GRUPO_ID, [w_msg_id, a_msg_id])
        eliminar_probacion(user.id)
        logger.info(f"Probación superada por {nombre} (id={user.id}).")

    username = user.username
    fecha    = msg.date

    registrar_mensaje(user.id, nombre, username, fecha)
    logger.debug(f"Mensaje registrado: {nombre} (id={user.id})")


# ---------------------------------------------------------------------------
# Reporte TOP 5 / DOWN 5
# ---------------------------------------------------------------------------

def _formatear_usuario(user_id: int, nombre: str,
                        username: str | None, total: int,
                        icono: str, ultimo: str | None, registro: str | None) -> str:
    alias = f"@{_escape_html(username)}" if username else f"id:{user_id}"
    ahora = datetime.now(timezone.utc)

    registro_str = "Desconocida"
    if registro:
        try:
            dt_reg = datetime.fromisoformat(registro)
            registro_str = dt_reg.strftime("%d/%m/%Y")
        except Exception:
            pass

    actividad_str = "Nunca"
    if ultimo:
        try:
            dt_ult = datetime.fromisoformat(ultimo)
            dias = (ahora - dt_ult).days
            actividad_str = f"{dt_ult.strftime('%d/%m/%Y')} ({dias} días)"
        except Exception:
            pass

    return (
        f"{icono} <b>{_escape_html(nombre)}</b> ({alias})\n"
        f"   ├ Mensajes: {total:,}\n"
        f"   ├ Registro: {registro_str}\n"
        f"   └ Última act.: {actividad_str}"
    )


def _construir_seccion_top5(top5: list) -> str:
    ahora    = datetime.now(tz=timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    medallas = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    lineas = [f"📊 <b>Estadísticas del grupo</b> — {ahora}\n"]
    lineas.append("🏆 <b>Top 5 — Más activos</b>\n")
    for i, (user_id, nombre, username, total, ultimo, registro) in enumerate(top5):
        lineas.append(_formatear_usuario(user_id, nombre, username, total, medallas[i], ultimo, registro))
    return "\n".join(lineas)


def _construir_seccion_down5(down5: list) -> str:
    ahora    = datetime.now(tz=timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    calavers = ["💀", "😴", "🐌", "🦥", "👻"]
    lineas = [f"📊 <b>Estadísticas del grupo</b> — {ahora}\n"]
    lineas.append("💤 <b>Down 5 — Menos activos</b>\n")
    for i, (user_id, nombre, username, total, ultimo, registro) in enumerate(down5):
        lineas.append(_formatear_usuario(user_id, nombre, username, total, calavers[i], ultimo, registro))
    return "\n".join(lineas)


def _construir_texto_reporte() -> str | None:
    """Construye el texto HTML completo del reporte TOP 5 + DOWN 5."""
    top5  = obtener_top5()
    down5 = obtener_down5()

    if not top5:
        return None

    ahora    = datetime.now(tz=timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    medallas = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    calavers = ["💀", "😴", "🐌", "🦥", "👻"]

    lineas = [f"📊 <b>Estadísticas del grupo</b> — {ahora}\n"]

    lineas.append("🏆 <b>Top 5 — Más activos</b>\n")
    for i, (user_id, nombre, username, total, ultimo, registro) in enumerate(top5):
        lineas.append(_formatear_usuario(user_id, nombre, username, total, medallas[i], ultimo, registro))

    lineas.append("\n💤 <b>Down 5 — Menos activos</b>\n")
    for i, (user_id, nombre, username, total, ultimo, registro) in enumerate(down5):
        lineas.append(_formatear_usuario(user_id, nombre, username, total, calavers[i], ultimo, registro))

    salidas = obtener_salidas_probacion()
    if salidas:
        lineas.append("\n🚪 <b>Salidas/Expulsiones en probación (últimas 24h)</b>\n")
        for user_id, nombre, username, _, motivo in salidas:
            alias = f"@{_escape_html(username)}" if username else f"id:<code>{user_id}</code>"
            if motivo == "expulsado":
                lineas.append(f"• <b>{_escape_html(nombre)}</b> ({alias}) — ❌ Expulsado/a por inactividad inicial")
            else:
                lineas.append(f"• <b>{_escape_html(nombre)}</b> ({alias}) — 🚪 Salió voluntariamente")

    return "\n".join(lineas)


def _loguear_reporte(top5, down5) -> None:
    logger.info("=== TOP 5 — Más activos ===")
    medallas = ["1º", "2º", "3º", "4º", "5º"]
    for i, (user_id, nombre, username, total, _, _) in enumerate(top5):
        alias = f"@{username}" if username else f"id:{user_id}"
        logger.info(f"  {medallas[i]} {nombre} ({alias}) — {total:,} mensajes")
    logger.info("=== DOWN 5 — Menos activos ===")
    for i, (user_id, nombre, username, total, _, _) in enumerate(down5):
        alias = f"@{username}" if username else f"id:{user_id}"
        logger.info(f"  {i+1}. {nombre} ({alias}) — {total:,} mensajes")


# ---------------------------------------------------------------------------
# Utilidades de expulsión
# ---------------------------------------------------------------------------

def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def _ejecutar_expulsion(bot, user_id: int, nombre: str,
                               username: str | None, tag: str) -> tuple[str, str]:
    """Ban + unban de un usuario con manejo diferenciado de errores (incluye RetryAfter).

    Devuelve (estado, texto) donde estado es:
      "expulsado" — ban+unban exitosos, eliminado de BD
      "ausente"   — Participant_id_invalid: ya no estaba en el grupo, eliminado de BD
      "error"     — otro error, NO eliminado de BD
    """
    alias = f"@{username}" if username else f"id:{user_id}"
    try:
        await bot.ban_chat_member(chat_id=GRUPO_ID, user_id=user_id)
        await bot.unban_chat_member(chat_id=GRUPO_ID, user_id=user_id)
        await procesar_salida_usuario(bot, user_id)
        logger.info(f"[{tag}] Expulsado: {nombre} ({alias})")
        return "expulsado", f"• {_escape_html(nombre)} ({alias})"
    except RetryAfter as exc:
        logger.warning(f"FloodWait detectado: esperando {exc.retry_after} segundos...")
        await asyncio.sleep(exc.retry_after)
        # Reintentar la operación
        return await _ejecutar_expulsion(bot, user_id, nombre, username, tag)
    except BadRequest as exc:
        if "participant_id_invalid" in str(exc).lower():
            await procesar_salida_usuario(bot, user_id)
            logger.info(f"[{tag}] Ya no estaba en el grupo, eliminado de BD: {nombre} ({alias})")
            return "ausente", f"• {_escape_html(nombre)} ({alias})"
        logger.warning(f"[{tag}] Error al expulsar {nombre} ({alias}): {exc}")
        return "error", f"• {_escape_html(nombre)} ({alias}): {_escape_html(str(exc))}"
    except Exception as exc:
        logger.warning(f"[{tag}] Error al expulsar {nombre} ({alias}): {exc}")
        return "error", f"• {_escape_html(nombre)} ({alias}): {_escape_html(str(exc))}"


def _construir_resultado_expulsion(expulsados: list[str],
                                    ausentes: list[str],
                                    errores: list[str]) -> str:
    """Construye el mensaje de resultado de una operación de expulsión masiva."""
    lineas = [f"✅ <b>{len(expulsados)} usuario(s) expulsados:</b>"] + expulsados
    if ausentes:
        lineas += [
            f"\n👻 <b>{len(ausentes)} ya no estaban en el grupo (eliminados de BD):</b>"
        ] + ausentes
    if errores:
        lineas += [f"\n⚠️ <b>{len(errores)} error(es):</b>"] + errores
    return "\n".join(lineas)


# ---------------------------------------------------------------------------
# Aviso de inactividad
# ---------------------------------------------------------------------------


_TELEGRAM_MAX_LEN = 4096


async def _borrar_mensajes(bot, chat_id: int, message_ids: list[int | None]) -> None:
    """Borra una lista de mensajes del chat de forma silenciosa."""
    for msg_id in message_ids:
        if msg_id:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception as e:
                logger.debug(f"No se pudo borrar el mensaje {msg_id}: {e}")


async def _send_long_message(bot, chat_id: int, texto: str, parse_mode: str) -> None:
    """Envía un texto al chat partiéndolo por líneas, respetando la integridad de HTML si se usa."""
    if parse_mode != "HTML":
        # Comportamiento simple para otros modos
        trozo: list[str] = []
        longitud = 0
        for linea in texto.splitlines(keepends=True):
            if longitud + len(linea) > _TELEGRAM_MAX_LEN and trozo:
                await bot.send_message(chat_id=chat_id, text="".join(trozo), parse_mode=parse_mode)
                trozo, longitud = [], 0
            trozo.append(linea)
            longitud += len(linea)
        if trozo:
            await bot.send_message(chat_id=chat_id, text="".join(trozo), parse_mode=parse_mode)
        return

    # Lógica avanzada para HTML
    trozo: list[str] = []
    longitud = 0
    stack_tags: list[str] = []
    re_tag = re.compile(r'<(/?)(b|i|code|a|pre|u|s|strike|del|span)[^>]*>')

    for linea in texto.splitlines(keepends=True):
        # Estimamos espacio extra para etiquetas de cierre/apertura automáticas
        espacio_reserva = len(stack_tags) * 15 
        if longitud + len(linea) + espacio_reserva > _TELEGRAM_MAX_LEN and trozo:
            mensaje_a_enviar = "".join(trozo)
            for tag in reversed(stack_tags):
                mensaje_a_enviar += f"</{tag}>"
            
            await bot.send_message(chat_id=chat_id, text=mensaje_a_enviar, parse_mode="HTML")
            
            # Reset y reabrir etiquetas en el nuevo trozo
            trozo = [f"<{tag}>" for tag in stack_tags]
            longitud = sum(len(s) for s in trozo)
        
        # Actualizar stack de etiquetas con la línea actual
        for slash, tag in re_tag.findall(linea):
            if slash:
                if stack_tags and stack_tags[-1] == tag:
                    stack_tags.pop()
            else:
                stack_tags.append(tag)
                
        trozo.append(linea)
        longitud += len(linea)

    if trozo:
        mensaje_final = "".join(trozo)
        for tag in reversed(stack_tags):
            mensaje_final += f"</{tag}>"
        await bot.send_message(chat_id=chat_id, text=mensaje_final, parse_mode="HTML")


async def enviar_aviso_inactivos(bot) -> None:
    """Detecta usuarios inactivos y envía al admin la lista con fecha de expulsión."""
    inactivos = obtener_usuarios_inactivos(MAX_DAYS_INACTIVE_WARNING)
    if not inactivos:
        logger.info("[inactividad] No hay usuarios que superen el umbral de aviso.")
        return
    inactivos, eliminados = await _filtrar_miembros_activos(bot, inactivos)
    if not inactivos:
        logger.info("[inactividad] No quedan inactivos tras validar membresía en el grupo.")
        return

    logger.info(f"[inactividad] {len(inactivos)} usuario(s) superan "
                f"{MAX_DAYS_INACTIVE_WARNING} días de inactividad:")

    lineas = [
        "🌿 <b>¡Hola a todos!</b>🌿\n\n",
        "Nos encanta la comunidad que estamos formando, y para que el grupo siga siendo "
        "un espacio vivo y dinámico, nos gusta contar con gente activa.\n\n"
        "Hemos notado que algunos de vosotros lleváis un tiempo sin pasaros por aquí. "
        "Si queréis seguir formando parte de este proyecto, solo tenéis que dar una señal "
        "de vida antes de la fecha indicada al lado de vuestro nombre. "
        "¡Nos encantaría que os quedarais! "
        "Si no es el momento, las puertas estarán abiertas para cuando decidáis volver.\n",
    ]

    for user_id, nombre, username, total, ultimo, registro in inactivos:
        alias = f"@{username}" if username else f"id:{user_id}"
        if ultimo:
            dt_ultimo = datetime.fromisoformat(ultimo)
            fecha_exp_str = (dt_ultimo + timedelta(days=MAX_DAYS_INACTIVE_REMOVAL)).strftime('%d/%m/%Y')
            ultimo_str    = dt_ultimo.strftime('%d/%m/%Y')
        else:
            fecha_exp_str = "Plazo vencido"
            ultimo_str    = "Nunca"
        lineas.append(
            f"• <b>{_escape_html(nombre)}</b> ({alias}) — "
            f"fin de plazo: <b>{fecha_exp_str}</b>"
        )
        logger.info(
            f"  · {nombre} ({alias}) | último mensaje: {ultimo_str} | "
            f"expulsión prevista: {fecha_exp_str}"
        )

    await _send_long_message(bot, ADMIN_ID, "\n".join(lineas), "HTML")
    logger.info(f"[inactividad] Aviso enviado al admin (id={ADMIN_ID}).")


# ---------------------------------------------------------------------------
# Expulsión de usuarios inactivos
# ---------------------------------------------------------------------------

async def enviar_reporte_expulsion(bot) -> None:
    """Detecta usuarios que superan MAX_DAYS_INACTIVE_REMOVAL y envía el reporte al admin."""
    global _pendientes_expulsion
    candidatos = obtener_usuarios_para_expulsar()
    candidatos, eliminados = await _filtrar_miembros_activos(bot, candidatos)
    _pendientes_expulsion = candidatos

    if not _pendientes_expulsion:
        logger.info("[expulsión] No hay usuarios que superen el plazo de expulsión.")
        return

    logger.info(f"[expulsión] {len(_pendientes_expulsion)} usuario(s) superan "
                f"{MAX_DAYS_INACTIVE_REMOVAL} días de inactividad. Pendientes de /ok:")

    lineas = [
        "🚨 <b>Usuarios pendientes de expulsión</b>\n",
        f"Han superado el plazo de {MAX_DAYS_INACTIVE_REMOVAL} días de inactividad. "
        "Responde /ok para expulsarlos del grupo.\n",
    ]
    if eliminados:
        lineas.append(f"ℹ️ {eliminados} usuario(s) ya no estaban en el grupo y han sido eliminados de BD.\n")

    for user_id, nombre, username, total, ultimo, registro in _pendientes_expulsion:
        alias        = f"@{username}" if username else f"id:{user_id}"
        ultimo_str   = (datetime.fromisoformat(ultimo).strftime('%d/%m/%Y')
                        if ultimo else "Nunca")
        lineas.append(
            f"• <b>{_escape_html(nombre)}</b> ({alias})\n"
            f"  └ Última actividad: {ultimo_str} | {total:,} mensajes"
        )
        logger.info(f"  · {nombre} ({alias}) | último: {ultimo_str} | {total} msgs")

    await _send_long_message(bot, ADMIN_ID, "\n".join(lineas), "HTML")
    logger.info(f"[expulsión] Reporte enviado al admin (id={ADMIN_ID}). Esperando /ok ...")


async def handler_setwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Configura el mensaje de bienvenida dinámico."""
    if not context.args:
        actual = obtener_config("welcome_template")
        if not actual:
            await update.message.reply_html(
                "No hay un mensaje personalizado configurado.\n"
                "Uso: <code>/setwelcome Hola {nombre}, tienes {minutos} min para saludar.</code>\n\n"
                "Variables disponibles: <code>{nombre}</code>, <code>{username}</code>, <code>{id}</code>, <code>{minutos}</code>"
            )
        else:
            await update.message.reply_html(f"Mensaje actual:\n\n{actual}")
        return

    nuevo_mensaje = " ".join(context.args)
    guardar_config("welcome_template", nuevo_mensaje)
    await update.message.reply_html("✅ Mensaje de bienvenida actualizado correctamente.")
    logger.info(f"Admin actualizó el mensaje de bienvenida: {nuevo_mensaje}")


async def handler_solicitud_union(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Aprueba automáticamente la solicitud de unión."""
    solicitud: ChatJoinRequest = update.chat_join_request
    user = solicitud.from_user
    
    try:
        await solicitud.approve()
        logger.info(f"[gate] Solicitud aprobada automáticamente para {user.first_name} ({user.id})")
    except Exception as e:
        logger.error(f"[gate] Error al aprobar automáticamente a {user.id}: {e}")


async def handler_ok(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Expulsa los usuarios pendientes cuando el admin confirma con /ok."""
    if update.effective_user.id != ADMIN_ID:
        return

    if not _pendientes_expulsion:
        await update.message.reply_text("No hay usuarios pendientes de expulsión.")
        return

    expulsados: list[str] = []
    ausentes:   list[str] = []
    errores:    list[str] = []

    for user_id, nombre, username, total, _, _ in _pendientes_expulsion:
        estado, texto = await _ejecutar_expulsion(
            context.bot, user_id, nombre, username, "expulsión"
        )
        if estado == "expulsado":
            expulsados.append(texto)
        elif estado == "ausente":
            ausentes.append(texto)
        else:
            errores.append(texto)
        
        # Rate limiting artificial para evitar FloodControl
        await asyncio.sleep(0.5)

    _pendientes_expulsion.clear()

    await _send_long_message(
        context.bot, ADMIN_ID,
        _construir_resultado_expulsion(expulsados, ausentes, errores),
        "HTML",
    )


# ---------------------------------------------------------------------------
# /noparticipa — usuarios sin ningún mensaje
# ---------------------------------------------------------------------------

def obtener_usuarios_sin_mensajes() -> list[tuple[int, str, str | None, int, str | None]]:
    """Usuarios con total_mensajes=0 que llevan más de MAX_DAYS_INACTIVE_WARNING días en el grupo."""
    limite = (datetime.now(timezone.utc) - timedelta(days=MAX_DAYS_INACTIVE_WARNING)).isoformat()
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, fecha_registro
        FROM   usuarios
        WHERE  total_mensajes = 0
          AND  (fecha_registro IS NULL OR fecha_registro < ?)
        ORDER BY COALESCE(fecha_registro, '1970-01-01') ASC
    """, (limite,))
    return cur.fetchall()


_pendientes_noparticipa: list[tuple[int, str, str | None, int, str | None]] = []


# ---------------------------------------------------------------------------
# Nuevos usuarios inactivos
# ---------------------------------------------------------------------------

def obtener_nuevos_usuarios_a_avisar() -> list[tuple[int, str, str | None, int, str | None]]:
    """Nuevos usuarios (total_mensajes=0) que vencen en exactamente NEW_USER_WARNING_DAYS_BEFORE días."""
    if NEW_USER_GRACE_PERIOD_DAYS == 0:
        return []
    dias_aviso = NEW_USER_GRACE_PERIOD_DAYS - NEW_USER_WARNING_DAYS_BEFORE
    if dias_aviso <= 0:
        return []
    limite_sup = (datetime.now(timezone.utc) - timedelta(days=dias_aviso)).isoformat()
    limite_inf = (datetime.now(timezone.utc) - timedelta(days=dias_aviso + 1)).isoformat()
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, fecha_registro
        FROM   usuarios
        WHERE  total_mensajes = 0
          AND  fecha_registro IS NOT NULL
          AND  fecha_registro <= ?
          AND  fecha_registro >  ?
        ORDER BY fecha_registro ASC
    """, (limite_sup, limite_inf))
    return cur.fetchall()


def obtener_nuevos_usuarios_a_expulsar() -> list[tuple[int, str, str | None, int, str | None]]:
    """Nuevos usuarios (total_mensajes=0) con plazo vencido, registrados hace entre N y N+1 días.
    Usuarios anteriores a N+1 días son gestionados por /noparticipa."""
    if NEW_USER_GRACE_PERIOD_DAYS == 0:
        return []
    limite_vencido = (datetime.now(timezone.utc) - timedelta(days=NEW_USER_GRACE_PERIOD_DAYS)).isoformat()
    limite_nuevo   = (datetime.now(timezone.utc) - timedelta(days=NEW_USER_GRACE_PERIOD_DAYS + 1)).isoformat()
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, fecha_registro
        FROM   usuarios
        WHERE  total_mensajes = 0
          AND  fecha_registro IS NOT NULL
          AND  fecha_registro <= ?
          AND  fecha_registro >  ?
        ORDER BY fecha_registro ASC
    """, (limite_vencido, limite_nuevo))
    return cur.fetchall()


_pendientes_nuevos: list[tuple[int, str, str | None, int, str | None]] = []


def obtener_todos_nuevos_sin_mensajes() -> list[tuple[int, str, str | None, int, str | None]]:
    """Todos los nuevos usuarios (total_mensajes=0) registrados en los últimos N+1 días.
    Incluye tanto los que aún están en período de gracia como los ya vencidos."""
    if NEW_USER_GRACE_PERIOD_DAYS == 0:
        return []
    limite = (datetime.now(timezone.utc) - timedelta(days=NEW_USER_GRACE_PERIOD_DAYS + 1)).isoformat()
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, fecha_registro
        FROM   usuarios
        WHERE  total_mensajes = 0
          AND  fecha_registro IS NOT NULL
          AND  fecha_registro > ?
        ORDER BY fecha_registro ASC
    """, (limite,))
    return cur.fetchall()


async def _filtrar_miembros_activos(
    bot, usuarios: list[tuple]
) -> tuple[list[tuple], int]:
    """Verifica que cada usuario siga en el grupo antes de incluirlo en un informe.

    Elimina de BD a los que ya no están (left/kicked o no encontrados).
    Devuelve (lista_filtrada, num_eliminados).
    Maneja RetryAfter con un reintento automático.
    En caso de error irrecuperable conserva el usuario en la lista.
    """
    activos: list[tuple] = []
    eliminados = 0
    for row in usuarios:
        user_id, nombre, username = row[0], row[1], row[2]
        alias = f"@{username}" if username else f"id:{user_id}"
        try:
            member = await bot.get_chat_member(chat_id=GRUPO_ID, user_id=user_id)
            if member.status in ("left", "kicked"):
                await procesar_salida_usuario(bot, user_id)
                eliminados += 1
                logger.info(f"[validacion] {nombre} ({alias}) ya no está en el grupo — eliminado de BD.")
            else:
                activos.append(row)
        except RetryAfter as exc:
            logger.warning(f"[validacion] FloodWait {exc.retry_after}s — reintentando {nombre} ({alias}).")
            await asyncio.sleep(exc.retry_after)
            try:
                member = await bot.get_chat_member(chat_id=GRUPO_ID, user_id=user_id)
                if member.status in ("left", "kicked"):
                    await procesar_salida_usuario(bot, user_id)
                    eliminados += 1
                    logger.info(f"[validacion] {nombre} ({alias}) ya no está en el grupo — eliminado de BD.")
                else:
                    activos.append(row)
            except Exception:
                activos.append(row)
        except BadRequest:
            await procesar_salida_usuario(bot, user_id)
            eliminados += 1
            logger.info(f"[validacion] {nombre} ({alias}) no encontrado en el grupo — eliminado de BD.")
        except Exception as exc:
            logger.warning(f"[validacion] Error verificando {nombre} ({alias}): {exc} — incluido en informe.")
            activos.append(row)
        await asyncio.sleep(0.05)
    return activos, eliminados


async def handler_noparticipa(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _pendientes_noparticipa
    candidatos = obtener_usuarios_sin_mensajes()
    candidatos, eliminados = await _filtrar_miembros_activos(context.bot, candidatos)
    _pendientes_noparticipa = candidatos

    if not _pendientes_noparticipa:
        msg = "No hay usuarios sin mensajes que superen el umbral."
        if eliminados:
            msg += f"\n({eliminados} usuario(s) ya no estaban en el grupo y han sido eliminados de BD.)"
        await update.message.reply_text(msg)
        return

    lineas = [
        f"🔇 <b>Usuarios sin ningún mensaje ({len(_pendientes_noparticipa)})</b>\n",
        f"Llevan más de {MAX_DAYS_INACTIVE_WARNING} días en el grupo sin participar.\n",
        "Responde /expulsarnoparticipa para expulsarlos.\n",
    ]
    if eliminados:
        lineas.append(f"ℹ️ {eliminados} usuario(s) ya no estaban en el grupo y han sido eliminados de BD.\n")
    for user_id, nombre, username, _, fecha_reg in _pendientes_noparticipa:
        alias = f"@{username}" if username else f"id:{user_id}"
        desde = datetime.fromisoformat(fecha_reg).strftime('%d/%m/%Y') if fecha_reg else "desconocido"
        lineas.append(f"• <b>{_escape_html(nombre)}</b> ({alias}) — en grupo desde: {desde}")
        logger.info(f"[noparticipa] {nombre} ({alias}) — desde {desde}")

    await _send_long_message(context.bot, ADMIN_ID, "\n".join(lineas), "HTML")


async def check_nuevos_proximos_a_vencer(bot) -> None:
    """Avisa al admin si hay nuevos usuarios que vencen en exactamente NEW_USER_WARNING_DAYS_BEFORE días."""
    usuarios = obtener_nuevos_usuarios_a_avisar()
    if not usuarios:
        return
    usuarios, _ = await _filtrar_miembros_activos(bot, usuarios)
    if not usuarios:
        return
    dias_restantes = NEW_USER_WARNING_DAYS_BEFORE
    lineas = [f"⚠️ <b>Nuevos sin participar — vencen en {dias_restantes} día(s)</b>\n"]
    for user_id, nombre, username, _, fecha_reg in usuarios:
        alias = f"@{username}" if username else f"id:{user_id}"
        if fecha_reg:
            dt_reg = datetime.fromisoformat(fecha_reg)
            fecha_venc = (dt_reg + timedelta(days=NEW_USER_GRACE_PERIOD_DAYS)).strftime('%d/%m/%Y')
            lineas.append(
                f"• <b>{_escape_html(nombre)}</b> ({alias}) "
                f"— entró el {dt_reg.strftime('%d/%m/%Y')} — vence el {fecha_venc}"
            )
        else:
            lineas.append(f"• <b>{_escape_html(nombre)}</b> ({alias})")
    await _send_long_message(bot, ADMIN_ID, "\n".join(lineas), "HTML")
    logger.info(f"[nuevos] Aviso previo: {len(usuarios)} usuario(s) vencen en {dias_restantes} día(s).")

    # Notificar también al grupo de forma amigable
    await enviar_aviso_grupo_inactivos(bot, usuarios)


async def enviar_aviso_grupo_inactivos(bot, usuarios) -> None:
    """Envía un mensaje amigable al grupo sobre nuevos usuarios que no han participado."""
    if not usuarios:
        return

    report_id = "aviso_grupo_inactivos"
    if not should_send_public_report(report_id):
        logger.info(f"[grupo] Omitiendo {report_id} (ya enviado hoy).")
        return

    ahora = datetime.now(timezone.utc)
    lineas = [
        "¡Hola a todos! 👋 Todos somos bienvenidos en este grupo, pero recordad que este es un espacio para "
        "intercambiar contenido, charlas e ideas y, sobre todo, para participar.\n",
        "Aprovechamos para saludar a los nuevos miembros que aún no se han animado a escribir:\n"
    ]

    for user_id, nombre, _, _, fecha_reg in usuarios:
        if fecha_reg:
            dt_reg = datetime.fromisoformat(fecha_reg)
            dias = (ahora - dt_reg).days
            lineas.append(f"• <b>{_escape_html(nombre)}</b> (ID: {user_id}) — {dias} días con nosotros")
        else:
            lineas.append(f"• <b>{_escape_html(nombre)}</b> (ID: {user_id})")

    lineas.append(
        "\nSi no saludáis o no queréis participar al uniros, recordad que seréis expulsados en el plazo "
        "establecido para mantener el grupo activo y dinámico. ¡Animaos a participar! 😊"
    )

    await _send_long_message(bot, GRUPO_ID, "\n".join(lineas), "HTML")
    mark_public_report_sent(report_id)
    logger.info(f"[grupo] Aviso de inactividad enviado al grupo ({len(usuarios)} usuarios).")


async def avisar_nuevos_vencidos(bot) -> None:
    """Avisa al admin si hay nuevos usuarios con plazo vencido. Requiere /expulsarnuevos para expulsar."""
    usuarios = obtener_nuevos_usuarios_a_expulsar()
    if not usuarios:
        return
    usuarios, _ = await _filtrar_miembros_activos(bot, usuarios)
    if not usuarios:
        return
    ahora = datetime.now(timezone.utc)
    lineas = [
        f"🚨 <b>Nuevos usuarios sin participar — plazo vencido ({len(usuarios)})</b>\n",
        f"Llevan más de {NEW_USER_GRACE_PERIOD_DAYS} días en el grupo sin enviar ningún mensaje.\n",
        "Usa /expulsarnuevos para expulsarlos.\n",
    ]
    for user_id, nombre, username, _, fecha_reg in usuarios:
        alias = f"@{username}" if username else f"id:{user_id}"
        if fecha_reg:
            dt_reg = datetime.fromisoformat(fecha_reg)
            dias = (ahora - dt_reg).days
            lineas.append(
                f"• <b>{_escape_html(nombre)}</b> ({alias}) "
                f"— entró el {dt_reg.strftime('%d/%m/%Y')} — {dias} días sin mensaje"
            )
        else:
            lineas.append(f"• <b>{_escape_html(nombre)}</b> ({alias})")
    await _send_long_message(bot, ADMIN_ID, "\n".join(lineas), "HTML")
    logger.info(f"[nuevos] Aviso vencidos: {len(usuarios)} usuario(s).")


async def handler_expulsarnoparticipa(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _pendientes_noparticipa

    if not _pendientes_noparticipa:
        await update.message.reply_text("No hay usuarios pendientes. Ejecuta /noparticipa primero.")
        return

    expulsados: list[str] = []
    ausentes:   list[str] = []
    errores:    list[str] = []

    for user_id, nombre, username, _, _ in _pendientes_noparticipa:
        estado, texto = await _ejecutar_expulsion(
            context.bot, user_id, nombre, username, "noparticipa"
        )
        if estado == "expulsado":
            expulsados.append(texto)
        elif estado == "ausente":
            ausentes.append(texto)
        else:
            errores.append(texto)
        
        # Rate limiting artificial para evitar FloodControl
        await asyncio.sleep(0.5)

    _pendientes_noparticipa.clear()

    await _send_long_message(
        context.bot, ADMIN_ID,
        _construir_resultado_expulsion(expulsados, ausentes, errores),
        "HTML",
    )


# ---------------------------------------------------------------------------
# /expulsarnuevos — expulsar nuevos usuarios con plazo vencido
# ---------------------------------------------------------------------------

async def handler_expulsarnuevos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lista los nuevos usuarios con plazo vencido y pide confirmación para expulsarlos."""
    global _pendientes_nuevos

    if NEW_USER_GRACE_PERIOD_DAYS == 0:
        await update.message.reply_text(
            "La gestión de nuevos usuarios inactivos está deshabilitada "
            "(NEW_USER_GRACE_PERIOD_DAYS=0)."
        )
        return

    _pendientes_nuevos = obtener_nuevos_usuarios_a_expulsar()

    if not _pendientes_nuevos:
        await update.message.reply_text("No hay nuevos usuarios con el plazo vencido.")
        return

    ahora = datetime.now(timezone.utc)
    lineas = [
        f"🚨 <b>Nuevos usuarios con plazo vencido ({len(_pendientes_nuevos)})</b>\n",
        f"Sin ningún mensaje tras {NEW_USER_GRACE_PERIOD_DAYS} días en el grupo.\n",
    ]
    for user_id, nombre, username, _, fecha_reg in _pendientes_nuevos:
        alias = f"@{username}" if username else f"id:{user_id}"
        if fecha_reg:
            dt_reg = datetime.fromisoformat(fecha_reg)
            dias = (ahora - dt_reg).days
            lineas.append(
                f"• <b>{_escape_html(nombre)}</b> ({alias}) "
                f"— entró el {dt_reg.strftime('%d/%m/%Y')} — {dias} días sin mensaje"
            )
        else:
            lineas.append(f"• <b>{_escape_html(nombre)}</b> ({alias})")

    keyboard = [[
        InlineKeyboardButton("✅ Confirmar", callback_data="nuevos_confirm"),
        InlineKeyboardButton("❌ Cancelar",  callback_data="nuevos_cancel"),
    ]]
    await _send_long_message(context.bot, ADMIN_ID, "\n".join(lineas), "HTML")
    await update.message.reply_text(
        "¿Confirmas la expulsión?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    logger.info(f"[nuevos] Confirmación solicitada: {len(_pendientes_nuevos)} usuario(s).")


async def handler_callback_nuevos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja la confirmación/cancelación de /expulsarnuevos."""
    query = update.callback_query
    
    if query.from_user.id != ADMIN_ID:
        try:
            await query.answer("No tienes permiso para realizar esta acción.", show_alert=True)
        except BadRequest:
            pass
        return

    global _pendientes_nuevos
    try:
        await query.answer()
    except BadRequest:
        pass

    if query.data == "nuevos_cancel":
        _pendientes_nuevos = []
        await query.edit_message_text("❌ Operación cancelada.")
        return

    if query.data == "nuevos_confirm":
        if not _pendientes_nuevos:
            await query.edit_message_text("No hay usuarios pendientes de expulsión.")
            return

        expulsados: list[str] = []
        ausentes:   list[str] = []
        errores:    list[str] = []

        for user_id, nombre, username, _, _ in _pendientes_nuevos:
            estado, texto = await _ejecutar_expulsion(
                context.bot, user_id, nombre, username, "nuevos"
            )
            if estado == "expulsado":
                expulsados.append(texto)
            elif estado == "ausente":
                ausentes.append(texto)
            else:
                errores.append(texto)
            
            # Rate limiting artificial para evitar FloodControl
            await asyncio.sleep(0.5)

        _pendientes_nuevos = []

        await query.edit_message_text(
            _construir_resultado_expulsion(expulsados, ausentes, errores),
            parse_mode="HTML",
        )


# ---------------------------------------------------------------------------
# /nuevos — informe de nuevos usuarios sin participar
# ---------------------------------------------------------------------------

async def handler_nuevos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra todos los nuevos usuarios sin mensajes: en período de gracia y vencidos."""
    if NEW_USER_GRACE_PERIOD_DAYS == 0:
        await update.message.reply_text(
            "La gestión de nuevos usuarios está deshabilitada (NEW_USER_GRACE_PERIOD_DAYS=0)."
        )
        return

    usuarios = obtener_todos_nuevos_sin_mensajes()
    usuarios, eliminados = await _filtrar_miembros_activos(context.bot, usuarios)
    ahora = datetime.now(timezone.utc)

    en_periodo: list[str] = []
    vencidos:   list[str] = []

    for user_id, nombre, username, _, fecha_reg in usuarios:
        alias = f"@{username}" if username else f"id:{user_id}"
        dt_reg = datetime.fromisoformat(fecha_reg)
        dias_transcurridos = (ahora - dt_reg).days
        dias_restantes = NEW_USER_GRACE_PERIOD_DAYS - dias_transcurridos
        fecha_str = dt_reg.strftime('%d/%m/%Y')
        if dias_restantes > 0:
            en_periodo.append(
                f"• <b>{_escape_html(nombre)}</b> ({alias}) "
                f"— entró el {fecha_str} — quedan {dias_restantes} día(s)"
            )
        else:
            dias_pasados = abs(dias_restantes)
            vencidos.append(
                f"• <b>{_escape_html(nombre)}</b> ({alias}) "
                f"— entró el {fecha_str} — vencido hace {dias_pasados} día(s)"
            )

    if not en_periodo and not vencidos:
        msg = f"No hay nuevos usuarios sin participar en los últimos {NEW_USER_GRACE_PERIOD_DAYS + 1} días."
        if eliminados:
            msg += f"\n({eliminados} usuario(s) ya no estaban en el grupo y han sido eliminados de BD.)"
        await update.message.reply_text(msg)
        return

    lineas = [f"📋 <b>Nuevos usuarios sin participar</b> (período: {NEW_USER_GRACE_PERIOD_DAYS} días)\n"]
    if eliminados:
        lineas.append(f"ℹ️ {eliminados} usuario(s) ya no estaban en el grupo y han sido eliminados de BD.\n")

    if en_periodo:
        lineas.append(f"⏳ <b>En período de gracia ({len(en_periodo)})</b>\n")
        lineas.extend(en_periodo)

    if vencidos:
        if en_periodo:
            lineas.append("")
        lineas.append(f"🚨 <b>Plazo vencido ({len(vencidos)}) — usa /expulsarnuevos</b>\n")
        lineas.extend(vencidos)

    await _send_long_message(context.bot, ADMIN_ID, "\n".join(lineas), "HTML")
    logger.info(f"[nuevos] Informe: {len(en_periodo)} en período, {len(vencidos)} vencidos.")


# ---------------------------------------------------------------------------
# /help — referencia de comandos disponibles
# ---------------------------------------------------------------------------

async def handler_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envía al admin la referencia completa de comandos disponibles."""
    if update.message is None:
        logger.warning("[help] update.message es None; ignorando.")
        return
    texto = (
        "📖 <b>Comandos disponibles</b>\n"
        "\n"
        "📊 <b>Estadísticas</b>\n"
        "/report — Reporte completo TOP5 + DOWN5 + avisos de inactividad\n"
        "/report TOP — Solo TOP 5 más activos\n"
        "/report DOWN — Solo DOWN 5 menos activos\n"
        "/infouser &lt;userid | @username | nombre&gt; — Info detallada de un usuario\n"
        "\n"
        "✨ <b>Personalización</b>\n"
        "/setwelcome &lt;mensaje&gt; — Configura el saludo de nuevos integrantes\n"
        "  Variables: {nombre}, {username}, {id}, {minutos}\n"
        "\n"
        "🔇 <b>Inactividad general</b>\n"
        "/noparticipa — Lista usuarios sin mensajes desde hace más de "
        f"{MAX_DAYS_INACTIVE_WARNING} días\n"
        "/expulsarnoparticipa — Expulsa los listados por /noparticipa\n"
        "/ok — Confirma las expulsiones pendientes del reporte diario\n"
        "/moratoria — Resetea el contador de inactividad para todos los inactivos\n"
        "\n"
        "👥 <b>Nuevos usuarios</b>\n"
        f"/nuevos — Informe de nuevos miembros sin participar (período: {NEW_USER_GRACE_PERIOD_DAYS} días)\n"
        "/expulsarnuevos — Expulsa nuevos con plazo vencido (con confirmación)\n"
        "\n"
        "🔧 <b>Herramientas</b>\n"
        "/kick DOWN — Expulsión interactiva de los 5 usuarios menos activos\n"
        "/recalcularfechas [YYYY-MM-DD] — Re-escanea el historial para corregir fechas\n"
        "\n"
        "/help — Muestra este mensaje"
    )
    await update.message.reply_html(texto)


# ---------------------------------------------------------------------------
# /recalcularfechas — re-escanear historial para corregir ultimo_mensaje
# ---------------------------------------------------------------------------

async def handler_recalcularfechas(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Re-escanea el historial del grupo desde una fecha dada para corregir ultimo_mensaje.
    Uso: /recalcularfechas [YYYY-MM-DD]  (por defecto: hace 365 días)
    No modifica total_mensajes; solo actualiza fechas de última actividad."""
    args = context.args
    if args:
        try:
            desde = datetime.fromisoformat(args[0]).replace(tzinfo=timezone.utc)
        except ValueError:
            await update.message.reply_text(
                "Formato inválido. Usa: /recalcularfechas YYYY-MM-DD\n"
                "Ejemplo: /recalcularfechas 2024-12-13"
            )
            return
    else:
        desde = datetime.now(timezone.utc) - timedelta(days=365)

    await update.message.reply_text(
        f"⏳ Recalculando fechas desde {desde.strftime('%d/%m/%Y')}...\n"
        "Esto puede tardar unos minutos."
    )

    total  = 0
    activos: set[int] = set()
    try:
        async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
            async for mensaje in client.iter_messages(GRUPO_ID):
                if mensaje.date <= desde:
                    break
                if not mensaje.sender or not isinstance(mensaje.sender, TelethonUser):
                    continue
                if mensaje.sender.bot:
                    continue
                tiene_contenido = (
                    bool(mensaje.text)
                    or mensaje.photo is not None
                    or mensaje.video is not None
                )
                if not tiene_contenido:
                    continue
                remitente = mensaje.sender
                nombre = (
                    f"{remitente.first_name or ''} {remitente.last_name or ''}".strip()
                    or str(remitente.id)
                )
                registrar_actividad_recovery(remitente.id, nombre, remitente.username, mensaje.date)
                activos.add(remitente.id)
                total += 1
    except Exception as exc:
        logger.warning(f"[recalcularfechas] Error Telethon: {exc}")
        await update.message.reply_text(f"❌ Error al conectar con Telethon: {exc}")
        return

    logger.info(f"[recalcularfechas] {total:,} mensajes, {len(activos)} usuarios actualizados.")
    await update.message.reply_text(
        f"✅ Recalculación completada.\n"
        f"{total:,} mensajes procesados · {len(activos)} usuarios actualizados."
    )


# ---------------------------------------------------------------------------
# /moratoria — resetear inactividad de todos los inactivos a hoy
# ---------------------------------------------------------------------------

def resetear_inactividad() -> int:
    """Actualiza ultimo_mensaje a ahora para todos los usuarios inactivos. Devuelve filas afectadas."""
    limite = (datetime.now(timezone.utc) - timedelta(days=MAX_DAYS_INACTIVE_WARNING)).isoformat()
    ahora  = datetime.now(timezone.utc).isoformat()
    cur = _conn.execute("""
        UPDATE usuarios
        SET    ultimo_mensaje = ?
        WHERE  (total_mensajes > 0 AND ultimo_mensaje < ?)
           OR  (total_mensajes = 0 AND (fecha_registro IS NULL OR fecha_registro < ?))
    """, (ahora, limite, limite))
    _conn.commit()
    return cur.rowcount


async def handler_moratoria(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    afectados = resetear_inactividad()
    logger.info(f"[moratoria] Inactividad reseteada para {afectados} usuario(s).")
    await update.message.reply_text(
        f"✅ Moratoria aplicada. {afectados} usuario(s) con inactividad reseteada a hoy.\n"
        "El contador de inactividad empieza desde ahora para todos ellos."
    )


async def handler_infouser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Proporciona información detallada de un usuario buscado por ID, username o nombre."""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("⚠️ Uso: /infouser <userid | @username | nombre>")
        return

    termino = " ".join(context.args)
    usuarios = buscar_usuarios(termino)

    if not usuarios:
        await update.message.reply_text(f"❌ No se encontró ningún usuario con: <b>{_escape_html(termino)}</b>", parse_mode="HTML")
        return

    if len(usuarios) > 1:
        lineas = [f"🔍 <b>Se encontraron {len(usuarios)} coincidencias:</b>\n"]
        for uid, nom, usr, total, _, _ in usuarios[:5]:
            alias = f"@{_escape_html(usr)}" if usr else "sin alias"
            lineas.append(f"• <b>{_escape_html(nom)}</b> ({alias}) — ID: <code>{uid}</code>")
        if len(usuarios) > 5:
            lineas.append(f"\n<i>...y {len(usuarios)-5} más. Refina la búsqueda o usa el ID.</i>")
        await update.message.reply_html("\n".join(lineas))
        return

    # Un solo usuario encontrado
    uid, nom, usr, total, ult, reg = usuarios[0]
    ahora = datetime.now(timezone.utc)
    
    # Determinar estado
    estado = "✅ Activo"
    limite_warning = (ahora - timedelta(days=MAX_DAYS_INACTIVE_WARNING)).isoformat()
    limite_removal = (ahora - timedelta(days=MAX_DAYS_INACTIVE_REMOVAL)).isoformat()
    
    ref_fecha = ult or reg
    if ref_fecha:
        if ref_fecha < limite_removal:
            estado = "🚨 Candidato a expulsión"
        elif ref_fecha < limite_warning:
            estado = "⚠️ Candidato a aviso"

    alias = f"@{_escape_html(usr)}" if usr else "n/a"
    
    def format_iso(iso_str):
        if not iso_str: return "n/a"
        try:
            dt = datetime.fromisoformat(iso_str)
            dias = (ahora - dt).days
            return f"{dt.strftime('%d/%m/%Y')} ({dias} días)"
        except: return iso_str

    texto = (
        f"👤 <b>Información de Usuario</b>\n\n"
        f"<b>Nombre:</b> {_escape_html(nom)}\n"
        f"<b>Username:</b> {alias}\n"
        f"<b>ID:</b> <code>{uid}</code>\n"
        f"<b>Estado:</b> {estado}\n"
        f"<b>Mensajes:</b> {total:,}\n"
        f"<b>Registrado:</b> {format_iso(reg)}\n"
        f"<b>Último msg:</b> {format_iso(ult)}"
    )
    
    await update.message.reply_html(texto)
    logger.info(f"[admin] /infouser ejecutado para: {termino}")


# ---------------------------------------------------------------------------
# /report — reporte manual bajo demanda
# ---------------------------------------------------------------------------

async def handler_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Envía reportes de actividad. Soporta subcomandos:
    /report      -> Reporte completo (TOP+DOWN) + inactividad
    /report TOP  -> Solo TOP 5
    /report DOWN -> Solo DOWN 5
    """
    args = context.args
    subcomando = args[0].upper() if args else None

    if subcomando == "TOP":
        top5 = obtener_top5()
        if not top5:
            await update.message.reply_text("Sin datos en la BD.")
            return
        texto = _construir_seccion_top5(top5)
        await update.message.reply_html(texto)
        logger.info(f"[report] Subcomando TOP enviado al admin (id={ADMIN_ID}).")
        return

    if subcomando == "DOWN":
        down5 = obtener_down5()
        if not down5:
            await update.message.reply_text("Sin datos en la BD.")
            return
        texto = _construir_seccion_down5(down5)
        await update.message.reply_html(texto)
        logger.info(f"[report] Subcomando DOWN enviado al admin (id={ADMIN_ID}).")
        return

    # Reporte completo (sin argumentos o argumento desconocido)
    texto = _construir_texto_reporte()
    if not texto:
        await update.message.reply_text("Sin datos en la BD.")
        return

    top5  = obtener_top5()
    down5 = obtener_down5()
    _loguear_reporte(top5, down5)

    await _send_long_message(context.bot, ADMIN_ID, texto, "HTML")
    logger.info(f"[report] Reporte completo enviado al admin (id={ADMIN_ID}).")

    await enviar_aviso_inactivos(context.bot)
    await enviar_reporte_expulsion(context.bot)
    if NEW_USER_GRACE_PERIOD_DAYS > 0:
        await check_nuevos_proximos_a_vencer(context.bot)
        await avisar_nuevos_vencidos(context.bot)


# Usuarios pendientes de expulsión vía /kick DOWN
_pendientes_kick: list[tuple[int, str, str | None, int, str | None, str | None]] = []


async def handler_kick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Identifica a los usuarios menos activos y pide confirmación para expulsarlos.
    Uso: /kick DOWN
    """
    args = context.args
    if not args or args[0].upper() != "DOWN":
        await update.message.reply_text("Uso: /kick DOWN")
        return

    global _pendientes_kick
    _pendientes_kick = obtener_down5()

    if not _pendientes_kick:
        await update.message.reply_text("No hay usuarios para expulsar.")
        return

    lineas = ["⚠️ <b>¿Expulsar a los usuarios menos activos?</b>\n"]
    for i, (user_id, nombre, username, total, ultimo, registro) in enumerate(_pendientes_kick):
        alias = f"@{username}" if username else f"id:{user_id}"
        icono = "👻" if total == 0 else "💤"
        lineas.append(f"{icono} <b>{_escape_html(nombre)}</b> ({alias}) — {total:,} msgs")

    keyboard = [
        [
            InlineKeyboardButton("✅ Confirmar", callback_data="kick_confirm"),
            InlineKeyboardButton("❌ Cancelar", callback_data="kick_cancel"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_html("\n".join(lineas), reply_markup=reply_markup)


async def handler_callback_kick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja la respuesta de los botones de confirmación de /kick."""
    query = update.callback_query

    if query.from_user.id != ADMIN_ID:
        try:
            await query.answer("No tienes permiso para realizar esta acción.", show_alert=True)
        except BadRequest:
            pass
        return

    try:
        await query.answer()
    except BadRequest:
        pass

    if query.data == "kick_cancel":
        global _pendientes_kick
        _pendientes_kick = []
        await query.edit_message_text("❌ Operación cancelada.")
        return

    if query.data == "kick_confirm":
        if not _pendientes_kick:
            await query.edit_message_text("No hay usuarios pendientes de expulsión.")
            return

        expulsados: list[str] = []
        ausentes:   list[str] = []
        errores:    list[str] = []

        for user_id, nombre, username, _, _, _ in _pendientes_kick:
            estado, texto = await _ejecutar_expulsion(
                context.bot, user_id, nombre, username, "kick"
            )
            if estado == "expulsado":
                expulsados.append(texto)
            elif estado == "ausente":
                ausentes.append(texto)
            else:
                errores.append(texto)
            
            # Rate limiting artificial para evitar FloodControl
            await asyncio.sleep(0.5)

        _pendientes_kick = []

        await query.edit_message_text(
            _construir_resultado_expulsion(expulsados, ausentes, errores),
            parse_mode="HTML",
        )


# ---------------------------------------------------------------------------
# Arranque: recuperar mensajes perdidos y enviar reporte al admin
# ---------------------------------------------------------------------------

async def actualizar_desde_ultima_ejecucion() -> tuple[int, set[int]]:
    """
    Usa Telethon para recuperar los mensajes del grupo enviados desde el
    ultimo_registro guardado en bot_state.json.

    Devuelve (total_mensajes_procesados, set de user_ids con actividad en ese periodo).
    """
    global _ultimo_registro
    ultimo_registro = leer_ultimo_registro()

    if ultimo_registro is None:
        logger.info("[arranque] Sin registro previo; omitiendo recuperación.")
        return 0, set()

    _ultimo_registro = ultimo_registro
    logger.info(f"[arranque] Recuperando mensajes desde {ultimo_registro.isoformat()} ...")

    total        = 0
    activos: set[int] = set()

    try:
        async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
            async for mensaje in client.iter_messages(GRUPO_ID):
                if mensaje.date <= ultimo_registro:
                    break

                if not mensaje.sender or not isinstance(mensaje.sender, TelethonUser):
                    continue
                if mensaje.sender.bot:
                    continue

                tiene_contenido = (
                    bool(mensaje.text)
                    or mensaje.photo is not None
                    or mensaje.video is not None
                )
                if not tiene_contenido:
                    continue

                remitente = mensaje.sender
                nombre = (
                    f"{remitente.first_name or ''} {remitente.last_name or ''}".strip()
                    or str(remitente.id)
                )
                registrar_actividad_recovery(remitente.id, nombre, remitente.username, mensaje.date)
                activos.add(remitente.id)
                total += 1

    except Exception as exc:
        logger.warning(f"[arranque] No se pudo conectar con Telethon: {exc}")
        logger.warning("[arranque] Se omite la recuperación de mensajes perdidos.")

    logger.info(f"[arranque] Recuperación completada: {total:,} mensajes, "
                f"{len(activos)} usuarios con actividad.")
    return total, activos


async def enviar_resumen_recuperados(bot, recuperados: list[tuple]) -> None:
    """Envía al admin la lista de usuarios inactivos que han vuelto a ser activos."""
    if not recuperados:
        return

    logger.info(f"[recuperados] {len(recuperados)} usuario(s) han vuelto a estar activos:")
    lineas = [
        "✅ <b>Usuarios que han vuelto a participar</b>\n",
        "Los siguientes usuarios estaban inactivos pero han registrado actividad "
        "mientras el bot estaba detenido.\n",
    ]
    for user_id, nombre, username, total, ultimo, registro in recuperados:
        alias = f"@{username}" if username else f"id:{user_id}"
        dt    = datetime.fromisoformat(ultimo)
        lineas.append(
            f"• <b>{_escape_html(nombre)}</b> ({alias}) — "
            f"última actividad: {dt.strftime('%d/%m/%Y %H:%M')}"
        )
        logger.info(f"  · {nombre} ({alias}) | última actividad: {dt.strftime('%d/%m/%Y')}")

    await _send_long_message(bot, ADMIN_ID, "\n".join(lineas), "HTML")


async def post_init(application: Application) -> None:
    """
    Callback ejecutado una vez que la aplicación está inicializada.
    1. Obtiene usuarios inactivos ANTES de procesar la recuperación.
    2. Recupera mensajes perdidos desde ultimo_registro.
    3. Detecta inactivos que han vuelto y envía resumen al admin.
    4. Guarda el nuevo estado en bot_state.json.
    5. Envía reporte de estadísticas, aviso de inactividad y reporte de expulsión.
    """
    global _ultimo_registro

    fecha_arranque = datetime.now(timezone.utc)

    # Snapshot de usuarios inactivos ANTES de procesar mensajes perdidos
    inactivos_antes = {
        row[0]: row
        for row in obtener_usuarios_inactivos(MAX_DAYS_INACTIVE_WARNING)
    }

    mensajes_nuevos, activos_recuperacion = await actualizar_desde_ultima_ejecucion()

    # Usuarios que estaban inactivos y han tenido actividad durante el periodo caído
    recuperados = [
        inactivos_antes[uid]
        for uid in activos_recuperacion
        if uid in inactivos_antes
    ]

    # Persistir estado: fecha de arranque y último registro procesado
    _ultimo_registro = _ultimo_registro or fecha_arranque
    guardar_bot_state(fecha_arranque, _ultimo_registro)
    logger.info(f"[arranque] bot_state.json actualizado. "
                f"arranque={fecha_arranque.isoformat()} | "
                f"ultimo_registro={_ultimo_registro.isoformat()}")

    await enviar_resumen_recuperados(application.bot, recuperados)

    top5  = obtener_top5()
    down5 = obtener_down5()

    if not top5:
        logger.info("[arranque] Sin datos en la BD; no se genera reporte.")
        return

    _loguear_reporte(top5, down5)

    texto = _construir_texto_reporte()
    if texto:
        nota = f"\n\n<i>🔄 {mensajes_nuevos:,} mensajes recuperados en este arranque</i>"
        await application.bot.send_message(
            chat_id=ADMIN_ID,
            text=texto + nota,
            parse_mode="HTML",
        )
        logger.info(f"[arranque] Reporte enviado al admin (id={ADMIN_ID}).")

    await enviar_aviso_inactivos(application.bot)
    await enviar_reporte_expulsion(application.bot)
    if NEW_USER_GRACE_PERIOD_DAYS > 0:
        await check_nuevos_proximos_a_vencer(application.bot)
        await avisar_nuevos_vencidos(application.bot)

    # Registrar comandos en el menú de Telegram solo para el admin
    comandos_admin = [
        BotCommand("report",              "Reporte TOP5 + DOWN5 + avisos"),
        BotCommand("infouser",            "Info detallada de un usuario"),
        BotCommand("setwelcome",          "Configura el mensaje de bienvenida"),
        BotCommand("noparticipa",         f"Usuarios sin mensajes > {MAX_DAYS_INACTIVE_WARNING}d"),
        BotCommand("expulsarnoparticipa", "Expulsa listados por /noparticipa"),
        BotCommand("ok",                  "Confirma expulsiones pendientes"),
        BotCommand("moratoria",           "Resetea contador de inactividad"),
        BotCommand("nuevos",              f"Nuevos sin participar (>{NEW_USER_GRACE_PERIOD_DAYS}d)"),
        BotCommand("expulsarnuevos",      "Expulsa nuevos con plazo vencido"),
        BotCommand("kick",                "Expulsión interactiva menos activos"),
        BotCommand("recalcularfechas",    "Re-escanea historial para corregir fechas"),
        BotCommand("help",                "Muestra este listado de comandos"),
    ]
    await application.bot.set_my_commands(
        comandos_admin,
        scope=BotCommandScopeChat(chat_id=ADMIN_ID),
    )
    logger.info("[arranque] Comandos registrados en el menú de Telegram del admin.")


async def job_revisar_probacion(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Tarea periódica que revisa los periodos de probación y actúa si vencen."""
    ahora = datetime.now(timezone.utc)
    usuarios = obtener_todos_probacion()

    for user_id, nombre, w_msg_id, a_msg_id, expiry_str, status in usuarios:
        try:
            expiry = datetime.fromisoformat(expiry_str)
        except (ValueError, TypeError) as e:
            logger.error(f"[probacion] Error en formato de fecha para {user_id}: {expiry_str}. Eliminando registro.")
            eliminar_probacion(user_id)
            continue

        if ahora > expiry:
            logger.info(f"[probacion] Vencimiento detectado para {nombre} (id={user_id}, status={status}).")
            
            # 1. Verificar si sigue en la BD de miembros activos
            if not usuario_existe(user_id):
                logger.info(f"[probacion] Usuario {nombre} (id={user_id}) no figura en tabla usuarios. Limpiando.")
                await procesar_salida_usuario(context.bot, user_id)
                continue

            # 2. Verificar membresía real en Telegram
            try:
                member = await context.bot.get_chat_member(chat_id=GRUPO_ID, user_id=user_id)
                logger.info(f"[probacion] Estado de {nombre} (id={user_id}) en Telegram: {member.status}")
                if member.status in ("left", "kicked"):
                    await procesar_salida_usuario(context.bot, user_id)
                    logger.info(f"[probacion] Usuario {nombre} (id={user_id}) ya no está en el grupo. Limpiando.")
                    continue
            except BadRequest as e:
                # Si el usuario o el chat no son válidos, o no se encuentra en el chat, limpiamos para no buclear
                await procesar_salida_usuario(context.bot, user_id)
                logger.info(f"[probacion] Error BadRequest al consultar {user_id}: {e}. Registro de probación limpiado.")
                continue
            except Exception as e:
                logger.warning(f"[probacion] Error comprobando membresía de {user_id}: {e}")
                continue

            if status == 0:
                # Primer plazo vencido (15m): enviar aviso
                expiry_aviso = ahora + timedelta(minutes=PROBATION_DEADLINE_2_MIN)
                txt_aviso = (
                    f"⚠️ <b>Atención {_escape_html(nombre)}</b>:\n\n"
                    f"Han pasado {PROBATION_DEADLINE_1_MIN} minutos sin participación. "
                    f"Te quedan <b>{PROBATION_DEADLINE_2_MIN} minutos</b> para enviar un mensaje "
                    "o serás expulsado/a automáticamente."
                )
                try:
                    msg_a = await context.bot.send_message(chat_id=GRUPO_ID, text=txt_aviso, parse_mode="HTML")
                    actualizar_probacion(user_id, msg_a.message_id, expiry_aviso, 1)
                    logger.info(f"Aviso de probación enviado a {nombre} (id={user_id}). Nuevo plazo: {expiry_aviso}")
                except Exception as e:
                    logger.error(f"Error al enviar aviso de probación a {user_id}: {e}")
            
            elif status == 1:
                # Segundo plazo vencido (2m): expulsar
                try:
                    try:
                        # Intentar obtener username de la BD de usuarios antes de la expulsión
                        username_db = None
                        cursor = _conn.execute("SELECT username FROM usuarios WHERE user_id = ?", (user_id,))
                        row = cursor.fetchone()
                        if row:
                            username_db = row[0]
                        
                        registrar_salida_probacion(user_id, nombre, username_db, "expulsado")
                        
                        # Expulsar (ban + unban silencioso)
                        await context.bot.ban_chat_member(chat_id=GRUPO_ID, user_id=user_id)
                        await context.bot.unban_chat_member(chat_id=GRUPO_ID, user_id=user_id)
                        logger.info(f"Usuario {nombre} (id={user_id}) expulsado automáticamente por inactividad inicial.")
                    except RetryAfter as exc:
                        logger.warning(f"FloodWait detectado al expulsar {nombre} ({user_id}): esperando {exc.retry_after} segundos...")
                        await asyncio.sleep(exc.retry_after)
                        # Reintentar una vez
                        await context.bot.ban_chat_member(chat_id=GRUPO_ID, user_id=user_id)
                        await context.bot.unban_chat_member(chat_id=GRUPO_ID, user_id=user_id)
                        logger.info(f"Usuario {nombre} (id={user_id}) expulsado automáticamente tras FloodWait.")
                    
                    # Si todo salió bien o se reintentó con éxito, limpiamos
                    await procesar_salida_usuario(context.bot, user_id)
                except Exception as e:
                    logger.error(f"Error al expulsar al usuario {user_id} por probación: {e}")
                    # En caso de error permanente (ej. bot no es admin o usuario es admin),
                    # limpiamos igualmente para no dejar mensajes huérfanos ni entrar en bucle infinito
                    await procesar_salida_usuario(context.bot, user_id)


# ---------------------------------------------------------------------------
# Tarea programada: resumen diario
# ---------------------------------------------------------------------------

async def enviar_resumen_diario(context: ContextTypes.DEFAULT_TYPE) -> None:
    texto = _construir_texto_reporte()
    if not texto:
        logger.info("Sin datos para el resumen diario.")
        return

    top5  = obtener_top5()
    down5 = obtener_down5()
    _loguear_reporte(top5, down5)

    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=texto,
        parse_mode="HTML",
    )
    logger.info("Resumen diario enviado al admin.")
    limpiar_salidas_probacion()
    logger.info("Registro de salidas de probación limpiado.")

    await enviar_aviso_inactivos(context.bot)
    await enviar_reporte_expulsion(context.bot)
    if NEW_USER_GRACE_PERIOD_DAYS > 0:
        await check_nuevos_proximos_a_vencer(context.bot)
        await avisar_nuevos_vencidos(context.bot)


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log the error and send a telegram message to notify the developer."""
    logger.error(f"Excepción manejada por el bot: {context.error}", exc_info=context.error)
    # Si es una query caducada, no hace falta asustar al admin
    if isinstance(context.error, BadRequest) and "query is too old" in str(context.error).lower():
        return


def main() -> None:
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN no encontrado en el archivo .env")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_error_handler(error_handler)

    app.add_handler(
        ChatMemberHandler(handler_miembro, ChatMemberHandler.CHAT_MEMBER)
    )
    app.add_handler(
        ChatJoinRequestHandler(handler_solicitud_union)
    )
    _filtro_actividad = (
        filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL
        | filters.AUDIO | filters.ANIMATION | filters.VOICE | filters.VIDEO_NOTE
    ) & filters.ChatType.GROUPS
    app.add_handler(MessageHandler(_filtro_actividad, handler_mensaje))

    _admin_privado = filters.ChatType.PRIVATE & filters.User(ADMIN_ID)
    app.add_handler(CommandHandler("ok",                   handler_ok,                   filters=_admin_privado))
    app.add_handler(CommandHandler("setwelcome",           handler_setwelcome,           filters=_admin_privado))
    app.add_handler(CommandHandler("noparticipa",          handler_noparticipa,          filters=_admin_privado))
    app.add_handler(CommandHandler("expulsarnoparticipa",  handler_expulsarnoparticipa,  filters=_admin_privado))
    app.add_handler(CommandHandler("moratoria",            handler_moratoria,            filters=_admin_privado))
    app.add_handler(CommandHandler("recalcularfechas",     handler_recalcularfechas,     filters=_admin_privado))
    app.add_handler(CommandHandler("report",               handler_report,               filters=_admin_privado))
    app.add_handler(CommandHandler("infouser",             handler_infouser,             filters=_admin_privado))
    app.add_handler(CommandHandler("kick",                 handler_kick,                 filters=_admin_privado))
    app.add_handler(CommandHandler("nuevos",               handler_nuevos,               filters=_admin_privado))
    app.add_handler(CommandHandler("expulsarnuevos",       handler_expulsarnuevos,       filters=_admin_privado))
    app.add_handler(CommandHandler("help",                 handler_help,                 filters=_admin_privado))

    # Notificaciones de acceso no autorizado (para cualquier mensaje privado no capturado por filtros de admin)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE, handler_unauthorized))

    app.add_handler(CallbackQueryHandler(handler_callback_kick,   pattern="^kick_"))
    app.add_handler(CallbackQueryHandler(handler_callback_nuevos, pattern="^nuevos_"))

    job_queue = app.job_queue
    job_queue.run_repeating(
        callback=job_revisar_probacion,
        interval=30,
        first=10,
        name="revisar_probacion",
    )
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
        # Persistir el último registro procesado antes de cerrar
        ahora = datetime.now(timezone.utc)
        state = leer_bot_state()
        fecha_arranque = datetime.fromisoformat(state["fecha_arranque"]) \
            if "fecha_arranque" in state else ahora
        guardar_bot_state(fecha_arranque, _ultimo_registro or ahora)
        logger.info(f"[shutdown] bot_state.json persistido. "
                    f"ultimo_registro={(_ultimo_registro or ahora).isoformat()}")
        _conn.close()
        logger.info("BD cerrada. Bot detenido.")


if __name__ == "__main__":
    main()
