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

import logging
import os
import sqlite3
from datetime import datetime, time, timedelta, timezone

from dotenv import load_dotenv
from telegram import Update, ChatMemberUpdated
from telegram.ext import (
    Application,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
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

DB_PATH      = "estadisticas_grupo.db"
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            clave TEXT PRIMARY KEY,
            valor TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


_conn: sqlite3.Connection = get_conn()


def get_last_run() -> datetime | None:
    """Devuelve el timestamp de la última ejecución del bot, o None si no existe."""
    cur = _conn.execute("SELECT valor FROM metadata WHERE clave = 'last_run'")
    row = cur.fetchone()
    if row is None:
        return None
    return datetime.fromisoformat(row[0])


def set_last_run(dt: datetime) -> None:
    """Guarda el timestamp de la ejecución actual."""
    _conn.execute("""
        INSERT INTO metadata (clave, valor) VALUES ('last_run', ?)
        ON CONFLICT(clave) DO UPDATE SET valor = excluded.valor
    """, (dt.isoformat(),))
    _conn.commit()


def registrar_miembro(user_id: int, nombre: str, username: str | None) -> None:
    """Inserta el miembro con 0 mensajes si no existe aún en la BD."""
    ahora = datetime.now(timezone.utc).isoformat()
    _conn.execute("""
        INSERT OR IGNORE INTO usuarios (user_id, nombre, username, total_mensajes, ultimo_mensaje)
        VALUES (?, ?, ?, 0, ?)
    """, (user_id, nombre, username, ahora))
    _conn.commit()


def eliminar_miembro(user_id: int) -> None:
    """Elimina al usuario de la BD cuando sale o es expulsado del grupo."""
    _conn.execute("DELETE FROM usuarios WHERE user_id = ?", (user_id,))
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


def obtener_usuarios_inactivos(dias_warning: int) -> list[tuple[int, str, str | None, int, str]]:
    """Devuelve usuarios cuyo último mensaje fue hace más de `dias_warning` días."""
    limite = (datetime.now(timezone.utc) - timedelta(days=dias_warning)).isoformat()
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, ultimo_mensaje
        FROM   usuarios
        WHERE  ultimo_mensaje IS NOT NULL
          AND  ultimo_mensaje < ?
        ORDER  BY ultimo_mensaje ASC
    """, (limite,))
    return cur.fetchall()


def obtener_usuarios_para_expulsar() -> list[tuple[int, str, str | None, int, str]]:
    """Devuelve usuarios cuyo último mensaje superó MAX_DAYS_INACTIVE_REMOVAL días."""
    limite = (datetime.now(timezone.utc) - timedelta(days=MAX_DAYS_INACTIVE_REMOVAL)).isoformat()
    cur = _conn.execute("""
        SELECT user_id, nombre, username, total_mensajes, ultimo_mensaje
        FROM   usuarios
        WHERE  ultimo_mensaje IS NOT NULL
          AND  ultimo_mensaje < ?
        ORDER  BY ultimo_mensaje ASC
    """, (limite,))
    return cur.fetchall()


# Usuarios pendientes de expulsión, poblado por enviar_reporte_expulsion()
_pendientes_expulsion: list[tuple[int, str, str | None, int, str]] = []


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
    elif nuevo.status in estados_salida:
        eliminar_miembro(usuario.id)
        logger.info(f"Miembro eliminado: id={usuario.id} (estado={nuevo.status})")


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
# Reporte TOP 5 / DOWN 5
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


def _construir_texto_reporte() -> str | None:
    """Construye el texto Markdown del reporte TOP 5 / DOWN 5. Devuelve None si no hay datos."""
    top5  = obtener_top5()
    down5 = obtener_down5()

    if not top5:
        return None

    ahora    = datetime.now(tz=timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    medallas = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    calavers = ["💀", "😴", "🐌", "🦥", "👻"]

    lineas = [f"📊 *Estadísticas del grupo* — {ahora}\n"]

    lineas.append("🏆 *Top 5 — Más activos*\n")
    for i, (user_id, nombre, username, total, _) in enumerate(top5):
        lineas.append(_formatear_usuario(user_id, nombre, username, total, medallas[i]))

    lineas.append("\n💤 *Down 5 — Menos activos*\n")
    for i, (user_id, nombre, username, total, _) in enumerate(down5):
        lineas.append(_formatear_usuario(user_id, nombre, username, total, calavers[i]))

    return "\n".join(lineas)


def _loguear_reporte(top5, down5) -> None:
    logger.info("=== TOP 5 — Más activos ===")
    medallas = ["1º", "2º", "3º", "4º", "5º"]
    for i, (user_id, nombre, username, total, _) in enumerate(top5):
        alias = f"@{username}" if username else f"id:{user_id}"
        logger.info(f"  {medallas[i]} {nombre} ({alias}) — {total:,} mensajes")
    logger.info("=== DOWN 5 — Menos activos ===")
    for i, (user_id, nombre, username, total, _) in enumerate(down5):
        alias = f"@{username}" if username else f"id:{user_id}"
        logger.info(f"  {i+1}. {nombre} ({alias}) — {total:,} mensajes")


# ---------------------------------------------------------------------------
# Aviso de inactividad
# ---------------------------------------------------------------------------

def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


_TELEGRAM_MAX_LEN = 4096


async def _send_long_message(bot, chat_id: int, texto: str, parse_mode: str) -> None:
    """Envía un texto al chat partiéndolo por líneas completas si supera el límite."""
    trozo: list[str] = []
    longitud = 0
    for linea in texto.splitlines(keepends=True):
        if longitud + len(linea) > _TELEGRAM_MAX_LEN and trozo:
            await bot.send_message(
                chat_id=chat_id,
                text="".join(trozo),
                parse_mode=parse_mode,
            )
            trozo = []
            longitud = 0
        trozo.append(linea)
        longitud += len(linea)
    if trozo:
        await bot.send_message(
            chat_id=chat_id,
            text="".join(trozo),
            parse_mode=parse_mode,
        )


async def enviar_aviso_inactivos(bot) -> None:
    """Detecta usuarios inactivos y envía al admin la lista con fecha de expulsión."""
    inactivos = obtener_usuarios_inactivos(MAX_DAYS_INACTIVE_WARNING)
    if not inactivos:
        logger.info("[inactividad] No hay usuarios que superen el umbral de aviso.")
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

    for user_id, nombre, username, total, ultimo in inactivos:
        dt_ultimo = datetime.fromisoformat(ultimo)
        fecha_exp = dt_ultimo + timedelta(days=MAX_DAYS_INACTIVE_REMOVAL)
        alias     = f"@{username}" if username else f"id:{user_id}"
        lineas.append(
            f"• <b>{_escape_html(nombre)}</b> ({alias}) — "
            f"fin de plazo: <b>{fecha_exp.strftime('%d/%m/%Y')}</b>"
        )
        logger.info(
            f"  · {nombre} ({alias}) | último mensaje: "
            f"{dt_ultimo.strftime('%d/%m/%Y')} | "
            f"expulsión prevista: {fecha_exp.strftime('%d/%m/%Y')}"
        )

    await _send_long_message(bot, ADMIN_ID, "\n".join(lineas), "HTML")
    logger.info(f"[inactividad] Aviso enviado al admin (id={ADMIN_ID}).")


# ---------------------------------------------------------------------------
# Expulsión de usuarios inactivos
# ---------------------------------------------------------------------------

async def enviar_reporte_expulsion(bot) -> None:
    """Detecta usuarios que superan MAX_DAYS_INACTIVE_REMOVAL y envía el reporte al admin."""
    global _pendientes_expulsion
    _pendientes_expulsion = obtener_usuarios_para_expulsar()

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

    for user_id, nombre, username, total, ultimo in _pendientes_expulsion:
        dt_ultimo = datetime.fromisoformat(ultimo)
        alias     = f"@{username}" if username else f"id:{user_id}"
        lineas.append(
            f"• <b>{_escape_html(nombre)}</b> ({alias})\n"
            f"  └ Última actividad: {dt_ultimo.strftime('%d/%m/%Y')} | "
            f"{total:,} mensajes"
        )
        logger.info(f"  · {nombre} ({alias}) | último: {dt_ultimo.strftime('%d/%m/%Y')} | {total} msgs")

    await _send_long_message(bot, ADMIN_ID, "\n".join(lineas), "HTML")
    logger.info(f"[expulsión] Reporte enviado al admin (id={ADMIN_ID}). Esperando /ok ...")


async def handler_ok(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Expulsa los usuarios pendientes cuando el admin confirma con /ok."""
    if update.effective_user.id != ADMIN_ID:
        return

    if not _pendientes_expulsion:
        await update.message.reply_text("No hay usuarios pendientes de expulsión.")
        return

    expulsados = []
    errores    = []

    for user_id, nombre, username, total, _ in _pendientes_expulsion:
        alias = f"@{username}" if username else f"id:{user_id}"
        try:
            # Ban + unban inmediato: expulsa pero permite volver en el futuro
            await context.bot.ban_chat_member(chat_id=GRUPO_ID, user_id=user_id)
            await context.bot.unban_chat_member(chat_id=GRUPO_ID, user_id=user_id)
            eliminar_miembro(user_id)
            expulsados.append(f"• {_escape_html(nombre)} ({alias})")
            logger.info(f"[expulsión] Expulsado: {nombre} ({alias})")
        except Exception as exc:
            errores.append(f"• {_escape_html(nombre)} ({alias}): {_escape_html(str(exc))}")
            logger.warning(f"[expulsión] Error al expulsar {nombre} ({alias}): {exc}")

    _pendientes_expulsion.clear()

    lineas = [f"✅ <b>{len(expulsados)} usuario(s) expulsados:</b>\n"] + expulsados
    if errores:
        lineas += [f"\n⚠️ <b>{len(errores)} error(es):</b>\n"] + errores

    await _send_long_message(context.bot, ADMIN_ID, "\n".join(lineas), "HTML")


# ---------------------------------------------------------------------------
# Arranque: recuperar mensajes perdidos y enviar reporte al admin
# ---------------------------------------------------------------------------

async def actualizar_desde_ultima_ejecucion() -> int:
    """
    Usa Telethon para recuperar los mensajes del grupo enviados desde la
    última ejecución del bot. Devuelve el número de mensajes procesados.
    """
    last_run = get_last_run()
    ahora    = datetime.now(timezone.utc)

    if last_run is None:
        logger.info("[arranque] Sin registro de última ejecución; omitiendo recuperación.")
        set_last_run(ahora)
        return 0

    logger.info(f"[arranque] Recuperando mensajes desde {last_run.isoformat()} ...")

    total = 0
    try:
        async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
            async for mensaje in client.iter_messages(GRUPO_ID):
                # iter_messages va de más reciente a más antiguo; parar al llegar al límite
                if mensaje.date <= last_run:
                    break

                if not mensaje.sender or not isinstance(mensaje.sender, TelethonUser):
                    continue
                if mensaje.sender.bot:
                    continue
                if mensaje.text is None and mensaje.message is None:
                    continue

                remitente = mensaje.sender
                nombre = (
                    f"{remitente.first_name or ''} {remitente.last_name or ''}".strip()
                    or str(remitente.id)
                )
                registrar_mensaje(remitente.id, nombre, remitente.username, mensaje.date)
                total += 1

    except Exception as exc:
        logger.warning(f"[arranque] No se pudo conectar con Telethon: {exc}")
        logger.warning("[arranque] Se omite la recuperación de mensajes perdidos.")

    set_last_run(ahora)
    logger.info(f"[arranque] Recuperación completada: {total:,} mensajes nuevos procesados.")
    return total


async def post_init(application: Application) -> None:
    """
    Callback ejecutado una vez que la aplicación está inicializada.
    Recupera mensajes perdidos, genera el reporte y lo envía al admin.
    """
    mensajes_nuevos = await actualizar_desde_ultima_ejecucion()

    top5  = obtener_top5()
    down5 = obtener_down5()

    if not top5:
        logger.info("[arranque] Sin datos en la BD; no se genera reporte.")
        return

    _loguear_reporte(top5, down5)

    texto = _construir_texto_reporte()
    if texto:
        nota = f"\n\n_🔄 {mensajes_nuevos:,} mensajes recuperados en este arranque_"
        await application.bot.send_message(
            chat_id=ADMIN_ID,
            text=texto + nota,
            parse_mode="Markdown",
        )
        logger.info(f"[arranque] Reporte enviado al admin (id={ADMIN_ID}).")

    await enviar_aviso_inactivos(application.bot)
    await enviar_reporte_expulsion(application.bot)


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
        parse_mode="Markdown",
    )
    logger.info("Resumen diario enviado al admin.")


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main() -> None:
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN no encontrado en el archivo .env")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(
        ChatMemberHandler(handler_miembro, ChatMemberHandler.CHAT_MEMBER)
    )
    app.add_handler(
        MessageHandler(
            (filters.TEXT | filters.PHOTO | filters.VIDEO) & filters.ChatType.GROUPS,
            handler_mensaje,
        )
    )
    app.add_handler(
        CommandHandler(
            "ok",
            handler_ok,
            filters=filters.ChatType.PRIVATE & filters.User(ADMIN_ID),
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
