"""
init_historial.py
=================
Script de inicialización — se ejecuta UNA SOLA VEZ.

Usa Telethon (userbot) para:
  1. Leer el historial reciente del grupo (últimos 5 000 mensajes o último año,
     lo que ocurra primero) y poblar la BD SQLite con el conteo de mensajes y
     la fecha del último mensaje de cada usuario.
  2. Importar todos los miembros actuales del grupo (incluyendo los que nunca
     han enviado mensajes), registrándolos con total_mensajes = 0.

Requisitos:
  - pip install telethon python-dotenv
  - Archivo .env con: API_ID, API_HASH, BOT_TOKEN, GRUPO_ID

Uso:
  python init_historial.py [--fecha DDMMYYYY]

  --fecha DDMMYYYY  (Opcional) Fecha que se asignará como último_mensaje a los
                    usuarios que no hayan enviado ningún mensaje en el historial.
                    Si se omite, esos usuarios quedan con último_mensaje = NULL.

  (La primera ejecución pedirá tu número de teléfono y el código de Telegram)
"""

import argparse
import asyncio
import sqlite3
import os
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import User

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

load_dotenv()

API_ID   = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
GRUPO_ID = int(os.getenv("GRUPO_ID"))

DB_PATH      = "estadisticas_grupo.db"
SESSION_NAME = "sesion_admin"      # Nombre del archivo de sesión Telethon (.session)


# ---------------------------------------------------------------------------
# Base de datos
# ---------------------------------------------------------------------------

def init_db() -> sqlite3.Connection:
    """Crea (o abre) la BD y garantiza que la tabla 'usuarios' exista."""
    conn = sqlite3.connect(DB_PATH)
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


def upsert_usuario(conn: sqlite3.Connection,
                   user_id: int,
                   nombre: str,
                   username: str | None,
                   fecha: datetime) -> None:
    """
    Inserta el usuario si no existe o actualiza sus contadores.
    Suma 1 al total de mensajes y actualiza el último mensaje solo si
    la fecha recibida es más reciente que la almacenada.
    """
    fecha_str = fecha.isoformat()
    conn.execute("""
        INSERT INTO usuarios (user_id, nombre, username, total_mensajes, ultimo_mensaje)
        VALUES (?, ?, ?, 1, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            nombre         = excluded.nombre,
            username       = excluded.username,
            total_mensajes = total_mensajes + 1,
            ultimo_mensaje = CASE
                WHEN ultimo_mensaje IS NULL
                     OR excluded.ultimo_mensaje > ultimo_mensaje
                THEN excluded.ultimo_mensaje
                ELSE ultimo_mensaje
            END
    """, (user_id, nombre, username, fecha_str))


# ---------------------------------------------------------------------------
# Lógica principal
# ---------------------------------------------------------------------------

LIMITE_MENSAJES = 5_000
LIMITE_DIAS     = 365


async def leer_historial(client: TelegramClient, conn: sqlite3.Connection) -> None:
    """Itera los mensajes del grupo y actualiza la BD.

    Se detiene cuando se cumpla la primera de estas condiciones:
      - Se han procesado LIMITE_MENSAJES mensajes (5 000).
      - Se alcanza un mensaje anterior a LIMITE_DIAS días (1 año).
    """

    total_mensajes = 0
    total_usuarios: set[int] = set()
    fecha_limite   = datetime.now(timezone.utc) - timedelta(days=LIMITE_DIAS)

    print(f"[INFO] Leyendo historial del grupo {GRUPO_ID}...")
    print(f"[INFO] Límite: {LIMITE_MENSAJES:,} mensajes o {LIMITE_DIAS} días "
          f"(desde {fecha_limite.strftime('%Y-%m-%d')}), lo que ocurra primero.")

    async for mensaje in client.iter_messages(GRUPO_ID, limit=LIMITE_MENSAJES):

        # Parar si el mensaje es anterior al límite temporal
        if mensaje.date < fecha_limite:
            break

        if not mensaje.sender or not isinstance(mensaje.sender, User):
            continue
        if mensaje.sender.bot:
            continue
        if mensaje.text is None and mensaje.message is None:
            continue

        remitente: User = mensaje.sender
        user_id  = remitente.id
        nombre   = (
            f"{remitente.first_name or ''} {remitente.last_name or ''}".strip()
            or str(user_id)
        )
        username = remitente.username
        fecha    = mensaje.date

        upsert_usuario(conn, user_id, nombre, username, fecha)

        total_mensajes += 1
        total_usuarios.add(user_id)

        if total_mensajes % 500 == 0:
            conn.commit()
            print(f"  → {total_mensajes:,} mensajes procesados "
                  f"({len(total_usuarios):,} usuarios únicos)...")

    conn.commit()
    print(f"\n[OK] Historial completado.")
    print(f"     Mensajes procesados : {total_mensajes:,}")
    print(f"     Usuarios únicos     : {len(total_usuarios):,}")


async def importar_miembros(client: TelegramClient, conn: sqlite3.Connection) -> None:
    """Registra todos los miembros actuales del grupo con total_mensajes = 0
    si aún no existen en la BD (no sobreescribe conteos ya acumulados)."""

    total_nuevos = 0
    total_procesados = 0
    print(f"\n[INFO] Importando miembros actuales del grupo {GRUPO_ID}...")

    async for miembro in client.iter_participants(GRUPO_ID):
        if not isinstance(miembro, User) or miembro.bot or miembro.deleted:
            continue

        nombre = (
            f"{miembro.first_name or ''} {miembro.last_name or ''}".strip()
            or str(miembro.id)
        )
        cambios_antes = conn.total_changes
        conn.execute("""
            INSERT OR IGNORE INTO usuarios (user_id, nombre, username, total_mensajes, ultimo_mensaje)
            VALUES (?, ?, ?, 0, NULL)
        """, (miembro.id, nombre, miembro.username))
        total_procesados += 1
        if conn.total_changes > cambios_antes:
            total_nuevos += 1

    conn.commit()
    print(f"[OK] Miembros procesados : {total_procesados:,}")
    print(f"     Nuevos insertados   : {total_nuevos:,}")
    print(f"     Ya existían         : {total_procesados - total_nuevos:,}")


def parsear_args() -> datetime | None:
    """Parsea el argumento opcional --fecha DDMMYYYY y devuelve un datetime UTC o None."""
    parser = argparse.ArgumentParser(
        description="Inicializa el historial del grupo en la BD."
    )
    parser.add_argument(
        "--fecha",
        metavar="DDMMYYYY",
        help="Fecha para usuarios sin mensajes (formato DDMMYYYY, ej: 01012024)",
    )
    args = parser.parse_args()

    if args.fecha is None:
        return None

    try:
        dt = datetime.strptime(args.fecha, "%d%m%Y")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        parser.error(f"Formato de fecha inválido: '{args.fecha}'. Use DDMMYYYY (ej: 01012024).")


async def main() -> None:
    fecha_sin_mensajes = parsear_args()

    conn = init_db()
    print(f"[INFO] Base de datos lista: {DB_PATH}")

    if fecha_sin_mensajes:
        print(f"[INFO] Fecha para usuarios sin mensajes: "
              f"{fecha_sin_mensajes.strftime('%d/%m/%Y')}")

    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        me = await client.get_me()
        print(f"[INFO] Sesión iniciada como: {me.first_name} (id={me.id})")
        await leer_historial(client, conn)
        await importar_miembros(client, conn)

    if fecha_sin_mensajes:
        fecha_str = fecha_sin_mensajes.isoformat()
        cur = conn.execute(
            "UPDATE usuarios SET ultimo_mensaje = ? WHERE ultimo_mensaje IS NULL",
            (fecha_str,),
        )
        conn.commit()
        print(f"[OK] {cur.rowcount} usuarios sin mensajes actualizados "
              f"con fecha {fecha_sin_mensajes.strftime('%d/%m/%Y')}.")

    conn.close()
    print("[INFO] Conexión a la BD cerrada. ¡Listo para usar bot_estadisticas.py!")


if __name__ == "__main__":
    asyncio.run(main())
