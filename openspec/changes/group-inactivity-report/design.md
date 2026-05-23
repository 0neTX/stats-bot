## Context

Currently, the bot only notifies the administrator about new users who have joined but haven't participated yet. To improve engagement and transparency, the user wants a friendly notification to be sent directly to the group. This notification will serve as a welcome and a reminder of the group's participation rules.

## Goals / Non-Goals

**Goals:**
- Send a public notification to the group identifying new inactive users.
- Use a friendly and welcoming tone.
- Communicate the group's purpose (participation and exchange of ideas).
- Inform about the potential for expulsion if inactivity persists.
- Limit personal data shared in the group (no usernames, only IDs and names).

**Non-Goals:**
- Replace the admin reports (both will coexist).
- Change the existing inactivity thresholds.
- Implement new commands for the group.

## Decisions

- **New Function**: `enviar_aviso_grupo_inactivos(bot, usuarios)` will be created.
- **Trigger**: This function will be called during the `post_init` sequence and the daily scheduled task, specifically when "proximos a vencer" (upcoming expirations) are identified.
- **Lookup Reuse**: Reuse `obtener_nuevos_usuarios_a_avisar()` to identify the users who should receive this public "ping".
- **Formatting**:
  - Greeting: "¡Hola a todos! 👋 Todos somos bienvenidos en este grupo..."
  - Purpose: "...pero recordad que este es un espacio para intercambiar contenido, charlas e ideas y, sobre todo, para participar."
  - Inactive list: "Aprovechamos para saludar a los nuevos miembros que aún no se han animado a escribir:"
    - `• <nombre> (ID: <user_id>) — <dias> días con nosotros`
  - Warning: "Si no saludáis o no queréis participar al uniros, recordad que seréis expulsados en el plazo establecido para mantener el grupo activo y dinámico. ¡Animaos a participar! 😊"
- **Data Protection**: Only `nombre` (escaped) and `user_id` will be shown. `username` is omitted to avoid direct mentions/spam if the user hasn't opted into the group yet.

## Risks / Trade-offs

- **[Risk] Noise in the group** → **Mitigation**: The report only runs once a day (scheduled) or on bot restart. It only lists users who are *approaching* the deadline, not every single new user.
- **[Risk] Privacy concerns** → **Mitigation**: Usernames are excluded, and only IDs are used for identification.
- **[Risk] Negative perception of "expulsion"** → **Mitigation**: The tone is carefully crafted to be "agradable y amigable" (pleasant and friendly).
