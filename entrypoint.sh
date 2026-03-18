#!/bin/sh
# Corrige la propiedad de /app/data y sus ficheros (p.ej. DB copiada desde otro sistema)
# y a continuación cede el proceso a botuser sin privilegios de root.
chown -R botuser:botuser /app/data
exec gosu botuser "$@"
