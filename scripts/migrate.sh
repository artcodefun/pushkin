#!/bin/sh
set -eu

: "${PUSHKIN_POSTGRES_DSN:?PUSHKIN_POSTGRES_DSN must be set}"

migrations_table="${PUSHKIN_MIGRATIONS_TABLE:-pushkin_schema_migrations}"
separator='?'
case "${PUSHKIN_POSTGRES_DSN}" in
  *'?'*) separator='&' ;;
esac

exec /usr/local/bin/migrate \
  -path /app/migrations \
  -database "${PUSHKIN_POSTGRES_DSN}${separator}x-migrations-table=${migrations_table}" \
  up
