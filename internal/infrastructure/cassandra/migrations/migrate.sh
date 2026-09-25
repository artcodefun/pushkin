#!/usr/bin/env bash
set -euo pipefail

: "${PUSHKIN_CASSANDRA_KEYSPACE:?PUSHKIN_CASSANDRA_KEYSPACE is required}"

cassandra_host="${PUSHKIN_CASSANDRA_HOST:-cassandra}"
cassandra_port="${PUSHKIN_CASSANDRA_PORT:-9042}"
migrations_dir="${PUSHKIN_CASSANDRA_MIGRATIONS_DIR:-/migrations}"

cqlsh() {
	command cqlsh "$cassandra_host" "$cassandra_port" "$@"
}

cqlsh -e "CREATE KEYSPACE IF NOT EXISTS ${PUSHKIN_CASSANDRA_KEYSPACE} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
cqlsh -k "$PUSHKIN_CASSANDRA_KEYSPACE" -e 'CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamp)'

while IFS= read -r migration; do
	filename="$(basename "$migration")"
	version="${filename%.up.cql}"

	if cqlsh -k "$PUSHKIN_CASSANDRA_KEYSPACE" -e "SELECT version FROM schema_migrations WHERE version = '$version'" |
		awk -v version="$version" '$1 == version { found = 1 } END { exit !found }'; then
		continue
	fi

	echo "Applying Cassandra migration $filename"
	cqlsh -k "$PUSHKIN_CASSANDRA_KEYSPACE" -f "$migration"
	cqlsh -k "$PUSHKIN_CASSANDRA_KEYSPACE" -e "INSERT INTO schema_migrations (version, applied_at) VALUES ('$version', toTimestamp(now()))"
done < <(find "$migrations_dir" -maxdepth 1 -type f -name '*.up.cql' ! -name '._*' -print | LC_ALL=C sort)
