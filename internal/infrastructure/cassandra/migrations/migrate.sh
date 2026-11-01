#!/usr/bin/env bash
set -euo pipefail

: "${PUSHKIN_CASSANDRA_KEYSPACE:?PUSHKIN_CASSANDRA_KEYSPACE is required}"

cassandra_host="${PUSHKIN_CASSANDRA_HOST:-cassandra}"
cassandra_port="${PUSHKIN_CASSANDRA_PORT:-9042}"
migrations_dir="${PUSHKIN_CASSANDRA_MIGRATIONS_DIR:-/migrations}"
cassandra_local_datacenter="${PUSHKIN_CASSANDRA_LOCAL_DATACENTER:-dc1}"
cassandra_replication_factor="${PUSHKIN_CASSANDRA_REPLICATION_FACTOR:-1}"
cassandra_username="${PUSHKIN_CASSANDRA_USERNAME:-}"
cassandra_password="${PUSHKIN_CASSANDRA_PASSWORD:-}"

if ! [[ "$cassandra_local_datacenter" =~ ^[[:alnum:]_-]+$ ]]; then
	echo "PUSHKIN_CASSANDRA_LOCAL_DATACENTER must contain only letters, digits, underscores, or hyphens" >&2
	exit 1
fi
if ! [[ "$cassandra_replication_factor" =~ ^[1-9][0-9]*$ ]]; then
	echo "PUSHKIN_CASSANDRA_REPLICATION_FACTOR must be a positive integer" >&2
	exit 1
fi
if [[ -n "$cassandra_username" && -z "$cassandra_password" ]] || [[ -z "$cassandra_username" && -n "$cassandra_password" ]]; then
	echo "PUSHKIN_CASSANDRA_USERNAME and PUSHKIN_CASSANDRA_PASSWORD must be set together" >&2
	exit 1
fi

cqlsh_auth_args=()
if [[ -n "$cassandra_username" ]]; then
	cqlsh_auth_args=(-u "$cassandra_username" -p "$cassandra_password")
fi

cqlsh() {
	command cqlsh "$cassandra_host" "$cassandra_port" "${cqlsh_auth_args[@]}" "$@"
}

cqlsh -e "CREATE KEYSPACE IF NOT EXISTS ${PUSHKIN_CASSANDRA_KEYSPACE} WITH replication = {'class': 'NetworkTopologyStrategy', '${cassandra_local_datacenter}': ${cassandra_replication_factor}}"
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
