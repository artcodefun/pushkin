#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: monitor.sh --output <metrics.ndjson> --summary <metrics-summary.json> --bastion <public-ip> [options] --host <name=private-ip>...

Continuously collects benchmark resource samples through SSH until it receives
SIGINT or SIGTERM. The script is an internal detail of run.sh.
EOF
}

output=""
summary=""
bastion=""
interval_seconds=1
component_interval_seconds=5
hosts=()

while (($# > 0)); do
  case "$1" in
    --output)
      output="$2"
      shift 2
      ;;
    --summary)
      summary="$2"
      shift 2
      ;;
    --bastion)
      bastion="$2"
      shift 2
      ;;
    --interval)
      interval_seconds="$2"
      shift 2
      ;;
    --component-interval)
      component_interval_seconds="$2"
      shift 2
      ;;
    --host)
      hosts+=("$2")
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$output" || -z "$summary" || -z "$bastion" || ${#hosts[@]} -eq 0 ]]; then
  usage
  exit 2
fi
if ! [[ "$interval_seconds" =~ ^[1-9][0-9]*$ ]] || ! [[ "$component_interval_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "monitor intervals must be positive whole seconds" >&2
  exit 2
fi

mkdir -p "$(dirname "$output")" "$(dirname "$summary")"
: >"$output"

control_dir="$(mktemp -d /tmp/pushkin-benchmark.XXXXXX)"
sample_dir="$(mktemp -d)"
running=true
sample_number=0

cleanup() {
  running=false
}
trap cleanup INT TERM

finish() {
  rm -rf "$control_dir" "$sample_dir"
}
trap finish EXIT

summarize() {
  jq -s '
    def numeric_values: map(select(type == "number"));
    def statistics:
      numeric_values as $values
      | if ($values | length) == 0 then null
        else {average: ($values | add / length), maximum: ($values | max)}
        end;
    def grouped_statistics($items; $key; $fields):
      reduce $items[] as $item ({};
        ($item[$key]) as $value
        | .[$value] = ((.[$value] // []) + [$item])
      )
      | with_entries(
          .value as $group
          | .value = (
              {host: $group[0].host}
              + reduce $fields[] as $field ({};
                  . + {($field): ($group | map(.[$field]) | statistics)})
            )
        );
    {
      sample_count: length,
      started_at: (if length == 0 then null else .[0].timestamp end),
      completed_at: (if length == 0 then null else .[-1].timestamp end),
      hosts: grouped_statistics(
        [ .[] | select(.error == null) | {
            host: .host,
            cpu_percent: .host_metrics.cpu_percent,
            memory_used_bytes: (.host_metrics.memory_total_bytes - .host_metrics.memory_available_bytes),
            load_1: .host_metrics.load_1
          } ];
        "host";
        ["cpu_percent", "memory_used_bytes", "load_1"]
      ),
      containers: grouped_statistics(
        [ .[] | select(.error == null) | . as $sample | $sample.containers[]? | {
            host: $sample.host,
            name: .name,
            id: ($sample.host + "/" + .name),
            cpu_percent: .cpu_percent,
            memory_percent: .memory_percent
          } ];
        "id";
        ["cpu_percent", "memory_percent"]
      ),
      postgres: grouped_statistics(
        [ .[] | select(.postgres != null) | {
            host: .host,
            connections: .postgres.connections,
            active_connections: .postgres.active_connections,
            waiting_locks: .postgres.waiting_locks
          } ];
        "host";
        ["connections", "active_connections", "waiting_locks"]
      ),
      redis: grouped_statistics(
        [ .[] | select(.redis != null) | {
            host: .host,
            connected_clients: .redis.connected_clients,
            used_memory_bytes: .redis.used_memory_bytes,
            total_commands_processed: .redis.total_commands_processed
          } ];
        "host";
        ["connected_clients", "used_memory_bytes", "total_commands_processed"]
      ),
      kafka: grouped_statistics(
        [ .[] | select(.kafka != null) | {
            host: .host,
            consumer_group_count: .kafka.consumer_group_count,
            total_consumer_lag: .kafka.total_consumer_lag,
            maximum_partition_lag: .kafka.maximum_partition_lag
          } ];
        "host";
        ["consumer_group_count", "total_consumer_lag", "maximum_partition_lag"]
      ),
      cassandra: grouped_statistics(
        [ .[] | select(.cassandra != null) | {
            host: .host,
            live_nodes: .cassandra.live_nodes,
            storage_load_bytes: .cassandra.storage_load_bytes,
            heap_used_bytes: .cassandra.heap_used_bytes
          } ];
        "host";
        ["live_nodes", "storage_load_bytes", "heap_used_bytes"]
      ),
      collection_errors: [ .[] | select(.error != null) | {timestamp, host, error} ]
    }
  ' "$output" >"$summary"
}

collect_host() {
  local name="$1"
  local address="$2"
  local collect_components="$3"
  local payload

  if ! payload="$(ssh \
    -o BatchMode=yes \
    -o ConnectTimeout=5 \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o "ProxyJump=ubuntu@$bastion" \
    -o ControlMaster=auto \
    -o ControlPersist=60 \
    -o "ControlPath=$control_dir/%r@%h:%p" \
    "ubuntu@$address" "bash -s -- $collect_components" <<'REMOTE'
set -euo pipefail

collect_components="$1"

numeric_or_null() {
  if [[ "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    printf '%s' "$1"
  else
    printf 'null'
  fi
}

cpu_ticks() {
  awk '/^cpu / { total = 0; for (field = 2; field <= NF; field++) total += $field; print total, $5 + $6; exit }' /proc/stat
}

read -r total_before idle_before < <(cpu_ticks)
sleep 0.1
read -r total_after idle_after < <(cpu_ticks)
host_cpu_percent="$(awk -v total_before="$total_before" -v idle_before="$idle_before" -v total_after="$total_after" -v idle_after="$idle_after" 'BEGIN {
  total_delta = total_after - total_before
  idle_delta = idle_after - idle_before
  if (total_delta == 0) print "0"
  else printf "%.4f", 100 * (1 - idle_delta / total_delta)
}')"
host_memory_total_bytes="$(awk '/MemTotal:/ { print $2 * 1024; exit }' /proc/meminfo)"
host_memory_available_bytes="$(awk '/MemAvailable:/ { print $2 * 1024; exit }' /proc/meminfo)"
host_load_1="$(awk '{ print $1 }' /proc/loadavg)"

containers='['
separator=''
while IFS='|' read -r container cpu_percent memory_percent; do
  [[ -n "$container" ]] || continue
  cpu_percent="${cpu_percent%%%}"
  memory_percent="${memory_percent%%%}"
  containers+="$separator{\"name\":\"$container\",\"cpu_percent\":$(numeric_or_null "$cpu_percent"),\"memory_percent\":$(numeric_or_null "$memory_percent")}"
  separator=','
done < <(docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemPerc}}')
containers+=']'

postgres='null'
redis='null'
kafka='null'
cassandra='null'

if [[ "$collect_components" == true ]]; then
  postgres_container="$(docker ps --filter label=com.docker.compose.service=postgres --format '{{.ID}}' | head -n 1)"
  if [[ -n "$postgres_container" ]]; then
    postgres="$(docker exec "$postgres_container" psql -U pushkin -d pushkin -Atc "
      SELECT json_build_object(
        'connections', (SELECT numbackends FROM pg_stat_database WHERE datname = current_database()),
        'active_connections', (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state = 'active'),
        'waiting_locks', (SELECT count(*) FROM pg_locks WHERE NOT granted)
      );
    " 2>/dev/null || printf 'null')"
  fi

  redis_container="$(docker ps --filter label=com.docker.compose.service=redis --format '{{.ID}}' | head -n 1)"
  if [[ -n "$redis_container" ]]; then
    redis_info="$(docker exec "$redis_container" redis-cli INFO clients memory stats 2>/dev/null || true)"
    redis_clients="$(awk -F: '/^connected_clients:/ { gsub("\\r", "", $2); print $2; exit }' <<<"$redis_info")"
    redis_memory="$(awk -F: '/^used_memory:/ { gsub("\\r", "", $2); print $2; exit }' <<<"$redis_info")"
    redis_commands="$(awk -F: '/^total_commands_processed:/ { gsub("\\r", "", $2); print $2; exit }' <<<"$redis_info")"
    redis="{\"connected_clients\":$(numeric_or_null "$redis_clients"),\"used_memory_bytes\":$(numeric_or_null "$redis_memory"),\"total_commands_processed\":$(numeric_or_null "$redis_commands")}"
  fi

  kafka_container="$(docker ps --filter label=com.docker.compose.service=kafka --format '{{.ID}}' | head -n 1)"
  if [[ -n "$kafka_container" ]]; then
    kafka="$(docker exec "$kafka_container" /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:19092 --all-groups --describe --timeout 5000 2>/dev/null \
      | awk '
          $1 ~ /^pushkin\./ && $6 ~ /^[0-9]+$/ {
            groups[$1] = 1
            lag = $6 + 0
            total += lag
            if (lag > maximum) maximum = lag
          }
          END {
            count = 0
            for (group in groups) count++
            printf "{\"consumer_group_count\":%d,\"total_consumer_lag\":%d,\"maximum_partition_lag\":%d}", count, total, maximum
          }
        ' || printf 'null')"
  fi

  cassandra_container="$(docker ps --filter label=com.docker.compose.service=cassandra --format '{{.ID}}' | head -n 1)"
  if [[ -n "$cassandra_container" ]]; then
    cassandra_info="$(docker exec "$cassandra_container" nodetool info 2>/dev/null || true)"
    cassandra_status="$(docker exec "$cassandra_container" nodetool status 2>/dev/null || true)"
    cassandra_live_nodes="$(awk '$1 == "UN" { count++ } END { print count + 0 }' <<<"$cassandra_status")"
    cassandra_storage_load_bytes="$(awk -F': ' '
      /^Load[[:space:]]*:/ {
        split($2, parts, " ")
        multiplier = 1
        if (parts[2] == "KiB" || parts[2] == "KB") multiplier = 1024
        else if (parts[2] == "MiB" || parts[2] == "MB") multiplier = 1024 * 1024
        else if (parts[2] == "GiB" || parts[2] == "GB") multiplier = 1024 * 1024 * 1024
        printf "%.0f", parts[1] * multiplier
        exit
      }
    ' <<<"$cassandra_info")"
    cassandra_heap_used_bytes="$(awk -F': ' '
      /^Heap Memory \(MB\)[[:space:]]*:/ {
        split($2, parts, " ")
        printf "%.0f", parts[1] * 1024 * 1024
        exit
      }
    ' <<<"$cassandra_info")"
    cassandra="{\"live_nodes\":$(numeric_or_null "$cassandra_live_nodes"),\"storage_load_bytes\":$(numeric_or_null "$cassandra_storage_load_bytes"),\"heap_used_bytes\":$(numeric_or_null "$cassandra_heap_used_bytes")}"
  fi
fi

printf '"host_metrics":{"cpu_percent":%s,"memory_total_bytes":%s,"memory_available_bytes":%s,"load_1":%s},"containers":%s,"postgres":%s,"redis":%s,"kafka":%s,"cassandra":%s' \
  "$(numeric_or_null "$host_cpu_percent")" \
  "$(numeric_or_null "$host_memory_total_bytes")" \
  "$(numeric_or_null "$host_memory_available_bytes")" \
  "$(numeric_or_null "$host_load_1")" \
  "$containers" "$postgres" "$redis" "$kafka" "$cassandra"
REMOTE
)"; then
    jq -cn --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg host "$name" --arg error "SSH collection failed for $address" \
      '{timestamp: $timestamp, host: $host, error: $error}'
    return
  fi

  if ! jq -cn \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg host "$name" \
    --argjson payload "{$payload}" \
    '{timestamp: $timestamp, host: $host} + $payload' 2>/dev/null; then
    jq -cn --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg host "$name" --arg error "invalid collection payload from $address" \
      '{timestamp: $timestamp, host: $host, error: $error}'
  fi
}

while [[ "$running" == true ]]; do
  collect_components=false
  if ((sample_number % component_interval_seconds == 0)); then
    collect_components=true
  fi

  for host in "${hosts[@]}"; do
    name="${host%%=*}"
    address="${host#*=}"
    collect_host "$name" "$address" "$collect_components" >"$sample_dir/$name" &
  done
  for job in $(jobs -p); do
    wait "$job" || true
  done
  cat "$sample_dir"/* >>"$output"
  rm -f "$sample_dir"/*

  sample_number=$((sample_number + 1))
  if [[ "$running" == true ]]; then
    sleep "$interval_seconds" || true
  fi
done

summarize
