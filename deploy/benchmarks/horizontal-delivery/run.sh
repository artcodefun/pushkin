#!/usr/bin/env bash

set -euo pipefail

benchmark_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
terraform_dir="$benchmark_dir/terraform"
inventory="$benchmark_dir/ansible/inventory.py"
results_dir="$benchmark_dir/results"
state_dir="$benchmark_dir/.state"
monitor="$benchmark_dir/scripts/monitor.sh"
topic_lag="$benchmark_dir/scripts/topic-lag.sh"
scenario="${1:-batched-ten}"
scenario_dir="$benchmark_dir/ansible/scenarios/$scenario"

if [[ ! -d "$scenario_dir" ]]; then
  echo "unsupported scenario: $scenario" >&2
  exit 2
fi
if [[ ! -f "$terraform_dir/terraform.tfvars" ]]; then
  echo "create $terraform_dir/terraform.tfvars from terraform.tfvars.example first" >&2
  exit 2
fi
for command in terraform ansible ansible-playbook ssh jq git; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 2; }
done

run_id="${scenario}-$(date -u +%Y%m%dT%H%M%SZ)"
run_results_dir="$results_dir/$run_id"
mkdir -p "$run_results_dir"

echo "==> Provisioning Yandex Cloud environment"
terraform -chdir="$terraform_dir" init
terraform -chdir="$terraform_dir" apply -auto-approve

bastion_ip="$(terraform -chdir="$terraform_dir" output -json bastion | jq -r '.public_ip')"
postgres_private_ip="$(terraform -chdir="$terraform_dir" output -json postgres | jq -r '.private_ip')"
kafka_private_ip="$(terraform -chdir="$terraform_dir" output -json kafka | jq -r '.private_ip')"
cassandra_private_ip="$(terraform -chdir="$terraform_dir" output -json cassandra | jq -r '.private_ip')"
fake_fcm_private_ip="$(terraform -chdir="$terraform_dir" output -json fake_fcm | jq -r '.private_ip')"
pushkin_private_ips=()
while IFS= read -r pushkin_private_ip; do
  pushkin_private_ips+=("$pushkin_private_ip")
done < <(terraform -chdir="$terraform_dir" output -json pushkin_instances | jq -r '.[].private_ip')

wait_for_ssh() {
  local host="$1"
  local label="$2"
  echo "==> Waiting for SSH on $label ($host)"
  for _ in {1..60}; do
    if ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "ubuntu@$host" true; then
      return
    fi
    sleep 5
  done
  echo "SSH did not become ready on $label within five minutes" >&2
  exit 1
}

wait_for_ssh "$bastion_ip" bastion

echo "==> Waiting for private nodes through the bastion"
ansible --forks 8 -i "$inventory" all -m ansible.builtin.wait_for_connection -a 'timeout=300 connect_timeout=5 sleep=1'

echo "==> Deploying Pushkin, Kafka, PostgreSQL/Redis, Cassandra, and Fake FCM"
ansible-playbook -i "$inventory" "$benchmark_dir/ansible/deploy.yml" \
  -e "pushkin_source_dir=$benchmark_dir/../../.."

scenario_args=(
  -e "pushkin_source_dir=$benchmark_dir/../../.."
  -e "benchmark_state_dir=$state_dir"
)

echo "==> Preparing scenario $scenario"
ansible-playbook -i "$inventory" "$scenario_dir/prepare.yml" "${scenario_args[@]}"

started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
started_epoch="$(date +%s)"
metrics_output="$run_results_dir/metrics.ndjson"
metrics_summary="$run_results_dir/metrics-summary.json"

echo "==> Starting resource monitor"
monitor_args=(
  --output "$metrics_output"
  --summary "$metrics_summary"
  --bastion "$bastion_ip"
  --host "postgres=$postgres_private_ip"
  --host "kafka=$kafka_private_ip"
  --host "cassandra=$cassandra_private_ip"
  --host "fake-fcm=$fake_fcm_private_ip"
)
for index in "${!pushkin_private_ips[@]}"; do
  monitor_args+=(--host "pushkin-$((index + 1))=${pushkin_private_ips[$index]}")
done
bash "$monitor" "${monitor_args[@]}" &
monitor_pid=$!

stop_monitor() {
  if [[ -n "${monitor_pid:-}" ]]; then
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" || true
    monitor_pid=""
  fi
}
trap stop_monitor EXIT

echo "==> Starting scenario $scenario"
ansible-playbook -i "$inventory" "$scenario_dir/start.yml" "${scenario_args[@]}"

echo "==> Waiting for scenario $scenario to finish"
BASTION_IP="$bastion_ip" \
FAKE_FCM_PRIVATE_IP="$fake_fcm_private_ip" \
BENCHMARK_RESULTS_DIR="$run_results_dir" \
"$scenario_dir/wait.sh"

echo "==> Collecting final Kafka topic lag"
bash "$topic_lag" \
  --bastion "$bastion_ip" \
  --host "$kafka_private_ip" \
  --compose-file /opt/pushkin-benchmark/kafka.compose.yml \
  --output "$run_results_dir/topic-lags.json"
cat "$run_results_dir/topic-lags.json"

stop_monitor
trap - EXIT

if [[ ! -s "$metrics_summary" ]]; then
  jq -n '{sample_count: 0, collection_errors: ["resource monitor did not produce a summary"]}' >"$metrics_summary"
fi

completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
duration_seconds="$(( $(date +%s) - started_epoch ))"
revision="uncommitted"
if resolved_revision="$(git -C "$benchmark_dir/../../.." rev-parse --verify --quiet HEAD)"; then
  revision="$resolved_revision"
fi

jq -n \
  --arg scenario "$scenario" \
  --arg revision "$revision" \
  --arg started_at "$started_at" \
  --arg completed_at "$completed_at" \
  --argjson duration_seconds "$duration_seconds" \
  --slurpfile fake_fcm "$run_results_dir/fake-fcm.json" \
  --slurpfile topic_lags "$run_results_dir/topic-lags.json" \
  --slurpfile metrics "$metrics_summary" \
  '{scenario: $scenario, revision: $revision, started_at: $started_at, completed_at: $completed_at, duration_seconds: $duration_seconds, fake_fcm: $fake_fcm[0], topic_lags: $topic_lags[0], metrics: $metrics[0]}' \
  >"$run_results_dir/summary.json"

echo "==> Complete: $run_results_dir/summary.json"
cat "$run_results_dir/summary.json"
