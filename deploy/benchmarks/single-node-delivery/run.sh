#!/usr/bin/env bash

set -euo pipefail

benchmark_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
terraform_dir="$benchmark_dir/terraform"
inventory="$benchmark_dir/ansible/inventory.py"
state_dir="$benchmark_dir/.state"
results_dir="$benchmark_dir/results"
monitor="$benchmark_dir/scripts/monitor.sh"
scenario="${1:-inline-100k}"
scenario_dir="$benchmark_dir/ansible/scenarios/$scenario"

if [[ ! -d "$scenario_dir" ]]; then
  echo "unsupported scenario: $scenario" >&2
  exit 2
fi
if [[ ! -f "$terraform_dir/terraform.tfvars" ]]; then
  echo "create $terraform_dir/terraform.tfvars from terraform.tfvars.example first" >&2
  exit 2
fi
for command in terraform ansible-playbook ssh jq curl git; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 2; }
done

run_id="${scenario}-$(date -u +%Y%m%dT%H%M%SZ)"
run_results_dir="$results_dir/$run_id"
mkdir -p "$run_results_dir"

echo "==> Provisioning Yandex Cloud environment"
terraform -chdir="$terraform_dir" init
terraform -chdir="$terraform_dir" apply -auto-approve

pushkin_ip="$(terraform -chdir="$terraform_dir" output -json pushkin | jq -r '.public_ip')"
fake_fcm_ip="$(terraform -chdir="$terraform_dir" output -json fake_fcm | jq -r '.public_ip')"
fake_fcm_private_ip="$(terraform -chdir="$terraform_dir" output -json fake_fcm | jq -r '.private_ip')"

wait_for_ssh() {
  local host="$1"
  local label="$2"
  echo "==> Waiting for SSH on $label ($host)"
  for _ in {1..60}; do
    if ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "ubuntu@$host" true; then
      return
    fi
    sleep 5
  done
  echo "SSH did not become ready on $label within five minutes" >&2
  exit 1
}

wait_for_ssh "$pushkin_ip" pushkin
wait_for_ssh "$fake_fcm_ip" fake-fcm

echo "==> Deploying current Pushkin revision"
ansible-playbook -i "$inventory" "$benchmark_dir/ansible/deploy.yml" \
  -e "pushkin_source_dir=$benchmark_dir/../../.."

tunnel_pid=""
monitor_pid=""
cleanup() {
  if [[ -n "$monitor_pid" ]]; then
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" || true
  fi
  if [[ -n "$tunnel_pid" ]]; then
    kill "$tunnel_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "==> Opening local Pushkin API tunnel"
ssh -o ExitOnForwardFailure=yes -o StrictHostKeyChecking=accept-new \
  -N -L 18080:127.0.0.1:8080 "ubuntu@$pushkin_ip" &
tunnel_pid=$!
for _ in {1..30}; do
  if curl --fail --silent --output /dev/null http://127.0.0.1:18080/health; then
    break
  fi
  sleep 1
done
curl --fail --silent --output /dev/null http://127.0.0.1:18080/health

scenario_args=(
  -e "benchmark_source_dir=$benchmark_dir/../../.."
  -e "benchmark_state_dir=$state_dir"
  -e "benchmark_results_dir=$run_results_dir"
  -e "fake_fcm_private_ip=$fake_fcm_private_ip"
)

echo "==> Preparing scenario $scenario"
ansible-playbook -i "$inventory" "$scenario_dir/prepare.yml" "${scenario_args[@]}"

started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
started_epoch="$(date +%s)"
metrics_output="$run_results_dir/metrics.ndjson"
metrics_summary="$run_results_dir/metrics-summary.json"

echo "==> Starting resource monitor"
bash "$monitor" \
  --output "$metrics_output" \
  --summary "$metrics_summary" \
  --host "pushkin=$pushkin_ip" \
  --host "fake-fcm=$fake_fcm_ip" &
monitor_pid=$!

echo "==> Starting scenario $scenario"
ansible-playbook -i "$inventory" "$scenario_dir/start.yml" "${scenario_args[@]}"

echo "==> Waiting for scenario $scenario to finish"
FAKE_FCM_IP="$fake_fcm_ip" \
FAKE_FCM_PRIVATE_IP="$fake_fcm_private_ip" \
BENCHMARK_RESULTS_DIR="$run_results_dir" \
"$scenario_dir/wait.sh"

kill -TERM "$monitor_pid" 2>/dev/null || true
wait "$monitor_pid" || true
monitor_pid=""

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
  --slurpfile workload "$run_results_dir/workload.json" \
  --slurpfile fake_fcm "$run_results_dir/fake-fcm.json" \
  --slurpfile metrics "$metrics_summary" \
  '{scenario: $scenario, revision: $revision, started_at: $started_at, completed_at: $completed_at, duration_seconds: $duration_seconds, workload: $workload[0], fake_fcm: $fake_fcm[0], metrics: $metrics[0]}' \
  >"$run_results_dir/summary.json"

echo "==> Complete: $run_results_dir/summary.json"
cat "$run_results_dir/summary.json"
