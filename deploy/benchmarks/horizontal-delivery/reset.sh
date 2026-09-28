#!/usr/bin/env bash

set -euo pipefail

benchmark_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
terraform_dir="$benchmark_dir/terraform"
state_dir="$benchmark_dir/.state"

case "${1:-}" in
  "")
    destroy=false
    ;;
  --destroy)
    destroy=true
    ;;
  --help|-h)
    echo "usage: ./reset.sh [--destroy]"
    echo "  without --destroy: remove benchmark data but keep the virtual machines"
    echo "  --destroy: remove all Terraform-managed benchmark infrastructure"
    exit 0
    ;;
  *)
    echo "usage: ./reset.sh [--destroy]" >&2
    exit 2
    ;;
esac

if [[ ! -f "$terraform_dir/terraform.tfvars" ]]; then
  echo "create $terraform_dir/terraform.tfvars from terraform.tfvars.example first" >&2
  exit 2
fi
for command in terraform ssh jq; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 2; }
done

if [[ "$destroy" == true ]]; then
  echo "==> Destroying Terraform-managed benchmark infrastructure"
  terraform -chdir="$terraform_dir" destroy -auto-approve
  rm -f "$state_dir/dataset.yml"
  echo "==> Benchmark infrastructure destroyed"
  exit 0
fi

bastion_ip="$(terraform -chdir="$terraform_dir" output -json bastion | jq -r '.public_ip')"
postgres_private_ip="$(terraform -chdir="$terraform_dir" output -json postgres | jq -r '.private_ip')"
kafka_private_ip="$(terraform -chdir="$terraform_dir" output -json kafka | jq -r '.private_ip')"
cassandra_private_ip="$(terraform -chdir="$terraform_dir" output -json cassandra | jq -r '.private_ip')"
fake_fcm_private_ip="$(terraform -chdir="$terraform_dir" output -json fake_fcm | jq -r '.private_ip')"
pushkin_private_ips=()
while IFS= read -r pushkin_private_ip; do
  pushkin_private_ips+=("$pushkin_private_ip")
done < <(terraform -chdir="$terraform_dir" output -json pushkin_instances | jq -r '.[].private_ip')

reset_compose() {
  local host="$1"
  local file="$2"
  local label="$3"

  echo "==> Resetting $label"
  ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -o "ProxyCommand=ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -W %h:%p ubuntu@$bastion_ip" "ubuntu@$host" \
    "sudo docker compose --file $file down --volumes --remove-orphans"
}

for index in "${!pushkin_private_ips[@]}"; do
  reset_compose "${pushkin_private_ips[$index]}" /opt/pushkin-benchmark/pushkin.compose.yml "Pushkin-$((index + 1))"
done
reset_compose "$postgres_private_ip" /opt/pushkin-benchmark/postgres.compose.yml "PostgreSQL and Redis"
reset_compose "$kafka_private_ip" /opt/pushkin-benchmark/kafka.compose.yml Kafka
reset_compose "$cassandra_private_ip" /opt/pushkin-benchmark/cassandra.compose.yml Cassandra
reset_compose "$fake_fcm_private_ip" /opt/pushkin-benchmark/fake-fcm.compose.yml "Fake FCM"

rm -f "$state_dir/dataset.yml"
echo "==> Benchmark environment reset; run ./run.sh batched-ten to start again"
