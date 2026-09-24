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
for command in terraform; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 2; }
done

if [[ "$destroy" == true ]]; then
  echo "==> Destroying Terraform-managed benchmark infrastructure"
  terraform -chdir="$terraform_dir" destroy -auto-approve
  rm -f "$state_dir"/*.yml
  echo "==> Benchmark infrastructure destroyed"
  exit 0
fi

for command in ssh jq; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 2; }
done

pushkin_ip="$(terraform -chdir="$terraform_dir" output -json pushkin | jq -r '.public_ip')"
fake_fcm_ip="$(terraform -chdir="$terraform_dir" output -json fake_fcm | jq -r '.public_ip')"

reset_compose() {
  local host="$1"
  local file="$2"
  local label="$3"

  echo "==> Resetting $label"
  ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "ubuntu@$host" \
    "sudo docker compose --file $file down --volumes --remove-orphans"
}

reset_compose "$pushkin_ip" /opt/pushkin-benchmark/pushkin.compose.yml "Pushkin stack"
reset_compose "$fake_fcm_ip" /opt/pushkin-benchmark/fake-fcm.compose.yml "Fake FCM"

rm -f "$state_dir"/*.yml
echo "==> Benchmark environment reset; run ./run.sh inline-100k to start again"
