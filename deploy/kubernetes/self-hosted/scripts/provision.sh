#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root_dir=$(cd -- "${script_dir}/.." && pwd)
terraform_dir="${root_dir}/terraform"
ansible_dir="${root_dir}/ansible"

terraform -chdir="${terraform_dir}" init
terraform -chdir="${terraform_dir}" apply

mkdir -p "${ansible_dir}/.generated"
terraform -chdir="${terraform_dir}" output -raw ansible_inventory >"${ansible_dir}/.generated/hosts.yml"

# node-01 has an ephemeral public address. If Google reuses an address from a
# previously destroyed test VM, remove only that stale host-key record before
# Ansible records the new key with StrictHostKeyChecking=accept-new.
bastion_public_ip=$(terraform -chdir="${terraform_dir}" output -raw administrative_node_public_ip)
ssh-keygen -R "${bastion_public_ip}" >/dev/null 2>&1 || true

for attempt in {1..30}; do
  if ssh \
    -o BatchMode=yes \
    -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=accept-new \
    "ubuntu@${bastion_public_ip}" true; then
    break
  fi

  if [[ "${attempt}" -eq 30 ]]; then
    printf 'The administrative node did not accept SSH after %s attempts: %s\n' "${attempt}" "${bastion_public_ip}" >&2
    exit 1
  fi

  sleep 2
done

ansible-playbook -i "${ansible_dir}/.generated/hosts.yml" "${ansible_dir}/bootstrap.yml"
ansible-playbook -i "${ansible_dir}/.generated/hosts.yml" "${ansible_dir}/storage.yml"

printf '%s\n' "Kubernetes and dynamic GCE Persistent Disk storage are ready. Run scripts/apply-platform.sh to install Kubernetes resources."
