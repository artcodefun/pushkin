#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root_dir=$(cd -- "${script_dir}/.." && pwd)
terraform_dir="${root_dir}/terraform"
platform_local_dir="${root_dir}/platform/local"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Required command is not installed: %s\n' "$1" >&2
    exit 1
  fi
}

json_key_is_current() {
  local key_file=$1
  local service_account=$2
  local key_id

  [[ -f "${key_file}" ]] || return 1
  key_id=$(jq -er --arg email "${service_account}" '
    select(.type == "service_account" and .client_email == $email) | .private_key_id
  ' "${key_file}") || return 1

  gcloud iam service-accounts keys list \
    --iam-account="${service_account}" \
    --managed-by=user \
    --format='value(name)' | grep -Fqx "projects/${project_id}/serviceAccounts/${service_account}/keys/${key_id}"
}

create_json_key_if_needed() {
  local key_file=$1
  local service_account=$2
  local temporary_key_file

  if json_key_is_current "${key_file}" "${service_account}"; then
    return
  fi

  temporary_key_file=$(mktemp "${platform_local_dir}/.service-account-key.XXXXXX")
  gcloud iam service-accounts keys create "${temporary_key_file}" \
    --iam-account="${service_account}"
  mv "${temporary_key_file}" "${key_file}"
  chmod 600 "${key_file}"
}

require_command gcloud
require_command jq
require_command terraform

if [[ ! -f "${terraform_dir}/terraform.tfstate" ]]; then
  printf '%s\n' 'Terraform state is missing. Provision the infrastructure before preparing platform inputs.' >&2
  exit 1
fi

mkdir -p "${platform_local_dir}"
umask 077

postgres_backup=$(terraform -chdir="${terraform_dir}" output -json postgres_backup_storage)
cassandra_backup=$(terraform -chdir="${terraform_dir}" output -json cassandra_backup_storage)
registry_puller=$(terraform -chdir="${terraform_dir}" output -raw pushkin_image_pull_service_account)

postgres_backup_bucket=$(jq -r '.bucket_name' <<<"${postgres_backup}")
postgres_backup_service_account=$(jq -r '.service_account_email' <<<"${postgres_backup}")
cassandra_backup_bucket=$(jq -r '.bucket_name' <<<"${cassandra_backup}")
cassandra_backup_service_account=$(jq -r '.service_account_email' <<<"${cassandra_backup}")
project_id=${cassandra_backup_service_account#*@}
project_id=${project_id%.iam.gserviceaccount.com}

printf 'destinationPath=s3://%s/pushkin-postgres\n' "${postgres_backup_bucket}" \
  >"${platform_local_dir}/postgres-backup-settings.env"
printf 'bucketName=%s\n' "${cassandra_backup_bucket}" \
  >"${platform_local_dir}/cassandra-backup-settings.env"

postgres_credentials_file="${platform_local_dir}/postgres-backup-credentials.env"
postgres_access_key_id=''
if [[ -f "${postgres_credentials_file}" ]]; then
  postgres_access_key_id=$(sed -n 's/^ACCESS_KEY_ID=//p' "${postgres_credentials_file}")
fi

if [[ -z "${postgres_access_key_id}" ]] || ! gcloud storage hmac list \
  --service-account="${postgres_backup_service_account}" \
  --format=json | jq -e --arg access_key_id "${postgres_access_key_id}" '
    any(.[]; .accessId == $access_key_id and .state == "ACTIVE")
  ' >/dev/null; then
  postgres_hmac=$(gcloud storage hmac create "${postgres_backup_service_account}" --format=json)
  printf 'ACCESS_KEY_ID=%s\nACCESS_SECRET_KEY=%s\n' \
    "$(jq -r '.accessId' <<<"${postgres_hmac}")" \
    "$(jq -r '.secret' <<<"${postgres_hmac}")" \
    >"${postgres_credentials_file}"
fi

cassandra_key_file="${platform_local_dir}/medusa-gcp-key.json"
registry_key_file="${platform_local_dir}/registry-puller-key.json"
create_json_key_if_needed "${cassandra_key_file}" "${cassandra_backup_service_account}"
create_json_key_if_needed "${registry_key_file}" "${registry_puller}"
printf 'key_file=%s\n' "${registry_key_file}" >"${platform_local_dir}/cloud-registry-pull.env"

chmod 600 \
  "${platform_local_dir}/postgres-backup-settings.env" \
  "${platform_local_dir}/cassandra-backup-settings.env" \
  "${postgres_credentials_file}" \
  "${platform_local_dir}/cloud-registry-pull.env"

printf '%s\n' 'Platform backup and registry credentials are ready.'
