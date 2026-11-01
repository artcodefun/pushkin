#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root_dir=$(cd -- "${script_dir}/.." && pwd)
repository_dir=$(cd -- "${root_dir}/../../.." && pwd)
terraform_dir="${root_dir}/terraform"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Required command is not installed: %s\n' "$1" >&2
    exit 1
  fi
}

require_command docker
require_command terraform
require_command gcloud

if [[ ! -f "${terraform_dir}/terraform.tfstate" ]]; then
  printf '%s\n' 'Terraform state is missing. Provision the infrastructure before publishing an image.' >&2
  exit 1
fi

image_repository=$(terraform -chdir="${terraform_dir}" output -raw pushkin_image_repository)
if [[ -n "${1:-}" ]]; then
  image_tag="$1"
elif git -C "${repository_dir}" diff --quiet && git -C "${repository_dir}" diff --cached --quiet; then
  image_tag=$(git -C "${repository_dir}" rev-parse --short=12 HEAD)
else
  image_tag="dev-$(date -u +%Y%m%d%H%M%S)"
fi
image="${image_repository}:${image_tag}"
platform_local_dir="${root_dir}/platform/local"

# The credential helper delegates authentication to the active gcloud profile;
# it avoids copying an operator credential into a shell history or this script.
registry_host=${image_repository%%/*}
gcloud auth configure-docker "${registry_host}" --quiet
docker buildx build --platform linux/amd64 --load --tag "${image}" "${repository_dir}"
docker push "${image}"

mkdir -p "${platform_local_dir}"
umask 077
printf 'image=%s\n' "${image}" >"${platform_local_dir}/pushkin-image.env"

printf 'Published image: %s\n' "${image}"
printf 'Updated %s\n' "${platform_local_dir}/pushkin-image.env"
