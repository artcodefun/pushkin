#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root_dir=$(cd -- "${script_dir}/.." && pwd)
platform_dir="${root_dir}/platform"
repository_dir=$(cd -- "${root_dir}/../../.." && pwd)
default_kubeconfig="${root_dir}/ansible/.generated/admin.local.conf"

export KUBECONFIG="${KUBECONFIG:-${default_kubeconfig}}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Required command is not installed: %s\n' "$1" >&2
    exit 1
  fi
}

require_command kubectl
require_command helm
require_command jq

for input in \
  "${platform_dir}/local/postgres-backup-credentials.env" \
  "${platform_dir}/local/postgres-backup-settings.env" \
  "${platform_dir}/local/postgres-app.env" \
  "${platform_dir}/local/cassandra-backup-settings.env" \
  "${platform_dir}/local/cloud-registry-pull.env" \
  "${platform_dir}/local/pushkin-image.env" \
  "${platform_dir}/local/pushkin-runtime.env" \
  "${platform_dir}/local/medusa-gcp-key.json"; do
  if [[ ! -f "${input}" ]]; then
    printf 'Missing local platform input: %s\n' "${input}" >&2
    exit 1
  fi
done

registry_pull_input="${platform_dir}/local/cloud-registry-pull.env"
registry_key_file=$(sed -n 's/^key_file=//p' "${registry_pull_input}")
if [[ -z "${registry_key_file}" || ! -f "${registry_key_file}" ]]; then
  printf '%s\n' 'cloud-registry-pull.env must define key_file as an existing absolute service-account JSON key path.' >&2
  exit 1
fi

image_repository=$(sed -n 's/^image=//p' "${platform_dir}/local/pushkin-image.env")
registry_host=${image_repository%%/*}
if [[ -z "${image_repository}" || "${registry_host}" != *-docker.pkg.dev ]]; then
  printf '%s\n' 'pushkin-image.env must name an Artifact Registry image in <region>-docker.pkg.dev.' >&2
  exit 1
fi

# Helm owns every third-party controller and its CRDs. The versions below are
# chart versions, intentionally distinct from an operator image version.
helm repo add --force-update jetstack https://charts.jetstack.io
helm repo add --force-update cnpg https://cloudnative-pg.github.io/charts
helm repo add --force-update k8ssandra https://helm.k8ssandra.io/stable
helm repo update

# ingress-nginx was archived upstream in March 2026. The NGINX chart creates
# the fixed NodePort expected by the Terraform-managed L4 load balancer.
helm upgrade --install nginx-ingress oci://ghcr.io/nginx/charts/nginx-ingress \
  --version 2.8.0 \
  --namespace nginx-ingress \
  --create-namespace \
  --wait \
  --timeout 5m \
  --set controller.replicaCount=2 \
  --set controller.service.type=NodePort \
  --set controller.service.name=nginx-ingress \
  --set controller.service.httpPort.nodePort=30080 \
  --set controller.service.httpsPort.nodePort=30443

# Operators define the CRDs consumed by the workload declaration, so they must
# be registered before Kustomize submits ObjectStore and Cluster resources.
helm upgrade --install cert-manager jetstack/cert-manager \
  --version v1.19.6 \
  --namespace cert-manager \
  --create-namespace \
  --wait \
  --timeout 5m \
  --set crds.enabled=true

helm upgrade --install cnpg cnpg/cloudnative-pg \
  --version 0.29.1 \
  --namespace cnpg-system \
  --create-namespace \
  --wait \
  --timeout 5m

helm upgrade --install plugin-barman-cloud cnpg/plugin-barman-cloud \
  --version 0.8.1 \
  --namespace cnpg-system \
  --wait \
  --timeout 5m

helm upgrade --install strimzi-cluster-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --version 1.3.0 \
  --namespace pushkin-system \
  --create-namespace \
  --wait \
  --timeout 5m \
  --set 'watchNamespaces={pushkin-data}'

helm upgrade --install k8ssandra-operator k8ssandra/k8ssandra-operator \
  --version 1.32.6 \
  --namespace k8ssandra-operator \
  --create-namespace \
  --wait \
  --timeout 5m

kubectl -n k8ssandra-operator create secret generic medusa-bucket-key \
  --from-file=credentials="${platform_dir}/local/medusa-gcp-key.json" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n pushkin-system create configmap pushkin-cassandra-migrations \
  --from-file="${repository_dir}/internal/infrastructure/cassandra/migrations" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n pushkin-system create secret docker-registry pushkin-image-registry \
  --docker-server="${registry_host}" \
  --docker-username=_json_key \
  --docker-password="$(<"${registry_key_file}")" \
  --dry-run=client -o yaml | kubectl apply -f -

# A Job pod template is immutable. Migrations are idempotent, so recreate the
# completed Jobs when a new Pushkin image changes their template.
kubectl -n pushkin-system delete job pushkin-postgres-migrations pushkin-cassandra-migrations \
  --ignore-not-found \
  --wait=true

kubectl apply -k "${platform_dir}"
kubectl wait --for=condition=Ready cluster/pushkin-postgres -n pushkin-data --timeout=20m
kubectl wait --for=condition=Ready kafka/pushkin-kafka -n pushkin-data --timeout=20m
kubectl rollout status -n pushkin-data statefulset/redis --timeout=5m
kubectl wait --for=condition=complete job/pushkin-postgres-migrations -n pushkin-system --timeout=20m
kubectl wait --for=condition=Ready cassandradatacenter/dc1 -n k8ssandra-operator --timeout=20m
kubectl get secret pushkin-cassandra-superuser -n k8ssandra-operator -o json |
  jq '
    del(
      .metadata.annotations,
      .metadata.creationTimestamp,
      .metadata.managedFields,
      .metadata.namespace,
      .metadata.ownerReferences,
      .metadata.resourceVersion,
      .metadata.uid
    ) | .metadata.namespace = "pushkin-system"
  ' |
  kubectl apply -f -
kubectl wait --for=condition=complete job/pushkin-cassandra-migrations -n pushkin-system --timeout=20m
kubectl rollout status -n pushkin-system deployment/pushkin --timeout=10m
