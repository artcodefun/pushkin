# Kubernetes platform layer

This directory is the declarative application platform that runs after
Terraform has created VMs and Ansible has bootstrapped Kubernetes and installed
the GCE Persistent Disk CSI driver. It deliberately contains no credentials.

## Components

- `namespaces.yaml` separates durable data (`pushkin-data`) from platform
  operators and later Pushkin runtime resources (`pushkin-system`). A namespace
  is Kubernetes' logical tenancy boundary: names, secrets, RBAC and many
  network policies are scoped to it.
- `redis.yaml` is an ordinary Kubernetes `StatefulSet`, the controller used
  when a pod needs a stable identity. Redis has no PVC because its cache and
  rate-limit state is intentionally disposable in this topology.
- Helm installs the two-replica NGINX Ingress Controller with stable NodePorts.
  Terraform's public Network Load Balancer forwards port 80 to HTTP NodePort
  `30080`; port 443 is reserved for the later TLS rollout.
- `postgres.yaml` declares a three-instance CloudNativePG cluster, one
  synchronous standby, a separate 100 GiB Persistent Disk for every instance,
  and a daily online base backup plus continuous WAL archiving.
- Kafka and Cassandra will be declared through their operators.
  An operator is a controller that watches a higher-level custom resource and
  creates the StatefulSets, Services, PVCs, failover and reconciliation logic
  needed to keep it true. This avoids treating database topology as a pile of
  hand-maintained Pods.

The data PVCs explicitly select `gce-pd-balanced`. The CSI driver turns
each PVC into an individual GCE Persistent Disk; it is never a shared disk
between database replicas.

## Apply order

1. Bootstrap Kubernetes with `ansible/bootstrap.yml`.
2. Install the Compute Disk CSI driver with `ansible/storage.yml`.
3. Create the application-owned local inputs before the platform is applied:

   ```bash
   mkdir -p local
   cp local/postgres-backup-credentials.env.example local/postgres-backup-credentials.env
   cp local/postgres-backup-settings.env.example local/postgres-backup-settings.env
   cp local/postgres-app.env.example local/postgres-app.env
   cp local/cassandra-backup-settings.env.example local/cassandra-backup-settings.env
   cp local/medusa-gcp-key.json.example local/medusa-gcp-key.json
   cp local/cloud-registry-pull.env.example local/cloud-registry-pull.env
   cp local/pushkin-image.env.example local/pushkin-image.env
   cp local/pushkin-runtime.env.example local/pushkin-runtime.env
   ```

   Set real values for `postgres-app.env` and `pushkin-runtime.env`; their
   passwords and encryption keys are application-owned and are intentionally
   never generated or rotated by deployment automation.

   The other files are derived from Terraform outputs. After every fresh
   `terraform apply`, prepare them with:

   ```bash
   ../scripts/prepare-platform-inputs.sh
   ```

   The script creates or reuses the required GCS HMAC key and JSON service
   account keys under this ignored directory. It recognises a stale key after
   `terraform destroy` and creates a replacement without placing a secret in
   Terraform state.

   Build and publish the matching Linux/amd64 image with:

   ```bash
   ../scripts/publish-image.sh
   ```

   The script writes the immutable image reference to `pushkin-image.env`.
4. Apply the entire platform in one operation:

   ```bash
   ../scripts/apply-platform.sh
   ```

   Kustomize creates three Secrets locally during rendering: PostgreSQL backup
   credentials, the PostgreSQL application role, and Pushkin runtime settings.
   `apply-platform.sh` creates the separate Medusa and private-registry Secrets
   from the remaining local inputs. None of their contents are committed to the
   repository.

`apply-platform.sh` requires local `kubectl` and Helm. Helm owns all
third-party releases and their CRDs: NGINX Ingress Controller, cert-manager,
CloudNativePG, the Barman Cloud plugin, Strimzi, and K8ssandra. It uses pinned
chart versions rather than mutable `latest` versions. Kustomize applies only
the Pushkin-specific resources after the CRDs are registered.

The script submits the platform resources together, then waits for PostgreSQL
and Cassandra migration Jobs before it reports success. It does not currently
make the Pushkin Deployment wait to start for those Jobs; the application must
therefore tolerate a short migration window during its own startup. The
Cassandra migration ConfigMap is rendered from the repository's canonical
`internal/infrastructure/cassandra/migrations/` directory at apply time, so
the deployment layer does not maintain a second copy of CQL migrations.

The load balancer deliberately uses a TCP health check for now: the ingress
controller's own readiness endpoint is not a public HTTP route, and Pushkin
does not yet provide its route. Once Pushkin is deployed, `terraform output
ingress_public_ip` gives a temporary HTTP endpoint for functional testing. A
domain name and TLS are intentionally deferred.
