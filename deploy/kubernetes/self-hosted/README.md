# Self-hosted Kubernetes

This directory provisions a production-shaped learning cluster in Google Cloud:
three combined control-plane/worker nodes and two additional workers. Every
node remains schedulable for workloads; the control-plane role only determines
which nodes participate in the kubeadm/etcd quorum.

Terraform owns only IaaS: the VPC network, subnet, firewall rules, VM nodes,
Cloud NAT, two private Cloud Storage buckets for database backups, a private
Artifact Registry repository for Pushkin images, and a public TCP Network Load
Balancer. The next layer, Ansible, installs and bootstraps Kubernetes.

Terraform also creates narrowly scoped service accounts for each backup bucket,
image pulls, and the GCE Persistent Disk CSI driver. It does not create
backup or registry private keys: Terraform state is not an appropriate place
for production credentials. The CSI identity is attached to each VM and uses
the Compute Engine metadata service instead.

The registry is intentionally private. Terraform creates a separate pull-only
service account but not its private key, because that value would otherwise be
stored in Terraform state.

## First use

```bash
cd deploy/kubernetes/self-hosted/terraform
cp terraform.tfvars.example terraform.tfvars
gcloud auth application-default login
terraform init
terraform plan
terraform apply
```

Only `node-01` receives a public IP. It is the SSH bastion and is also used to
tunnel the private Kubernetes API; the other four nodes have private addresses
only. `operator_allowed_cidr` permits SSH to the bastion from the operator's
current public IP. Cloud NAT gives every private node outbound internet access
for package and image downloads without exposing it publicly.

The public HTTP load balancer is a separate endpoint. It targets the ingress
controller's private NodePort on every cluster node; it does not make any
additional node address public. Its IP is emitted as `ingress_public_ip` and
is intended only for temporary HTTP testing until a domain and TLS are added.

After Kubernetes bootstrap, access the API through the bastion:

```bash
ssh -L 6443:<node-01-private-ip>:6443 ubuntu@<node-01-public-ip>
```

Follow the complete bootstrap and kubeconfig instructions in
[`ansible/README.md`](ansible/README.md). Terraform emits the matching
Ansible inventory after `apply`; this keeps the VM topology in one source of
truth rather than duplicating private IP addresses by hand.

For the normal workflow, use the external entrypoints instead of running the
layers manually. `scripts/provision.sh` creates cloud infrastructure and turns
the VMs into Kubernetes; `scripts/apply-platform.sh` then applies all
Kubernetes operators and workloads as one declarative platform. See
[`platform/README.md`](platform/README.md) for its required local secret inputs.
The operator workstation needs `kubectl` and Helm; neither tool needs to be
installed on a Kubernetes node.

This initial topology is deliberately single-zone. It provides node-level
replica and etcd-quorum practice, but not availability-zone fault tolerance or
a highly available API endpoint: node-01 is the sole API endpoint. A
multi-zone topology and private load balancer will be introduced only after the
single-zone bootstrap is fully reproducible.

## Disposing of a test cluster

Delete the Kubernetes workloads before destroying the VM infrastructure. This
lets the CSI driver remove the dynamically provisioned Persistent Disks while
the cluster and its cloud identity are still available. A disposable test
cluster can delete the Pushkin namespaces and then wait for all PVCs to be
gone:

```bash
KUBECONFIG=ansible/.generated/admin.local.conf \
  kubectl delete namespace pushkin-system pushkin-data k8ssandra-operator \
  --wait=true --timeout=20m
KUBECONFIG=ansible/.generated/admin.local.conf kubectl get pvc -A
```

Only run `terraform destroy` after that command shows no PVCs. Backup buckets
deliberately use `force_destroy = false`: if a PostgreSQL or Cassandra backup
has been written, Terraform will refuse to delete its non-empty bucket. This
is a protection against accidental backup loss. Inspect those objects and make
an explicit retention/deletion decision before retrying destroy.

## Cloud prerequisites

Before the first apply, enable Compute Engine, Artifact Registry, Cloud Storage,
and IAM APIs in the selected project. The local operator also needs authenticated
`gcloud`, Terraform, Ansible, `kubectl`, Helm, Docker, and `jq`.

```bash
gcloud services enable \
  compute.googleapis.com \
  artifactregistry.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com \
  --project <your-project-id>
```

Replace `<your-project-id>` with the value in `terraform.tfvars`. Enabling APIs
is kept outside Terraform: many organisations restrict that permission, and an
otherwise harmless plan should not silently change project-wide service state.
