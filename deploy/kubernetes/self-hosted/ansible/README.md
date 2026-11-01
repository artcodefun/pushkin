# Fixed-capacity Kubernetes bootstrap

This layer turns the virtual machines made by `../terraform` into a kubeadm
cluster. It deliberately does **not** install a node autoscaler, an ingress
controller, or Pushkin's stateful dependencies. Those choices are workload
architecture, not prerequisites of a fixed node cluster.

The result is five schedulable nodes: three control-plane/etcd members and two
workers. `node-01` is also the SSH bastion and API endpoint. Therefore this is
not a highly available API endpoint despite the etcd quorum; introduce a
private load balancer or virtual IP before claiming control-plane HA.

## Prerequisites

- Terraform 1.6+, Google Cloud credentials, and an SSH key accepted by the Terraform
  configuration.
- Ansible Core 2.17+ on the operator machine. The playbook needs no
  third-party Ansible collections.

- SSH access to `node-01`; private nodes are reached with ProxyJump through
  it. Keep the SSH agent loaded, or add `ansible_ssh_private_key_file` to the
  generated inventory locally.

## Bootstrap

Apply Terraform first, then render its output into an ignored local inventory:

```bash
cd deploy/kubernetes/self-hosted/terraform
terraform apply
mkdir -p ../ansible/.generated
terraform output -raw ansible_inventory > ../ansible/.generated/hosts.yml

cd ../ansible
ansible-playbook -i .generated/hosts.yml bootstrap.yml
```

The final play fetches the admin kubeconfig to `.generated/admin.conf`. The API
is private, so open the tunnel in another terminal and use a copy whose server
address points to the local side of that tunnel. Preserve the private node IP
as the TLS server name: it is a certificate SAN, whereas `127.0.0.1` is not.

```bash
ssh -N -L 6443:<node-01-private-ip>:6443 ubuntu@<node-01-public-ip>

KUBECONFIG=.generated/admin.conf kubectl config view --raw > /tmp/pushkin-admin.local.conf
KUBECONFIG=/tmp/pushkin-admin.local.conf kubectl config set-cluster kubernetes \
  --server=https://127.0.0.1:6443 \
  --tls-server-name=<node-01-private-ip>
mv /tmp/pushkin-admin.local.conf .generated/admin.local.conf

KUBECONFIG=.generated/admin.local.conf kubectl get nodes
```

Do not commit `.generated/`: `admin.conf` grants `cluster-admin` access.

## Dynamic persistent volumes

Before deploying PostgreSQL, Kafka, or Cassandra, install the GCE Persistent
Disk CSI driver. It supplies one independently provisioned `ReadWriteOnce`
disk per database replica through the `gce-pd-balanced` StorageClass.

Terraform attaches its dedicated `gce_pd_csi_service_account` to every
cluster VM and grants it `roles/compute.storageAdmin` plus a narrowly scoped
custom role with `compute.instances.get`, `compute.instances.attachDisk`, and
`compute.instances.detachDisk`, plus `roles/iam.serviceAccountUser` on its own
service account. The latter permits attaching a disk only to VMs that use that
same identity. The CSI driver uses the VM metadata identity, so there is no private key in
Terraform state, Ansible inventory, or a Kubernetes Secret. This is a
pragmatic fixed-capacity kubeadm design; a future hardened multi-tenant
cluster should replace it with workload identity or a driver-specific identity
boundary.

```bash
ansible-playbook -i .generated/hosts.yml storage.yml
```

Confirm that dynamic volumes are available before continuing:

```bash
KUBECONFIG=.generated/admin.local.conf kubectl get storageclass gce-pd-balanced
```

Do not make this class the cluster default: ordinary Pods should not acquire
billable disks accidentally. Stateful platform manifests name it explicitly.

## Lifecycle boundaries

Terraform is the source of truth for the fixed VM count. Change
`control_plane_count` or `worker_count`, apply Terraform, regenerate the
inventory, and use a dedicated controlled node-add/remove runbook; do not
blindly rerun this bootstrap after removing existing nodes. Kubernetes may
scale **pods** within this fixed capacity, but there is no Cluster Autoscaler
in this profile.
