# Horizontal delivery benchmark

This experiment compares a single Pushkin instance with a group of four
instances sharing Kafka consumer groups. One infrastructure VM runs PostgreSQL,
Cassandra, Redis, Kafka and migrations. Four Pushkin VMs run one service process each. A
separate VM runs Fake FCM.

Kafka topics use four partitions in the application. This matches the four
Pushkin instances in the benchmark. Partition count is an immutable
topic-topology decision: use `./reset.sh` after changing it so the benchmark
recreates Kafka from an empty volume.

## Prerequisites

- Terraform 1.6+, authenticated Yandex Cloud CLI, and an SSH public key.
- Ansible Core 2.21+ available as `ansible-playbook`.
- SSH and `jq` on the operator machine.

Set the normal Yandex Cloud environment variables; do not place tokens in this
repository:

```bash
export YC_TOKEN="$(yc iam create-token)"
export YC_CLOUD_ID="$(yc config get cloud-id)"
export YC_FOLDER_ID="$(yc config get folder-id)"
```

Create local Terraform variables:

```bash
cd deploy/benchmarks/horizontal-delivery/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply
```

## Run

The benchmark has one entrypoint. It applies Terraform, deploys the current
source revision, prepares the ten-channel fixture, starts the workload, prints
Fake FCM progress, and saves a timestamped summary under `results/`. While the
workload runs, an internal monitor polls every VM through SSH. Each result
directory therefore also contains `metrics.ndjson` with raw samples and
`metrics-summary.json` with average and maximum host, container, PostgreSQL,
Cassandra, Redis, and Kafka consumer-lag metrics. Collection failures are reported in the
summary without aborting the workload. It also records final lag for
`pushkin.campaign.progress` and `pushkin.notification.accepted` after Fake FCM
finishes.

```bash
cd deploy/benchmarks/horizontal-delivery
./run.sh batched-ten
```

`dataset.yml` assumes a fresh benchmark database. It fails when its marker is
already present, rather than silently duplicating data.

## Repeat a run

To repeat the workload on the same VMs, reset only the disposable Compose
stacks and their volumes. Results and Terraform resources are preserved:

```bash
./reset.sh
./run.sh batched-ten
```

`reset.sh` removes PostgreSQL, Cassandra, Redis, and Kafka data from the infrastructure
VM, stops all Pushkin instances, resets Fake FCM counters, and deletes only the
local dataset marker. It does not delete the VMs, network, benchmark secrets,
or prior result files.

## Cleanup

Stop a run without losing its disks from the cloud console if you plan to
resume it soon. When it is no longer needed, remove every Terraform-managed,
billable resource through the benchmark entrypoint:

```bash
./reset.sh --destroy
```
