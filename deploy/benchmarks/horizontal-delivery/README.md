# Horizontal delivery benchmark

This experiment runs four Pushkin instances sharing Kafka consumer groups. The
first Pushkin VM is the only host that permits SSH from the operator; every
other VM is reached through it over private addresses. Kafka, PostgreSQL with
Redis, Cassandra, and Fake FCM
each run on their own VM, so database and broker contention can be measured
separately from Pushkin.

The default shape uses 48 vCPU and 190 GB of network SSD: four 6-vCPU
Pushkin VMs plus four 6-vCPU service VMs. Every VM uses 12 GB of memory.
PostgreSQL/Redis and Fake FCM each receive dedicated resources rather than
competing with Kafka or Cassandra on a shared host.

Kafka topics use four partitions in the application. This matches the four
Pushkin instances in the benchmark. Partition count is an immutable
topic-topology decision: use `./reset.sh` after changing it so the benchmark
recreates Kafka from an empty volume.

## Prerequisites

- Terraform 1.6+, Google Cloud CLI (`gcloud`), and an SSH public key.
- Ansible Core 2.21+ available as `ansible` and `ansible-playbook`.
- SSH and `jq` on the operator machine.

Select the dedicated Google Cloud project, enable Compute Engine, and create
Application Default Credentials for Terraform. The credentials stay in the
local Google Cloud CLI configuration; do not place a service-account key or
token in this repository:

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud services enable compute.googleapis.com
gcloud auth application-default login
```

Create local Terraform variables:

```bash
cd deploy/benchmarks/horizontal-delivery/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init -reconfigure
terraform apply
```

Google Cloud uses the separate ignored `google-cloud.tfstate` file; do not
migrate the former Yandex Cloud state into it. Destroy any still-needed Yandex
resources from a checkout containing the former configuration before removing
its state.

The default VM sizes reproduce the prior 48-vCPU topology. Set `spot = true`
in `terraform.tfvars` only for exploratory runs: Google can stop a Spot VM at
any time.

Every VM receives an ephemeral external IP for package and container-image
downloads, avoiding a separately billed Cloud NAT gateway. The firewall permits
external SSH only to the bastion; Ansible reaches all other hosts over their
private addresses through that bastion.

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
finishes. The runner waits until both groups settle to the current four-record
Kafka commit boundary before writing the summary.

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

`reset.sh` removes PostgreSQL, Cassandra, Redis, and Kafka data from their
respective VMs, stops all Pushkin instances, resets Fake FCM counters, and
deletes only the local dataset marker. It does not delete the VMs, network,
benchmark secrets, or prior result files.

## Cleanup

Stop a run without losing its disks from the cloud console if you plan to
resume it soon. When it is no longer needed, remove every Terraform-managed,
billable resource through the benchmark entrypoint:

```bash
./reset.sh --destroy
```
