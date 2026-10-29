# Single-node delivery benchmark

This is the baseline for the horizontal benchmark. One Pushkin VM runs the
application together with PostgreSQL, Cassandra, Redis, Kafka, and migrations. A separate
VM runs Fake FCM, keeping provider latency and network work outside Pushkin.

The workload is 100,000 immediate one-recipient inline campaigns. It measures
both HTTP campaign submission and subsequent end-to-end delivery to Fake FCM.

## Prerequisites

- Terraform 1.6+, Google Cloud CLI (`gcloud`), and an SSH public key.
- Ansible Core 2.21+ available as `ansible-playbook`.
- Go, SSH, `curl`, and `jq` on the operator machine.

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
cd deploy/benchmarks/single-node-delivery/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init -reconfigure
terraform plan
```

Google Cloud uses the separate ignored `google-cloud.tfstate` file; do not
migrate the former Yandex Cloud state into it. Destroy any still-needed Yandex
resources from a checkout containing the former configuration before removing
its state.

The default VM sizes reproduce the prior 8-vCPU/16-GB Pushkin host and
4-vCPU/8-GB Fake FCM host. Set `spot = true` in `terraform.tfvars` only for
exploratory runs: Google can stop a Spot VM at any time.

Both VMs receive ephemeral external IP addresses for package and container-image
downloads, avoiding a separately billed Cloud NAT gateway. The firewall permits
external SSH to both nodes only from `ssh_allowed_cidr`; Fake FCM's application
port remains reachable only over its private address.

## Run

The benchmark has one entrypoint. It applies Terraform, deploys the current
source revision, prepares the fixture, opens a local API tunnel, starts the Go
load generator, prints Fake FCM counters, and writes a timestamped summary
under `results/`. During the workload it polls both VMs over SSH. The result
directory contains raw `metrics.ndjson`, an aggregated `metrics-summary.json`,
and the same metrics nested in `summary.json`. Per-host CPU, memory and
container load are always sampled; PostgreSQL, Cassandra, Redis, and Kafka consumer lag
are sampled on the Pushkin VM. After Fake FCM finishes, the summary also records
the final consumer lag for `pushkin.campaign.progress` and
`pushkin.notification.accepted`.

```bash
cd deploy/benchmarks/single-node-delivery
./run.sh inline-100k
```

`dataset.yml` assumes a fresh benchmark database. It fails when its marker is
already present, rather than silently duplicating data.

## Repeat a run

To repeat the workload on the same VMs, reset only the disposable Compose
stacks and their volumes. Results and Terraform resources are preserved:

```bash
./reset.sh
./run.sh inline-100k
```

Available scenarios:

- `inline-100k` creates 100,000 immediate inline campaigns for one channel.
- `batched-ten` creates ten prepared batched campaigns with 100,000 recipients
  each, then starts all ten together.

Every scenario owns `prepare.yml`, `start.yml`, and `wait.yml` under
`ansible/scenarios/<scenario>/`; the top-level runner owns only infrastructure
lifecycle and invokes those phases in order.

`reset.sh` removes PostgreSQL, Cassandra, Redis, and Kafka data from the Pushkin VM,
resets Fake FCM counters, and deletes only the local dataset marker. It does
not delete the VMs, network, benchmark secrets, or prior result files.

## Cleanup

Stop a run without losing its disks from the cloud console if you plan to
resume it soon. When it is no longer needed, remove every Terraform-managed,
billable resource through the benchmark entrypoint:

```bash
./reset.sh --destroy
```

Generated inventory, random test keys, Terraform state, and result files live
outside version control. `summary.json` includes the source revision,
submission duration, and Fake FCM counters.
