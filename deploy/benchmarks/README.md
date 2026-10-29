# Pushkin benchmarks

Each directory is one reproducible benchmark environment. It owns its Terraform
topology, Ansible deployment, dataset definition, and workload scenarios.

Terraform is deliberately limited to Google Cloud resources: VMs, the private
network, firewall rules, and their addresses. Ansible configures hosts, builds
the current source revision, starts containers, and prepares data. Terraform
state, generated inventories, credentials, and collected results are local and
ignored by Git.

The first benchmark is [horizontal-delivery](./horizontal-delivery/). It
measures delivery throughput with PostgreSQL, Redis, Kafka, Cassandra, four
Pushkin VMs, and one Fake FCM VM on separate hosts.

Do not treat these files as production deployment configuration. They optimize
for repeatable, disposable experiments and intentionally use one-node Kafka and
PostgreSQL.
