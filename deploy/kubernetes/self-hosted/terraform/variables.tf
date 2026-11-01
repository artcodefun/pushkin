variable "project_id" {
  type        = string
  description = "Google Cloud project ID that owns the self-hosted Kubernetes infrastructure."
}

variable "region" {
  type        = string
  description = "Google Cloud region for networking, Cloud NAT, Artifact Registry, load balancer, and backup buckets."
  default     = "europe-west1"
}

variable "zone" {
  type        = string
  description = "Google Cloud zone for this initial single-zone kubeadm cluster."
  default     = "europe-west1-b"
}

variable "ssh_public_key_path" {
  type        = string
  description = "Absolute path to the public SSH key installed for the ubuntu user on every node."
}

variable "operator_allowed_cidr" {
  type        = string
  description = "Operator public IPv4 CIDR allowed to use SSH on the administrative node."

  validation {
    condition     = can(cidrnetmask(var.operator_allowed_cidr))
    error_message = "operator_allowed_cidr must be a valid IPv4 CIDR."
  }
}

variable "postgres_backup_bucket_name" {
  type        = string
  description = "Globally unique private Cloud Storage bucket for PostgreSQL base backups and WAL archives."

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$", var.postgres_backup_bucket_name))
    error_message = "postgres_backup_bucket_name must be a 3-63 character lowercase Cloud Storage bucket name."
  }
}

variable "cassandra_backup_bucket_name" {
  type        = string
  description = "Globally unique private Cloud Storage bucket for Cassandra backups."

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$", var.cassandra_backup_bucket_name))
    error_message = "cassandra_backup_bucket_name must be a 3-63 character lowercase Cloud Storage bucket name."
  }
}

variable "image_family" {
  type        = string
  description = "Ubuntu public image family used for cluster nodes."
  default     = "ubuntu-2404-lts-amd64"
}

variable "control_plane_count" {
  type        = number
  description = "Number of kubeadm control-plane nodes. Three gives etcd quorum after one node failure."
  default     = 3

  validation {
    condition     = var.control_plane_count == 1 || var.control_plane_count == 3 || var.control_plane_count == 5
    error_message = "control_plane_count must be 1, 3, or 5."
  }
}

variable "worker_count" {
  type        = number
  description = "Additional schedulable worker nodes."
  default     = 2
}

variable "node_machine_type" {
  type        = string
  description = "Compute Engine machine type for each Kubernetes node."
  default     = "n2-custom-6-12288"
}

variable "node_boot_disk_gb" {
  type        = number
  description = "Boot disk size in GiB per cluster node."
  default     = 30
}

variable "spot" {
  type        = bool
  description = "Use interruptible Spot VMs. Keep false for a durable cluster."
  default     = false
}
