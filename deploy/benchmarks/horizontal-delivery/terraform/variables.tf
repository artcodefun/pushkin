variable "project_id" {
  type        = string
  description = "Google Cloud project ID that owns this disposable benchmark environment."
}

variable "region" {
  type        = string
  description = "Google Cloud region that owns the benchmark subnet."
  default     = "europe-west1"
}

variable "zone" {
  type        = string
  description = "Google Cloud zone for all benchmark VMs."
  default     = "europe-west1-b"
}

variable "ssh_public_key_path" {
  type        = string
  description = "Absolute path to the public SSH key installed for the ubuntu user."
}

variable "ssh_allowed_cidr" {
  type        = string
  description = "Operator public IPv4 CIDR allowed to SSH to the bastion, for example 203.0.113.10/32."
}

variable "image_family" {
  type        = string
  description = "Ubuntu public image family."
  default     = "ubuntu-2404-lts-amd64"
}

variable "pushkin_instance_count" {
  type        = number
  description = "Number of Pushkin VMs sharing the Kafka consumer groups."
  default     = 4

  validation {
    condition     = var.pushkin_instance_count > 0
    error_message = "pushkin_instance_count must be positive."
  }
}

variable "pushkin_machine_type" {
  type    = string
  default = "n2-custom-6-12288"
}

variable "pushkin_boot_disk_gb" {
  type    = number
  default = 20
}

variable "kafka_machine_type" {
  type    = string
  default = "n2-custom-6-12288"
}

variable "kafka_boot_disk_gb" {
  type    = number
  default = 30
}

variable "postgres_machine_type" {
  type    = string
  default = "n2-custom-6-12288"
}

variable "postgres_boot_disk_gb" {
  type    = number
  default = 30
}

variable "cassandra_machine_type" {
  type    = string
  default = "n2-custom-6-12288"
}

variable "cassandra_boot_disk_gb" {
  type    = number
  default = 30
}

variable "fake_fcm_machine_type" {
  type    = string
  default = "n2-custom-6-12288"
}

variable "fake_fcm_boot_disk_gb" {
  type    = number
  default = 20
}

variable "spot" {
  type        = bool
  description = "Use interruptible Spot VMs. Keep false for a stable comparison run."
  default     = false
}
