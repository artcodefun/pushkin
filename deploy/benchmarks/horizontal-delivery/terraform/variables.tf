variable "zone" {
  type        = string
  description = "Yandex Cloud availability zone for all benchmark VMs."
  default     = "ru-central1-a"
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
  description = "Yandex Cloud public image family."
  default     = "ubuntu-2404-lts"
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

variable "pushkin_cores" {
  type    = number
  default = 6
}

variable "pushkin_memory_gb" {
  type    = number
  default = 12
}

variable "pushkin_boot_disk_gb" {
  type    = number
  default = 20
}

variable "kafka_cores" {
  type    = number
  default = 6
}

variable "kafka_memory_gb" {
  type    = number
  default = 12
}

variable "kafka_boot_disk_gb" {
  type    = number
  default = 30
}

variable "postgres_cores" {
  type    = number
  default = 6
}

variable "postgres_memory_gb" {
  type    = number
  default = 12
}

variable "postgres_boot_disk_gb" {
  type    = number
  default = 30
}

variable "cassandra_cores" {
  type    = number
  default = 6
}

variable "cassandra_memory_gb" {
  type    = number
  default = 12
}

variable "cassandra_boot_disk_gb" {
  type    = number
  default = 30
}

variable "fake_fcm_cores" {
  type    = number
  default = 6
}

variable "fake_fcm_memory_gb" {
  type    = number
  default = 12
}

variable "fake_fcm_boot_disk_gb" {
  type    = number
  default = 20
}

variable "preemptible" {
  type        = bool
  description = "Allow Yandex Cloud to reclaim VMs. Keep false for stable benchmark runs."
  default     = false
}
