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
  description = "Google Cloud zone for both benchmark VMs."
  default     = "europe-west1-b"
}

variable "ssh_public_key_path" {
  type        = string
  description = "Absolute path to the operator public SSH key."
}

variable "ssh_allowed_cidr" {
  type        = string
  description = "Operator public IPv4 CIDR permitted to use SSH."
}

variable "image_family" {
  type        = string
  description = "Ubuntu public image family."
  default     = "ubuntu-2404-lts-amd64"
}

variable "pushkin_machine_type" {
  type        = string
  description = "Compute Engine machine type for Pushkin and its local dependencies."
  default     = "n2-custom-8-16384"
}

variable "pushkin_boot_disk_gb" {
  type    = number
  default = 40
}

variable "fake_fcm_machine_type" {
  type        = string
  description = "Compute Engine machine type for Fake FCM."
  default     = "n2-custom-4-8192"
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
