variable "zone" {
  type    = string
  default = "ru-central1-a"
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
  type    = string
  default = "ubuntu-2404-lts"
}

variable "pushkin_cores" {
  type    = number
  default = 8
}

variable "pushkin_memory_gb" {
  type    = number
  default = 16
}

variable "fake_fcm_cores" {
  type    = number
  default = 4
}

variable "fake_fcm_memory_gb" {
  type    = number
  default = 8
}

variable "preemptible" {
  type    = bool
  default = false
}
