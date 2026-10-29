locals {
  name_prefix = "pushkin-single-node-benchmark"
  labels = {
    project     = "pushkin"
    environment = "benchmark"
    benchmark   = "single-node-delivery"
    managed_by  = "terraform"
  }
  ssh_key = trimspace(file(var.ssh_public_key_path))
}

data "google_compute_image" "ubuntu" {
  family  = var.image_family
  project = "ubuntu-os-cloud"
}

resource "google_compute_network" "benchmark" {
  name                    = "${local.name_prefix}-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "benchmark" {
  name          = "${local.name_prefix}-subnet"
  ip_cidr_range = "10.130.0.0/24"
  network       = google_compute_network.benchmark.id
  region        = var.region
}

resource "google_compute_firewall" "internal" {
  name          = "${local.name_prefix}-internal"
  network       = google_compute_network.benchmark.name
  source_ranges = [google_compute_subnetwork.benchmark.ip_cidr_range]

  allow {
    protocol = "all"
  }
}

resource "google_compute_firewall" "ssh" {
  name          = "${local.name_prefix}-ssh"
  network       = google_compute_network.benchmark.name
  direction     = "INGRESS"
  source_ranges = [var.ssh_allowed_cidr]
  target_tags   = ["pushkin-benchmark-node"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_instance" "pushkin" {
  name         = "${local.name_prefix}-pushkin"
  machine_type = var.pushkin_machine_type
  zone         = var.zone
  labels       = local.labels
  tags         = ["pushkin-benchmark-node", "pushkin-benchmark-bastion"]

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
      size  = var.pushkin_boot_disk_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.benchmark.id
    access_config {}
  }

  metadata = {
    ssh-keys = "ubuntu:${local.ssh_key}"
  }

  scheduling {
    automatic_restart           = !var.spot
    instance_termination_action = var.spot ? "STOP" : null
    preemptible                 = var.spot
    provisioning_model          = var.spot ? "SPOT" : "STANDARD"
  }
}

resource "google_compute_instance" "fake_fcm" {
  name         = "${local.name_prefix}-fake-fcm"
  machine_type = var.fake_fcm_machine_type
  zone         = var.zone
  labels       = local.labels
  tags         = ["pushkin-benchmark-node"]

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
      size  = var.fake_fcm_boot_disk_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.benchmark.id

    # This VM needs package and container-image downloads. In this disposable
    # benchmark topology the firewall still blocks all external ingress.
    access_config {}
  }

  metadata = {
    ssh-keys = "ubuntu:${local.ssh_key}"
  }

  scheduling {
    automatic_restart           = !var.spot
    instance_termination_action = var.spot ? "STOP" : null
    preemptible                 = var.spot
    provisioning_model          = var.spot ? "SPOT" : "STANDARD"
  }
}
