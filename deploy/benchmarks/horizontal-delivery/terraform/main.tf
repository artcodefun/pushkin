locals {
  name_prefix = "pushkin-horizontal-benchmark"
  labels = {
    project     = "pushkin"
    environment = "benchmark"
    benchmark   = "horizontal-delivery"
    managed_by  = "terraform"
  }
  ssh_key = trimspace(file(var.ssh_public_key_path))

  pushkin_nodes = {
    for number in range(var.pushkin_instance_count) : "pushkin-${number + 1}" => {
      machine_type = var.pushkin_machine_type
      boot_disk_gb = var.pushkin_boot_disk_gb
      bastion      = number == 0
    }
  }
  service_nodes = {
    kafka = {
      machine_type = var.kafka_machine_type
      boot_disk_gb = var.kafka_boot_disk_gb
      bastion      = false
    }
    postgres = {
      machine_type = var.postgres_machine_type
      boot_disk_gb = var.postgres_boot_disk_gb
      bastion      = false
    }
    cassandra = {
      machine_type = var.cassandra_machine_type
      boot_disk_gb = var.cassandra_boot_disk_gb
      bastion      = false
    }
    fake-fcm = {
      machine_type = var.fake_fcm_machine_type
      boot_disk_gb = var.fake_fcm_boot_disk_gb
      bastion      = false
    }
  }
  nodes = merge(local.pushkin_nodes, local.service_nodes)
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
  ip_cidr_range = "10.129.0.0/24"
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
  target_tags   = ["pushkin-benchmark-bastion"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_instance" "node" {
  for_each     = local.nodes
  name         = "${local.name_prefix}-${each.key}"
  machine_type = each.value.machine_type
  zone         = var.zone
  labels       = local.labels
  tags         = each.value.bastion ? ["pushkin-benchmark-node", "pushkin-benchmark-bastion"] : ["pushkin-benchmark-node"]

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
      size  = each.value.boot_disk_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.benchmark.id

    # Each node needs outbound package and image downloads. Firewall rules still
    # deny every external ingress connection except SSH to the bastion.
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
