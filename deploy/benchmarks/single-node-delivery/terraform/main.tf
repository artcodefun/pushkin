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

data "yandex_compute_image" "ubuntu" {
  family = var.image_family
}

resource "yandex_vpc_network" "benchmark" {
  name   = "${local.name_prefix}-network"
  labels = local.labels
}

resource "yandex_vpc_subnet" "benchmark" {
  name           = "${local.name_prefix}-subnet"
  zone           = var.zone
  network_id     = yandex_vpc_network.benchmark.id
  v4_cidr_blocks = ["10.130.0.0/24"]
  labels         = local.labels
}

resource "yandex_vpc_security_group" "pushkin" {
  name       = "${local.name_prefix}-pushkin"
  network_id = yandex_vpc_network.benchmark.id
  labels     = local.labels

  ingress {
    protocol       = "TCP"
    v4_cidr_blocks = [var.ssh_allowed_cidr]
    port           = 22
  }

  egress {
    protocol       = "ANY"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "yandex_vpc_security_group" "fake_fcm" {
  name       = "${local.name_prefix}-fake-fcm"
  network_id = yandex_vpc_network.benchmark.id
  labels     = local.labels

  ingress {
    protocol       = "TCP"
    v4_cidr_blocks = [var.ssh_allowed_cidr]
    port           = 22
  }

  ingress {
    protocol       = "TCP"
    v4_cidr_blocks = yandex_vpc_subnet.benchmark.v4_cidr_blocks
    port           = 8081
  }

  egress {
    protocol       = "ANY"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "yandex_compute_instance" "pushkin" {
  name        = "${local.name_prefix}-pushkin"
  platform_id = "standard-v3"
  zone        = var.zone
  labels      = local.labels

  resources {
    cores  = var.pushkin_cores
    memory = var.pushkin_memory_gb
  }

  scheduling_policy { preemptible = var.preemptible }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = 40
      type     = "network-ssd"
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.benchmark.id
    security_group_ids = [yandex_vpc_security_group.pushkin.id]
    nat                = true
  }

  metadata = { ssh-keys = "ubuntu:${local.ssh_key}" }
}

resource "yandex_compute_instance" "fake_fcm" {
  name        = "${local.name_prefix}-fake-fcm"
  platform_id = "standard-v3"
  zone        = var.zone
  labels      = local.labels

  resources {
    cores  = var.fake_fcm_cores
    memory = var.fake_fcm_memory_gb
  }

  scheduling_policy { preemptible = var.preemptible }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = 20
      type     = "network-ssd"
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.benchmark.id
    security_group_ids = [yandex_vpc_security_group.fake_fcm.id]
    nat                = true
  }

  metadata = { ssh-keys = "ubuntu:${local.ssh_key}" }
}
