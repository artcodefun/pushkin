locals {
  name_prefix = "pushkin-horizontal-benchmark"
  labels = {
    project     = "pushkin"
    environment = "benchmark"
    benchmark   = "horizontal-delivery"
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

resource "yandex_vpc_gateway" "egress" {
  name = "${local.name_prefix}-egress"

  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "benchmark" {
  name       = "${local.name_prefix}-routes"
  network_id = yandex_vpc_network.benchmark.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.egress.id
  }
}

resource "yandex_vpc_subnet" "benchmark" {
  name           = "${local.name_prefix}-subnet"
  zone           = var.zone
  network_id     = yandex_vpc_network.benchmark.id
  v4_cidr_blocks = ["10.129.0.0/24"]
  route_table_id = yandex_vpc_route_table.benchmark.id
  labels         = local.labels
}

resource "yandex_vpc_security_group" "benchmark" {
  name       = "${local.name_prefix}-nodes"
  network_id = yandex_vpc_network.benchmark.id
  labels     = local.labels

  ingress {
    protocol       = "ANY"
    description    = "Traffic between benchmark nodes"
    v4_cidr_blocks = yandex_vpc_subnet.benchmark.v4_cidr_blocks
  }

  ingress {
    protocol       = "TCP"
    description    = "SSH from the benchmark operator to the bastion"
    v4_cidr_blocks = [var.ssh_allowed_cidr]
    port           = 22
  }

  egress {
    protocol       = "ANY"
    description    = "Package downloads, container images, and diagnostics"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "yandex_compute_instance" "pushkin" {
  count                     = var.pushkin_instance_count
  name                      = "${local.name_prefix}-pushkin-${count.index + 1}"
  platform_id               = "standard-v3"
  zone                      = var.zone
  labels                    = local.labels
  allow_stopping_for_update = true

  resources {
    cores  = var.pushkin_cores
    memory = var.pushkin_memory_gb
  }

  scheduling_policy {
    preemptible = var.preemptible
  }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = var.pushkin_boot_disk_gb
      type     = "network-ssd"
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.benchmark.id
    security_group_ids = [yandex_vpc_security_group.benchmark.id]
    nat                = count.index == 0
  }

  metadata = { ssh-keys = "ubuntu:${local.ssh_key}" }
}

resource "yandex_compute_instance" "kafka" {
  name                      = "${local.name_prefix}-kafka"
  platform_id               = "standard-v3"
  zone                      = var.zone
  labels                    = local.labels
  allow_stopping_for_update = true

  resources {
    cores  = var.kafka_cores
    memory = var.kafka_memory_gb
  }

  scheduling_policy {
    preemptible = var.preemptible
  }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = var.kafka_boot_disk_gb
      type     = "network-ssd"
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.benchmark.id
    security_group_ids = [yandex_vpc_security_group.benchmark.id]
  }

  metadata = { ssh-keys = "ubuntu:${local.ssh_key}" }
}

resource "yandex_compute_instance" "postgres" {
  name                      = "${local.name_prefix}-postgres"
  platform_id               = "standard-v3"
  zone                      = var.zone
  labels                    = local.labels
  allow_stopping_for_update = true

  resources {
    cores  = var.postgres_cores
    memory = var.postgres_memory_gb
  }

  scheduling_policy {
    preemptible = var.preemptible
  }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = var.postgres_boot_disk_gb
      type     = "network-ssd"
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.benchmark.id
    security_group_ids = [yandex_vpc_security_group.benchmark.id]
  }

  metadata = { ssh-keys = "ubuntu:${local.ssh_key}" }
}

resource "yandex_compute_instance" "cassandra" {
  name                      = "${local.name_prefix}-cassandra"
  platform_id               = "standard-v3"
  zone                      = var.zone
  labels                    = local.labels
  allow_stopping_for_update = true

  resources {
    cores  = var.cassandra_cores
    memory = var.cassandra_memory_gb
  }

  scheduling_policy {
    preemptible = var.preemptible
  }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = var.cassandra_boot_disk_gb
      type     = "network-ssd"
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.benchmark.id
    security_group_ids = [yandex_vpc_security_group.benchmark.id]
  }

  metadata = { ssh-keys = "ubuntu:${local.ssh_key}" }
}

resource "yandex_compute_instance" "fake_fcm" {
  name                      = "${local.name_prefix}-fake-fcm"
  platform_id               = "standard-v3"
  zone                      = var.zone
  labels                    = local.labels
  allow_stopping_for_update = true

  resources {
    cores  = var.fake_fcm_cores
    memory = var.fake_fcm_memory_gb
  }

  scheduling_policy {
    preemptible = var.preemptible
  }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = var.fake_fcm_boot_disk_gb
      type     = "network-ssd"
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.benchmark.id
    security_group_ids = [yandex_vpc_security_group.benchmark.id]
  }

  metadata = { ssh-keys = "ubuntu:${local.ssh_key}" }
}
