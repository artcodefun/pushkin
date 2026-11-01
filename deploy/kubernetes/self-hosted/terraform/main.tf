locals {
  name_prefix = "pushkin-self-hosted"
  labels      = { project = "pushkin", environment = "self-hosted", managed_by = "terraform" }
  ssh_key     = trimspace(file(var.ssh_public_key_path))
  node_count  = var.control_plane_count + var.worker_count
  nodes = { for index in range(local.node_count) : format("node-%02d", index + 1) => {
    control_plane = index < var.control_plane_count
  } }
}

data "google_compute_image" "ubuntu" {
  family  = var.image_family
  project = "ubuntu-os-cloud"
}

resource "google_compute_network" "cluster" {
  name                    = "${local.name_prefix}-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "cluster" {
  name          = "${local.name_prefix}-subnet"
  ip_cidr_range = "10.130.0.0/24"
  network       = google_compute_network.cluster.id
  region        = var.region
}

resource "google_compute_router" "egress" {
  name    = "${local.name_prefix}-egress"
  network = google_compute_network.cluster.id
  region  = var.region
}

# Private nodes download packages and images through Cloud NAT. Only node-01
# receives an external IP for SSH and the API tunnel.
resource "google_compute_router_nat" "egress" {
  name                               = "${local.name_prefix}-egress"
  router                             = google_compute_router.egress.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

resource "google_compute_firewall" "internal" {
  name          = "${local.name_prefix}-internal"
  network       = google_compute_network.cluster.name
  source_ranges = [google_compute_subnetwork.cluster.ip_cidr_range]
  target_tags   = ["pushkin-kubernetes-node"]
  allow { protocol = "all" }
}

resource "google_compute_firewall" "ssh" {
  name          = "${local.name_prefix}-ssh"
  network       = google_compute_network.cluster.name
  direction     = "INGRESS"
  source_ranges = [var.operator_allowed_cidr]
  target_tags   = ["pushkin-kubernetes-bastion"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "ingress_http" {
  name          = "${local.name_prefix}-ingress-http"
  network       = google_compute_network.cluster.name
  direction     = "INGRESS"
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["pushkin-kubernetes-node"]
  allow {
    protocol = "tcp"
    ports    = ["30080"]
  }
}

# Google health-check probes originate outside the cluster subnet.
resource "google_compute_firewall" "ingress_health_check" {
  name          = "${local.name_prefix}-ingress-health-check"
  network       = google_compute_network.cluster.name
  direction     = "INGRESS"
  source_ranges = ["35.191.0.0/16", "130.211.0.0/22"]
  target_tags   = ["pushkin-kubernetes-node"]
  allow {
    protocol = "tcp"
    ports    = ["30080"]
  }
}

resource "google_storage_bucket" "postgres_backups" {
  name                        = var.postgres_backup_bucket_name
  location                    = upper(var.region)
  storage_class               = "STANDARD"
  force_destroy               = false
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = local.labels
}

resource "google_storage_bucket" "cassandra_backups" {
  name                        = var.cassandra_backup_bucket_name
  location                    = upper(var.region)
  storage_class               = "STANDARD"
  force_destroy               = false
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = local.labels
}

resource "google_artifact_registry_repository" "pushkin" {
  location      = var.region
  repository_id = "${local.name_prefix}-images"
  description   = "Private OCI images for the Pushkin self-hosted Kubernetes cluster."
  format        = "DOCKER"
  labels        = local.labels
}

# Private keys are deliberately created outside Terraform, so Terraform state
# never becomes a production secret store.
resource "google_service_account" "postgres_backups" {
  account_id   = "pushkin-postgres-backups"
  display_name = "Pushkin PostgreSQL backups"
}

resource "google_service_account" "cassandra_backups" {
  account_id   = "pushkin-cassandra-backups"
  display_name = "Pushkin Cassandra backups"
}

resource "google_service_account" "registry_puller" {
  account_id   = "pushkin-registry-puller"
  display_name = "Pushkin Artifact Registry image puller"
}

resource "google_service_account" "gce_pd_csi" {
  account_id   = "pushkin-gce-pd-csi"
  display_name = "Pushkin GCE Persistent Disk CSI driver"
}

resource "google_storage_bucket_iam_member" "postgres_backups" {
  bucket = google_storage_bucket.postgres_backups.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.postgres_backups.email}"
}

resource "google_storage_bucket_iam_member" "cassandra_backups" {
  bucket = google_storage_bucket.cassandra_backups.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.cassandra_backups.email}"
}

resource "google_artifact_registry_repository_iam_member" "registry_puller" {
  location   = google_artifact_registry_repository.pushkin.location
  repository = google_artifact_registry_repository.pushkin.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.registry_puller.email}"
}

resource "google_project_iam_member" "gce_pd_csi_storage_admin" {
  project = var.project_id
  role    = "roles/compute.storageAdmin"
  member  = "serviceAccount:${google_service_account.gce_pd_csi.email}"
}

// Storage Admin covers the disk resource itself but not the three operations
// the CSI controller needs on the VM to publish or unpublish that disk.
resource "google_project_iam_custom_role" "gce_pd_csi_instance_attachment" {
  project = var.project_id
  role_id = "pushkinGcePdCsiInstanceAttachment"
  title   = "Pushkin GCE PD CSI instance attachment"

  permissions = [
    "compute.instances.get",
    "compute.instances.attachDisk",
    "compute.instances.detachDisk",
  ]
}

resource "google_project_iam_member" "gce_pd_csi_instance_attachment" {
  project = var.project_id
  role    = google_project_iam_custom_role.gce_pd_csi_instance_attachment.name
  member  = "serviceAccount:${google_service_account.gce_pd_csi.email}"
}

# Attaching a disk to a VM also requires permission to act as that VM's
# service account. The CSI identity is allowed to act only as itself, which is
# the sole identity attached to the cluster VMs.
resource "google_service_account_iam_member" "gce_pd_csi_self_user" {
  service_account_id = google_service_account.gce_pd_csi.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.gce_pd_csi.email}"
}

resource "google_compute_instance" "node" {
  for_each     = local.nodes
  name         = "${local.name_prefix}-${each.key}"
  machine_type = var.node_machine_type
  zone         = var.zone
  labels = merge(local.labels, {
    kubernetes_role = each.value.control_plane ? "control-plane-worker" : "worker"
  })
  tags = each.key == "node-01" ? ["pushkin-kubernetes-node", "pushkin-kubernetes-bastion"] : ["pushkin-kubernetes-node"]

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
      size  = var.node_boot_disk_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.cluster.id
    dynamic "access_config" {
      for_each = each.key == "node-01" ? [1] : []
      content {}
    }
  }

  metadata = { ssh-keys = "ubuntu:${local.ssh_key}" }

  # The GCE PD CSI driver obtains this identity through the metadata server.
  # A key is intentionally not created or persisted in Kubernetes.
  service_account {
    email  = google_service_account.gce_pd_csi.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  scheduling {
    automatic_restart           = !var.spot
    instance_termination_action = var.spot ? "STOP" : null
    preemptible                 = var.spot
    provisioning_model          = var.spot ? "SPOT" : "STANDARD"
  }
}

resource "google_compute_instance_group" "ingress" {
  name      = "${local.name_prefix}-ingress"
  zone      = var.zone
  instances = [for node in google_compute_instance.node : node.self_link]
  named_port {
    name = "http"
    port = 30080
  }
}

resource "google_compute_region_health_check" "ingress" {
  name   = "${local.name_prefix}-ingress"
  region = var.region
  tcp_health_check {
    port = 30080
  }
}

resource "google_compute_region_backend_service" "ingress" {
  name                  = "${local.name_prefix}-ingress"
  region                = var.region
  protocol              = "TCP"
  load_balancing_scheme = "EXTERNAL"
  health_checks         = [google_compute_region_health_check.ingress.id]
  backend {
    group          = google_compute_instance_group.ingress.self_link
    balancing_mode = "CONNECTION"
  }
}

resource "google_compute_address" "ingress" {
  name         = "${local.name_prefix}-ingress"
  region       = var.region
  address_type = "EXTERNAL"
  network_tier = "PREMIUM"
}

resource "google_compute_forwarding_rule" "ingress" {
  name                  = "${local.name_prefix}-ingress"
  region                = var.region
  ip_protocol           = "TCP"
  load_balancing_scheme = "EXTERNAL"
  network_tier          = "PREMIUM"
  ip_address            = google_compute_address.ingress.address
  ports                 = ["80"]
  backend_service       = google_compute_region_backend_service.ingress.id
}
