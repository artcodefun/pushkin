output "control_plane_nodes" {
  description = "Control-plane nodes. They remain available for workload scheduling after bootstrap."
  value = [for name, node in google_compute_instance.node : {
    name = name, private_ip = node.network_interface[0].network_ip, public_ip = try(node.network_interface[0].access_config[0].nat_ip, null)
  } if local.nodes[name].control_plane]
}

output "administrative_node" {
  description = "The sole public node used as the SSH bastion and private Kubernetes API tunnel endpoint."
  value = {
    name       = "node-01"
    private_ip = google_compute_instance.node["node-01"].network_interface[0].network_ip
    public_ip  = google_compute_instance.node["node-01"].network_interface[0].access_config[0].nat_ip
  }
}

output "administrative_node_public_ip" {
  description = "Public SSH address of the disposable administrative bastion."
  value       = google_compute_instance.node["node-01"].network_interface[0].access_config[0].nat_ip
}

output "ingress_public_ip" { value = google_compute_address.ingress.address }
output "worker_nodes" {
  description = "All schedulable nodes, including the control-plane nodes."
  value = [for name, node in google_compute_instance.node : {
    name = name, control_plane = local.nodes[name].control_plane, private_ip = node.network_interface[0].network_ip, public_ip = try(node.network_interface[0].access_config[0].nat_ip, null)
  }]
}

output "cluster_network" {
  value = { network_id = google_compute_network.cluster.id, subnet_id = google_compute_subnetwork.cluster.id, subnet_cidr = google_compute_subnetwork.cluster.ip_cidr_range }
}

output "postgres_backup_storage" {
  value = { bucket_name = google_storage_bucket.postgres_backups.name, service_account_email = google_service_account.postgres_backups.email }
}

output "cassandra_backup_storage" {
  value = { bucket_name = google_storage_bucket.cassandra_backups.name, service_account_email = google_service_account.cassandra_backups.email }
}

output "pushkin_image_repository" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.pushkin.repository_id}/pushkin"
}

output "pushkin_image_pull_service_account" { value = google_service_account.registry_puller.email }
output "gce_pd_csi_service_account" { value = google_service_account.gce_pd_csi.email }

output "ansible_inventory" {
  description = "Ansible inventory for bootstrapping the fixed-capacity cluster."
  value       = <<-YAML
    all:
      vars:
        ansible_user: ubuntu
        ansible_ssh_common_args: "-o StrictHostKeyChecking=accept-new"
      children:
        kube_bootstrap_control_plane:
          hosts:
            node-01:
        kube_additional_control_plane:
          hosts:
%{for name, node in google_compute_instance.node~}
%{if local.nodes[name].control_plane && name != "node-01"~}
            ${name}:
%{endif~}
%{endfor~}
        kube_control_plane:
          hosts:
%{for name, node in google_compute_instance.node~}
%{if local.nodes[name].control_plane~}
            ${name}:
              ansible_host: ${try(node.network_interface[0].access_config[0].nat_ip, node.network_interface[0].network_ip)}
              kube_node_ip: ${node.network_interface[0].network_ip}
%{if name != "node-01"~}
              ansible_ssh_common_args: "-o StrictHostKeyChecking=accept-new -o ProxyJump=ubuntu@${google_compute_instance.node["node-01"].network_interface[0].access_config[0].nat_ip}"
%{endif~}
%{endif~}
%{endfor~}
        kube_workers:
          hosts:
%{for name, node in google_compute_instance.node~}
%{if !local.nodes[name].control_plane~}
            ${name}:
              ansible_host: ${node.network_interface[0].network_ip}
              kube_node_ip: ${node.network_interface[0].network_ip}
              ansible_ssh_common_args: "-o StrictHostKeyChecking=accept-new -o ProxyJump=ubuntu@${google_compute_instance.node["node-01"].network_interface[0].access_config[0].nat_ip}"
%{endif~}
%{endfor~}
  YAML
}
