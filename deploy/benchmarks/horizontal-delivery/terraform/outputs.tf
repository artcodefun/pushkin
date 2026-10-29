output "bastion" {
  description = "Public SSH entry point. This is also the first Pushkin VM."
  value = {
    private_ip = google_compute_instance.node["pushkin-1"].network_interface[0].network_ip
    public_ip  = google_compute_instance.node["pushkin-1"].network_interface[0].access_config[0].nat_ip
  }
}

output "pushkin_instances" {
  description = "Pushkin VMs which share all Kafka consumer groups."
  value = [for number in range(var.pushkin_instance_count) : {
    private_ip = google_compute_instance.node["pushkin-${number + 1}"].network_interface[0].network_ip
  }]
}

output "kafka" {
  description = "Private Kafka endpoint."
  value = {
    private_ip = google_compute_instance.node["kafka"].network_interface[0].network_ip
  }
}

output "postgres" {
  description = "Private PostgreSQL and Redis endpoint."
  value = {
    private_ip = google_compute_instance.node["postgres"].network_interface[0].network_ip
  }
}

output "cassandra" {
  description = "Private Cassandra endpoint."
  value = {
    private_ip = google_compute_instance.node["cassandra"].network_interface[0].network_ip
  }
}

output "fake_fcm" {
  description = "Private Fake FCM endpoint."
  value = {
    private_ip = google_compute_instance.node["fake-fcm"].network_interface[0].network_ip
  }
}
