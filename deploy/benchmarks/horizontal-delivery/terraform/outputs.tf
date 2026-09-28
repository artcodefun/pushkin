output "bastion" {
  description = "Public SSH entry point. This is also the first Pushkin VM."
  value = {
    private_ip = yandex_compute_instance.pushkin[0].network_interface[0].ip_address
    public_ip  = yandex_compute_instance.pushkin[0].network_interface[0].nat_ip_address
  }
}

output "pushkin_instances" {
  description = "Pushkin VMs which share all Kafka consumer groups."
  value = [for instance in yandex_compute_instance.pushkin : {
    private_ip = instance.network_interface[0].ip_address
  }]
}

output "kafka" {
  description = "Private Kafka endpoint."
  value = {
    private_ip = yandex_compute_instance.kafka.network_interface[0].ip_address
  }
}

output "postgres" {
  description = "Private PostgreSQL and Redis endpoint."
  value = {
    private_ip = yandex_compute_instance.postgres.network_interface[0].ip_address
  }
}

output "cassandra" {
  description = "Private Cassandra endpoint."
  value = {
    private_ip = yandex_compute_instance.cassandra.network_interface[0].ip_address
  }
}

output "fake_fcm" {
  description = "Private Fake FCM endpoint."
  value = {
    private_ip = yandex_compute_instance.fake_fcm.network_interface[0].ip_address
  }
}
