output "infrastructure" {
  description = "The VM hosting PostgreSQL, Redis, Kafka, and migrations."
  value = {
    private_ip = yandex_compute_instance.infrastructure.network_interface[0].ip_address
    public_ip  = yandex_compute_instance.infrastructure.network_interface[0].nat_ip_address
  }
}

output "pushkin_instances" {
  description = "Pushkin VMs which share all Kafka consumer groups."
  value = [for instance in yandex_compute_instance.pushkin : {
    private_ip = instance.network_interface[0].ip_address
    public_ip  = instance.network_interface[0].nat_ip_address
  }]
}

output "fake_fcm" {
  description = "The private Fake FCM target and public SSH endpoint."
  value = {
    private_ip = yandex_compute_instance.fake_fcm.network_interface[0].ip_address
    public_ip  = yandex_compute_instance.fake_fcm.network_interface[0].nat_ip_address
  }
}
