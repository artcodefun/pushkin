output "pushkin" {
  value = {
    private_ip = yandex_compute_instance.pushkin.network_interface[0].ip_address
    public_ip  = yandex_compute_instance.pushkin.network_interface[0].nat_ip_address
  }
}

output "fake_fcm" {
  value = {
    private_ip = yandex_compute_instance.fake_fcm.network_interface[0].ip_address
    public_ip  = yandex_compute_instance.fake_fcm.network_interface[0].nat_ip_address
  }
}
