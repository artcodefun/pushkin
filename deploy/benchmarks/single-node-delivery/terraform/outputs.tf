output "pushkin" {
  value = {
    private_ip = google_compute_instance.pushkin.network_interface[0].network_ip
    public_ip  = google_compute_instance.pushkin.network_interface[0].access_config[0].nat_ip
  }
}

output "fake_fcm" {
  value = {
    private_ip = google_compute_instance.fake_fcm.network_interface[0].network_ip
    public_ip  = google_compute_instance.fake_fcm.network_interface[0].access_config[0].nat_ip
  }
}
