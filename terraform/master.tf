resource "digitalocean_droplet" "master" {
  name     = "krowl-master"
  image    = "ubuntu-24-04-x64"
  size     = var.master_size
  region   = var.region
  vpc_uuid = digitalocean_vpc.krowl.id
  ssh_keys = [var.ssh_key_fingerprint]
  ipv6     = true

  user_data = templatefile("${path.module}/scripts/master-init.sh", {
    tailscale_auth_key   = var.tailscale_auth_key
    consul_encrypt_key   = var.consul_encrypt_key
    prometheus_alerts_b64  = base64encode(file("${path.module}/../monitoring/prometheus/alerts.yml"))
  })

  tags = ["krowl", "master"]

  lifecycle {
    ignore_changes = [user_data, ipv6]
  }
}
