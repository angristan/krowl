resource "digitalocean_droplet" "worker" {
  count = var.worker_count

  name     = "krowl-worker-${count.index}"
  image    = "ubuntu-24-04-x64"
  size     = var.worker_size
  region   = var.region
  vpc_uuid = digitalocean_vpc.krowl.id
  ssh_keys = [var.ssh_key_fingerprint]

  user_data = templatefile("${path.module}/scripts/worker-init.sh", {
    node_id            = count.index
    tailscale_auth_key = var.tailscale_auth_key
    consul_encrypt_key = var.consul_encrypt_key
    master_private_ip  = digitalocean_droplet.master.ipv4_address_private
    spaces_bucket      = digitalocean_spaces_bucket.krowl.name
    spaces_region      = var.region
    spaces_access_key  = var.spaces_access_key
    spaces_secret_key  = var.spaces_secret_key
  })

  tags = ["krowl", "worker"]

  depends_on = [digitalocean_droplet.master]
}
