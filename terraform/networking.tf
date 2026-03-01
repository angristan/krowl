# VPC for private inter-node communication
resource "digitalocean_vpc" "krowl" {
  name     = "krowl-vpc"
  region   = var.region
  ip_range = "10.100.0.0/16"
}

# Firewall: block ALL inbound on public interface
# Nodes communicate via VPC private IPs
# Management access via Tailscale
resource "digitalocean_firewall" "krowl" {
  name = "krowl-firewall"

  droplet_ids = concat(
    [digitalocean_droplet.master.id],
    digitalocean_droplet.worker[*].id,
  )

  # Allow all inbound from VPC (inter-node communication)
  inbound_rule {
    protocol         = "tcp"
    port_range       = "1-65535"
    source_addresses = ["10.100.0.0/16"]
  }

  inbound_rule {
    protocol         = "udp"
    port_range       = "1-65535"
    source_addresses = ["10.100.0.0/16"]
  }

  inbound_rule {
    protocol         = "icmp"
    source_addresses = ["10.100.0.0/16"]
  }

  # Allow all outbound (crawling, package installs, Tailscale DERP)
  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}
