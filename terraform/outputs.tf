output "master_public_ip" {
  description = "Master public IP (for reference only, not used directly)"
  value       = digitalocean_droplet.master.ipv4_address
}

output "master_private_ip" {
  description = "Master VPC private IP"
  value       = digitalocean_droplet.master.ipv4_address_private
}

output "worker_private_ips" {
  description = "Worker VPC private IPs"
  value       = digitalocean_droplet.worker[*].ipv4_address_private
}

output "worker_names" {
  description = "Worker droplet names"
  value       = digitalocean_droplet.worker[*].name
}

output "spaces_bucket" {
  description = "DO Spaces bucket name for JuiceFS"
  value       = digitalocean_spaces_bucket.krowl.name
}

output "spaces_endpoint" {
  description = "DO Spaces endpoint"
  value       = "https://${digitalocean_spaces_bucket.krowl.name}.${var.region}.digitaloceanspaces.com"
}

output "tailscale_hosts" {
  description = "Access via Tailscale hostnames"
  value = {
    master  = "krowl-master"
    grafana = "http://krowl-master:3000"
    consul  = "http://krowl-master:8500"
    prometheus = "http://krowl-master:9090"
    workers = [for i in range(var.worker_count) : "krowl-worker-${i}"]
  }
}
