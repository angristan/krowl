variable "do_token" {
  description = "DigitalOcean API token"
  type        = string
  sensitive   = true
}

variable "tailscale_auth_key" {
  description = "Tailscale auth key (reusable, ephemeral recommended)"
  type        = string
  sensitive   = true
}

variable "region" {
  description = "DigitalOcean region"
  type        = string
  default     = "fra1"
}

variable "worker_count" {
  description = "Number of crawler worker nodes"
  type        = number
  default     = 3
}

variable "master_size" {
  description = "Droplet size for master node"
  type        = string
  default     = "s-2vcpu-4gb"
}

variable "worker_size" {
  description = "Droplet size for worker nodes"
  type        = string
  default     = "s-4vcpu-8gb-amd"
}

variable "ssh_key_fingerprint" {
  description = "SSH key fingerprint registered in DigitalOcean"
  type        = string
}

variable "spaces_access_key" {
  description = "DigitalOcean Spaces access key (full-access, for Terraform provider)"
  type        = string
  sensitive   = true
}

variable "spaces_secret_key" {
  description = "DigitalOcean Spaces secret key (full-access, for Terraform provider)"
  type        = string
  sensitive   = true
}

variable "spaces_worker_access_key" {
  description = "DigitalOcean Spaces access key (scoped to krowl-data bucket, for JuiceFS)"
  type        = string
  sensitive   = true
}

variable "spaces_worker_secret_key" {
  description = "DigitalOcean Spaces secret key (scoped to krowl-data bucket, for JuiceFS)"
  type        = string
  sensitive   = true
}

variable "consul_encrypt_key" {
  description = "Consul gossip encryption key (generate with: consul keygen)"
  type        = string
  sensitive   = true
}
