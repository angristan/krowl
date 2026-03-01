terraform {
  required_version = ">= 1.5"

  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

provider "digitalocean" {
  token             = var.do_token
  spaces_access_id  = var.spaces_access_key
  spaces_secret_key = var.spaces_secret_key
}

# Group all resources under a dedicated DO project
resource "digitalocean_project" "krowl" {
  name        = "krowl"
  description = "Distributed web crawler"
  purpose     = "Service or API"
  environment = "Production"
}

resource "digitalocean_project_resources" "krowl" {
  project = digitalocean_project.krowl.id
  resources = concat(
    [digitalocean_droplet.master.urn],
    digitalocean_droplet.worker[*].urn,
    [digitalocean_spaces_bucket.krowl.urn],
  )
}
