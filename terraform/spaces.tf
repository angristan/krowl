# DO Spaces bucket for JuiceFS data backend
resource "digitalocean_spaces_bucket" "krowl" {
  name   = "krowl-data"
  region = var.region
  acl    = "private"
}
