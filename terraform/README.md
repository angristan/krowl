# terraform

OpenTofu (Terraform-compatible) infrastructure definitions for the krowl cluster on DigitalOcean.

## Architecture

```
                        ┌─── Tailscale mesh (management) ───┐
                        │                                    │
    ┌───────────────────┴────────────────┐                   │
    │         Master (s-2vcpu-4gb)       │                   │
    │  10.100.0.6       krowl-master     │                   │
    │                                    │                   │
    │  Consul server ◄──── gossip ────────────┐              │
    │  Redis (JuiceFS metadata)          │    │              │
    │  Prometheus ◄── scrape (Consul SD) │    │              │
    │  Grafana                           │    │              │
    │  node_exporter, redis_exporter     │    │              │
    └────────────────────────────────────┘    │              │
                        │ VPC 10.100.0.0/16   │              │
         ┌──────────────┼──────────────┐      │              │
         ▼              ▼              ▼      │              │
    ┌─────────┐   ┌─────────┐   ┌─────────┐  │              │
    │Worker 0 │   │Worker 1 │   │Worker 2 │  │              │
    │10.100.  │   │10.100.  │   │10.100.  │  │              │
    │  0.7    │   │  0.8    │   │  0.9    │  │              │
    │         │   │         │   │         │  │              │
    │ crawler │   │ crawler │   │ crawler │  │              │
    │ Consul ─┼───┼─ agent ─┼───┼─ agent ─┼──┘              │
    │ Redis   │   │ Redis   │   │ Redis   │                  │
    │ Pebble  │   │ Pebble  │   │ Pebble  │                  │
    │ JuiceFS ┼───┼─────────┼───┼─────────┼──► DO Spaces    │
    └─────────┘   └─────────┘   └─────────┘   (krowl-data)  │
         s-4vcpu-8gb-amd (each)                              │
                                                             │
    ┌────────────────────────────────────────────────────────┘
    │  DO Firewall: public inbound BLOCKED, VPC inbound ALLOWED
    └────────────────────────────────────────────────────────
```

- **Region:** fra1 (Frankfurt)
- **VPC:** `10.100.0.0/16`
- **Master node** (`s-2vcpu-4gb`): Consul server, Redis (JuiceFS metadata), Prometheus, Grafana
- **Worker nodes** (`s-4vcpu-8gb-amd`, default 3): Crawler binary, local Redis, JuiceFS mount, Pebble
- **Storage:** DO Spaces bucket (`krowl-data`) as JuiceFS object storage backend
- **Networking:** Firewall blocks all public inbound; VPC CIDR allowed for inter-node traffic; management via Tailscale only

## Files

| File | Description |
|------|-------------|
| `main.tf` | Provider config, DO project |
| `master.tf` | Master droplet with cloud-init |
| `workers.tf` | Worker droplets (count = `worker_count`) |
| `networking.tf` | VPC and firewall rules |
| `spaces.tf` | DO Spaces bucket |
| `variables.tf` | All input variables |
| `outputs.tf` | IPs, hostnames, Tailscale URLs |
| `terraform.tfvars.example` | Template for required variables |
| `scripts/master-init.sh` | Master cloud-init: Tailscale, Consul server, Redis, Prometheus, Grafana, exporters, DNS fix |
| `scripts/worker-init.sh` | Worker cloud-init: Tailscale, Consul agent, Redis, JuiceFS, exporters, crawler systemd unit, DNS fix |

## Required variables

See `terraform.tfvars.example`. Key variables:

- `do_token` — DigitalOcean API token
- `tailscale_auth_key` — Tailscale auth key with `tag:krowl`
- `ssh_key_fingerprint` — SSH key registered in DO
- `spaces_access_key` / `spaces_secret_key` — Full-access Spaces keys (for TF provider)
- `spaces_worker_access_key` / `spaces_worker_secret_key` — Bucket-scoped `readwrite` keys (for JuiceFS)
- `consul_encrypt_key` — Consul gossip encryption key (`consul keygen`)

## Usage

```
make tf-init    # tofu init
make tf-plan    # tofu plan
make tf-apply   # tofu apply
make tf-destroy # tofu destroy
```
