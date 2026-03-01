#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

# --- System setup ---
apt-get update -qq
apt-get install -y -qq curl gnupg lsb-release unzip jq fuse3

# Raise file descriptor limits for crawler
cat >>/etc/security/limits.conf <<'LIMITS'
* soft nofile 65535
* hard nofile 65535
LIMITS

cat >/etc/sysctl.d/99-krowl.conf <<'SYSCTL'
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.core.netdev_max_backlog = 65535
SYSCTL
sysctl --system

# --- Tailscale ---
curl -fsSL https://tailscale.com/install.sh | sh
tailscale up --auth-key="${tailscale_auth_key}" --advertise-tags=tag:krowl --hostname=krowl-worker-${node_id} --ssh

# --- Consul agent ---
curl -fsSL https://releases.hashicorp.com/consul/1.20.0/consul_1.20.0_linux_amd64.zip -o /tmp/consul.zip
unzip /tmp/consul.zip -d /usr/local/bin/
rm /tmp/consul.zip
useradd --system --home /etc/consul.d --shell /bin/false consul
mkdir -p /opt/consul /etc/consul.d

cat >/etc/consul.d/consul.hcl <<EOF
datacenter = "fra1"
data_dir   = "/opt/consul"
server     = false
bind_addr  = "{{ GetPrivateInterfaces | include \"network\" \"10.100.0.0/16\" | attr \"address\" }}"
retry_join = ["${master_private_ip}"]
encrypt    = "${consul_encrypt_key}"
EOF

cat >/etc/systemd/system/consul.service <<'UNIT'
[Unit]
Description=Consul Agent
After=network-online.target
Wants=network-online.target

[Service]
User=consul
ExecStart=/usr/local/bin/consul agent -config-dir=/etc/consul.d/
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
UNIT

chown -R consul:consul /opt/consul /etc/consul.d
systemctl enable consul
systemctl start consul

# --- Redis (local inbox + frontier) ---
apt-get install -y -qq redis-server

cat >/etc/redis/redis.conf <<'REDIS'
bind 0.0.0.0
port 6379
protected-mode no
maxmemory 1gb
maxmemory-policy noeviction
save ""
appendonly no
REDIS

systemctl restart redis-server
systemctl enable redis-server

# --- JuiceFS ---
curl -fsSL https://github.com/juicedata/juicefs/releases/download/v1.2.0/juicefs-1.2.0-linux-amd64.tar.gz |
	tar xzf - -C /usr/local/bin/ juicefs

# Format JuiceFS (idempotent, only first run actually formats)
juicefs format \
	--storage s3 \
	--bucket "https://${spaces_bucket}.${spaces_region}.digitaloceanspaces.com" \
	--access-key "${spaces_access_key}" \
	--secret-key "${spaces_secret_key}" \
	"redis://${master_private_ip}:6379/1" \
	krowl || true

# Mount JuiceFS
mkdir -p /mnt/jfs
cat >/etc/systemd/system/juicefs.service <<EOF
[Unit]
Description=JuiceFS Mount
After=network-online.target consul.service
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/juicefs mount \
  --cache-size 0 \
  --buffer-size 300 \
  --max-uploads 20 \
  "redis://${master_private_ip}:6379/1" \
  /mnt/jfs
ExecStop=/usr/local/bin/juicefs umount /mnt/jfs
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

systemctl enable juicefs
systemctl start juicefs

# --- Prepare local data directories ---
mkdir -p /var/data/pebble

# --- Register services in Consul ---
cat >/etc/consul.d/crawler.hcl <<EOF
services {
  name = "crawler"
  port = 9090
  tags = ["worker", "metrics"]
  meta {
    node_id    = "${node_id}"
    redis_addr = "{{ GetPrivateInterfaces | include \"network\" \"10.100.0.0/16\" | attr \"address\" }}:6379"
  }
  check {
    http     = "http://localhost:9090/health"
    interval = "5s"
    timeout  = "2s"
    deregister_critical_service_after = "30s"
  }
}

services {
  name = "redis"
  port = 6379
  tags = ["worker", "metrics"]
  meta {
    node_id = "${node_id}"
    role    = "worker-inbox"
  }
  check {
    tcp      = "localhost:6379"
    interval = "10s"
    timeout  = "2s"
  }
}
EOF

consul reload || true

echo "=== krowl worker ${node_id} init complete ==="
