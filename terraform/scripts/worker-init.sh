#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

# --- CoreDNS: local caching resolver with Prometheus metrics ---
# systemd-resolved crashes under heavy DNS load; replace with CoreDNS
systemctl disable --now systemd-resolved || true

curl -fsSL https://github.com/coredns/coredns/releases/download/v1.14.1/coredns_1.14.1_linux_amd64.tgz |
	tar xzf - -C /usr/local/bin/ coredns

useradd --system --no-create-home --shell /bin/false coredns || true

mkdir -p /etc/coredns
cat >/etc/coredns/Corefile <<'COREFILE'
. {
    forward . 8.8.8.8 1.1.1.1 2001:4860:4860::8888 2606:4700:4700::1111 {
        health_check 30s
    }
    cache {
        success 65536 3600 300
        denial 8192 600 60
        prefetch 10 1h 10%
    }
    prometheus 0.0.0.0:9153
    errors
    log . {
        class denial error
    }
    ready
}
COREFILE

cat >/etc/systemd/system/coredns.service <<'UNIT'
[Unit]
Description=CoreDNS DNS Server
After=network-online.target
Wants=network-online.target

[Service]
User=coredns
ExecStart=/usr/local/bin/coredns -conf /etc/coredns/Corefile
Restart=always
RestartSec=2
LimitNOFILE=65535
AmbientCapabilities=CAP_NET_BIND_SERVICE

[Install]
WantedBy=multi-user.target
UNIT

systemctl enable coredns
systemctl start coredns

# Point resolv.conf to local CoreDNS
rm -f /etc/resolv.conf
cat >/etc/resolv.conf <<'DNS'
nameserver 127.0.0.1
DNS
chattr +i /etc/resolv.conf

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
curl -fsSL https://releases.hashicorp.com/consul/1.20.5/consul_1.20.5_linux_amd64.zip -o /tmp/consul.zip
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

mkdir -p /mnt/jfs/data/worker-${node_id}/redis
cat >/etc/redis/redis.conf <<REDIS
bind 0.0.0.0
port 6379
protected-mode no
maxmemory 1gb
maxmemory-policy noeviction
dir /mnt/jfs/data/worker-${node_id}/redis
save 300 1
appendonly no
REDIS

systemctl restart redis-server
systemctl enable redis-server

# --- Node exporter ---
curl -fsSL https://github.com/prometheus/node_exporter/releases/download/v1.10.2/node_exporter-1.10.2.linux-amd64.tar.gz |
	tar xzf - -C /tmp/
cp /tmp/node_exporter-1.10.2.linux-amd64/node_exporter /usr/local/bin/
rm -rf /tmp/node_exporter-*

useradd --system --no-create-home --shell /bin/false node_exporter || true
cat >/etc/systemd/system/node_exporter.service <<'UNIT'
[Unit]
Description=Node Exporter
After=network-online.target

[Service]
User=node_exporter
ExecStart=/usr/local/bin/node_exporter \
  --collector.tcpstat \
  --collector.conntrack
Restart=always

[Install]
WantedBy=multi-user.target
UNIT

systemctl enable node_exporter
systemctl start node_exporter

# --- Redis exporter ---
curl -fsSL https://github.com/oliver006/redis_exporter/releases/download/v1.81.0/redis_exporter-v1.81.0.linux-amd64.tar.gz |
	tar xzf - -C /tmp/
cp /tmp/redis_exporter-v1.81.0.linux-amd64/redis_exporter /usr/local/bin/
rm -rf /tmp/redis_exporter-*

cat >/etc/systemd/system/redis_exporter.service <<'UNIT'
[Unit]
Description=Redis Exporter
After=redis-server.service

[Service]
ExecStart=/usr/local/bin/redis_exporter --redis.addr=redis://localhost:6379
Restart=always

[Install]
WantedBy=multi-user.target
UNIT

systemctl enable redis_exporter
systemctl start redis_exporter

# --- JuiceFS ---
curl -fsSL https://github.com/juicedata/juicefs/releases/download/v1.3.1/juicefs-1.3.1-linux-amd64.tar.gz |
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
  --metrics 0.0.0.0:9567 \
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
# Ensure shared WARC and seeds dirs exist on JuiceFS
# (JuiceFS mount is async, so wait for it to be ready)
for i in $(seq 1 30); do
	mountpoint -q /mnt/jfs && break
	sleep 1
done
mkdir -p /mnt/jfs/warcs /mnt/jfs/seeds

# --- Register services in Consul ---
# Crawler service is self-registered by the binary on startup
cat >/etc/consul.d/worker.hcl <<EOF
services {
  name = "redis"
  port = 6379
  tags = ["worker"]
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

services {
  name = "node-exporter"
  port = 9100
  check {
    http     = "http://localhost:9100/metrics"
    interval = "15s"
    timeout  = "2s"
  }
}

services {
  name = "redis-exporter"
  port = 9121
  check {
    http     = "http://localhost:9121/metrics"
    interval = "15s"
    timeout  = "2s"
  }
}

services {
  name = "juicefs"
  port = 9567
  tags = ["worker"]
  meta {
    node_id = "${node_id}"
  }
  check {
    http     = "http://localhost:9567/metrics"
    interval = "15s"
    timeout  = "2s"
  }
}

services {
  name = "coredns"
  port = 9153
  check {
    http     = "http://localhost:9153/metrics"
    interval = "15s"
    timeout  = "2s"
  }
}
EOF

consul reload || true

# --- Crawler systemd unit ---
cat >/etc/systemd/system/crawler.service <<EOF
[Unit]
Description=krowl Crawler
After=network-online.target consul.service redis-server.service juicefs.service
Wants=network-online.target
Requires=consul.service redis-server.service juicefs.service

[Service]
ExecStart=/usr/local/bin/crawler \
  --node-id=${node_id} \
  --seeds=/mnt/jfs/seeds/top10k.txt \
  --pebble=/mnt/jfs/data/worker-${node_id}/pebble \
  --redis=localhost:6379 \
  --consul=localhost:8500 \
  --metrics-port=9090 \
  --warc-dir=/mnt/jfs/warcs \
  --checkpoint=/mnt/jfs/data/worker-${node_id}/frontier.ckpt
Restart=on-failure
RestartSec=5
LimitNOFILE=65535
KillSignal=SIGTERM
TimeoutStopSec=60
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable crawler
# Don't start crawler yet - binary needs to be deployed first
# systemctl start crawler

echo "=== krowl worker ${node_id} init complete ==="
