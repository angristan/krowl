#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

# --- CoreDNS: local caching resolver with Prometheus metrics ---
# systemd-resolved can crash under heavy DNS load; replace with CoreDNS
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
apt-get install -y -qq curl gnupg lsb-release unzip jq

# --- Tailscale ---
curl -fsSL https://tailscale.com/install.sh | sh
tailscale up --auth-key="${tailscale_auth_key}" --advertise-tags=tag:krowl --hostname=krowl-master --ssh

# --- Consul ---
curl -fsSL https://releases.hashicorp.com/consul/1.20.5/consul_1.20.5_linux_amd64.zip -o /tmp/consul.zip
unzip /tmp/consul.zip -d /usr/local/bin/
rm /tmp/consul.zip
useradd --system --home /etc/consul.d --shell /bin/false consul
mkdir -p /opt/consul /etc/consul.d

cat >/etc/consul.d/consul.hcl <<EOF
datacenter = "fra1"
data_dir   = "/opt/consul"
server     = true
bootstrap_expect = 1
bind_addr  = "{{ GetPrivateInterfaces | include \"network\" \"10.100.0.0/16\" | attr \"address\" }}"
client_addr = "0.0.0.0"
ui_config {
  enabled = true
}
encrypt = "${consul_encrypt_key}"
EOF

cat >/etc/systemd/system/consul.service <<'UNIT'
[Unit]
Description=Consul
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

# --- Redis (for JuiceFS metadata) ---
apt-get install -y -qq redis-server

cat >/etc/redis/redis.conf <<'REDIS'
bind 0.0.0.0
port 6379
protected-mode no
maxmemory 2gb
maxmemory-policy noeviction
save 900 1
save 300 10
save 60 10000
dir /var/lib/redis
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

# --- Prometheus ---
useradd --system --no-create-home --shell /bin/false prometheus
mkdir -p /etc/prometheus /var/lib/prometheus

curl -fsSL https://github.com/prometheus/prometheus/releases/download/v3.10.0/prometheus-3.10.0.linux-amd64.tar.gz |
	tar xzf - -C /tmp/
cp /tmp/prometheus-3.10.0.linux-amd64/{prometheus,promtool} /usr/local/bin/
rm -rf /tmp/prometheus-*

# Install alert rules
echo "${prometheus_alerts_b64}" | base64 -d >/etc/prometheus/alerts.yml

cat >/etc/prometheus/prometheus.yml <<'PROM'
global:
  scrape_interval: 15s

rule_files:
  - alerts.yml

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'crawler'
    consul_sd_configs:
      - server: 'localhost:8500'
        services: ['crawler']
    relabel_configs:
      - source_labels: [__meta_consul_service_metadata_node_id]
        target_label: node_id
      - source_labels: [__meta_consul_tags]
        regex: .*,metrics,.*
        action: keep

  - job_name: 'redis-exporter'
    consul_sd_configs:
      - server: 'localhost:8500'
        services: ['redis-exporter']

  - job_name: 'node-exporter'
    consul_sd_configs:
      - server: 'localhost:8500'
        services: ['node-exporter']

  - job_name: 'juicefs'
    consul_sd_configs:
      - server: 'localhost:8500'
        services: ['juicefs']

  - job_name: 'coredns'
    consul_sd_configs:
      - server: 'localhost:8500'
        services: ['coredns']
PROM

cat >/etc/systemd/system/prometheus.service <<'UNIT'
[Unit]
Description=Prometheus
After=network-online.target

[Service]
User=prometheus
ExecStart=/usr/local/bin/prometheus \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/var/lib/prometheus \
  --storage.tsdb.retention.time=7d \
  --web.listen-address=0.0.0.0:9090
Restart=always

[Install]
WantedBy=multi-user.target
UNIT

chown -R prometheus:prometheus /etc/prometheus /var/lib/prometheus
systemctl enable prometheus
systemctl start prometheus

# --- Grafana ---
apt-get install -y -qq apt-transport-https software-properties-common
curl -fsSL https://apt.grafana.com/gpg.key | gpg --dearmor -o /usr/share/keyrings/grafana-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/grafana-archive-keyring.gpg] https://apt.grafana.com stable main" \
	>/etc/apt/sources.list.d/grafana.list
apt-get update -qq
apt-get install -y -qq grafana

# Bind to all interfaces (accessed via Tailscale)
sed -i 's/;http_addr =.*/http_addr = 0.0.0.0/' /etc/grafana/grafana.ini

# Provision Prometheus datasource
cat >/etc/grafana/provisioning/datasources/prometheus.yml <<'DS'
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    uid: prometheus
    url: http://localhost:9090
    access: proxy
    isDefault: true
    editable: false
DS

# Provision dashboard directory
mkdir -p /var/lib/grafana/dashboards
cat >/etc/grafana/provisioning/dashboards/default.yml <<'DASH'
apiVersion: 1
providers:
  - name: default
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /var/lib/grafana/dashboards
      foldersFromFilesStructure: false
DASH

# Decode and install krowl dashboard
echo "${grafana_dashboard_b64}" | base64 -d >/var/lib/grafana/dashboards/krowl.json
chown -R grafana:grafana /var/lib/grafana/dashboards

systemctl enable grafana-server
systemctl start grafana-server

# --- Pyroscope (continuous profiling) ---
curl -fsSL https://github.com/grafana/pyroscope/releases/download/v1.18.1/pyroscope_1.18.1_linux_amd64.tar.gz |
	tar xzf - -C /usr/local/bin/ pyroscope

mkdir -p /mnt/jfs/pyroscope

cat >/etc/systemd/system/pyroscope.service <<'UNIT'
[Unit]
Description=Grafana Pyroscope
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/pyroscope \
  -server.http-listen-address=0.0.0.0 \
  -server.http-listen-port=4040 \
  -pyroscopedb.data-path=/mnt/jfs/pyroscope
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
UNIT

systemctl enable pyroscope
systemctl start pyroscope

# Provision Pyroscope datasource for Grafana
cat >/etc/grafana/provisioning/datasources/pyroscope.yaml <<'DS'
apiVersion: 1
datasources:
  - name: Pyroscope
    type: grafana-pyroscope-datasource
    access: proxy
    url: http://localhost:4040
    isDefault: false
    editable: true
DS

# --- Register services in Consul ---
cat >/etc/consul.d/services.hcl <<EOF
services {
  name = "redis"
  port = 6379
  tags = ["juicefs", "metrics"]
  meta {
    role = "juicefs-metadata"
  }
  check {
    tcp  = "localhost:6379"
    interval = "10s"
    timeout  = "2s"
  }
}

services {
  name = "prometheus"
  port = 9090
  check {
    http     = "http://localhost:9090/-/healthy"
    interval = "10s"
    timeout  = "2s"
  }
}

services {
  name = "grafana"
  port = 3000
  check {
    http     = "http://localhost:3000/api/health"
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
  name = "coredns"
  port = 9153
  check {
    http     = "http://localhost:9153/metrics"
    interval = "15s"
    timeout  = "2s"
  }
}

services {
  name = "pyroscope"
  port = 4040
  check {
    http     = "http://localhost:4040/ready"
    interval = "10s"
    timeout  = "2s"
  }
}
EOF

consul reload

echo "=== krowl master init complete ==="
