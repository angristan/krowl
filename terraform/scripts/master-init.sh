#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

# --- System setup ---
apt-get update -qq
apt-get install -y -qq curl gnupg lsb-release unzip jq

# --- Tailscale ---
curl -fsSL https://tailscale.com/install.sh | sh
tailscale up --auth-key="${tailscale_auth_key}" --advertise-tags=tag:krowl --hostname=krowl-master --ssh

# --- Consul ---
curl -fsSL https://releases.hashicorp.com/consul/1.20.0/consul_1.20.0_linux_amd64.zip -o /tmp/consul.zip
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

# --- Prometheus ---
useradd --system --no-create-home --shell /bin/false prometheus
mkdir -p /etc/prometheus /var/lib/prometheus

curl -fsSL https://github.com/prometheus/prometheus/releases/download/v2.53.0/prometheus-2.53.0.linux-amd64.tar.gz |
	tar xzf - -C /tmp/
cp /tmp/prometheus-2.53.0.linux-amd64/{prometheus,promtool} /usr/local/bin/
rm -rf /tmp/prometheus-*

cat >/etc/prometheus/prometheus.yml <<'PROM'
global:
  scrape_interval: 15s

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

  - job_name: 'redis'
    consul_sd_configs:
      - server: 'localhost:8500'
        services: ['redis']
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
EOF

consul reload

echo "=== krowl master init complete ==="
