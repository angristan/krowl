.PHONY: build build-linux test clean deploy tf-init tf-plan tf-apply tf-destroy

BINARY := crawler
BUILD_DIR := bin

# Build for local OS
build:
	CGO_ENABLED=0 go build -o $(BUILD_DIR)/$(BINARY) ./cmd/crawler/

# Build for Linux (deployment target)
build-linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o $(BUILD_DIR)/$(BINARY)-linux-amd64 ./cmd/crawler/

test:
	go test ./...

clean:
	rm -rf $(BUILD_DIR)

# Deploy binary to all workers via Tailscale
deploy: build-linux
	@echo "Deploying to workers..."
	@for i in $$(seq 0 $$(($${WORKER_COUNT:-3} - 1))); do \
		echo "  -> krowl-worker-$$i"; \
		scp $(BUILD_DIR)/$(BINARY)-linux-amd64 krowl-worker-$$i:/usr/local/bin/crawler; \
		ssh krowl-worker-$$i "systemctl restart crawler || true"; \
	done

# Deploy seeds file
deploy-seeds:
	@echo "Uploading seeds to first worker (JuiceFS)..."
	ssh krowl-worker-0 "mkdir -p /mnt/jfs/seeds"
	scp seeds/top10k.txt krowl-worker-0:/mnt/jfs/seeds/top10k.txt

# Download seed list
seeds/top10k.txt:
	mkdir -p seeds
	curl -s https://tranco-list.eu/top-1m.csv.zip | funzip | head -10000 | cut -d',' -f2 > seeds/top10k.txt

# --- Terraform ---
tf-init:
	cd terraform && terraform init

tf-plan:
	cd terraform && terraform plan

tf-apply:
	cd terraform && terraform apply

tf-destroy:
	cd terraform && terraform destroy
