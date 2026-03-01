.PHONY: build build-linux test clean deploy tf-init tf-plan tf-apply tf-destroy

BINARY := crawler
BUILD_DIR := bin

# Build for local OS
build:
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/crawler/

# Build for Linux (deployment target)
build-linux:
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/$(BINARY)-linux-amd64 ./cmd/crawler/

test:
	go test ./...

clean:
	rm -rf $(BUILD_DIR)

# Deploy binary to all workers via Tailscale
deploy: build-linux
	@echo "Stopping crawlers..."
	@for i in $$(seq 0 $$(($${WORKER_COUNT:-3} - 1))); do \
		ssh krowl-worker-$$i "systemctl stop crawler" & \
	done; wait
	@echo "Deploying binary..."
	@for i in $$(seq 0 $$(($${WORKER_COUNT:-3} - 1))); do \
		echo "  -> krowl-worker-$$i"; \
		scp $(BUILD_DIR)/$(BINARY)-linux-amd64 krowl-worker-$$i:/usr/local/bin/crawler; \
		ssh krowl-worker-$$i "chmod +x /usr/local/bin/crawler"; \
	done
	@echo "Starting crawlers..."
	@for i in $$(seq 0 $$(($${WORKER_COUNT:-3} - 1))); do \
		ssh krowl-worker-$$i "systemctl start crawler"; \
	done
	@echo "Verifying..."
	@sleep 2; for i in $$(seq 0 $$(($${WORKER_COUNT:-3} - 1))); do \
		printf "  worker-$$i: "; ssh krowl-worker-$$i "systemctl is-active crawler"; \
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

# --- OpenTofu ---
tf-init:
	cd terraform && tofu init

tf-plan:
	cd terraform && tofu plan

tf-apply:
	cd terraform && tofu apply

tf-destroy:
	cd terraform && tofu destroy
