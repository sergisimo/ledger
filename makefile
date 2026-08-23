# Define dependencies

GOLANG          := golang:1.26
ALPINE          := alpine:3.23
KIND            := kindest/node:v1.36.1

KIND_CLUSTER    := ledger-cluster
NAMESPACE       := ledger
LEDGER_APP       := ledger
BASE_IMAGE_NAME := localhost/sergisimo
VERSION         := 0.0.1
LEDGER_IMAGE     := $(BASE_IMAGE_NAME)/$(LEDGER_APP):$(VERSION)

# Install dependencies

dev-gotooling:
	go install github.com/rakyll/hey@latest
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install golang.org/x/tools/cmd/goimports@latest

asdf-install:
	asdf plugin add kind
	asdf plugin add kubectl
	asdf plugin add helm
	asdf install

dev-docker:
	docker pull docker.io/$(GOLANG)
	docker pull docker.io/$(ALPINE)
	docker pull docker.io/$(KIND)

# Building containers

ledger:
	docker build \
		-f zarf/docker/dockerfile.ledger \
		-t $(LEDGER_IMAGE) \
		--build-arg BUILD_TAG=$(VERSION) \
		--build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") \
		.

# Running from within k8s/kind
dev-up:
	kind create cluster \
		--image $(KIND) \
		--name $(KIND_CLUSTER) \
		--config zarf/k8s/dev/kind-config.yaml

	kubectl wait --timeout=120s -n=local-path-storage --for=condition=Available deployment/local-path-provisioner

dev-down:
	kind delete cluster --name $(KIND_CLUSTER)

# Building containers
dev-load:
	kind load docker-image $(LEDGER_IMAGE) --name $(KIND_CLUSTER)
	wait;

dev-apply:
	helm template $(LEDGER_APP) zarf/helm/ledger -n $(NAMESPACE) -f zarf/helm/ledger/values.yaml -f zarf/helm/ledger/values/dev.yaml | kubectl apply -f -
	kubectl wait pods -n $(NAMESPACE) --selector app=$(LEDGER_APP) --timeout=120s --for=condition=Ready

dev-restart:
	kubectl rollout restart deployment $(LEDGER_APP) -n $(NAMESPACE)

dev-run: ledger dev-up dev-load dev-apply

dev-update: ledger dev-load dev-restart

# Logs

dev-logs:
	kubectl logs -n $(NAMESPACE) --selector app=$(LEDGER_APP) --follow

dev-pod-describe:
	kubectl describe pods -n $(NAMESPACE) --selector app=$(LEDGER_APP)

# Modules support
tidy:
	go mod tidy