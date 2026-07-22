# Define dependencies
GOLANG          := golang:1.26
ALPINE          := alpine:3.23
KIND            := kindest/node:v1.36.1

KIND_CLUSTER    := ledger-cluster
NAMESPACE       := ledger
BASE_IMAGE_NAME := localhost/sergisimo
VERSION         := 0.0.1
LEDGER_APP       := ledger

LEDGER_IMAGE     := $(BASE_IMAGE_NAME)/$(LEDGER_APP):$(VERSION)

# Install dependencies
init: go-tools asdf-tools docker-images

go-tools:
	go install github.com/rakyll/hey@latest
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install golang.org/x/tools/cmd/goimports@latest

asdf-tools:
	asdf plugin add kind
	asdf plugin add kubectl
	asdf plugin add helm
	asdf install

docker-images:
	docker pull docker.io/$(GOLANG)
	docker pull docker.io/$(ALPINE)
	docker pull docker.io/$(KIND)

# Building containers
build: build-ledger

build-ledger:
	docker build \
		-f zarf/docker/dockerfile.ledger \
		-t $(LEDGER_IMAGE) \
		--build-arg BUILD_TAG=$(VERSION) \
		--build-arg BUILD_DATE=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ") \
		.

# Running locally
local: local-cluster-up build local-load-images local-apply local-logs

local-cluster-up:
	kind get clusters | grep -q $(KIND_CLUSTER) || \
	( \
		kind create cluster \
			--image $(KIND) \
			--name $(KIND_CLUSTER) \
			--config zarf/k8s/dev/kind-config.yaml && \
		kubectl wait --timeout=120s -n=local-path-storage --for=condition=Available deployment/local-path-provisioner \
	)
	kubectl config use-context kind-$(KIND_CLUSTER)

local-cluster-down:
	kind delete cluster --name $(KIND_CLUSTER)

local-load-images:
	kind load docker-image $(LEDGER_IMAGE) --name $(KIND_CLUSTER)
	wait;

local-apply:
	helm template $(LEDGER_APP) zarf/helm/ledger -n $(NAMESPACE) -f zarf/helm/ledger/values.yaml -f zarf/helm/ledger/values/dev.yaml | kubectl apply -f -
	kubectl wait pods -n $(NAMESPACE) --selector app=$(LEDGER_APP) --timeout=120s --for=condition=Ready

# Logs
local-logs:
	kubectl logs -n $(NAMESPACE) --selector app=$(LEDGER_APP) --follow

# Port forwarding
forward-debug:
	kubectl port-forward -n $(NAMESPACE) svc/$(LEDGER_APP) 5000:8081

forward-http:
	kubectl port-forward -n $(NAMESPACE) svc/$(LEDGER_APP) 8080:8080

# Modules support
tidy:
	go mod tidy