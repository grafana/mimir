GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GIT_REVISION := $(shell git rev-parse --short HEAD)

IMAGE_PREFIX ?= andyasp 
IMAGE_TAG ?= $(GIT_REVISION)

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)


copyblocks:
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags '-extldflags "-static"' .

.PHONY: build-image
build-image: clean
	docker buildx build --load --platform linux/amd64 --build-arg revision=$(GIT_REVISION) -t copyblocks:latest -t copyblocks:$(IMAGE_TAG) .

.PHONY: publish-image
publish-image: clean
	docker buildx build --push --platform linux/amd64,linux/arm64 --build-arg revision=$(GIT_REVISION) -t $(IMAGE_PREFIX)/copyblocks:$(IMAGE_TAG) .

.PHONY: lint
lint:
	golangci-lint run --timeout=5m

.PHONY: clean
clean:
	rm -f copyblocks
	go clean ./...

