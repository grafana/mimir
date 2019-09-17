.DEFAULT_GOAL := all
IMAGE_PREFIX ?= grafana
IMAGE_TAG := $(shell ./tools/image-tag)
GIT_REVISION := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GO_FLAGS := -mod=vendor -ldflags "-extldflags \"-static\" -s -w -X $(VPREFIX).Branch=$(GIT_BRANCH) -X $(VPREFIX).Version=$(IMAGE_TAG) -X $(VPREFIX).Revision=$(GIT_REVISION)" -tags netgo

all: cortex-cli
images: cortex-cli-image
cortex-cli: cmd/cortex-cli/cortex-cli

cortex-cli-image:
	$(SUDO) docker build -t $(IMAGE_PREFIX)/cortex-cli -f cmd/cortex-cli/Dockerfile .
	$(SUDO) docker tag $(IMAGE_PREFIX)/cortex-cli $(IMAGE_PREFIX)/cortex-cli:$(IMAGE_TAG)

cmd/cortex-cli/cortex-cli: $(APP_GO_FILES) cmd/cortex-cli/main.go
	CGO_ENABLED=0 go build $(GO_FLAGS) -o $@ ./$(@D)

lint:
	GOGC=20 golangci-lint --deadline=20m run

cross:
	CGO_ENABLED=0 gox -output="dist/{{.Dir}}-{{.OS}}-{{.Arch}}" -ldflags=${LDFLAGS} -arch="amd64 arm64 arm" -os="linux" -osarch="darwin/amd64" ./cmd/cortex-cli

test:
	go test -p=8 ./...

clean:
	rm -rf cmd/cortex-cli/cortex-cli

