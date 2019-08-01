.DEFAULT_GOAL := all
#############
# Variables #
#############

# We don't want find to scan inside a bunch of directories, to accelerate the
# 'make: Entering directory '/go/src/github.com/grafana/loki' phase.
DONT_FIND := -name tools -prune -o -name vendor -prune -o -name .git -prune -o -name .cache -prune -o -name .pkg -prune -o

# These are all the application files, they are included in the various binary rules as dependencies
# to make sure binaries are rebuilt if any source files change.
APP_GO_FILES := $(shell find . $(DONT_FIND) -name .y.go -prune -o -name .pb.go -prune -o -name cmd -prune -o -type f -name '*.go' -print)

# Build flags
GO_FLAGS := -ldflags "-extldflags \"-static\" -s -w -X $(VPREFIX).Branch=$(GIT_BRANCH) -X $(VPREFIX).Version=$(IMAGE_TAG) -X $(VPREFIX).Revision=$(GIT_REVISION)" -tags netgo
# Per some websites I've seen to add `-gcflags "all=-N -l"`, the gcflags seem poorly if at all documented
# the best I could dig up is -N disables optimizations and -l disables inlining which should make debugging match source better.
# Also remove the -s and -w flags present in the normal build which strip the symbol table and the DWARF symbol table.
DEBUG_GO_FLAGS := -gcflags "all=-N -l" -ldflags "-extldflags \"-static\" -X $(VPREFIX).Branch=$(GIT_BRANCH) -X $(VPREFIX).Version=$(IMAGE_TAG) -X $(VPREFIX).Revision=$(GIT_REVISION)" -tags netgo
NETGO_CHECK = @strings $@ | grep cgo_stub\\\.go >/dev/null || { \
       rm $@; \
       echo "\nYour go standard library was built without the 'netgo' build tag."; \
       echo "To fix that, run"; \
       echo "    sudo go clean -i net"; \
       echo "    sudo go install -tags netgo std"; \
       false; \
}

# Docker image info
IMAGE_PREFIX ?= grafana
IMAGE_TAG := $(shell ./tools/image-tag)
#GIT_REVISION := $(shell git rev-parse --short HEAD)
#GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

# RM is parameterized to allow CircleCI to run builds, as it
# currently disallows `docker run --rm`. This value is overridden
# in circle.yml
RM := --rm
# TTY is parameterized to allow Google Cloud Builder to run builds,
# as it currently disallows TTY devices. This value needs to be overridden
# in any custom cloudbuild.yaml files
TTY := --tty

all: cortex-tool chunk-tool
images: cortex-tool-image
cortex-tool: cmd/cortex-tool/cortex-tool
chunk-tool: cmd/chunk-tool/chunk-tool

# Images
cortex-tool-image:
	$(SUDO) docker build -t $(IMAGE_PREFIX)/cortex-tool -f cmd/cortex-tool/Dockerfile .
	$(SUDO) docker tag $(IMAGE_PREFIX)/cortex-tool $(IMAGE_PREFIX)/cortex-tool:$(IMAGE_TAG)

cmd/cortex-tool/cortex-tool: $(APP_GO_FILES) cmd/cortex-tool/main.go
	CGO_ENABLED=0 go build $(GO_FLAGS) -o $@ ./$(@D)
	$(NETGO_CHECK)

cmd/chunk-tool/chunk-tool: $(APP_GO_FILES) cmd/chunk-tool/main.go
	CGO_ENABLED=0 go build $(GO_FLAGS) -o $@ ./$(@D)
	$(NETGO_CHECK)


lint:
	GOGC=20 golangci-lint run

test:
	go test -p=8 ./...

clean:
	rm -rf cmd/cortex-tool/cortex-tool
	rm -rf cmd/chunk-tool/chunk-tool
