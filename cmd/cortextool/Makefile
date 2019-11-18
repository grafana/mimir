.DEFAULT_GOAL := all
IMAGE_PREFIX ?= grafana
IMAGE_TAG := $(shell ./tools/image-tag)
GIT_REVISION := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GO_FLAGS := -mod=vendor -ldflags "-extldflags \"-static\" -s -w -X $(VPREFIX).Branch=$(GIT_BRANCH) -X $(VPREFIX).Version=$(IMAGE_TAG) -X $(VPREFIX).Revision=$(GIT_REVISION)" -tags netgo

all: cortextool
images: cortextool-image
cortextool: cmd/cortextool/cortextool

cortextool-image:
	$(SUDO) docker build -t $(IMAGE_PREFIX)/cortextool -f cmd/cortextool/Dockerfile .
	$(SUDO) docker tag $(IMAGE_PREFIX)/cortextool $(IMAGE_PREFIX)/cortextool:$(IMAGE_TAG)

cmd/cortextool/cortextool: $(APP_GO_FILES) cmd/cortextool/main.go
	CGO_ENABLED=0 go build $(GO_FLAGS) -o $@ ./$(@D)

lint:
	GOGC=20 golangci-lint --deadline=20m run

cross:
	CGO_ENABLED=0 gox -output="dist/{{.Dir}}-{{.OS}}-{{.Arch}}" -ldflags=${LDFLAGS} -arch="amd64" -os="linux" -osarch="darwin/amd64" ./cmd/cortextool

test:
	go test -p=8 ./...

clean:
	rm -rf cmd/cortextool/cortextool
