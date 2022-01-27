# Adapted from https://www.thapaliya.com/en/writings/well-documented-makefiles/
.PHONY: help
help: ## Display this help and any documented user-facing targets. Other undocumented targets may be present in the Makefile.
help:
	@awk 'BEGIN {FS = ": ##"; printf "Usage:\n  make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_\.\-\/%]+: ##/ { printf "  %-45s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# Local settings (optional). See Makefile.local.example for an example.
# WARNING: do not commit to a repository!
-include Makefile.local

.PHONY: all test test-with-race integration-tests cover clean images protos exes dist doc clean-doc check-doc push-multiarch-build-image license check-license format check-mixin check-mixin-jb check-mixin-mixtool checkin-mixin-playbook build-mixin format-mixin check-jsonnet-manifests format-jsonnet-manifests push-multiarch-mimir list-image-targets check-jsonnet-getting-started
.DEFAULT_GOAL := all

# Version number
VERSION=$(shell cat "./VERSION" 2> /dev/null)
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

HOSTNAME := $(shell hostname)

# Don't export GOOS and GOARCH as environment variables. They get exported when passed via CLI options,
# but that breaks tools ran via "go run". We use GOOS/GOARCH explicitly in places where needed.
unexport GOOS
unexport GOARCH

GOPROXY_VALUE=$(shell go env GOPROXY)

# Suffix added to the name of built binary (via exes target)
BINARY_SUFFIX ?= ""

# Boiler plate for building Docker containers.
# All this must go at top of file I'm afraid.
IMAGE_PREFIX ?= us.gcr.io/kubernetes-dev/
BUILD_IMAGE ?= $(IMAGE_PREFIX)mimir-build-image

# For a tag push GITHUB_REF will look like refs/tags/<tag_name>,
# If finding refs/tags/ does not equal emptystring then use
# the tag we are at as the image tag.
ifneq (,$(findstring refs/tags/, $(GITHUB_REF)))
	GIT_TAG := $(shell git tag --points-at HEAD)
endif
IMAGE_TAG ?= $(if $(GIT_TAG),$(GIT_TAG),$(shell ./tools/image-tag))
GIT_REVISION := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
UPTODATE := .uptodate

# path to jsonnetfmt
JSONNET_FMT := jsonnetfmt

# path to the mimir-mixin
MIXIN_PATH := operations/mimir-mixin
MIXIN_OUT_PATH := operations/mimir-mixin-compiled

# path to the mimir jsonnet manifests
JSONNET_MANIFESTS_PATH := operations/mimir

.PHONY: image-tag
image-tag:
	@echo $(IMAGE_TAG)

# Support gsed on OSX (installed via brew), falling back to sed. On Linux
# systems gsed won't be installed, so will use sed as expected.
SED ?= $(shell which gsed 2>/dev/null || which sed)

# Building Docker images is now automated. The convention is every directory
# with a Dockerfile in it builds an image called us.gcr.io/kubernetes-dev/<directory>.
# Dependencies (i.e. things that go in the image) still need to be explicitly
# declared.
#
# When building for docker, always build for Linux. This doesn't set GOARCH, which
# really depends on whether the image is going to be used locally (then GOARCH should be set based on
# host architecture), or pushed remotely. Ideally one would use push-multiarch-* targets instead
# in that case.
%/$(UPTODATE): GOOS=linux
%/$(UPTODATE): %/Dockerfile
	@echo
	$(SUDO) docker build --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)$(shell basename $(@D)) -t $(IMAGE_PREFIX)$(shell basename $(@D)):$(IMAGE_TAG) $(@D)/
	@echo
	@echo Go binaries were built using GOOS=$(GOOS) and GOARCH=$(GOARCH)
	@echo
	@echo Please use '"make push-multiarch-build-image"' to build and push build image.
	@echo Please use '"make push-multiarch-mimir"' to build and push Mimir image.
	@echo
	@touch $@

# This target compiles mimir for linux/amd64 and linux/arm64 and then builds and pushes a multiarch image to the target repository.
# Images are first built for each platform separately, as building both at once used to fail more often.
push-multiarch-mimir:
	@echo
	$(MAKE) GOOS=linux GOARCH=amd64 BINARY_SUFFIX=_linux_amd64 cmd/mimir/mimir
	$(SUDO) docker buildx build --platform linux/amd64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) --build-arg=USE_BINARY_SUFFIX=true cmd/mimir
	$(MAKE) GOOS=linux GOARCH=arm64 BINARY_SUFFIX=_linux_arm64 cmd/mimir/mimir
	$(SUDO) docker buildx build --platform linux/arm64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) --build-arg=USE_BINARY_SUFFIX=true cmd/mimir
	# This command will run the same build as above, but it will reuse existing platform-specific images,
	# put them together and push to registry.
	$(SUDO) docker buildx build -o type=registry --platform linux/amd64,linux/arm64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) --build-arg=USE_BINARY_SUFFIX=true -t $(IMAGE_PREFIX)mimir:$(IMAGE_TAG) cmd/mimir

# This target fetches current build image, and tags it with "latest" tag. It can be used instead of building the image locally.
fetch-build-image:
	docker pull $(BUILD_IMAGE):$(LATEST_BUILD_IMAGE_TAG)
	docker tag $(BUILD_IMAGE):$(LATEST_BUILD_IMAGE_TAG) $(BUILD_IMAGE):latest
	touch mimir-build-image/.uptodate

# push-multiarch-build-image requires the ability to build images for multiple platforms:
# https://docs.docker.com/buildx/working-with-buildx/#build-multi-platform-images
push-multiarch-build-image:
	@echo
	# Build image for each platform separately... it tends to generate fewer errors.
	$(SUDO) docker buildx build --platform linux/amd64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) mimir-build-image/
	$(SUDO) docker buildx build --platform linux/arm64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) mimir-build-image/
	# This command will run the same build as above, but it will reuse existing platform-specific images,
	# put them together and push to registry.
	$(SUDO) docker buildx build -o type=registry --platform linux/amd64,linux/arm64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(BUILD_IMAGE):$(IMAGE_TAG) mimir-build-image/

# We don't want find to scan inside a bunch of directories, to accelerate the
# 'make: Entering directory '/go/src/github.com/grafana/mimir' phase.
DONT_FIND := -name vendor -prune -o -name .git -prune -o -name .cache -prune -o -name .pkg -prune -o -name packaging -prune -o

MAKEFILES = $(shell find . $(DONT_FIND) \( -name 'Makefile' -o -name '*.mk' \) -print)

# Get a list of directories containing Dockerfiles
DOCKERFILES := $(shell find . $(DONT_FIND) -type f -name 'Dockerfile' -print)
UPTODATE_FILES := $(patsubst %/Dockerfile,%/$(UPTODATE),$(DOCKERFILES))
DOCKER_IMAGE_DIRS := $(patsubst %/Dockerfile,%,$(DOCKERFILES))
IMAGE_NAMES := $(foreach dir,$(DOCKER_IMAGE_DIRS),$(patsubst %,$(IMAGE_PREFIX)%,$(shell basename $(dir))))
images:
	$(info $(IMAGE_NAMES))
	@echo > /dev/null

# Generating proto code is automated.
PROTO_DEFS := $(shell find . $(DONT_FIND) -type f -name '*.proto' -print)
PROTO_GOS := $(patsubst %.proto,%.pb.go,$(PROTO_DEFS))

# Building binaries is now automated.  The convention is to build a binary
# for every directory with main.go in it, in the ./cmd directory.
MAIN_GO := $(shell find . $(DONT_FIND) -type f -name 'main.go' -print)
EXES := $(foreach exe, $(patsubst ./cmd/%/main.go, %, $(MAIN_GO)), ./cmd/$(exe)/$(exe))
GO_FILES := $(shell find . $(DONT_FIND) -name cmd -prune -o -name '*.pb.go' -prune -o -type f -name '*.go' -print)
define dep_exe
$(1): $(dir $(1))/main.go $(GO_FILES) protos
$(dir $(1))$(UPTODATE): $(1)
endef
$(foreach exe, $(EXES), $(eval $(call dep_exe, $(exe))))

# Manually declared dependencies And what goes into each exe
pkg/mimirpb/mimir.pb.go: pkg/mimirpb/mimir.proto
pkg/ingester/client/ingester.pb.go: pkg/ingester/client/ingester.proto
pkg/distributor/distributorpb/distributor.pb.go: pkg/distributor/distributorpb/distributor.proto
pkg/ring/ring.pb.go: pkg/ring/ring.proto
pkg/frontend/v1/frontendv1pb/frontend.pb.go: pkg/frontend/v1/frontendv1pb/frontend.proto
pkg/frontend/v2/frontendv2pb/frontend.pb.go: pkg/frontend/v2/frontendv2pb/frontend.proto
pkg/frontend/querymiddleware/model.pb.go: pkg/frontend/querymiddleware/model.proto
pkg/querier/stats/stats.pb.go: pkg/querier/stats/stats.proto
pkg/distributor/ha_tracker.pb.go: pkg/distributor/ha_tracker.proto
pkg/ruler/rulespb/rules.pb.go: pkg/ruler/rulespb/rules.proto
pkg/ruler/ruler.pb.go: pkg/ruler/ruler.proto
pkg/ring/kv/memberlist/kv.pb.go: pkg/ring/kv/memberlist/kv.proto
pkg/scheduler/schedulerpb/scheduler.pb.go: pkg/scheduler/schedulerpb/scheduler.proto
pkg/storegateway/storegatewaypb/gateway.pb.go: pkg/storegateway/storegatewaypb/gateway.proto
pkg/alertmanager/alertmanagerpb/alertmanager.pb.go: pkg/alertmanager/alertmanagerpb/alertmanager.proto
pkg/alertmanager/alertspb/alerts.pb.go: pkg/alertmanager/alertspb/alerts.proto

all: $(UPTODATE_FILES)
test: protos
test-with-race: protos
mod-check: protos
lint: lint-packaging-scripts protos
mimir-build-image/$(UPTODATE): mimir-build-image/*

# All the boiler plate for building golang follows:
SUDO := $(shell docker info >/dev/null 2>&1 || echo "sudo -E")
BUILD_IN_CONTAINER := true
LATEST_BUILD_IMAGE_TAG ?= import-jsonnet-readme-2418fd778-WIP

# TTY is parameterized to allow Google Cloud Builder to run builds,
# as it currently disallows TTY devices. This value needs to be overridden
# in any custom cloudbuild.yaml files
TTY := --tty
GO_FLAGS := -ldflags "-X main.Branch=$(GIT_BRANCH) -X main.Revision=$(GIT_REVISION) -X main.Version=$(VERSION) -extldflags \"-static\" -s -w" -tags netgo

ifeq ($(BUILD_IN_CONTAINER),true)

GOVOLUMES=	-v $(shell pwd)/.cache:/go/cache:delegated,z \
			-v $(shell pwd)/.pkg:/go/pkg:delegated,z \
			-v $(shell pwd):/go/src/github.com/grafana/mimir:delegated,z

# Mount local ssh credentials to be able to clone private repos when doing `mod-check`
SSHVOLUME=  -v ~/.ssh/:/root/.ssh:delegated,z

exes $(EXES) protos $(PROTO_GOS) lint lint-packaging-scripts test test-with-race cover shell mod-check check-protos web-build web-pre web-deploy doc format: mimir-build-image/$(UPTODATE)
	@mkdir -p $(shell pwd)/.pkg
	@mkdir -p $(shell pwd)/.cache
	@echo
	@echo ">>>> Entering build container: $@"
	$(SUDO) time docker run --rm $(TTY) -i $(SSHVOLUME) $(GOVOLUMES) $(BUILD_IMAGE) GOOS=$(GOOS) GOARCH=$(GOARCH) BINARY_SUFFIX=$(BINARY_SUFFIX) $@;

else

exes: $(EXES)

$(EXES):
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(GO_FLAGS) -o "$@$(BINARY_SUFFIX)" ./$(@D)

protos: $(PROTO_GOS)

%.pb.go:
	@# The store-gateway RPC is based on Thanos which uses relative references to other protos, so we need
	@# to configure all such relative paths.
	protoc -I $(GOPATH)/src:./vendor/github.com/thanos-io/thanos/pkg:./vendor/github.com/gogo/protobuf:./vendor:./$(@D) --gogoslick_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,:./$(@D) ./$(patsubst %.pb.go,%.proto,$@)

lint-packaging-scripts: packaging/deb/control/postinst packaging/deb/control/prerm packaging/rpm/control/post packaging/rpm/control/preun
	shellcheck $?

lint: lint-packaging-scripts check-makefiles
	misspell -error docs/sources

	# Configured via .golangci.yml.
	golangci-lint run

	# Ensure no blocklisted package is imported.
	GOFLAGS="-tags=requires_docker" faillint -paths "github.com/bmizerany/assert=github.com/stretchr/testify/assert,\
		golang.org/x/net/context=context,\
		sync/atomic=go.uber.org/atomic,\
		github.com/go-kit/kit/log/...=github.com/go-kit/log,\
		github.com/prometheus/client_golang/prometheus.{MultiError}=github.com/prometheus/prometheus/tsdb/errors.{NewMulti},\
		github.com/weaveworks/common/user.{ExtractOrgID}=github.com/grafana/mimir/pkg/tenant.{TenantID,TenantIDs},\
		github.com/weaveworks/common/user.{ExtractOrgIDFromHTTPRequest}=github.com/grafana/mimir/pkg/tenant.{ExtractTenantIDFromHTTPRequest}" ./pkg/... ./cmd/... ./tools/... ./integration/...

	# Ensure clean pkg structure.
	faillint -paths "\
		github.com/grafana/mimir/pkg/scheduler,\
		github.com/grafana/mimir/pkg/frontend,\
		github.com/grafana/mimir/pkg/frontend/transport,\
		github.com/grafana/mimir/pkg/frontend/v1,\
		github.com/grafana/mimir/pkg/frontend/v2" \
		./pkg/querier/...
	faillint -paths "github.com/grafana/mimir/pkg/querier/..." ./pkg/scheduler/...
	faillint -paths "github.com/grafana/mimir/pkg/storage/tsdb/..." ./pkg/storage/bucket/...
	faillint -paths "github.com/grafana/mimir/pkg/..." ./pkg/alertmanager/alertspb/...
	faillint -paths "github.com/grafana/mimir/pkg/..." ./pkg/ruler/rulespb/...
	faillint -paths "github.com/grafana/mimir/pkg/..." ./pkg/storage/sharding/...
	faillint -paths "github.com/grafana/mimir/pkg/..." ./pkg/querier/engine/...

	# Ensure all errors are report as APIError
	faillint -paths "github.com/weaveworks/common/httpgrpc.{Errorf}=github.com/grafana/mimir/pkg/api/error.Newf" ./pkg/frontend/querymiddleware/...

	# Ensure the query path is supporting multiple tenants
	faillint -paths "\
		github.com/grafana/mimir/pkg/tenant.{TenantID}=github.com/grafana/mimir/pkg/tenant.{TenantIDs}" \
		./pkg/scheduler/... \
		./pkg/frontend/... \
		./pkg/querier/tenantfederation/... \
		./pkg/frontend/querymiddleware/...

	# Ensure packages that no longer use a global logger don't reintroduce it
	faillint -paths "github.com/grafana/mimir/pkg/util/log.{Logger}" \
		./pkg/alertmanager/alertstore/... \
		./pkg/ingester/... \
		./pkg/flusher/... \
		./pkg/querier/... \
		./pkg/ruler/...

	faillint -paths "github.com/thanos-io/thanos/pkg/block.{NewIgnoreDeletionMarkFilter}" \
		./pkg/compactor/...

	# Ensure packages we imported from Thanos are no longer used.
	GOFLAGS="-tags=requires_docker" faillint -paths \
		"github.com/thanos/thanos-io/pkg/store,\
		github.com/thanos-io/thanos/pkg/testutil/..., \
		github.com/thanos-io/thanos/pkg/store/cache" \
		./pkg/... ./cmd/... ./tools/... ./integration/...

format:
	find . $(DONT_FIND) -name '*.pb.go' -prune -o -type f -name '*.go' -exec gofmt -w -s {} \;
	find . $(DONT_FIND) -name '*.pb.go' -prune -o -type f -name '*.go' -exec goimports -w -local github.com/grafana/mimir {} \;

test:
	go test -timeout 30m ./...

test-with-race:
	go test -tags netgo -timeout 30m -race -count 1 ./...

cover:
	$(eval COVERDIR := $(shell mktemp -d coverage.XXXXXXXXXX))
	$(eval COVERFILE := $(shell mktemp $(COVERDIR)/unit.XXXXXXXXXX))
	go test -tags netgo -timeout 30m -race -count 1 -coverprofile=$(COVERFILE) ./...
	go tool cover -html=$(COVERFILE) -o cover.html
	go tool cover -func=cover.html | tail -n1

shell:
	bash

mod-check:
	GO111MODULE=on go mod download
	GO111MODULE=on go mod verify
	GO111MODULE=on go mod tidy
	GO111MODULE=on go mod vendor
	@git diff --exit-code -- go.sum go.mod vendor/

check-protos: clean-protos protos
	@git diff --exit-code -- $(PROTO_GOS)

web-pre:
	cd website && git submodule update --init --recursive
	./tools/website/web-pre.sh

web-build: web-pre
	cd website && HUGO_ENV=production hugo --config config.toml  --minify -v

web-deploy:
	./tools/website/web-deploy.sh

# Generates the config file documentation.
doc: clean-doc
	go run ./tools/doc-generator ./docs/sources/configuration/config-file-reference.template > ./docs/sources/configuration/config-file-reference.md
	go run ./tools/doc-generator ./docs/sources/blocks-storage/compactor.template            > ./docs/sources/blocks-storage/compactor.md
	go run ./tools/doc-generator ./docs/sources/blocks-storage/store-gateway.template        > ./docs/sources/blocks-storage/store-gateway.md
	go run ./tools/doc-generator ./docs/sources/blocks-storage/querier.template              > ./docs/sources/blocks-storage/querier.md
	go run ./tools/doc-generator ./docs/sources/guides/encryption-at-rest.template           > ./docs/sources/guides/encryption-at-rest.md
	embedmd -w docs/sources/operations/requests-mirroring-to-secondary-cluster.md
	embedmd -w docs/sources/guides/overrides-exporter.md
	embedmd -w operations/mimir/README.md

	# Make up markdown files prettier. When running with check-doc target, it will fail if this produces any change.
	prettier --write "**/*.md"

# Add license header to files.
license:
	go run ./tools/add-license ./cmd ./integration ./pkg ./tools ./packaging ./development ./mimir-build-image

check-license: license
	@git diff --exit-code || (echo "Please add the license header running 'make BUILD_IN_CONTAINER=false license'" && false)

endif

.PHONY: check-makefiles
check-makefiles: format-makefiles
	@git diff --exit-code -- $(MAKEFILES) || (echo "Please format Makefiles by running 'make format-makefiles'" && false)

.PHONY: format-makefiles
format-makefiles: ## Format all Makefiles.
format-makefiles: $(MAKEFILES)
	sed -i -e 's/^\(\t*\)  /\1\t/g' -e 's/^\(\t*\) /\1/' -- $?

clean:
	$(SUDO) docker rmi $(IMAGE_NAMES) >/dev/null 2>&1 || true
	rm -rf -- $(UPTODATE_FILES) $(EXES) .cache dist
	go clean ./...

clean-protos:
	rm -rf $(PROTO_GOS)

# List all images building make targets.
list-image-targets:
	@echo $(UPTODATE_FILES) | tr " " "\n"

save-images:
	@mkdir -p docker-images
	for image_name in $(IMAGE_NAMES); do \
		if echo $$image_name | grep -q build; then \
			continue; \
		fi; \
		if [ "$$(docker images -q $$image_name:$(IMAGE_TAG) 2> /dev/null)" = "" ]; then \
			echo "Skipping $$image_name:$(IMAGE_TAG) because image does not exist"; \
		else \
			echo "Saving $$image_name:$(IMAGE_TAG)"; \
			docker save $$image_name:$(IMAGE_TAG) -o docker-images/$$(echo $$image_name | tr "/" _):$(IMAGE_TAG); \
		fi; \
	done

load-images:
	for image_name in $(IMAGE_NAMES); do \
		if echo $$image_name | grep -q build; then \
			continue; \
		fi; \
		image_path=docker-images/$$(echo $$image_name | tr "/" _):$(IMAGE_TAG); \
		if [ -e "$$image_path" ]; then \
			echo "Loading $$image_path"; \
			docker load -i "$$image_path"; \
		else \
			echo "Skipping $$image_path because image does not exist"; \
		fi; \
	done

clean-doc:
	rm -f \
		./docs/sources/configuration/config-file-reference.md \
		./docs/sources/blocks-storage/compactor.md \
		./docs/sources/blocks-storage/store-gateway.md \
		./docs/sources/blocks-storage/querier.md \
		./docs/sources/guides/encryption-at-rest.md

check-doc: doc
	@git diff --exit-code -- \
		./docs/sources/configuration/config-file-reference.md \
		./docs/sources/blocks-storage/*.md \
		./docs/sources/configuration/*.md \
		./docs/sources/operations/*.md \
		./operations/mimir/*.md \
		./operations/mimir-mixin/docs/sources/*.md \
	|| (echo "Please update generated documentation by running 'make doc'" && false)

.PHONY: reference-help
reference-help: cmd/mimir/mimir
	@(./cmd/mimir/mimir -h || true) > cmd/mimir/help.txt.tmpl
	@sed -i s/$(HOSTNAME)/\{\{.Hostname\}\}/g cmd/mimir/help.txt.tmpl
	@(./cmd/mimir/mimir -help-all || true) > cmd/mimir/help-all.txt.tmpl
	@sed -i s/$(HOSTNAME)/\{\{.Hostname\}\}/g cmd/mimir/help-all.txt.tmpl

clean-white-noise:
	@find . -path ./.pkg -prune -o -path ./vendor -prune -o -path ./website -prune -or -type f -name "*.md" -print | \
	SED_BIN="$(SED)" xargs ./tools/cleanup-white-noise.sh

check-white-noise: clean-white-noise
	@git diff --exit-code -- '*.md' || (echo "Please remove trailing whitespaces running 'make clean-white-noise'" && false)

check-mixin: format-mixin check-mixin-jb check-mixin-mixtool check-mixin-playbook
	@echo "Checking diff:"
	@git diff --exit-code -- $(MIXIN_PATH) || (echo "Please build and format mixin by running 'make build-mixin format-mixin'" && false)

	@cd $(MIXIN_PATH) && \
	jb install && \
	mixtool lint mixin.libsonnet

check-mixin-jb:
	@cd $(MIXIN_PATH) && \
	jb install

check-mixin-mixtool: check-mixin-jb
	@cd $(MIXIN_PATH) && \
	mixtool lint mixin.libsonnet

check-mixin-playbook: build-mixin
	@$(MIXIN_PATH)/scripts/lint-playbooks.sh

build-mixin: check-mixin-jb
	@rm -rf $(MIXIN_OUT_PATH) && mkdir $(MIXIN_OUT_PATH)
	@mixtool generate all --output-alerts $(MIXIN_OUT_PATH)/alerts.yaml --output-rules $(MIXIN_OUT_PATH)/rules.yaml --directory $(MIXIN_OUT_PATH)/dashboards ${MIXIN_PATH}/mixin.libsonnet
	@cd $(MIXIN_OUT_PATH)/.. && zip -q -r mimir-mixin.zip $$(basename "$(MIXIN_OUT_PATH)")
	@echo "The mixin has been compiled to $(MIXIN_OUT_PATH) and archived to $$(realpath --relative-to=$$(pwd) $(MIXIN_OUT_PATH)/../mimir-mixin.zip)"

format-mixin:
	@find $(MIXIN_PATH) -type f -name '*.libsonnet' -print -o -name '*.jsonnet' -print | xargs jsonnetfmt -i

check-jsonnet-manifests: format-jsonnet-manifests
	@echo "Checking diff:"
	@git diff --exit-code -- $(JSONNET_MANIFESTS_PATH) || (echo "Please format jsonnet manifests by running 'make format-jsonnet-manifests'" && false)

format-jsonnet-manifests:
	@find $(JSONNET_MANIFESTS_PATH) -type f -name '*.libsonnet' -print -o -name '*.jsonnet' -print | xargs jsonnetfmt -i

check-jsonnet-getting-started:
	# Start from a clean setup.
	rm -rf jsonnet-example

	# We want to test any change in this branch/PR, so after installing Mimir jsonnet
	# we replace it with the current code.
	cat ./operations/mimir/getting-started.sh \
		| sed 's/\(jb install github.com\/grafana\/mimir\/operations\/mimir@main\)/\1 \&\& rm -fr .\/vendor\/mimir \&\& cp -r ..\/operations\/mimir .\/vendor\/mimir\//g' \
		| bash

build-jsonnet-tests:
	@./operations/mimir-tests/build.sh

check-jsonnet-tests: build-jsonnet-tests
	@git diff --exit-code -- ./operations/mimir-tests || (echo "Please rebuild jsonnet tests output 'make build-jsonnet-tests'" && false)

check-tsdb-blocks-storage-s3-docker-compose-yaml:
	cd development/tsdb-blocks-storage-s3 && make check

web-serve:
	cd website && hugo --config config.toml --minify -v server

# Generate binaries for a Mimir release
dist: dist/$(UPTODATE)

dist/$(UPTODATE):
	rm -fr ./dist
	mkdir -p ./dist
	for os in linux darwin; do \
		for arch in amd64 arm64; do \
			echo "Building Mimir for $$os/$$arch"; \
			GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/mimir-$$os-$$arch ./cmd/mimir; \
			sha256sum ./dist/mimir-$$os-$$arch | cut -d ' ' -f 1 > ./dist/mimir-$$os-$$arch-sha-256; \
			echo "Building query-tee for $$os/$$arch"; \
			GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/query-tee-$$os-$$arch ./cmd/query-tee; \
			sha256sum ./dist/query-tee-$$os-$$arch | cut -d ' ' -f 1 > ./dist/query-tee-$$os-$$arch-sha-256; \
			done; \
		done; \
		touch $@

# Generate packages for a Mimir release.
FPM_OPTS := fpm -s dir -v $(VERSION) -n mimir -f \
	--license "Apache 2.0" \
	--url "https://github.com/grafana/mimir"

PACKAGE_IN_CONTAINER := true
PACKAGE_IMAGE ?= $(IMAGE_PREFIX)fpm
ifeq ($(PACKAGE_IN_CONTAINER), true)

.PHONY: packages
packages: dist packaging/fpm/$(UPTODATE)
	@mkdir -p $(shell pwd)/.pkg
	@mkdir -p $(shell pwd)/.cache
	@echo ">>>> Entering build container: $@"
	$(SUDO) time docker run --rm $(TTY) \
		-v  $(shell pwd):/src/github.com/grafana/mimir:delegated,z \
		-i $(PACKAGE_IMAGE) $@;

else

packages: dist/$(UPTODATE)-packages

dist/$(UPTODATE)-packages: dist $(wildcard packaging/deb/**) $(wildcard packaging/rpm/**)
	for arch in amd64 arm64; do \
		rpm_arch=x86_64; \
		deb_arch=x86_64; \
		if [ "$$arch" = "arm64" ]; then \
			rpm_arch=aarch64; \
			deb_arch=arm64; \
		fi; \
		$(FPM_OPTS) -t deb \
			--architecture $$deb_arch \
			--after-install packaging/deb/control/postinst \
			--before-remove packaging/deb/control/prerm \
			--package dist/mimir-$(VERSION)_$$arch.deb \
			dist/mimir-linux-$$arch=/usr/local/bin/mimir \
			docs/sources/chunks-storage/single-process-config.yaml=/etc/mimir/single-process-config.yaml \
			packaging/deb/default/mimir=/etc/default/mimir \
			packaging/deb/systemd/mimir.service=/etc/systemd/system/mimir.service; \
		$(FPM_OPTS) -t rpm  \
			--architecture $$rpm_arch \
			--after-install packaging/rpm/control/post \
			--before-remove packaging/rpm/control/preun \
			--package dist/mimir-$(VERSION)_$$arch.rpm \
			dist/mimir-linux-$$arch=/usr/local/bin/mimir \
			docs/sources/chunks-storage/single-process-config.yaml=/etc/mimir/single-process-config.yaml \
			packaging/rpm/sysconfig/mimir=/etc/sysconfig/mimir \
			packaging/rpm/systemd/mimir.service=/etc/systemd/system/mimir.service; \
	done
	for pkg in dist/*.deb dist/*.rpm; do \
		sha256sum $$pkg | cut -d ' ' -f 1 > $${pkg}-sha-256; \
	done; \
	touch $@

endif

integration-tests: cmd/mimir/$(UPTODATE)
	go test -tags=requires_docker ./integration/...

# Build both arm64 and amd64 images, so that we can test deb/rpm packages for both architectures.
packaging/rpm/centos-systemd/$(UPTODATE): packaging/rpm/centos-systemd/Dockerfile
	$(SUDO) docker build --platform linux/amd64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)$(shell basename $(@D)):amd64 $(@D)/
	$(SUDO) docker build --platform linux/arm64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)$(shell basename $(@D)):arm64 $(@D)/
	touch $@

packaging/deb/debian-systemd/$(UPTODATE): packaging/deb/debian-systemd/Dockerfile
	$(SUDO) docker build --platform linux/amd64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)$(shell basename $(@D)):amd64 $(@D)/
	$(SUDO) docker build --platform linux/arm64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)$(shell basename $(@D)):arm64 $(@D)/
	touch $@

.PHONY: test-packages
test-packages: packages packaging/rpm/centos-systemd/$(UPTODATE) packaging/deb/debian-systemd/$(UPTODATE)
	./tools/packaging/test-packages $(IMAGE_PREFIX) $(VERSION)

include docs.mk
docs: doc
