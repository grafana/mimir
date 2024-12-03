SHELL = /usr/bin/env bash

# Adapted from https://www.thapaliya.com/en/writings/well-documented-makefiles/
.PHONY: help
help: ## Display this help and any documented user-facing targets. Other undocumented targets may be present in the Makefile.
help:
	@awk 'BEGIN {FS = ": ##"; printf "Usage:\n  make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_\.\-\/%]+: ##/ { printf "  %-45s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# Local settings (optional). See Makefile.local.example for an example.
# WARNING: do not commit to a repository!
-include Makefile.local

.PHONY: all test test-with-race integration-tests cover clean images protos exes dist doc clean-doc check-doc push-multiarch-build-image license check-license format check-mixin check-mixin-jb check-mixin-mixtool check-mixin-runbooks check-mixin-mimirtool-rules build-mixin format-mixin check-jsonnet-manifests format-jsonnet-manifests push-multiarch-mimir list-image-targets check-jsonnet-getting-started mixin-screenshots
.DEFAULT_GOAL := all

# Version number
VERSION=$(shell cat "./VERSION" 2> /dev/null)
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

# Don't export GOOS and GOARCH as environment variables. They get exported when passed via CLI options,
# but that breaks tools ran via "go run". We use GOOS/GOARCH explicitly in places where needed.
unexport GOOS
unexport GOARCH

GOPROXY_VALUE=$(shell go env GOPROXY)

# Suffix added to the name of built binary (via exes target)
BINARY_SUFFIX ?= ""

# Boiler plate for building Docker containers.
# All this must go at top of file I'm afraid.
IMAGE_PREFIX ?= grafana/
BUILD_IMAGE ?= $(IMAGE_PREFIX)mimir-build-image
CONTAINER_MOUNT_OPTIONS ?= delegated,z

# For a tag push, $GITHUB_REF will look like refs/tags/<tag_name>.
# If finding refs/tags/ does not equal empty string, then use
# the tag we are at as the image tag.
ifneq (,$(findstring refs/tags/, $(GITHUB_REF)))
	GIT_TAG := $(shell git tag --points-at HEAD)
	# If the git tag starts with "mimir-" (eg. "mimir-2.0.0") we strip
	# the "mimir-" prefix in order to keep only the version.
	IMAGE_TAG_FROM_GIT_TAG := $(patsubst mimir-%,%,$(GIT_TAG))
endif
IMAGE_TAG ?= $(if $(IMAGE_TAG_FROM_GIT_TAG),$(IMAGE_TAG_FROM_GIT_TAG),$(shell ./tools/image-tag))
IMAGE_TAG_RACE = race-$(IMAGE_TAG)
GIT_REVISION := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
UPTODATE := .uptodate
UPTODATE_RACE := .uptodate_race

# path to jsonnetfmt
JSONNET_FMT := jsonnetfmt

# path to the mimir-mixin
MIXIN_PATH := operations/mimir-mixin
MIXIN_OUT_PATH := operations/mimir-mixin-compiled
MIXIN_OUT_PATH_SUFFIXES := "" "-baremetal"

# path to the mimir jsonnet manifests
JSONNET_MANIFESTS_PATHS := operations/mimir operations/mimir-tests development

# path to the mimir doc sources
DOC_SOURCES_PATH := docs/sources/mimir

# Doc templates in use
DOC_TEMPLATES := $(DOC_SOURCES_PATH)/configure/configuration-parameters/index.template

# Documents to run through embedding
DOC_EMBED := $(DOC_SOURCES_PATH)/configure/configure-the-query-frontend-work-with-prometheus.md \
	$(DOC_SOURCES_PATH)/configure/mirror-requests-to-a-second-cluster/index.md \
	$(DOC_SOURCES_PATH)/references/architecture/components/overrides-exporter.md \
	$(DOC_SOURCES_PATH)/get-started/_index.md \
	$(DOC_SOURCES_PATH)/set-up/jsonnet/deploy.md

.PHONY: image-tag
image-tag: ## Print the docker image tag.
	@echo $(IMAGE_TAG)

.PHONY: image-tag-race
image-tag-race: ## Print the docker image tag for race-enabled image.
	@echo $(IMAGE_TAG_RACE)

# Support gsed on OSX (installed via brew), falling back to sed. On Linux
# systems gsed won't be installed, so will use sed as expected.
SED ?= $(shell which gsed 2>/dev/null || which sed)

# Building Docker images is now automated. The convention is every directory
# with a Dockerfile in it builds an image called grafana/<directory>.
# Dependencies (i.e. things that go in the image) still need to be explicitly
# declared.
#
# When building for docker, always build for Linux. This doesn't set GOARCH, which
# really depends on whether the image is going to be used locally (then GOARCH should be set based on
# host architecture), or pushed remotely. Ideally one would use push-multiarch-* targets instead
# in that case.
%/$(UPTODATE): GOOS=linux
%/$(UPTODATE): %/Dockerfile
	if [ -f $(@D)/Dockerfile.continuous-test ]; then \
		$(SUDO) docker build -f $(@D)/Dockerfile.continuous-test \
			--build-arg=revision=$(GIT_REVISION) \
			--build-arg=goproxyValue=$(GOPROXY_VALUE) \
			-t $(IMAGE_PREFIX)$(shell basename $(@D))-continuous-test:$(IMAGE_TAG) $(@D)/; \
	fi;
	if [ -f $(@D)/Dockerfile.alpine ]; then \
		$(SUDO) docker build -f $(@D)/Dockerfile.alpine \
			--build-arg=revision=$(GIT_REVISION) \
			--build-arg=goproxyValue=$(GOPROXY_VALUE) \
			-t $(IMAGE_PREFIX)$(shell basename $(@D))-alpine:$(IMAGE_TAG) $(@D)/; \
	fi;
	@echo
	$(SUDO) docker build --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)$(shell basename $(@D)) -t $(IMAGE_PREFIX)$(shell basename $(@D)):$(IMAGE_TAG) $(@D)/
	@echo
	@echo Go binaries were built using GOOS=$(GOOS) and GOARCH=$(GOARCH)
	@echo
	@echo Image name: $(IMAGE_PREFIX)$(shell basename $(@D))
	@echo Image name: $(IMAGE_PREFIX)$(shell basename $(@D)):$(IMAGE_TAG)
	@echo Image name: $(IMAGE_PREFIX)$(shell basename $(@D))-continuous-test:$(IMAGE_TAG)
	@echo Image name: $(IMAGE_PREFIX)$(shell basename $(@D))-alpine:$(IMAGE_TAG)
	@echo
	@echo Please use '"make push-multiarch-build-image"' to build and push build image.
	@echo Please use '"make push-multiarch-mimir"' to build and push Mimir image.
	@echo
	@touch $@

%/$(UPTODATE_RACE): GOOS=linux
%/$(UPTODATE_RACE): %/Dockerfile
	# Build Dockerfile.alpine if it exists
	if [ -f $(@D)/Dockerfile.alpine ]; then \
		$(SUDO) docker build -f $(@D)/Dockerfile.alpine \
			--build-arg=revision=$(GIT_REVISION) \
			--build-arg=goproxyValue=$(GOPROXY_VALUE) \
			--build-arg=USE_BINARY_SUFFIX=true \
			--build-arg=BINARY_SUFFIX=_race \
			--build-arg=EXTRA_PACKAGES="gcompat" \
			-t $(IMAGE_PREFIX)$(shell basename $(@D))-alpine:$(IMAGE_TAG_RACE) $(@D)/; \
	fi;
	@echo
	# We need gcompat -- compatibility layer with glibc, as race-detector currently requires glibc, but Alpine uses musl libc instead.
	$(SUDO) docker build \
		--build-arg=revision=$(GIT_REVISION) \
		--build-arg=goproxyValue=$(GOPROXY_VALUE) \
		--build-arg=USE_BINARY_SUFFIX=true \
		--build-arg=BINARY_SUFFIX=_race \
		--build-arg=BASEIMG="gcr.io/distroless/base-nossl-debian12" \
		-t $(IMAGE_PREFIX)$(shell basename $(@D)):$(IMAGE_TAG_RACE) $(@D)/
	@echo
	@echo Go binaries were built using GOOS=$(GOOS) and GOARCH=$(GOARCH)
	@echo
	@echo Image name: $(IMAGE_PREFIX)$(shell basename $(@D)):$(IMAGE_TAG_RACE)
	@echo Image name: $(IMAGE_PREFIX)$(shell basename $(@D))-alpine:$(IMAGE_TAG_RACE)
	@echo
	@touch $@

# This variable controls where result of building of multiarch image should be sent. Default is registry.
# Other options are documented in https://docs.docker.com/engine/reference/commandline/buildx_build/#output.
# CI workflow uses PUSH_MULTIARCH_TARGET="type=oci,dest=file.oci" to store images locally for next steps in the pipeline.
PUSH_MULTIARCH_TARGET ?= type=registry
PUSH_MULTIARCH_TARGET_ALPINE ?= type=registry
PUSH_MULTIARCH_TARGET_CONTINUOUS_TEST ?= type=registry

# This target compiles mimir for linux/amd64 and linux/arm64 and then builds and pushes a multiarch image to the target repository.
# We don't do separate building of single-platform and multiplatform images here (as we do for push-multiarch-build-image), as
# these Dockerfiles are not doing much, and are unlikely to fail.
push-multiarch-%/$(UPTODATE):
	$(eval DIR := $(patsubst push-multiarch-%/$(UPTODATE),%,$@))

	if [ -f $(DIR)/main.go ]; then \
		$(MAKE) GOOS=linux GOARCH=amd64 BINARY_SUFFIX=_linux_amd64 $(DIR)/$(shell basename $(DIR)); \
		$(MAKE) GOOS=linux GOARCH=arm64 BINARY_SUFFIX=_linux_arm64 $(DIR)/$(shell basename $(DIR)); \
	fi
	$(SUDO) docker buildx build \
		-o $(PUSH_MULTIARCH_TARGET) \
		--platform linux/amd64,linux/arm64 \
		--build-arg=revision=$(GIT_REVISION) \
		--build-arg=goproxyValue=$(GOPROXY_VALUE) \
		--build-arg=USE_BINARY_SUFFIX=true \
		-t $(IMAGE_PREFIX)$(shell basename $(DIR)):$(IMAGE_TAG) $(DIR)/

	# Build Dockerfile.continuous-test
	if [ -f $(DIR)/Dockerfile.continuous-test ]; then \
		$(SUDO) docker buildx build -f $(DIR)/Dockerfile.continuous-test \
			-o $(PUSH_MULTIARCH_TARGET_CONTINUOUS_TEST) \
			--platform linux/amd64,linux/arm64 \
			--build-arg=revision=$(GIT_REVISION) \
			--build-arg=goproxyValue=$(GOPROXY_VALUE) \
			--build-arg=USE_BINARY_SUFFIX=true \
			-t $(IMAGE_PREFIX)$(shell basename $(DIR))-continuous-test:$(IMAGE_TAG) $(DIR)/; \
	fi;
	# Build Dockerfile.alpine if it exists
	if [ -f $(DIR)/Dockerfile.alpine ]; then \
		$(SUDO) docker buildx build -f $(DIR)/Dockerfile.alpine \
			-o $(PUSH_MULTIARCH_TARGET_ALPINE) \
			--platform linux/amd64,linux/arm64 \
			--build-arg=revision=$(GIT_REVISION) \
			--build-arg=goproxyValue=$(GOPROXY_VALUE) \
			--build-arg=USE_BINARY_SUFFIX=true \
			-t $(IMAGE_PREFIX)$(shell basename $(DIR))-alpine:$(IMAGE_TAG) $(DIR)/; \
	fi;

push-multiarch-mimir: ## Push mimir docker image.
push-multiarch-mimir: push-multiarch-cmd/mimir/.uptodate

# This target fetches current build image, and tags it with "latest" tag. It can be used instead of building the image locally.
.PHONY: fetch-build-image
fetch-build-image: ## Fetch latest the docker build image if it isn't already present. It can be used instead of building the image locally.
	docker image inspect $(BUILD_IMAGE):$(LATEST_BUILD_IMAGE_TAG) >/dev/null 2>&1 || docker pull $(BUILD_IMAGE):$(LATEST_BUILD_IMAGE_TAG)
	docker tag $(BUILD_IMAGE):$(LATEST_BUILD_IMAGE_TAG) $(BUILD_IMAGE):latest
	touch mimir-build-image/.uptodate

# push-multiarch-build-image requires the ability to build images for multiple platforms:
# https://docs.docker.com/buildx/working-with-buildx/#build-multi-platform-images
push-multiarch-build-image: ## Push the docker build image.
	@echo
	# Build and push mimir build image for linux/amd64 and linux/arm64
	$(SUDO) docker buildx build -o type=registry --platform linux/amd64,linux/arm64 --progress=plain --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(BUILD_IMAGE):$(IMAGE_TAG) mimir-build-image/

.PHONY: print-build-image
print-build-image:
	@echo $(BUILD_IMAGE):$(LATEST_BUILD_IMAGE_TAG)

# We don't want find to scan inside a bunch of directories, to accelerate the
# 'make: Entering directory '/go/src/github.com/grafana/mimir' phase.
DONT_FIND := -name vendor -prune -o -name .git -prune -o -name .cache -prune -o -name .pkg -prune -o -name packaging -prune -o -name mimir-mixin-tools -prune -o -name trafficdump -prune -o

# MAKE_FILES is a list of make files to lint.
# We purposefully avoid MAKEFILES as a variable name as it influences
# the files included in recursive invocations of make
MAKE_FILES = $(shell find . $(DONT_FIND) \( -name 'Makefile' -o -name '*.mk' \) -print)

# Get a list of directories containing Dockerfiles
DOCKERFILES := $(shell find . $(DONT_FIND) -type f -name 'Dockerfile' -print)
UPTODATE_FILES := $(patsubst %/Dockerfile,%/$(UPTODATE),$(DOCKERFILES))
UPTODATE_RACE_FILES := $(patsubst %/Dockerfile,%/$(UPTODATE_RACE),$(DOCKERFILES))
DOCKER_IMAGE_DIRS := $(patsubst %/Dockerfile,%,$(DOCKERFILES))
IMAGE_NAMES := $(foreach dir,$(DOCKER_IMAGE_DIRS),$(patsubst %,$(IMAGE_PREFIX)%,$(shell basename $(dir))))
images: ## Print all image names.
	$(info $(IMAGE_NAMES))
	@echo > /dev/null

# Generating proto code is automated.
PROTO_DEFS := $(shell find . $(DONT_FIND) -type f -name '*.proto' -print)
PROTO_GOS := $(patsubst %.proto,%.pb.go,$(PROTO_DEFS))

# Generating OTLP translation code is automated.
OTLP_GOS := $(shell find ./pkg/distributor/otlp/ -type f -name '*_generated.go' -print)

# Building binaries is now automated. The convention is to build a binary
# for every directory with main.go in it.
MAIN_GO := $(shell find . $(DONT_FIND) -type f -name 'main.go' -print)
EXES := $(foreach exeDir,$(patsubst %/main.go, %, $(MAIN_GO)),$(exeDir)/$(notdir $(exeDir)))
EXES_RACE := $(foreach exeDir,$(patsubst %/main.go, %, $(MAIN_GO)),$(exeDir)/$(addsuffix _race, $(notdir $(exeDir))))
GO_FILES := $(shell find . $(DONT_FIND) -name cmd -prune -o -name '*.pb.go' -prune -o -type f -name '*.go' -print)

define dep_exe
$(1): $(dir $(1))/main.go $(GO_FILES) protos
$(dir $(1))$(UPTODATE): $(1)
endef
$(foreach exe, $(EXES), $(eval $(call dep_exe, $(exe))))

define dep_exe_race
$(1): $(dir $(1))/main.go $(GO_FILES) protos
$(dir $(1))$(UPTODATE_RACE): $(1)
endef
$(foreach exe, $(EXES_RACE), $(eval $(call dep_exe_race, $(exe))))

all: $(UPTODATE_FILES)
test: protos
test-with-race: protos
mod-check: protos
lint: lint-packaging-scripts protos
mimir-build-image/$(UPTODATE): mimir-build-image/*

# All the boiler plate for building golang follows:
SUDO := $(shell docker info >/dev/null 2>&1 || echo "sudo -E")
BUILD_IN_CONTAINER ?= true
LATEST_BUILD_IMAGE_TAG ?= pr10097-2f1432e4d6

# TTY is parameterized to allow Google Cloud Builder to run builds,
# as it currently disallows TTY devices. This value needs to be overridden
# in any custom cloudbuild.yaml files
TTY := --tty
MIMIR_VERSION := github.com/grafana/mimir/pkg/util/version

REGO_POLICIES_PATH=operations/policies

GO_FLAGS := -ldflags "\
		-X $(MIMIR_VERSION).Branch=$(GIT_BRANCH) \
		-X $(MIMIR_VERSION).Revision=$(GIT_REVISION) \
		-X $(MIMIR_VERSION).Version=$(VERSION) \
		-extldflags \"-static\" -s -w" -tags netgo,stringlabels

ifeq ($(BUILD_IN_CONTAINER),true)

GOVOLUMES=	-v mimir-go-cache:/go/cache \
			-v mimir-go-pkg:/go/pkg \
			-v $(shell pwd):/go/src/github.com/grafana/mimir:$(CONTAINER_MOUNT_OPTIONS)

# Mount local ssh credentials to be able to clone private repos when doing `mod-check`
SSHVOLUME=  -v ~/.ssh/:/root/.ssh:$(CONTAINER_MOUNT_OPTIONS)

exes $(EXES) $(EXES_RACE) protos $(PROTO_GOS) lint lint-packaging-scripts test test-with-race cover shell mod-check check-protos doc format dist build-mixin format-mixin check-mixin-tests license check-license conftest-fmt check-conftest-fmt helm-conftest-test helm-conftest-quick-test conftest-verify check-helm-tests build-helm-tests print-go-version: fetch-build-image
	@echo ">>>> Entering build container: $@"
	$(SUDO) time docker run --rm $(TTY) -i $(SSHVOLUME) $(GOVOLUMES) $(BUILD_IMAGE) GOOS=$(GOOS) GOARCH=$(GOARCH) BINARY_SUFFIX=$(BINARY_SUFFIX) $@;

else

exes: $(EXES)

$(EXES):
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(GO_FLAGS) -o "$@$(BINARY_SUFFIX)" ./$(@D)

exes_race: $(EXES_RACE)

$(EXES_RACE):
	CGO_ENABLED=1 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -race $(GO_FLAGS) -o "$@$(BINARY_SUFFIX)" ./$(@D)

protos: ## Generates protobuf files.
protos: $(PROTO_GOS)

GENERATE_FILES ?= true

%.pb.go: %.proto
ifeq ($(GENERATE_FILES),true)
	protoc -I $(GOPATH)/src:./vendor/github.com/gogo/protobuf:./vendor:./$(@D):./pkg/storegateway/storepb --gogoslick_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,:./$(@D) ./$(patsubst %.pb.go,%.proto,$@)
else
	@echo "Warning: generating files has been disabled, but the following file needs to be regenerated: $@"
	@echo "If this is unexpected, check if the last modified timestamps on $@ and $(patsubst %.pb.go,%.proto,$@) are correct."
endif

lint-packaging-scripts: packaging/nfpm/mimir/postinstall.sh packaging/nfpm/mimir/preremove.sh
	shellcheck $?

lint: ## Run lints to check for style issues.
lint: check-makefiles
	misspell -error $(DOC_SOURCES_PATH)

	# Configured via .golangci.yml.
	golangci-lint run

	# Ensure no blocklisted package is imported.
	GOFLAGS="-tags=requires_docker,stringlabels" faillint -paths "github.com/bmizerany/assert=github.com/stretchr/testify/assert,\
		golang.org/x/net/context=context,\
		sync/atomic=go.uber.org/atomic,\
		regexp=github.com/grafana/regexp,\
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
	faillint -paths "github.com/grafana/mimir/pkg/..." ./pkg/querier/api/...

	# Ensure all errors are report as APIError
	faillint -paths "github.com/weaveworks/common/httpgrpc.{Errorf}=github.com/grafana/mimir/pkg/api/error.Newf" ./pkg/frontend/querymiddleware/...

	# errors.Cause() only work on errors wrapped by github.com/pkg/errors, while it doesn't work
	# on errors wrapped by golang standard errors package. In Mimir we currently use github.com/pkg/errors
	# but other vendors we depend on (e.g. Prometheus) just uses the standard errors package.
	# For this reason, we recommend to not use errors.Cause() anywhere, so that we don't have to
	# question whether the usage is safe or not.
	faillint -paths "github.com/pkg/errors.{Cause}" ./pkg/... ./cmd/... ./tools/... ./integration/...

	# gogo/status allows to easily customize error details while grpc/status doesn't:
	# for this reason we use gogo/status in several places. However, gogo/status.FromError()
	# doesn't support wrapped errors, while grpc/status.FromError() does.
	#
	# Since we want support for errors wrapping everywhere, to avoid subtle bugs depending
	# on which status package is imported, we don't allow .FromError() from both packages
	# and we require to use grpcutil.ErrorToStatus() instead.
	faillint -paths "\
		google.golang.org/grpc/status.{FromError}=github.com/grafana/dskit/grpcutil.ErrorToStatus,\
		github.com/gogo/status.{FromError}=github.com/grafana/dskit/grpcutil.ErrorToStatus" \
		./pkg/... ./cmd/... ./tools/... ./integration/...

	# Ensure the query path is supporting multiple tenants
	faillint -paths "\
		github.com/grafana/mimir/pkg/tenant.{TenantID}=github.com/grafana/mimir/pkg/tenant.{TenantIDs}" \
		./pkg/scheduler/... \
		./pkg/frontend/... \
		./pkg/querier/tenantfederation/... \
		./pkg/frontend/querymiddleware/...

	# Ensure packages that no longer use a global logger don't reintroduce it
	faillint -paths "github.com/grafana/mimir/pkg/util/log.{Logger}" \
		./pkg/alertmanager/... \
		./pkg/compactor/... \
		./pkg/distributor/... \
		./pkg/flusher/... \
		./pkg/frontend/... \
		./pkg/ingester/... \
		./pkg/querier/... \
		./pkg/ruler/... \
		./pkg/scheduler/... \
		./pkg/storage/... \
		./pkg/storegateway/...

	# We've copied github.com/NYTimes/gziphandler to pkg/util/gziphandler
	# at least until https://github.com/nytimes/gziphandler/pull/112 is merged
	faillint -paths "github.com/NYTimes/gziphandler" \
		./pkg/... ./cmd/... ./tools/... ./integration/...

	# We don't want to use yaml.v2 anywhere, because we use yaml.v3 now,
	# and UnamrshalYAML signature is not compatible between them.
	faillint -paths "gopkg.in/yaml.v2" \
		./pkg/... ./cmd/... ./tools/... ./integration/...

	# Ensure packages we imported from Thanos are no longer used.
	GOFLAGS="-tags=requires_docker,stringlabels" faillint -paths \
		"github.com/thanos-io/thanos/pkg/..." \
		./pkg/... ./cmd/... ./tools/... ./integration/...

	# Ensure we never use the default registerer and we allow to use a custom one (improves testability).
	# Also, ensure we use promauto.With() to reduce the chances we forget to register metrics.
	faillint -paths \
		"github.com/prometheus/client_golang/prometheus/promauto.{NewCounter,NewCounterVec,NewCounterFunc,NewGauge,NewGaugeVec,NewGaugeFunc,NewSummary,NewSummaryVec,NewHistogram,NewHistogramVec}=github.com/prometheus/client_golang/prometheus/promauto.With,\
		github.com/prometheus/client_golang/prometheus.{MustRegister,Register,DefaultRegisterer}=github.com/prometheus/client_golang/prometheus/promauto.With,\
		github.com/prometheus/client_golang/prometheus.{NewCounter,NewCounterVec,NewCounterFunc,NewGauge,NewGaugeVec,NewGaugeFunc,NewSummary,NewSummaryVec,NewHistogram,NewHistogramVec}=github.com/prometheus/client_golang/prometheus/promauto.With" \
		./pkg/...

	# Use the faster slices.Sort where we can.
	# Note that we don't automatically suggest replacing sort.Float64s() with slices.Sort() as the documentation for slices.Sort()
	# at the time of writing warns that slices.Sort() may not correctly handle NaN values.
	faillint -paths \
		"sort.{Strings,Ints}=golang.org/x/exp/slices.Sort" \
		./pkg/... ./cmd/... ./tools/... ./integration/...

	# Don't use generic ring.Read operation.
	# ring.Read usually isn't the right choice, and we prefer that each component define its operations explicitly.
	faillint -paths \
		"github.com/grafana/dskit/ring.{Read}" \
		./pkg/... ./cmd/... ./tools/... ./integration/...

	# Do not directly call flag.Parse() and argument getters, to try to reduce risk of misuse.
	faillint -paths \
		"flag.{Parse,NArg,Arg,Args}=github.com/grafana/dskit/flagext.{ParseFlagsAndArguments,ParseFlagsWithoutArguments}" \
		./pkg/... ./cmd/... ./tools/... ./integration/...

	# Ensure we use our custom gRPC clients.
	faillint -paths \
		"github.com/grafana/mimir/pkg/storegateway/storegatewaypb.{NewStoreGatewayClient}=github.com/grafana/mimir/pkg/storegateway/storegatewaypb.NewCustomStoreGatewayClient" \
		./pkg/... ./cmd/... ./tools/... ./integration/...

	# Prefer using WithCancelCause in production code, so that cancelled contexts have more information available from context.Cause(ctx).
	faillint -ignore-tests -paths \
		"context.{WithCancel}=context.WithCancelCause" \
		./pkg/... ./cmd/... ./tools/... ./integration/...

	# Do not use the object storage client intended only for tools within Mimir itself
	faillint -paths \
		"github.com/grafana/mimir/pkg/util/objtools" \
		./pkg/... ./cmd/... ./integration/...

	# Use the more performant metadata.ValueFromIncomingContext wherever possible (if not possible, we can always put
	# a lint ignore directive to skip linting).
	faillint -paths \
		"google.golang.org/grpc/metadata.{FromIncomingContext}=google.golang.org/grpc/metadata.ValueFromIncomingContext" \
		./pkg/... ./cmd/... ./integration/...

format: ## Run gofmt and goimports.
	find . $(DONT_FIND) -name '*.pb.go' -prune -o -type f -name '*.go' -exec gofmt -w -s {} \;
	find . $(DONT_FIND) -name '*.pb.go' -prune -o -type f -name '*.go' -exec goimports -w -local github.com/grafana/mimir {} \;

test: ## Run all unit tests.
	go test -timeout 30m ./...

print-go-version: ## Print the go version.
	@go version | awk '{print $$3}' | sed 's/go//'

test-with-race: ## Run all unit tests with data race detect.
	go test -tags netgo,stringlabels -timeout 30m -race -count 1 ./...

cover: ## Run all unit tests with code coverage and generates reports.
	$(eval COVERDIR := $(shell mktemp -d coverage.XXXXXXXXXX))
	$(eval COVERFILE := $(shell mktemp $(COVERDIR)/unit.XXXXXXXXXX))
	go test -tags netgo,stringlabels -timeout 30m -race -count 1 -coverprofile=$(COVERFILE) ./...
	go tool cover -html=$(COVERFILE) -o cover.html
	go tool cover -func=cover.html | tail -n1

shell:
	bash

mod-check: ## Check the go mod is clean and tidy.
	GO111MODULE=on go mod download
	GO111MODULE=on go mod verify
	GO111MODULE=on go mod tidy
	GO111MODULE=on go mod vendor
	@./tools/find-diff-or-untracked.sh go.sum go.mod vendor/ || (echo "Please update vendoring by running 'make mod-check'" && false)

check-protos: ## Check the protobuf files are up to date.
check-protos: clean-protos protos
	@./tools/find-diff-or-untracked.sh $(PROTO_GOS) || (echo "Please rebuild protobuf code by running 'check-protos'" && false)

%.md : %.template
	go run ./tools/doc-generator $< > $@

.PHONY: %.md.embedmd
%.md.embedmd : %.md
	embedmd -w $<

doc: ## Generates the config file documentation.
doc: clean-doc $(DOC_TEMPLATES:.template=.md) $(DOC_EMBED:.md=.md.embedmd)
	# Make up markdown files prettier. When running with check-doc target, it will fail if this produces any change.
	prettier --write "**/*.md"
	# Make operations/helm/charts/*/README.md
	helm-docs

license: ## Add license header to files.
	go run ./tools/add-license ./cmd ./integration ./pkg ./tools ./packaging ./development ./mimir-build-image ./operations ./.github

check-license: ## Check license header of files.
check-license: license
	@git diff --exit-code || (echo "Please add the license header running 'make BUILD_IN_CONTAINER=false license'" && false)

dist: ## Generates binaries for a Mimir release.
	echo "Cleaning up dist/"
	@rm -fr ./dist
	@mkdir -p ./dist
	@# Build binaries for various architectures and operating systems. Only
	@# mimirtool supports Windows for now.
	@for os in linux darwin windows freebsd; do \
		for arch in amd64 arm64; do \
			suffix="" ; \
			if [ "$$os" = "windows" ]; then \
				suffix=".exe" ; \
			fi; \
			echo "Building mimirtool for $$os/$$arch"; \
			GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/mimirtool-$$os-$$arch$$suffix ./cmd/mimirtool; \
			sha256sum ./dist/mimirtool-$$os-$$arch$$suffix | cut -d ' ' -f 1 > ./dist/mimirtool-$$os-$$arch$$suffix-sha-256; \
			if [ "$$os" = "windows" ]; then \
				continue; \
			fi; \
			echo "Building Mimir for $$os/$$arch"; \
			GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/mimir-$$os-$$arch$$suffix ./cmd/mimir; \
			sha256sum ./dist/mimir-$$os-$$arch$$suffix | cut -d ' ' -f 1 > ./dist/mimir-$$os-$$arch$$suffix-sha-256; \
			echo "Building query-tee for $$os/$$arch"; \
			GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/query-tee-$$os-$$arch$$suffix ./cmd/query-tee; \
			sha256sum ./dist/query-tee-$$os-$$arch$$suffix | cut -d ' ' -f 1 > ./dist/query-tee-$$os-$$arch$$suffix-sha-256; \
			echo "Building metaconvert for $$os/$$arch"; \
			GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/metaconvert-$$os-$$arch$$suffix ./cmd/metaconvert; \
			sha256sum ./dist/metaconvert-$$os-$$arch$$suffix | cut -d ' ' -f 1 > ./dist/metaconvert-$$os-$$arch$$suffix-sha-256; \
			echo "Building mimir-continuous-test for $$os/$$arch"; \
			GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/mimir-continuous-test-$$os-$$arch$$suffix ./cmd/mimir-continuous-test; \
			sha256sum ./dist/mimir-continuous-test-$$os-$$arch$$suffix | cut -d ' ' -f 1 > ./dist/mimir-continuous-test-$$os-$$arch$$suffix-sha-256; \
			echo "Building markblocks for $$os/$$arch"; \
			GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/markblocks-$$os-$$arch$$suffix ./tools/markblocks; \
			sha256sum ./dist/markblocks-$$os-$$arch$$suffix | cut -d ' ' -f 1 > ./dist/markblocks-$$os-$$arch$$suffix-sha-256; \
			done; \
		done; \
		touch $@

build-mixin: ## Generates the mimir mixin zip file.
build-mixin: check-mixin-jb
	@# Empty the compiled mixin directories content, without removing the directories itself,
	@# so that Grafana can refresh re-build dashboards when using "make mixin-serve".
	@# If any rule group has more than 20 rules, fail. 20 is our default per-tenant limit in the ruler.
	@for suffix in $(MIXIN_OUT_PATH_SUFFIXES); do \
		mkdir -p "$(MIXIN_OUT_PATH)$$suffix"; \
		find "$(MIXIN_OUT_PATH)$$suffix" -type f -delete; \
		mixtool generate all --output-alerts "$(MIXIN_OUT_PATH)$$suffix/alerts.yaml" --output-rules "$(MIXIN_OUT_PATH)$$suffix/rules.yaml" --directory "$(MIXIN_OUT_PATH)$$suffix/dashboards" "${MIXIN_PATH}/mixin-compiled$$suffix.libsonnet"; \
		./tools/check-rules.sh "$(MIXIN_OUT_PATH)$$suffix/rules.yaml" 20 ; \
		cd "$(MIXIN_OUT_PATH)$$suffix/.." && zip -q -r "mimir-mixin$$suffix.zip" $$(basename "$(MIXIN_OUT_PATH)$$suffix"); \
		cd -; \
		echo "The mixin has been compiled to $(MIXIN_OUT_PATH)$$suffix and archived to $$(realpath $(MIXIN_OUT_PATH)$$suffix/../mimir-mixin$$suffix.zip)"; \
	done

check-mixin-tests: ## Test the mixin files.
	@./operations/mimir-mixin-tests/run.sh || (echo "Mixin tests are failing. Please fix the reported issues. You can run mixin tests with 'make check-mixin-tests'" && false)

format-mixin: ## Format the mixin files.
	@find $(MIXIN_PATH) -type f -name '*.libsonnet' | xargs jsonnetfmt -i

# Helm static tests

HELM_SCRIPTS_PATH=operations/helm/scripts
HELM_RAW_MANIFESTS_PATH=operations/helm/manifests-intermediate
HELM_REFERENCE_MANIFESTS=operations/helm/tests

conftest-fmt:
	@conftest fmt $(REGO_POLICIES_PATH)

check-conftest-fmt: conftest-fmt
	@./tools/find-diff-or-untracked.sh $(REGO_POLICIES_PATH) || (echo "Format the rego policies by running 'make conftest-fmt' and commit the changes" && false)

conftest-verify:
	@conftest verify -p $(REGO_POLICIES_PATH) --report notes

update-helm-dependencies:
	@./$(HELM_SCRIPTS_PATH)/update-helm-dependencies.sh operations/helm/charts/mimir-distributed

build-helm-tests: ## Build the helm golden records.
build-helm-tests: update-helm-dependencies
	@./$(HELM_SCRIPTS_PATH)/build.sh --intermediate-path $(HELM_RAW_MANIFESTS_PATH) --output-path $(HELM_REFERENCE_MANIFESTS)

helm-conftest-quick-test: ## Does not rebuild the yaml manifests, use the target helm-conftest-test for that
helm-conftest-quick-test:
	@./$(HELM_SCRIPTS_PATH)/run-conftest.sh --policies-path $(REGO_POLICIES_PATH) --manifests-path $(HELM_RAW_MANIFESTS_PATH)

helm-conftest-test: build-helm-tests helm-conftest-quick-test

check-helm-tests: ## Check the helm golden records.
check-helm-tests: build-helm-tests helm-conftest-test
	@./tools/find-diff-or-untracked.sh $(HELM_REFERENCE_MANIFESTS) || (echo "Rebuild the Helm tests output by running 'make build-helm-tests' and commit the changes" && false)

endif

.PHONY: check-makefiles
check-makefiles: ## Check the makefiles format.
check-makefiles: format-makefiles
	@git diff --exit-code -- $(MAKE_FILES) || (echo "Please format Makefiles by running 'make format-makefiles'" && false)

.PHONY: format-makefiles
format-makefiles: ## Format all Makefiles.
format-makefiles: $(MAKE_FILES)
	$(SED) -i -e 's/^\(\t*\)  /\1\t/g' -e 's/^\(\t*\) /\1/' -- $?

clean: ## Cleanup the docker images, object files and executables.
	$(SUDO) docker rmi $(IMAGE_NAMES) >/dev/null 2>&1 || true
	$(SUDO) docker volume rm -f mimir-go-pkg mimir-go-cache
	rm -rf -- $(UPTODATE_FILES) $(UPTODATE_RACE_FILES) $(EXES) $(EXES_RACE) .cache dist
	# Remove executables built for multiarch images.
	find . -type f -name '*_linux_arm64' -perm +u+x -exec rm {} \;
	find . -type f -name '*_linux_amd64' -perm +u+x -exec rm {} \;
	go clean ./...

clean-protos: ## Clean protobuf files.
	rm -rf $(PROTO_GOS)

list-image-targets: ## List all images building make targets.
	@echo $(UPTODATE_FILES) | tr " " "\n"

clean-doc: ## Clean the documentation files generated from templates.
	rm -f $(DOC_TEMPLATES:.template=.md)

check-doc: ## Check the documentation files are up to date.
check-doc: doc
	@find . -name "*.md" | xargs git diff --exit-code -- \
	|| (echo "Please update generated documentation by running 'make doc' and committing the changes" && false)

# Tool is developed in the grafana/technical-documentation repository:
# https://github.com/grafana/technical-documentation/tree/main/tools/doc-validator
check-doc-validator: ## Check documentation using doc-validator tool
	docker run -v "$(CURDIR)/$(DOC_SOURCES_PATH):/$(DOC_SOURCES_PATH)" grafana/doc-validator:v1.5.0 ./$(DOC_SOURCES_PATH)
	docker run -v "$(CURDIR)/docs/sources/helm-charts:/docs/sources/helm-charts" grafana/doc-validator:v1.5.0 ./docs/sources/helm-charts

.PHONY: reference-help
reference-help: ## Generates the reference help documentation.
reference-help: cmd/mimir/mimir tools/config-inspector/config-inspector
	@(./cmd/mimir/mimir -h || true) > cmd/mimir/help.txt.tmpl
	@(./cmd/mimir/mimir -help-all || true) > cmd/mimir/help-all.txt.tmpl
	@(./tools/config-inspector/config-inspector || true) > cmd/mimir/config-descriptor.json

clean-white-noise: ## Clean the white noise in the markdown files.
	@find . -path ./.pkg -prune -o -path ./.cache -prune -o -path "*/vendor/*" -prune -or -type f -name "*.md" -print | \
	SED_BIN="$(SED)" xargs ./tools/cleanup-white-noise.sh

check-white-noise: ## Check the white noise in the markdown files.
check-white-noise: clean-white-noise
	@git diff --exit-code -- '*.md' || (echo "Please remove trailing whitespaces running 'make clean-white-noise'" && false)

check-mixin: ## Build, format and check the mixin files.
check-mixin: build-mixin format-mixin check-mixin-jb check-mixin-mixtool check-mixin-runbooks
	@echo "Checking diff:"
	@for suffix in $(MIXIN_OUT_PATH_SUFFIXES); do \
		./tools/find-diff-or-untracked.sh $(MIXIN_PATH) "$(MIXIN_OUT_PATH)$$suffix" || (echo "Please build and format mixin by running 'make build-mixin format-mixin'" && false); \
	done

	@cd $(MIXIN_PATH) && \
	jb install && \
	mixtool lint mixin.libsonnet

check-mixin-jb:
	@cd $(MIXIN_PATH) && \
	jb install

check-mixin-mixtool: check-mixin-jb
	@cd $(MIXIN_PATH) && \
	mixtool lint mixin.libsonnet

check-mixin-runbooks: build-mixin
	@tools/lint-runbooks.sh

check-mixin-mimirtool-rules: build-mixin
	@echo "Checking 'mimirtool rules check':"
	@for suffix in $(MIXIN_OUT_PATH_SUFFIXES); do \
		go run ./cmd/mimirtool rules check --rule-dirs "$(MIXIN_OUT_PATH)$$suffix"; \
	done

mixin-serve: ## Runs Grafana loading the mixin dashboards compiled at operations/mimir-mixin-compiled.
	@./operations/mimir-mixin-tools/serve/run.sh

mixin-screenshots: ## Generates mixin dashboards screenshots.
	@find $(DOC_SOURCES_PATH)/manage/monitor-grafana-mimir/dashboards -name '*.png' -delete
	@./operations/mimir-mixin-tools/screenshots/run.sh

check-jsonnet-manifests: ## Check the jsonnet manifests.
check-jsonnet-manifests: format-jsonnet-manifests
	@echo "Checking diff:"
	@./tools/find-diff-or-untracked.sh $(JSONNET_MANIFESTS_PATHS) || (echo "Please format jsonnet manifests by running 'make format-jsonnet-manifests'" && false)

format-jsonnet-manifests: ## Format the jsonnet manifests.
	@find $(JSONNET_MANIFESTS_PATHS) -type f -name '*.libsonnet' -print -o -name '*.jsonnet' -print | xargs jsonnetfmt -i

check-jsonnet-getting-started: ## Check the jsonnet getting started examples.
	# Start from a clean setup.
	rm -rf jsonnet-example

	# We want to test any change in this branch/PR, so after installing Mimir jsonnet
	# we replace it with the current code.
	cat ./operations/mimir/getting-started.sh \
		| sed 's/\(jb install github.com\/grafana\/mimir\/operations\/mimir@main\)/\1 \&\& rm -fr .\/vendor\/mimir \&\& cp -r ..\/operations\/mimir .\/vendor\/mimir\//g' \
		| bash

operations/helm/charts/mimir-distributed/charts: operations/helm/charts/mimir-distributed/Chart.yaml operations/helm/charts/mimir-distributed/Chart.lock
	@cd ./operations/helm/charts/mimir-distributed && helm dependency update

check-helm-jsonnet-diff: ## Check the helm jsonnet diff.
check-helm-jsonnet-diff: operations/helm/charts/mimir-distributed/charts build-jsonnet-tests
	@./operations/compare-helm-with-jsonnet/compare-helm-with-jsonnet.sh

build-jsonnet-tests: ## Build the jsonnet tests.
	@./operations/mimir-tests/build.sh

jsonnet-conftest-quick-test: ## Does not rebuild the yaml manifests, use the target jsonnet-conftest-test for that
jsonnet-conftest-quick-test:
	@./operations/mimir-tests/run-conftest.sh --policies-path $(REGO_POLICIES_PATH) --manifests-path ./operations/mimir-tests

jsonnet-conftest-test: build-jsonnet-tests jsonnet-conftest-quick-test

check-jsonnet-tests: ## Check the jsonnet tests output.
check-jsonnet-tests: build-jsonnet-tests jsonnet-conftest-test
	@./tools/find-diff-or-untracked.sh operations/mimir-tests || (echo "Please rebuild jsonnet tests output 'make build-jsonnet-tests'" && false)

check-mimir-microservices-mode-docker-compose-yaml: ## Check the jsonnet and docker-compose diff for development/mimir-microservices-mode.
	cd development/mimir-microservices-mode && make check

check-mimir-read-write-mode-docker-compose-yaml: ## Check the jsonnet and docker-compose diff for development/mimir-read-write-mode.
	cd development/mimir-read-write-mode && make check

integration-tests: ## Run all integration tests.
integration-tests: cmd/mimir/$(UPTODATE)
	go test -tags=requires_docker,stringlabels ./integration/...

integration-tests-race: ## Run all integration tests with race-enabled distroless docker image.
integration-tests-race: export MIMIR_IMAGE=$(IMAGE_PREFIX)mimir:$(IMAGE_TAG_RACE)
integration-tests-race: cmd/mimir/$(UPTODATE_RACE)
	go test -timeout 30m -tags=requires_docker,stringlabels ./integration/...

web-serve:
	cd website && hugo --config config.toml --minify -v server

# Those vars are needed for packages target
export VERSION

packages: dist
	@packaging/nfpm/nfpm.sh

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

docs: doc
	cd docs && $(MAKE) docs

.PHONY: generate-otlp
generate-otlp:
	cd pkg/distributor/otlp && rm -f *_generated.go && go generate

.PHONY: check-generated-otlp-code
check-generated-otlp-code: generate-otlp
	@./tools/find-diff-or-untracked.sh $(OTLP_GOS) || (echo "Please rebuild OTLP code by running 'make generate-otlp'" && false)
