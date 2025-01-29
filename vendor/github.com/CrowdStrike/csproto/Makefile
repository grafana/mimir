PROJECT_BASE_DIR := $(realpath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
# prepend the project-local bin/ folder to $PATH so that we find build-time tools there
PATH := ${PROJECT_BASE_DIR}/bin:${PATH}

# metadata for generating "version" info for local builds
CURRENT_USER ?= $(shell whoami)
CURRENT_DATE ?= $(shell date -u +'%FT%R:%SZ')
CURRENT_DATE_DIGITS_ONLY ?= $(shell date -u +'%Y%m%d%H%M%S')
CURRENT_COMMIT ?= $(shell git rev-parse HEAD)
CURRENT_COMMIT_SHORT ?= $(shell git rev-parse --short HEAD)
LOCAL_DEV_VERSION ?= "v0.0.0-devel-${CURRENT_USER}-${CURRENT_DATE_DIGITS_ONLY}-${CURRENT_COMMIT_SHORT}"
LOCAL_LDFLAGS = -ldflags="-X main.version=${LOCAL_DEV_VERSION} -X main.commit=${CURRENT_COMMIT} -X main.builtBy=${CURRENT_USER} -X main.date=${CURRENT_DATE}"

PROTOC_GEN_GOGO  = github.com/gogo/protobuf/protoc-gen-gogo@$(shell go list -f '{{.Version}}' -m github.com/gogo/protobuf)
PROTOC_GEN_GO_V1 = github.com/golang/protobuf/protoc-gen-go@$(shell go list -f '{{.Version}}' -m github.com/golang/protobuf)
PROTOC_GEN_GO_V2 = google.golang.org/protobuf/cmd/protoc-gen-go@$(shell go list -f '{{.Version}}' -m google.golang.org/protobuf)

# re-map Protobuf well-known types used in examples to Gogo's package instead of Google's
# . need 2 variables b/c protoc-gen-gogo (which is using its fork of the github.com/golang/protobuf V1 API)
#   doesn't support the ';type' syntax to explicitly specify the target Go package but protoc-gen-fastmarshal
#   (which is based on the V2 protobuf API) requires it to correctly handle re-mapping the WKT .proto
#   files that have 'go_package' options with different packages specified.
GOGO_WKT_OPTIONS = ,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types
FASTMARSHAL_WKT_OPTIONS = ,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types\;types,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types\;types,Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types\;types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types\;types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types\;types

TEST_OPTS ?= -race
BENCHMARKS_PATTERN ?= .

.PHONY: test
test:
	$(info Running csproto library tests ...)
	@go test ${TEST_OPTS} ./...
	$(info Running example tests ...)
	@cd ${PROJECT_BASE_DIR}/example && go test ${TEST_OPTS} ./...

.PHONY: bench
bench:
	@cd ${PROJECT_BASE_DIR}/example &&\
		go test -run='^$$' -bench=${BENCHMARKS_PATTERN} -benchmem ./proto2 ./proto3

GENERATE_TARGETS =\
   example-proto2-gogo example-proto2-googlev1 example-proto2-googlev2\
   example-proto3-gogo example-proto3-googlev1 example-proto3-googlev2\
   example-permessage-gogo example-permessage-googlev1 example-permessage-googlev2
.PHONY: generate
generate: ${GENERATE_TARGETS}
	$(info Done)

.PHONY: regenerate
regenerate: clean clean-generated generate

.PHONY: example-proto2-gogo
example-proto2-gogo: install-protoc-gen-gogo protoc-gen-fastmarshal
	$(info Generating Protobuf code for example/proto2/gogo ...)
	@cd ${PROJECT_BASE_DIR}/example/proto2/gogo &&\
		protoc -I . --gogo_out=paths=source_relative${GOGO_WKT_OPTIONS}:. --fastmarshal_out=paths=source_relative${FASTMARSHAL_WKT_OPTIONS},specialname=Size:. *.proto

.PHONY: example-proto3-gogo
example-proto3-gogo: install-protoc-gen-gogo protoc-gen-fastmarshal
	$(info Generating Protobuf code for example/proto3/gogo ...)
	@cd ${PROJECT_BASE_DIR}/example/proto3/gogo &&\
		protoc -I . --gogo_out=paths=source_relative${GOGO_WKT_OPTIONS}:. --fastmarshal_out=paths=source_relative${FASTMARSHAL_WKT_OPTIONS}:. *.proto

.PHONY: example-proto2-googlev1
example-proto2-googlev1: install-protoc-gen-go-v1 protoc-gen-fastmarshal
	$(info Generating Protobuf code for example/proto2/googlev1 ...)
	@cd ${PROJECT_BASE_DIR}/example/proto2/googlev1 &&\
		protoc -I . --plugin=protoc-gen-go=${PROJECT_BASE_DIR}/bin/protoc-gen-go-v1 --go_out=paths=source_relative:. --fastmarshal_out=apiversion=v2,paths=source_relative:. *.proto

.PHONY: example-proto3-googlev1
example-proto3-googlev1: install-protoc-gen-go-v1 protoc-gen-fastmarshal
	$(info Generating Protobuf code for example/proto3/googlev1 ...)
	@cd ${PROJECT_BASE_DIR}/example/proto3/googlev1 &&\
		protoc -I . --plugin=protoc-gen-go=${PROJECT_BASE_DIR}/bin/protoc-gen-go-v1 --go_out=paths=source_relative:. --fastmarshal_out=apiversion=v2,paths=source_relative:. *.proto

.PHONY: example-proto2-googlev2
example-proto2-googlev2: install-protoc-gen-go-v2 protoc-gen-fastmarshal
	$(info Generating Protobuf code for example/proto2/googlev2 ...)
	@cd ${PROJECT_BASE_DIR}/example/proto2/googlev2 &&\
		protoc -I . --plugin=protoc-gen-go=${PROJECT_BASE_DIR}/bin/protoc-gen-go-v2 --go_out=paths=source_relative:. --fastmarshal_out=apiversion=v2,paths=source_relative:. *.proto

.PHONY: example-proto3-googlev2
example-proto3-googlev2: install-protoc-gen-go-v2 protoc-gen-fastmarshal
	$(info Generating Protobuf code for example/proto3/googlev2 ...)
	@cd ${PROJECT_BASE_DIR}/example/proto3/googlev2 &&\
		protoc -I . --plugin=protoc-gen-go=${PROJECT_BASE_DIR}/bin/protoc-gen-go-v2 --go_out=paths=source_relative:. --fastmarshal_out=apiversion=v2,paths=source_relative:. *.proto

.PHONY: example-permessage-gogo
example-permessage-gogo: install-protoc-gen-gogo protoc-gen-fastmarshal
	$(info Generating Protobuf code for example/permessage/gogo ...)
	@cd ${PROJECT_BASE_DIR}/example/permessage/gogo &&\
		protoc -I . --gogo_out=paths=source_relative${GOGO_WKT_OPTIONS}:. --fastmarshal_out=filepermessage=true,paths=source_relative${FASTMARSHAL_WKT_OPTIONS},specialname=Size:. *.proto

.PHONY: example-permessage-googlev1
example-permessage-googlev1: install-protoc-gen-go-v1 protoc-gen-fastmarshal
	$(info Generating Protobuf code for example/permessage/googlev1 ...)
	@cd ${PROJECT_BASE_DIR}/example/permessage/googlev1 &&\
		protoc -I . --plugin=protoc-gen-go=${PROJECT_BASE_DIR}/bin/protoc-gen-go-v1 --go_out=paths=source_relative:. --fastmarshal_out=apiversion=v2,filepermessage=true,paths=source_relative:. *.proto

.PHONY: example-permessage-googlev2
example-permessage-googlev2: install-protoc-gen-go-v2 protoc-gen-fastmarshal
	$(info Generating Protobuf code for example/permessage/googlev2 ...)
	@cd ${PROJECT_BASE_DIR}/example/permessage/googlev2 &&\
		protoc -I . --plugin=protoc-gen-go=${PROJECT_BASE_DIR}/bin/protoc-gen-go-v2 --go_out=paths=source_relative:. --fastmarshal_out=apiversion=v2,filepermessage=true,paths=source_relative:. *.proto

.PHONY: protoc-gen-fastmarshal
protoc-gen-fastmarshal: export GOBIN=${PROJECT_BASE_DIR}/bin
protoc-gen-fastmarshal: create-local-bin-dir
	$(info Building protoc-gen-fastmarshal ...)
	@cd ${PROJECT_BASE_DIR}/cmd/protoc-gen-fastmarshal &&\
		go install ${LOCAL_LDFLAGS}

.PHONY: install-protoc-gen-gogo
install-protoc-gen-gogo: export GOBIN=${PROJECT_BASE_DIR}/bin
install-protoc-gen-gogo: create-local-bin-dir
	$(info Installing protoc-gen-gogo ...)
	@go install ${PROTOC_GEN_GOGO}

.PHONY: install-protoc-gen-go-v1
install-protoc-gen-go-v1: export GOBIN=${PROJECT_BASE_DIR}/bin
install-protoc-gen-go-v1: create-local-bin-dir
	$(info Installing ${PROTOC_GEN_GO_V1} to ${PROJECT_BASE_DIR}/bin/protoc-gen-go-v1 ...)
	@go install ${PROTOC_GEN_GO_V1}
	@mv ${PROJECT_BASE_DIR}/bin/protoc-gen-go ${PROJECT_BASE_DIR}/bin/protoc-gen-go-v1

.PHONY: install-protoc-gen-go-v2
install-protoc-gen-go-v2: export GOBIN=${PROJECT_BASE_DIR}/bin
install-protoc-gen-go-v2: create-local-bin-dir
	$(info Installing ${PROTOC_GEN_GO_V2} to ${PROJECT_BASE_DIR}/bin/protoc-gen-go-v2 ...)
	@go install ${PROTOC_GEN_GO_V2}
	@mv ${PROJECT_BASE_DIR}/bin/protoc-gen-go ${PROJECT_BASE_DIR}/bin/protoc-gen-go-v2

.PHONY: protodump
protodump: export GOBIN=${PROJECT_BASE_DIR}/bin
protodump: create-local-bin-dir
	$(info Building protodump ...)
	@cd ${PROJECT_BASE_DIR}/cmd/protodump &&\
		go install ${LOCAL_LDFLAGS}

.PHONY: create-local-bin-dir
create-local-bin-dir:
	@mkdir -p ${PROJECT_BASE_DIR}/bin

.PHONY: clean
clean:
	$(info Removing build artifacts ...)
	@rm -rf ${PROJECT_BASE_DIR}/bin/*
	@find ${PROJECT_BASE_DIR} \( -name "*.test" -o -name "*profile.out" \) -print -delete

.PHONY: clean-generated
clean-generated:
	$(info Removing generated code ...)
	@find ${PROJECT_BASE_DIR} \( -name "*.pb.go" -o -name "*.fm.go" \) -print -delete

.PHONY: lint
lint: check-golangci-lint-install
	@golangci-lint run ./...
	@cd example && golangci-lint run ./...

.PHONY: check-golangci-lint-install
check-golangci-lint-install:
ifeq ("$(shell command -v golangci-lint)", "")
	$(error golangci-lint was not found.  Please install it using the method of your choice. (https://golangci-lint.run/usage/install/#local-installation))
endif

.PHONY: snapshot
snapshot: check-goreleaser-install
	@goreleaser release --snapshot --rm-dist

.PHONY: check-goreleaser-install
check-goreleaser-install:
ifeq ("$(shell command -v goreleaser)", "")
	$(error goreleaser was not found.  Please install it using the method of your choice. (https://goreleaser.com/install))
endif
