# Auto generated binary variables helper managed by https://github.com/bwplotka/bingo v0.5.2. DO NOT EDIT.
# All tools are designed to be build inside $GOBIN.
BINGO_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
GOPATH ?= $(shell go env GOPATH)
GOBIN  ?= $(firstword $(subst :, ,${GOPATH}))/bin
GO     ?= $(shell which go)

# Below generated variables ensure that every time a tool under each variable is invoked, the correct version
# will be used; reinstalling only if needed.
# For example for ko variable:
#
# In your main Makefile (for non array binaries):
#
#include .bingo/Variables.mk # Assuming -dir was set to .bingo .
#
#command: $(KO)
#	@echo "Running ko"
#	@$(KO) <flags/args..>
#
KO := $(GOBIN)/ko-v0.9.4-0.20220208160355-a8eae133463f
$(KO): $(BINGO_DIR)/ko.mod
	@# Install binary/ries using Go 1.14+ build command. This is using bwplotka/bingo-controlled, separate go module with pinned dependencies.
	@echo "(re)installing $(GOBIN)/ko-v0.9.4-0.20220208160355-a8eae133463f"
	@cd $(BINGO_DIR) && $(GO) build -mod=mod -modfile=ko.mod -o=$(GOBIN)/ko-v0.9.4-0.20220208160355-a8eae133463f "github.com/grafana/ko"

