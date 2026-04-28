GO           ?= go
FIRST_GOPATH := $(firstword $(subst :, ,$(shell $(GO) env GOPATH)))

SKIP_GOLANGCI_LINT :=
GOLANGCI_LINT :=
GOLANGCI_LINT_OPTS ?=
GOLANGCI_LINT_VERSION ?= v2.4.0

# golangci-lint only supports linux, darwin and windows platforms on i386/amd64/arm64.
# windows isn't included here because of the path separator being different.
ifeq ($(GOHOSTOS),$(filter $(GOHOSTOS),linux darwin))
	ifeq ($(GOHOSTARCH),$(filter $(GOHOSTARCH),amd64 i386 arm64))
		ifneq (,$(SKIP_GOLANGCI_LINT))
			GOLANGCI_LINT :=
		else
			GOLANGCI_LINT := $(FIRST_GOPATH)/bin/golangci-lint
		endif
	endif
endif

ifdef GOLANGCI_LINT
$(GOLANGCI_LINT):
	mkdir -p $(FIRST_GOPATH)/bin
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/$(GOLANGCI_LINT_VERSION)/install.sh \
		| sed -e '/install -d/d' \
		| sh -s -- -b $(FIRST_GOPATH)/bin $(GOLANGCI_LINT_VERSION)
endif

.PHONY: print-golangci-lint-version
print-golangci-lint-version:
	@echo $(GOLANGCI_LINT_VERSION)

.PHONY: lint
lint: $(GOLANGCI_LINT)
ifdef GOLANGCI_LINT
	@echo ">> running golangci-lint"
	$(GOLANGCI_LINT) run $(GOLANGCI_LINT_OPTS) $(pkgs)
endif

.PHONY: lint-fix
lint-fix: $(GOLANGCI_LINT)
ifdef GOLANGCI_LINT
	@echo ">> running golangci-lint fix"
	$(GOLANGCI_LINT) run --fix $(GOLANGCI_LINT_OPTS) $(pkgs)
endif
