GIT_ROOT := $(shell git rev-parse --show-toplevel)

MIMIR_DOCS_VERSION := $(shell sed -n 's, *MIMIR_VERSION: "\([^"]*\)",\1,p' "$(GIT_ROOT)/docs/sources/helm-charts/mimir-distributed/_index.md")
ifeq ($(MIMIR_DOCS_VERSION),next)
MIMIR_DOCS_BRANCH := main
else
MIMIR_DOCS_BRANCH := $(patsubst v%.x,release-%,$(MIMIR_DOCS_VERSION))
endif

# List of projects to provide to the make-docs script.
PROJECTS := mimir:next:mimir mimir:$(MIMIR_DOCS_VERSION):mimir-$(MIMIR_DOCS_VERSION) helm-charts/mimir-distributed

# Parent directory of the current git repository.
GIT_ROOT_PARENT := $(realpath $(GIT_ROOT)/..)

# Use the doc-validator image defined in CI by default.
export DOC_VALIDATOR_IMAGE := $(shell sed -n 's, *image: \(grafana/doc-validator.*\),\1,p' "$(GIT_ROOT)/.github/workflows/test-build-deploy.yml")

# Skip some doc-validator checks.
export DOC_VALIDATOR_SKIP_CHECKS := ^canonical-does-not-match-pretty-URL$
