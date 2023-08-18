GIT_ROOT := $(shell git rev-parse --show-toplevel)

MIMIR_DOCS_VERSION := $(shell sed -ne '/^---$/,/^---$/{ s/^ *mimir_docs_version: "\([^"]\{1,\}\)"/\1/p; }' $(GIT_ROOT)/docs/sources/helm-charts/mimir-distributed/_index.md)

# List of projects to provide to the make-docs script.
PROJECTS := mimir:$(MIMIR_DOCS_VERSION):mimir-$(MIMIR_DOCS_VERSION) helm-charts/mimir-distributed

# Use the doc-validator image defined in CI by default.
export DOC_VALIDATOR_IMAGE := $(shell sed -n 's, *image: \(grafana/doc-validator.*\),\1,p' "$(GIT_ROOT)/.github/workflows/test-build-deploy.yml")

# Skip some doc-validator checks.
export DOC_VALIDATOR_SKIP_CHECKS := ^canonical-does-not-match-pretty-URL$
