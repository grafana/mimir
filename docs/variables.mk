# List of projects to provide to the make-docs script.
PROJECTS := mimir helm-charts/mimir-distributed

# Use the doc-validator image defined in CI by default.
export DOC_VALIDATOR_IMAGE := $(shell sed -n 's, *image: \(grafana/doc-validator.*\),\1,p' "$(shell git rev-parse --show-toplevel)/.github/workflows/test-build-deploy.yml")

# Use alternative image until make-docs 3.0.0 is rolled out.
export DOCS_IMAGE := grafana/docs-base:dbd975af06
