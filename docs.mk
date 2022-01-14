DOCS_IMAGE   = grafana/docs-base:latest
DOCS_PROJECT = mimir
DOCS_DIR     = docs

# This allows ports and base URL to be overridden, so services like ngrok.io can
# be used to share a local running docs instances.
DOCS_HOST_PORT    = 3002
DOCS_LISTEN_PORT  = 3002
DOCS_BASE_URL    ?= "localhost:$(DOCS_HOST_PORT)"

DOCS_VERSION = next

DOCS_DOCKER_RUN_FLAGS = -ti -v $(CURDIR)/$(DOCS_DIR):/hugo/content/docs/$(DOCS_PROJECT)/$(DOCS_VERSION):ro,z -p $(DOCS_HOST_PORT):$(DOCS_LISTEN_PORT) --rm $(DOCS_IMAGE)

# This wrapper will delete the pre-existing Grafana and Loki docs, which
# significantly slows down the build process, due to the duplication of pages
# through versioning.
define docs_docker_run
	docker run $(DOCS_DOCKER_RUN_FLAGS) /bin/bash -c 'find content/docs/ -mindepth 1 -maxdepth 1 -type d -a ! -name "$(DOCS_PROJECT)" -exec rm -rf {} \; && exec $(1)'
endef

.PHONY: pull
pull:
	docker pull $(DOCS_IMAGE)

.PHONY: docs
docs: pull
	$(call docs_docker_run,hugo server --debug --baseUrl=$(DOCS_BASE_URL) -p $(DOCS_LISTEN_PORT) --bind 0.0.0.0)
