SHELL = /usr/bin/env bash

DOCS_IMAGE   = grafana/docs-base:latest
DOCS_CONTAINER = mimir-docs

# This allows ports and base URL to be overridden, so services like ngrok.io can
# be used to share a local running docs instances.
DOCS_HOST_PORT    = 3002
DOCS_LISTEN_PORT  = 3002
DOCS_BASE_URL    ?= "localhost:$(DOCS_HOST_PORT)"

MIMIR_VERSION       = next
HELM_CHARTS_VERSION = next

HUGO_REFLINKSERRORLEVEL ?= WARNING

GIT_ROOT = $(shell git rev-parse --show-toplevel)

# Support podman over Docker if it is available.
PODMAN := $(shell if command -v podman &>/dev/null; then echo podman; else echo docker; fi)

MIMIR_CONTENT_PATH="/hugo/content/docs/mimir"
MIMIR_REDIRECT_TEMPLATE="'---\\nredirectURL: /docs/mimir/$(MIMIR_VERSION)\\ntype: redirect\\n---\\n'"
HELM_CONTENT_PATH="/hugo/content/docs/helm-charts/mimir-distributed"
HELM_REDIRECT_TEMPLATE="'---\\nredirectURL: /docs/helm-charts/mimir-distributed/$(HELM_CHARTS_VERSION)\\ntype: redirect\\n---\\n'"

# This wrapper will serve documentation on a local webserver.
define docs_podman_run
	@echo "Documentation will be served at:"
	@echo "http://$(DOCS_BASE_URL)/docs/mimir/$(MIMIR_VERSION)/"
	@echo "http://$(DOCS_BASE_URL)/docs/helm-charts/mimir-distributed/$(HELM_CHARTS_VERSION)/"
	@echo ""
	@if [[ -z $${NON_INTERACTIVE} ]]; then \
		read -p "Press a key to continue"; \
	fi
	@$(PODMAN) run -ti \
		--init \
		-v $(GIT_ROOT)/docs/sources/mimir:$(MIMIR_CONTENT_PATH)/$(MIMIR_VERSION):ro,z \
		-v $(GIT_ROOT)/docs/sources/helm-charts/mimir-distributed:$(HELM_CONTENT_PATH)/$(HELM_CHARTS_VERSION):ro,z \
		-e HUGO_REFLINKSERRORLEVEL=$(HUGO_REFLINKSERRORLEVEL) \
		-p $(DOCS_HOST_PORT):$(DOCS_LISTEN_PORT) \
		--name $(DOCS_CONTAINER) \
		--rm \
		$(DOCS_IMAGE) \
			/bin/bash -c "echo -e $(MIMIR_REDIRECT_TEMPLATE) > $(MIMIR_CONTENT_PATH)/_index.md && echo -e $(HELM_REDIRECT_TEMPLATE) > $(HELM_CONTENT_PATH)/_index.md && exec $(1)"
endef

.PHONY: docs-rm
docs-rm: ## Remove the docs container.
	$(PODMAN) rm -f $(DOCS_CONTAINER)

.PHONY: docs-pull
docs-pull: ## Pull documentation base image.
	$(PODMAN) pull $(DOCS_IMAGE)

.PHONY: docs
docs: ## Serve documentation locally.
docs: docs-pull
	$(call docs_podman_run,make server HUGO_PORT=$(DOCS_LISTEN_PORT))
