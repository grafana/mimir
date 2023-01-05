SHELL = /usr/bin/env bash

DOCS_IMAGE   = grafana/docs-base:latest
DOCS_DOCKER_CONTAINER = mimir-docs

# This allows ports and base URL to be overridden, so services like ngrok.io can
# be used to share a local running docs instances.
DOCS_HOST_PORT    = 3002
DOCS_LISTEN_PORT  = 3002
DOCS_BASE_URL    ?= "localhost:$(DOCS_HOST_PORT)"

MIMIR_VERSION       = next
HELM_CHARTS_VERSION = next

HUGO_REFLINKSERRORLEVEL ?= WARNING

# This wrapper will serve documentation on a local webserver.
define docs_docker_run
	@echo "Documentation will be served at:"
	@echo "http://$(DOCS_BASE_URL)/docs/mimir/$(MIMIR_VERSION)/"
	@echo "http://$(DOCS_BASE_URL)/docs/helm-charts/mimir-distributed/$(HELM_CHARTS_VERSION)/"
	@echo ""
	@if [[ -z $${NON_INTERACTIVE} ]]; then \
		read -p "Press a key to continue"; \
	fi
	@docker run -ti \
		-v $(CURDIR)/sources/mimir:/hugo/content/docs/mimir/$(MIMIR_VERSION):ro,z \
		-v $(CURDIR)/sources/helm-charts/mimir-distributed:/hugo/content/docs/helm-charts/mimir-distributed/$(HELM_CHARTS_VERSION):ro,z \
		-e HUGO_REFLINKSERRORLEVEL=$(HUGO_REFLINKSERRORLEVEL) \
		-p $(DOCS_HOST_PORT):$(DOCS_LISTEN_PORT) \
		--name $(DOCS_DOCKER_CONTAINER) \
		--rm \
		$(DOCS_IMAGE) \
			/bin/bash -c "sed -i'' -e s/latest/$(DOCS_VERSION)/ content/docs/mimir/_index.md && exec $(1)"
endef

.PHONY: docs-docker-rm
docs-docker-rm: ## Remove the docs container.
	docker rm -f $(DOCS_DOCKER_CONTAINER)

.PHONY: docs-pull
docs-pull: ## Pull documentation base image.
	docker pull $(DOCS_IMAGE)

.PHONY: docs
docs: ## Serve documentation locally.
docs: docs-pull
	$(call docs_docker_run,make server HUGO_PORT=$(DOCS_LISTEN_PORT))
