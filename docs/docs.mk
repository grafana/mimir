SHELL = /usr/bin/env bash

DOCS_IMAGE   = grafana/docs-base:latest
DOCS_PROJECT = mimir
DOCS_DIR     = sources

# This allows ports and base URL to be overridden, so services like ngrok.io can
# be used to share a local running docs instances.
DOCS_HOST_PORT    = 3002
DOCS_LISTEN_PORT  = 3002
DOCS_BASE_URL    ?= "localhost:$(DOCS_HOST_PORT)"

DOCS_VERSION = next

HUGO_REFLINKSERRORLEVEL ?= WARNING
DOCS_DOCKER_RUN_FLAGS = -ti -v $(CURDIR)/$(DOCS_DIR):/hugo/content/docs/$(DOCS_PROJECT)/$(DOCS_VERSION):ro,z -e HUGO_REFLINKSERRORLEVEL=$(HUGO_REFLINKSERRORLEVEL) -p $(DOCS_HOST_PORT):$(DOCS_LISTEN_PORT) --rm $(DOCS_IMAGE)
DOCS_DOCKER_CONTAINER = $(DOCS_PROJECT)-docs

# This wrapper will serve documentation on a local webserver.
define docs_docker_run
	@echo "Documentation will be served at:"
	@echo "http://$(DOCS_BASE_URL)/docs/$(DOCS_PROJECT)/$(DOCS_VERSION)/"
	@echo ""
	@if [[ -z $${NON_INTERACTIVE} ]]; then \
		read -p "Press a key to continue"; \
	fi
	# The loki _index.md file is intentionally used until the equivalent file in the grafana/website repository is
	# created for Mimir.
	@docker run --name $(DOCS_DOCKER_CONTAINER) $(DOCS_DOCKER_RUN_FLAGS) /bin/bash -c 'mv content/docs/loki/_index.md content/docs/$(DOCS_PROJECT)/ && find content/docs/ -mindepth 1 -maxdepth 1 -type d -a ! -name "$(DOCS_PROJECT)" -exec rm -rf {} \;  && exec $(1)'
endef

.PHONY: docs-docker-rm
docs-docker-rm:
	docker rm -f $(DOCS_DOCKER_CONTAINER)

.PHONY: docs-pull
docs-pull:
	docker pull $(DOCS_IMAGE)

.PHONY: docs
docs: ## Serve documentation locally.
docs: docs-pull
	$(call docs_docker_run,make server HUGO_PORT=$(DOCS_LISTEN_PORT))
