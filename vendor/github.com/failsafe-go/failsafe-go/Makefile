.DEFAULT_GOAL := help

.PHONY: help
help:	## Show the help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build Failsafe-go
	go build ./...

.PHONY: test
test: ## Test Failsafe-go
	go run gotest.tools/gotestsum@latest

.PHONY: fmt
fmt: ## Format Failsafe-go
	go fmt ./...
