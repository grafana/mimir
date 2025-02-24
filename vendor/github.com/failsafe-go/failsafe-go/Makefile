.DEFAULT_GOAL := help

.PHONY: help
help:	## Show the help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build Failsafe-go
	go build ./...

.PHONY: test
test: ## Test Failsafe-go
	go run gotest.tools/gotestsum@latest `go list ./... | grep -vE 'examples|policytesting|testutil'`

.PHONY: test-with-race
test-with-race: ## Test Failsafe-go
	go test -race ./...

.PHONY: fmt
fmt: ## Format Failsafe-go
	go fmt ./...

.PHONY: lint
lint: ## Lint Failsafe-go
	golangci-lint run -D errcheck,unused

.PHONY: check
check: fmt test ## Check Failsafe-go for a commit or release
	go mod tidy
