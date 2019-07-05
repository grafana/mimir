VERSION=$(shell git describe --tags --always | sed 's/^v//')

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
BINARY_NAME=cortex-tool

all: build

build: 
	$(GOBUILD) -o bin/$(BINARY_NAME) -v

build-linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-X main.Version=$(VERSION)" -o bin/$(BINARY_NAME) .

test: 
	$(GOTEST) -v -race ./...

clean: 
	$(GOCLEAN)
	rm -f bin/$(BINARY_NAME)

build-docker: build-linux
	docker build -t grafana/$(BINARY_NAME):$(VERSION) .