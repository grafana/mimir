#!/usr/bin/env bash

GOEXPERIMENT=boringcrypto CGO_ENABLED=1 GOOS=linux go build -mod=vendor -tags=stringlabels,boringcrypto -gcflags "all=-N -l" -o ./mimir-boringcrypto-debug ./cmd/mimir