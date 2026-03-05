# SPDX-License-Identifier: AGPL-3.0-only

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod_ ./go.mod
COPY main.go_ ./main.go
RUN CGO_ENABLED=0 go build -o /oauthtokenserver .

FROM alpine:3.23.3
COPY --from=build /oauthtokenserver /oauthtokenserver
ENTRYPOINT ["/oauthtokenserver"]
