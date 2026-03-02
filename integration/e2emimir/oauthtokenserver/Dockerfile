FROM golang:1.24-alpine AS build
WORKDIR /src
COPY cmd/oauthtokenserver/* .
RUN CGO_ENABLED=0 go build -o /oauthtokenserver .

FROM alpine:3.21
COPY --from=build /oauthtokenserver /oauthtokenserver
ENTRYPOINT ["/oauthtokenserver"]
