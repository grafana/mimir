proto:
	protoc github.com.hashicorp.go.kms.wrapping.v2.types.proto --go_out=paths=source_relative:.
	protoc plugin/github.com.hashicorp.go.kms.wrapping.plugin.v2.proto --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:.

.PHONY: proto
