package tracing

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

func NewResource(serviceName string, customAttribs []attribute.KeyValue) (*resource.Resource, error) {
	customAttribs = append(customAttribs, semconv.ServiceName(serviceName))
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			customAttribs...,
		),
	)
}
