{
  _config+:: {
    // otlp_traces_endpoint configures tracing using the OTLP protocol.
    otlp_traces_endpoint: null,
    // otel_traces_sampler and otel_traces_sampler_arg optionally configure the OpenTelemetry traces sampler.
    // See OTel SDK documentation and Mimir's tracing documentation for available options.
    otel_traces_sampler: null,
    otel_traces_sampler_arg: null,
    otel_resource_attributes: [
      { key: 'k8s.namespace.name', value: $._config.namespace },
      { key: 'service.namespace', value: $._config.namespace },
      { key: 'k8s.cluster.name', value: $._config.cluster },
    ],
  },

  tracing_env_mixin::
    if $._config.otlp_traces_endpoint != null
    then $.otel_tracing_mixin
    else {},

  otel_tracing_mixin:: {
    local constructOtelResourceAttributes(fields) =
      local values = std.map(
        function(k) k.key + '=' + k.value,
        fields
      );
      std.join(',', values),

    local thisContainer = self,
    local resourceAttributes =
      $._config.otel_resource_attributes
      + (if 'name' in thisContainer then [{ key: 'k8s.container.name', value: thisContainer.name }] else [{}]),

    env+: std.prune([
      { name: 'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', value: $._config.otlp_traces_endpoint },
      { name: 'OTEL_RESOURCE_ATTRIBUTES', value: constructOtelResourceAttributes(resourceAttributes) },
      if $._config.otel_traces_sampler != null then { name: 'OTEL_TRACES_SAMPLER', value: $._config.otel_traces_sampler },
      if $._config.otel_traces_sampler_arg != null then { name: 'OTEL_TRACES_SAMPLER_ARG', value: $._config.otel_traces_sampler_arg },
    ]),
  },
}
