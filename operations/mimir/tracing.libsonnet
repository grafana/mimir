{
  _config+:: {
    // otlp_traces_endpoint configures tracing using the OTLP protocol.
    otlp_traces_endpoint: null,
    // otel_traces_sampler and otel_traces_sampler_arg optionally configure the OpenTelemetry traces sampler.
    // See OTel SDK documentation and Mimir's tracing documentation for available options.
    otel_traces_sampler: null,
    otel_traces_sampler_arg: null,
  },

  tracing_env_mixin::
    if $._config.otlp_traces_endpoint != null
    then $.otel_tracing_mixin
    else {},

  otel_tracing_mixin:: {
    local resourceAttributes =
      ('k8s.namespace.name=%s,service.namespace=%s,k8s.cluster.name=%s' % [$._config.namespace, $._config.namespace, $._config.cluster])
      + (if 'name' in self then ',k8s.container.name=%s' % self.name else ''),

    env+: std.prune([
      { name: 'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', value: $._config.otlp_traces_endpoint },
      { name: 'OTEL_RESOURCE_ATTRIBUTES', value: resourceAttributes },
      if $._config.otel_traces_sampler != null then { name: 'OTEL_TRACES_SAMPLER', value: $._config.otel_traces_sampler },
      if $._config.otel_traces_sampler_arg != null then { name: 'OTEL_TRACES_SAMPLER_ARG', value: $._config.otel_traces_sampler_arg },
    ]),
  },
}
