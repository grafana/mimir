{
  _config+:: {
    // otlp_traces_endpoint configures tracing using the OTLP protocol.
    otlp_traces_endpoint: null,
    // otel_traces_sampler and otel_traces_sampler_arg optionally configure the OpenTelemetry traces sampler.
    // See OTel SDK documentation and Mimir's tracing documentation for available options.
    otel_traces_sampler: null,
    otel_traces_sampler_arg: null,
    // otel_span_event_count_limit configures the max number of events that can be
    // added to a span. The SDK default is 128 events.
    otel_span_event_count_limit: null,

    otel_resource_attributes: [
      { key: 'k8s.cluster.name', value: $._config.cluster },
      { key: 'k8s.namespace.name', value: $._config.namespace },
      { key: 'k8s.pod.name', value: '$(POD_NAME)' },
      // https://opentelemetry.io/docs/specs/semconv/non-normative/k8s-attributes/#how-serviceinstanceid-should-be-calculated
      { key: 'service.instance.id', value: '%(namespace)s.$(POD_NAME).$(CONTAINER_NAME)' % $._config },
      // https://opentelemetry.io/docs/specs/semconv/registry/attributes/service/#service-attributes
      { key: 'service.name', value: '$(CONTAINER_NAME)' },  // This is a weird way of doing this, once this upstreamed., we can use the container name directly.
      { key: 'service.namespace', value: $._config.namespace },
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
      $.core.v1.envVar.fromFieldPath('POD_NAME', 'metadata.name'),
      if ('name' in thisContainer) then $.core.v1.envVar.new('CONTAINER_NAME', thisContainer.name),
      $.core.v1.envVar.new('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', $._config.otlp_traces_endpoint),
      $.core.v1.envVar.new('OTEL_RESOURCE_ATTRIBUTES', constructOtelResourceAttributes(resourceAttributes)),
      if $._config.otel_traces_sampler != null then $.core.v1.envVar.new('OTEL_TRACES_SAMPLER', $._config.otel_traces_sampler),
      if $._config.otel_traces_sampler_arg != null then $.core.v1.envVar.new('OTEL_TRACES_SAMPLER_ARG', $._config.otel_traces_sampler_arg),
      if $._config.otel_span_event_count_limit != null then $.core.v1.envVar.new('OTEL_SPAN_EVENT_COUNT_LIMIT', $._config.otel_span_event_count_limit),
    ]),
  },
}
