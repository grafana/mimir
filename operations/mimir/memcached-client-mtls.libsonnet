{
  // If mTLS is enabled for connections to Memcached, add the appropriate secrets to all
  // components that need access to the cache. This needs to be included last because it
  // overrides all components of the read path (including those for read-write mode).

  local statefulSet = $.apps.v1.statefulSet,
  local deployment = $.apps.v1.deployment,
  local container = $.core.v1.container,
  local mount = $.core.v1.volumeMount,
  local volume = $.core.v1.volume,

  // Utility used to override a field only if exists in super.
  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,

  local mtls_enabled = ($._config.memcached_frontend_mtls_enabled || $._config.memcached_index_queries_mtls_enabled || $._config.memcached_chunks_mtls_enabled || $._config.memcached_metadata_mtls_enabled),

  local client_container = if !mtls_enabled then {} else container.withVolumeMountsMixin([
    mount.new($._config.memcached_mtls_ca_cert_secret, $._config.memcached_ca_cert_path, true),
    mount.new($._config.memcached_mtls_client_cert_secret, $._config.memcached_client_cert_path, true),
    mount.new($._config.memcached_mtls_client_key_secret, $._config.memcached_client_key_path, true),
  ]),

  local client_deployment = if !mtls_enabled then {} else deployment.mixin.spec.template.spec.withVolumesMixin([
    volume.fromSecret($._config.memcached_mtls_ca_cert_secret, $._config.memcached_mtls_ca_cert_secret),
    volume.fromSecret($._config.memcached_mtls_client_cert_secret, $._config.memcached_mtls_client_cert_secret),
    volume.fromSecret($._config.memcached_mtls_client_key_secret, $._config.memcached_mtls_client_key_secret),
  ]),

  local client_statefulset = if !mtls_enabled then {} else statefulSet.mixin.spec.template.spec.withVolumesMixin([
    volume.fromSecret($._config.memcached_mtls_ca_cert_secret, $._config.memcached_mtls_ca_cert_secret),
    volume.fromSecret($._config.memcached_mtls_client_cert_secret, $._config.memcached_mtls_client_cert_secret),
    volume.fromSecret($._config.memcached_mtls_client_key_secret, $._config.memcached_mtls_client_key_secret),
  ]),

  querier_container+:: if !$._config.is_microservices_deployment_mode then {} else client_container,
  querier_deployment: overrideSuperIfExists('querier_deployment', client_deployment),

  ruler_querier_container+:: if !$._config.ruler_remote_evaluation_enabled then {} else client_container,
  ruler_querier_deployment: overrideSuperIfExists(
    'ruler_querier_deployment',
    if !$._config.ruler_remote_evaluation_enabled then {} else client_deployment,
  ),

  query_frontend_container+:: if !$._config.is_microservices_deployment_mode then {} else client_container,
  query_frontend_deployment: overrideSuperIfExists('query_frontend_deployment', client_deployment),

  ruler_query_frontend_container+:: if !$._config.ruler_remote_evaluation_enabled then {} else client_container,
  ruler_query_frontend_deployment: overrideSuperIfExists(
    'ruler_query_frontend_deployment',
    if !$._config.ruler_remote_evaluation_enabled then {} else client_deployment,
  ),

  store_gateway_container+:: if !$._config.is_microservices_deployment_mode then {} else client_container,
  store_gateway_statefulset: overrideSuperIfExists('store_gateway_statefulset', client_statefulset),
  store_gateway_zone_a_statefulset: overrideSuperIfExists('store_gateway_zone_a_statefulset', client_statefulset),
  store_gateway_zone_b_statefulset: overrideSuperIfExists('store_gateway_zone_b_statefulset', client_statefulset),
  store_gateway_zone_c_statefulset: overrideSuperIfExists('store_gateway_zone_c_statefulset', client_statefulset),

  mimir_read_container:: overrideSuperIfExists('mimir_read_container', client_container),
  mimir_read_deployment: overrideSuperIfExists('mimir_read_deployment', client_deployment),

  mimir_backend_container:: overrideSuperIfExists('mimir_backend_container', client_container),
  mimir_backend_zone_a_statefulset: overrideSuperIfExists('mimir_backend_zone_a_statefulset', client_statefulset),
  mimir_backend_zone_b_statefulset: overrideSuperIfExists('mimir_backend_zone_b_statefulset', client_statefulset),
  mimir_backend_zone_c_statefulset: overrideSuperIfExists('mimir_backend_zone_c_statefulset', client_statefulset),
}
