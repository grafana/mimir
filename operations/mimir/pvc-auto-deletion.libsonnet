local k = import 'ksonnet-util/kausal.libsonnet';
local statefulset = k.apps.v1.statefulSet;

{
  _config+:: {
    // These can only be enabled if the target Kubernetes cluster supports PVC auto-deletion.
    //
    // The beta PVC auto-deletion feature was only enabled by default starting in Kubernetes 1.27.
    //
    // https://kubernetes.io/blog/2023/05/04/kubernetes-1-27-statefulset-pvc-auto-deletion-beta/ has more details on PVC auto-deletion.
    enable_pvc_auto_deletion_for_store_gateways: false,
    enable_pvc_auto_deletion_for_compactors: false,
    enable_pvc_auto_deletion_for_ingesters: false,
    enable_pvc_auto_deletion_for_read_write_mode_backend: false,
  },

  local store_gateway_pvc_auto_deletion = if !$._config.enable_pvc_auto_deletion_for_store_gateways then {} else
    statefulset.spec.persistentVolumeClaimRetentionPolicy.withWhenScaled('Delete'),

  local compactor_pvc_auto_deletion = if !$._config.enable_pvc_auto_deletion_for_compactors then {} else
    statefulset.spec.persistentVolumeClaimRetentionPolicy.withWhenScaled('Delete'),

  local ingester_pvc_auto_deletion = if !$._config.enable_pvc_auto_deletion_for_ingesters then {} else
    statefulset.spec.persistentVolumeClaimRetentionPolicy.withWhenScaled('Delete'),

  local backend_pvc_auto_deletion = if !$._config.enable_pvc_auto_deletion_for_read_write_mode_backend then {} else
    statefulset.spec.persistentVolumeClaimRetentionPolicy.withWhenScaled('Delete'),

  store_gateway_statefulset: overrideSuperIfExists('store_gateway_statefulset', store_gateway_pvc_auto_deletion),
  store_gateway_zone_a_statefulset: overrideSuperIfExists('store_gateway_zone_a_statefulset', store_gateway_pvc_auto_deletion),
  store_gateway_zone_b_statefulset: overrideSuperIfExists('store_gateway_zone_b_statefulset', store_gateway_pvc_auto_deletion),
  store_gateway_zone_c_statefulset: overrideSuperIfExists('store_gateway_zone_c_statefulset', store_gateway_pvc_auto_deletion),

  compactor_statefulset: overrideSuperIfExists('compactor_statefulset', compactor_pvc_auto_deletion),

  ingester_statefulset: overrideSuperIfExists('ingester_statefulset', ingester_pvc_auto_deletion),
  ingester_zone_a_statefulset: overrideSuperIfExists('ingester_zone_a_statefulset', ingester_pvc_auto_deletion),
  ingester_zone_b_statefulset: overrideSuperIfExists('ingester_zone_b_statefulset', ingester_pvc_auto_deletion),
  ingester_zone_c_statefulset: overrideSuperIfExists('ingester_zone_c_statefulset', ingester_pvc_auto_deletion),

  mimir_backend_zone_a_statefulset: overrideSuperIfExists('mimir_backend_zone_a_statefulset', backend_pvc_auto_deletion),
  mimir_backend_zone_b_statefulset: overrideSuperIfExists('mimir_backend_zone_b_statefulset', backend_pvc_auto_deletion),
  mimir_backend_zone_c_statefulset: overrideSuperIfExists('mimir_backend_zone_c_statefulset', backend_pvc_auto_deletion),

  local overrideSuperIfExists(name, override) = if !( name in super) || super[name] == null || super[name] == {} then null else
    super[name] + override,
}
