// Migration step 2:
// - Migrate store-gateways to multi-zone deployment - Delete store-gateway-zone-b PVC's
(import 'test-multi-az-read-path-migration-step-2c.jsonnet') {
  /*
  # Cleanup PVCs
    kubectl \
      --cluster <CLUSTER> \
      --namespace <NAMESPACE> \
      delete pvc store-gateway-zone-b<.*> # replace or use matchers, etc.
  */
}
