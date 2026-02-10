// Migration step 2:
// - Migrate store-gateways to multi-zone deployment - Delete store-gateway-zone-c PVC's
(import 'test-multi-az-read-path-migration-step-2f.jsonnet') {
  /*
  # Cleanup PVCs
    kubectl \
      --cluster <CLUSTER> \
      --namespace <NAMESPACE> \
      delete pvc store-gateway-zone-c<.*> # replace or use matchers, etc.
  */
}
