// Based on test-usage-tracker.jsonnet.
(import 'test-usage-tracker.jsonnet') {
  _config+:: {
    local availabilityZones = ['us-east-2a', 'us-east-2b'],
    multi_zone_availability_zones: availabilityZones,
    multi_zone_write_path_enabled: true,

    // Enable memberlist bridge for zone-aware routing.
    multi_zone_memberlist_bridge_enabled: true,
    memberlist_zone_aware_routing_enabled: true,
  },
}
