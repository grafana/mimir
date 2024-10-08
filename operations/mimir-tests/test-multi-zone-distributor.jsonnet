// Based on test-multi-zone.jsonnet.
(import 'test-multi-zone.jsonnet') {
  _config+:: {
    local availabilityZones = ['us-east-2a', 'us-east-2b'],
    multi_zone_distributor_enabled: true,
    multi_zone_distributor_availability_zones: availabilityZones,

    autoscaling_distributor_enabled: true,
    autoscaling_distributor_min_replicas: 3,
    autoscaling_distributor_max_replicas: 30,
  },
}
