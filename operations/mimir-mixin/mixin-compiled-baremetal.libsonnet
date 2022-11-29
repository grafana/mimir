(import 'mixin-compiled.libsonnet') + {
  _config+:: {
    deployment_type: 'baremetal',

    // Change the default ("pod") which doesn't make sense when deploying on baremetal.
    // We use "instance" because it's the default naming used by Prometheus.
    per_instance_label: 'instance',
  },
}
