(import 'mixin-compiled.libsonnet') + {
  _config+:: {
    mimir_scaling_rules_selector: 'namespace=~"mimir.*"',
  },
}
