(import 'mixin-compiled.libsonnet') + {
  _config+:: {
    compartments_enabled: true,

    // The scheduler is deployed per read compartment, so enable it to generate and check its alerts too.
    compactor_scheduler_enabled: true,
  },
}
