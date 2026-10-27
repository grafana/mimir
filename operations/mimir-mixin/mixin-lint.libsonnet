// Entrypoint for mixtool lint, which only supports v1 dashboards: v2 dashboards are linted in their
// v1 form, which has the same panels and variables.
local mixin = import 'mixin.libsonnet';

mixin {
  grafanaDashboards: {
    [filename]: std.get(mixin.grafanaDashboards[filename], 'v1', mixin.grafanaDashboards[filename])
    for filename in std.objectFields(mixin.grafanaDashboards)
  },
}
