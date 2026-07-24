{
  local isEnabled = $._config.compartments_enabled,

  overrides_exporter_args+:: if !isEnabled then {} else $.mimirCompartmentsCommonArgs,

  // Config validation.
  local overridesExporterCompartmentConfigError = if !isEnabled then null else $.validateMimirCompartmentsConfig([
    'overrides_exporter_deployment',
  ]),
  assert overridesExporterCompartmentConfigError == null : overridesExporterCompartmentConfigError,
}
