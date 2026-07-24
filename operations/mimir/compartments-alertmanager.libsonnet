{
  local isEnabled = $._config.compartments_enabled,

  alertmanager_args+:: if !isEnabled then {} else $.mimirCompartmentsCommonArgs,

  // Config validation.
  local alertmanagerCompartmentConfigError = if !isEnabled then null else $.validateMimirCompartmentsConfig([
    'alertmanager_statefulset',
  ]),
  assert alertmanagerCompartmentConfigError == null : alertmanagerCompartmentConfigError,
}
