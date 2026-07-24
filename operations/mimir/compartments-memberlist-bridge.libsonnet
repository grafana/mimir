{
  local isEnabled = $._config.compartments_enabled,

  memberlist_bridge_args+:: if !isEnabled then {} else $.mimirCompartmentsCommonArgs,

  // Config validation.
  local memberlistBridgeCompartmentConfigError = if !isEnabled then null else $.validateMimirCompartmentsConfig([
    'memberlist_bridge_zone_a_deployment',
    'memberlist_bridge_zone_b_deployment',
    'memberlist_bridge_zone_c_deployment',
  ]),
  assert memberlistBridgeCompartmentConfigError == null : memberlistBridgeCompartmentConfigError,
}
