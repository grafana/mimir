{
  // The alert name is prefixed with the product name (eg. AlertName -> MimirAlertName).
  alertName(name)::
    $._config.product + name,
}
