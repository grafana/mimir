{
  _config:: error 'must provide _config for alerts',

  namespace_matcher(prefix='')::
    if std.length($._config.alert_namespace_matcher) != 0
    then '%s namespace=~"%s"' % [prefix, $._config.alert_namespace_matcher]
    else '',
}
