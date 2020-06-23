{
  _config:: error 'must provide _config for alerts',

  namespace_matcher(prefix='')::
    if std.length($._config.alert_namespace_matcher) != 0
    then '%s namespace=~"%s"' % [prefix, $._config.alert_namespace_matcher]
    else '',

  aggregation_labels(replace='')::
    if $._config.singleBinary == true
    then 'job'
    else replace,

  annotation_labels(replace='$labels.namespace')::
    if $._config.singleBinary == true
    then '$labels.job'
    else replace,
}
