{
  _config:: error 'must provide _config for alerts',

  annotation_labels(replace='$labels.namespace')::
    if $._config.singleBinary == true
    then '$labels.job'
    else replace,
}
