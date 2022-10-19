#!/bin/env bash

python3 extract-yamls.py ../../sources/migration-guide/migrating-from-single-zone-with-helm.md

SED="sed"

function write_template {
  local step_dir="$1"
  local step_yaml="$2"

  rm -rf "${step_dir}"
  if [ -n "${step_yaml}" ] ; then
    helm template krajo ../../../../operations/helm/charts/mimir-distributed --output-dir "${step_dir}" -f ../base.yaml -f "${step_yaml}"
  else
    helm template krajo ../../../../operations/helm/charts/mimir-distributed --output-dir "${step_dir}" -f ../base.yaml
  fi
  find "${step_dir}" -type f -print0 | xargs -0 "${SED}" -E -i -- "/^\s+(checksum\/config|(helm.sh\/)?chart|app.kubernetes.io\/version|image: \"grafana\/(mimir|mimir-continuous-test|enterprise-metrics)):/d"
}

for component in alertmanager ingester storegateway ; do
  pushd "${component}"
  write_template "step0"
  i=1
  while [ -e "${component}-step${i}.yaml" ] ; do
    step_yaml="${component}-step${i}.yaml"
    step_dir="step${i}"
    prev=`expr $i - 1`
    prev_dir="step$(expr $i - 1)"
    echo "Component=${component} Prev step=${prev_dir} Current step=${step_dir}"
    rm -rf "${step_dir}"
    ${SED} -i "s/<N>/3/g" "${step_yaml}"
    write_template "${step_dir}" "${step_yaml}"
    diff_filename="diff-${prev_dir}-${step_dir}.patch"
    diff -c -r "${prev_dir}" "${step_dir}" > "${diff_filename}"
    "${SED}" -E -i -- 's/^((\*\*\*|---) .*yaml).*/\1/g' ${diff_filename}
    rm -rf "${prev_dir}"
    ((i++))
    if ! [ -e "${component}-step${i}.yaml" ] ; then
      rm -rf ${step_dir}
    fi
  done
  popd
done
